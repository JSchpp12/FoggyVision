from fogvis.db import (
    DatabaseWriter,
    ImageImporter,
    SceneEntity,
    CoordinateEntity,
    ImageEntity,
    Database,
)
from fogvis.common import Latitude, Longitude
from tqdm import tqdm
from dataclasses import dataclass
from typing import Optional
import os
import shutil
import queue
import threading
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Any

# used to shutdown the writer thread
_DONE = object()

DB_FILE_NAME: str = "database.sqlite3"


@dataclass
class InputImage:
    image_path: Path
    image_data_file_path: Path


def collect_input_images(root_dir: str | Path) -> list[InputImage]:
    """
    Recurse through a directory and its children, collecting all .png files
    and their partner .json files into InputImage dataclasses.

    Args:
        root_dir: The root directory to search from.

    Returns:
        A list of InputImage instances for every .png/.json pair found.

    Raises:
        FileNotFoundError: If root_dir does not exist.
        ValueError: If a .png file has no partner .json file.
    """
    root = Path(root_dir)
    if not root.exists():
        raise FileNotFoundError(f"Directory not found: {root}")

    input_images = []

    for png_path in sorted(root.rglob("*.png")):
        json_path = png_path.with_suffix(".json")
        if not json_path.exists():
            raise ValueError(f"Missing partner .json for: {png_path}")
        input_images.append(
            InputImage(image_path=png_path, image_data_file_path=json_path)
        )

    return input_images


def move_image_into_db_dir(image_path: Path, db_dir: Path) -> Path:
    # going to assume for now that the image always has a parent dir with a date
    image_name: str = os.path.basename(image_path)
    parent_dir: str = os.path.split(image_path.parent.resolve())[-1]
    new_image_name = f"{parent_dir}_{image_name}"
    new_image_path: Path = Path(os.path.join(db_dir, new_image_name))
    shutil.copy2(image_path, new_image_path)
    return new_image_path


def parse_image_data_file(filepath: Path) -> dict[str, Any]:
    reader: ImageImporter = ImageImporter(filepath)
    scene_data = reader.read_scene()

    return {
        "scene_name": scene_data.name,
        "scene_center": CoordinateEntity(
            lat=Latitude(str(scene_data.center.x)),
            lon=Longitude(str(scene_data.center.y)),
        ),
        "scene_vis_range": scene_data.vis_range,
        "camera": reader.read_camera(),
        "fog": reader.read_fog(),
        "fog_type": reader.read_fog_type(),
        "light": reader.read_light(),
        "light_type": reader.read_light_type(),
        "environment": reader.read_environment(),
        "image": reader.read_image(),
    }


def process_image_data(image: InputImage, image_dir: Path) -> dict[str, Any]:
    imported_image_path: Path = move_image_into_db_dir(image.image_path, image_dir)
    parsed = parse_image_data_file(image.image_data_file_path)

    parsed["image"].file_path = os.path.basename(imported_image_path)
    final_info = {
        "scene_name": parsed["scene_name"],
        "scene_center": parsed["scene_center"],
        "scene_vis_range": parsed["scene_vis_range"],
        "camera": parsed["camera"],
        "fog": parsed["fog"],
        "fog_type": parsed["fog_type"],
        "light": parsed["light"],
        "light_type": parsed["light_type"],
        "environment": parsed["environment"],
        "images": [parsed["image"]],
    }

    return final_info


def writer_thread(
    db_path: Path, write_queue: queue.Queue, batch_size: int = 50
) -> None:
    """Single dedicated thread — owns all DB access."""
    writer = DatabaseWriter(db_path)
    batch = []

    with writer.db as db:
        while True:
            item = write_queue.get()
            if item is _DONE:
                if batch:
                    _flush(writer, batch)
                break

            batch.append(item)
            if len(batch) >= batch_size:
                _flush(writer, batch)
                batch.clear()

            write_queue.task_done()


def process_files(
    importFilePaths: list[InputImage],
    target_output_dir: list[Path],
    db_dir: Path,
    max_workers: int = 16,
):
    write_queue = queue.Queue(maxsize=500)
    db_file_path = os.path.join(db_dir, DB_FILE_NAME)
    total = len(importFilePaths)

    writer = threading.Thread(
        target=writer_thread, args=(db_file_path, write_queue), daemon=True
    )
    writer.start()

    with tqdm(total=total, desc="Processing files", unit="file") as progress_bar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for result in executor.map(
                process_image_data, importFilePaths, target_output_dir
            ):
                write_queue.put(result)
                progress_bar.update(1)

    write_queue.put(_DONE)
    writer.join()


def _flush(writer: DatabaseWriter, batch: list[dict]) -> None:
    for item in batch:
        writer.write_full_scene(**item)["scene"]


def init_db(db_file_path: Path):
    db = Database(db_file_path)
    db.init_tables()


def main(db_dir: Path, import_dir: Path):
    if not os.path.exists(import_dir):
        raise Exception("Import image directory does not exist")

    db_file_path: Optional[Path] = None
    if not os.path.exists(db_dir):
        logging.info("Database does not exist. Initializing")
        db_file_path = Path(os.path.join(db_dir, DB_FILE_NAME))
        os.makedirs(db_dir, exist_ok=True)
        init_db(db_file_path)

    inputs = collect_input_images(import_dir)
    image_output_dir: Path = Path(os.path.join(db_dir, "images"))
    if not os.path.exists(image_output_dir):
        os.makedirs(image_output_dir)

    target_dirs = []
    for input in inputs:
        target_dirs.append(image_output_dir)

    process_files(inputs, target_dirs, db_dir)
