from fogvis.db import (
    DatabaseWriter,
    DatabaseCleanup,
    ImageImporter,
    SceneEntity,
    CoordinateEntity,
    Database,
)
from fogvis.db.entities import (
    ImageEntity,
    ViewEntity,
    ViewImageEntity,
    VisibilityDistanceEntity,
)
from fogvis.common import Latitude, Longitude
from tqdm import tqdm
from dataclasses import dataclass
from typing import Optional
import os
import shutil
import queue
import threading
from PIL import Image
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Any
import re

# used to shutdown the writer thread
_DONE = object()

# When True, the file-copy helpers skip copying (files are already in place
# in the db images dir with their final prefixed names) and just return the
# existing paths. Set by rebuild_db; the import path leaves it False.
_REBUILD_MODE: bool = False

# Matches the leading date-time stamp prefix on flattened image filenames,
# e.g. "2026-06-22_09-44-47_Frame-0.json" -> "2026-06-22_09-44-47".
_PREFIX_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})_")


@dataclass
class InputImage:
    image_path: Path
    image_data_file_path: Path


def collect_input_images(root_dir: str) -> list:
    """
    Recurse through a directory and its children, collecting all .json files
    and their partner images into InputImage dataclasses.

    JSON files are the primary source of truth and may sit next to their
    images inside date-stamped subdirectories. Image paths are resolved
    relative to each JSON file using the file_name and ray_masks fields.
    """
    root = Path(root_dir)
    if not root.exists():
        raise FileNotFoundError(f"Directory not found: {root}")

    input_images = []

    for json_path in sorted(root.rglob("*.json")):
        reader = ImageImporter(json_path)
        data = reader._data

        # Support both old flat format and new image_files wrapper.
        if "image_files" in data:
            color_rel = data["image_files"].get("file_name", "")
        else:
            color_rel = data.get("file_name", "")

        if not color_rel:
            raise ValueError(f"JSON file missing file_name: {json_path}")

        color_path = (json_path.parent / color_rel).resolve()
        if not color_path.exists():
            raise ValueError(f"Missing partner image for: {json_path}")

        input_images.append(
            InputImage(image_path=color_path, image_data_file_path=json_path)
        )

    return input_images


def _derive_prefix(json_path: Path) -> str:
    """Extract the date-time prefix from a flattened image filename.

    Files in the db images dir are named "<dateDir>_<originalName>", e.g.
    "2026-06-22_09-44-47_Frame-0.json". The JSON internals still reference
    the original unprefixed names ("Frame-0.png"), so the prefix must be
    prepended when resolving sibling file paths during a rebuild.
    """
    m = _PREFIX_RE.match(json_path.name)
    if not m:
        raise ValueError(
            f"Rebuild source JSON lacks a date-time prefix: {json_path}"
        )
    return m.group(1) + "_"


def collect_input_images_rebuild(root_dir: str) -> list:
    """Collect InputImages from an already-flattened db images directory.

    Unlike collect_input_images, this expects every file to live directly
    under root_dir (no date-stamped subdirectories) with the date-time
    prefix already baked into each filename. The JSON files still reference
    sibling files by their original unprefixed names, so the prefix is
    derived from each JSON's own filename and prepended to resolve paths.
    """
    root = Path(root_dir)
    if not root.exists():
        raise FileNotFoundError(f"Directory not found: {root}")

    input_images = []

    for json_path in sorted(root.glob("*.json")):
        prefix = _derive_prefix(json_path)
        reader = ImageImporter(json_path)
        data = reader._data

        if "image_files" in data:
            color_rel = data["image_files"].get("file_name", "")
        else:
            color_rel = data.get("file_name", "")

        if not color_rel:
            raise ValueError(f"JSON file missing file_name: {json_path}")

        color_path = (json_path.parent / f"{prefix}{color_rel}").resolve()
        if not color_path.exists():
            raise ValueError(f"Missing partner image for: {json_path}")

        input_images.append(
            InputImage(image_path=color_path, image_data_file_path=json_path)
        )

    if not input_images:
        raise ValueError(
            f"No JSON files found in rebuild images directory: {root}"
        )

    return input_images


def move_image_into_db_dir(
    image_path: Path,
    db_dir: Path,
) -> Path:
    if _REBUILD_MODE:
        # File is already in the db images dir with its final prefixed name.
        return image_path
    parent_dir: str = image_path.parent.name
    new_image_path: Path = db_dir / "images" / f"{parent_dir}_{image_path.name}"
    new_image_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(image_path, new_image_path)
    return new_image_path


def copy_image_file(source: Path, db_dir: Path) -> Path:
    """Copy an image file into the db images dir and return the destination path."""
    if _REBUILD_MODE:
        # Source is already the prefixed, in-place file.
        return source
    dest_name = f"{source.parent.name}_{source.name}"
    dest = db_dir / "images" / dest_name
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, dest)
    return dest


def copy_ray_masks(json_path: Path, db_dir: Path) -> dict[str, str]:
    """Copy the ray mask files referenced by a JSON file into the db images dir.

    Returns a mapping of mask type to the new file name.
    """
    reader = ImageImporter(json_path)
    data = reader._data

    # Support both old flat format and new image_files wrapper.
    if "image_files" in data:
        ray_masks = data["image_files"].get("distance_mask_images", {})
    else:
        ray_masks = data.get("ray_masks", {})

    result: dict[str, str] = {}

    for key in (
        "ray_distance_name",
        "ray_normalized_distance_name",
        "ray_validity_name",
    ):
        rel_path = ray_masks.get(key, "")
        if not rel_path:
            result[key] = ""
            continue

        if _REBUILD_MODE:
            # JSON references the original unprefixed name; prepend the
            # date-time prefix derived from the JSON's own filename.
            prefix = _derive_prefix(json_path)
            source = (json_path.parent / f"{prefix}{rel_path}").resolve()
        else:
            source = (json_path.parent / rel_path).resolve()
        if not source.exists():
            result[key] = ""
            continue

        dest = copy_image_file(source, db_dir)
        result[key] = dest.name

    return result


def parse_image_data_file(filepath: Path) -> dict[str, Any]:
    reader: ImageImporter = ImageImporter(filepath)
    scene_data = reader.read_scene()

    return {
        "scene_name": scene_data.name,
        "scene_rendering_type": scene_data.rendering_type,
        "scene_center": CoordinateEntity(
            lat=Latitude(str(scene_data.center.y)),
            lon=Longitude(str(scene_data.center.x)),
        ),
        "scene_vis_range": scene_data.vis_range,
        "terrain_shape_center": scene_data.center.to_json(),
        "camera": reader.read_camera(),
        "fog": reader.read_fog(),
        "fog_type": reader.read_fog_type(),
        "light": reader.read_light(),
        "light_type": reader.read_light_type(),
        "environment": reader.read_environment(),
        # environment.light_ids will be filled in by write_full_scene once the light id is known
    }


def Get_Image_Metadata(img_path):
    with Image.open(img_path) as img:
        return img.size

def copy_data_file(original_data_file_path : Path, image_dir : Path) -> None:
    if _REBUILD_MODE:
        # The data file is already in place; nothing to copy.
        return
    parent_dir: str = original_data_file_path.parent.name
    new_data_file_path: Path = image_dir / "images" / f"{parent_dir}_{original_data_file_path.name}"
    new_data_file_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(original_data_file_path, new_data_file_path)


def process_image_data(
    image: InputImage,
    image_dir: Path,
) -> dict[str, Any]:
    imported_image_path: Path = move_image_into_db_dir(image.image_path, image_dir)
    parsed = parse_image_data_file(image.image_data_file_path)
    masks = copy_ray_masks(image.image_data_file_path, image_dir)
    copy_data_file(image.image_data_file_path, image_dir)
    width, height = Get_Image_Metadata(imported_image_path)

    color_image = ImageEntity(
        file_name=imported_image_path.name,
        file_type="color",
        width=width,
        height=height,
    )

    images = [color_image]
    images_by_role: dict[str, ImageEntity] = {}

    for role, key in (
        ("ray_distance", "ray_distance_name"),
        ("ray_normalized_distance", "ray_normalized_distance_name"),
        ("ray_validity", "ray_validity_name"),
    ):
        mask_name = masks.get(key, "")
        if mask_name:
            mask_path = image_dir / "images" / mask_name
            mask_width, mask_height = Get_Image_Metadata(mask_path)
            mask_entity = ImageEntity(
                file_name=mask_name,
                file_type=role,
                width=mask_width,
                height=mask_height,
            )
            images.append(mask_entity)
            images_by_role[role] = mask_entity

    view = ViewEntity(
        color_image_id=0,
        camera_id=0,
        scene_id=0,
        environment_id=0,
    )

    view_images: list[ViewImageEntity] = []
    for role, mask_entity in images_by_role.items():
        view_images.append(ViewImageEntity(view_id=0, image_id=0, role=role))

    json_reader: ImageImporter = ImageImporter(image.image_data_file_path)
    visibility_distances: list[VisibilityDistanceEntity] = (
        json_reader.read_visibility_distances(view_id=0)
    )

    parsed["images"] = images
    parsed["views"] = [view]
    parsed["view_images"] = view_images
    parsed["visibility_distances"] = visibility_distances

    return parsed


def writer_thread(d: Database, write_queue: queue.Queue, batch_size: int = 50) -> None:
    """Single dedicated thread — owns all DB access."""
    writer = DatabaseWriter(d)
    batch = []

    # Keep a single connection open for the lifetime of the writer so that
    # the per-row write_* calls reuse it instead of opening/closing on
    # every insert.
    with d:
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
    db_dir: Path,
    max_workers: int = 16,
):
    write_queue = queue.Queue(maxsize=500)
    total = len(importFilePaths)
    target_output_dir: list[Path] = []
    db = Database(db_dir)
    db.init_tables()
    for f in importFilePaths:
        target_output_dir.append(Path(db.import_dir).parent)

    writer = threading.Thread(target=writer_thread, args=(db, write_queue), daemon=True)
    writer.start()

    with tqdm(total=total, desc="Processing files", unit="file") as progress_bar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for result in executor.map(process_image_data, importFilePaths, target_output_dir):
                write_queue.put(result)
                progress_bar.update(1)

    write_queue.put(_DONE)
    writer.join()


def _flush(writer: DatabaseWriter, batch: list[dict]) -> None:
    for item in batch:
        writer.write_full_scene(**item)


def init_db(db_file_path: Path):
    db = Database(db_file_path)
    db.init_tables()


def cleanup_db(db_dir: Path) -> object:
    """Remove duplicate views and orphaned rows/files left by re-imports.

    Prints a one-line summary to the console and returns the
    DatabaseCleanup report.
    """
    db = Database(db_dir)
    report = DatabaseCleanup(db).remove_duplicate_views()
    print(report)
    return report


def main(
    db_dir: Path,
    import_dir: Path,
):
    if not os.path.exists(import_dir):
        raise Exception("Import image directory does not exist")

    db = Database(db_dir)

    inputs = collect_input_images(import_dir)
    image_output_dir: Path = db.import_dir
    if not os.path.exists(image_output_dir):
        os.makedirs(image_output_dir)

    process_files(inputs, db_dir)

    cleanup_db(db_dir)


def rebuild_db(db_dir: Path):
    """Wipe the database and rebuild it from the existing db images dir.

    Unlike main(), this does not copy any files: the images directory is
    the source of truth and already contains files with their final
    prefixed names. Only the database.sqlite3 file is removed; the images
    directory is left untouched.
    """
    global _REBUILD_MODE

    db = Database(db_dir)
    images_dir: Path = db.import_dir
    if not os.path.exists(images_dir):
        raise FileNotFoundError(
            f"Cannot rebuild: images directory does not exist: {images_dir}"
        )

    db_path = Path(db.db_path)
    if os.path.exists(db_path):
        os.remove(db_path)

    inputs = collect_input_images_rebuild(images_dir)

    try:
        _REBUILD_MODE = True
        process_files(inputs, db_dir)
    finally:
        _REBUILD_MODE = False

    cleanup_db(db_dir)
