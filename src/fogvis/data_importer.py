from fogvis.db import DatabaseWriter, ImageImporter, SceneEntity, CoordinateEntity
from fogvis.common import Latitude, Longitude

import queue
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Any

# used to shutdown the writer thread
_DONE = object()


def parse_file(filepath: Path) -> dict[str, Any]:
    reader: ImageImporter = ImageImporter(filepath)
    return {
        "scene": SceneEntity(name="", upper_right_id=0, lower_left_id=0, center_id=0),
        "coordinate": CoordinateEntity(lat=Latitude("0.0"), lon=Longitude("0.0")),
        "camera": reader.read_camera(),
        "fog": reader.read_fog(),
        "fog_type": reader.read_fog_type(),
        "lights": reader.read_light(),
        "environment": reader.read_environment(),
        "images": reader.read_image(),
    }


def _flush(writer: DatabaseWriter, batch: list[dict]) -> None:
    for item in batch:
        writer.write_full_scene(**item)


def writer_thread(
    db_path: Path, write_queue: queue.Queue, batch_size: int = 50
) -> None:
    """Single dedicated thread — owns all DB access."""
    writer = DatabaseWriter(db_path)
    batch = []

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


def process_files(importFilePaths: list[Path], db_dirPath: Path, max_workers: int = 16):
    write_queue = queue.Queue(maxsize=500)

    writer = threading.Thread(
        target=writer_thread, args=(db_dirPath, write_queue), daemon=True
    )
    writer.start()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(parse_file, importFilePaths):
            write_queue.put(result)

    write_queue.put(_DONE)
    writer.join()
