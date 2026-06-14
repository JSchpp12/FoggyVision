import shutil
from pathlib import Path
from typing import Iterable


def copy_images_to_db(
    source_json_path: Path,
    db_dir: Path,
    *,
    color_file_path: str,
    ray_distance_file_path: str,
    ray_normalized_distance_file_path: str,
    ray_validity_file_path: str,
) -> dict[str, str]:
    """Copy image files into a flat images directory under the DB directory.

    Parameters
    ----------
    source_json_path:
        Path to the JSON file that references the images.
    db_dir:
        Root directory of the database.
    color_file_path, ray_distance_file_path, ray_normalized_distance_file_path, ray_validity_file_path:
        Relative paths from the JSON file's directory to each image file.

    Returns
    -------
    Mapping of image type keys to the new relative paths stored in the database.
    """
    source_dir = source_json_path.parent
    images_dir = db_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "color_file_path": color_file_path,
        "ray_distance_file_path": ray_distance_file_path,
        "ray_normalized_distance_file_path": ray_normalized_distance_file_path,
        "ray_validity_file_path": ray_validity_file_path,
    }

    result: dict[str, str] = {}
    for key, rel_path in paths.items():
        if not rel_path:
            result[key] = ""
            continue

        source_file = source_dir / rel_path
        dest_file = images_dir / Path(rel_path).name

        shutil.copy2(source_file, dest_file)

        result[key] = str(dest_file.relative_to(db_dir))

    return result
