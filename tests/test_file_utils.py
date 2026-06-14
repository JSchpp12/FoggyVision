from pathlib import Path

from fogvis.db.file_utils import copy_images_to_db


def test_copy_images_to_db_creates_flat_directory(sample_json_path, tmp_path):
    db_dir = tmp_path / "db"
    result = copy_images_to_db(
        sample_json_path,
        db_dir,
        color_file_path="Frame-0.png",
        ray_distance_file_path="Frame-0_distMask.tif",
        ray_normalized_distance_file_path="Frame-0_distNormSmlMask.tif",
        ray_validity_file_path="Frame-0_validMask.png",
    )

    images_dir = db_dir / "images"
    assert images_dir.exists()

    expected_files = {
        "color_file_path": "Frame-0.png",
        "ray_distance_file_path": "Frame-0_distMask.tif",
        "ray_normalized_distance_file_path": "Frame-0_distNormSmlMask.tif",
        "ray_validity_file_path": "Frame-0_validMask.png",
    }

    for key, filename in expected_files.items():
        dest = images_dir / filename
        assert dest.exists()
        assert result[key] == str(Path("images") / filename)


def test_copy_images_to_db_overwrites_existing(sample_json_path, tmp_path):
    db_dir = tmp_path / "db"
    images_dir = db_dir / "images"
    images_dir.mkdir(parents=True)

    existing = images_dir / "Frame-0.png"
    existing.write_text("old content")

    copy_images_to_db(
        sample_json_path,
        db_dir,
        color_file_path="Frame-0.png",
        ray_distance_file_path="",
        ray_normalized_distance_file_path="",
        ray_validity_file_path="",
    )

    # The file should have been overwritten by the copy.
    assert existing.read_bytes() != b"old content"
