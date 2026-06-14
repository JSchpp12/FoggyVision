import shutil
import sqlite3
from pathlib import Path

import pytest

from fogvis.data_importer import collect_input_images, process_files


SAMPLE_DIR = Path(__file__).resolve().parent.parent / "media" / "test"
SAMPLE_DATE_DIRS = sorted(SAMPLE_DIR.iterdir())


def _copy_sample_import_tree(dest_root: Path) -> None:
    """Copy the canonical sample import tree into a temp directory."""
    for sample_date_dir in SAMPLE_DATE_DIRS:
        dest = dest_root / sample_date_dir.name
        dest.mkdir(parents=True, exist_ok=True)
        for src in sample_date_dir.iterdir():
            if src.is_file():
                shutil.copy2(src, dest / src.name)


def test_collect_input_images_finds_all_frames(tmp_path):
    import_dir = tmp_path / "import"
    _copy_sample_import_tree(import_dir)

    inputs = collect_input_images(import_dir)
    assert len(inputs) == 6
    assert all(input.image_path.exists() for input in inputs)
    assert all(input.image_data_file_path.exists() for input in inputs)


def test_process_files_imports_sample_frame(tmp_path):
    import_dir = tmp_path / "import"
    _copy_sample_import_tree(import_dir)
    db_dir = tmp_path / "db"

    inputs = collect_input_images(import_dir)
    process_files(inputs, db_dir, max_workers=1)

    db_path = db_dir / "database.sqlite3"
    assert db_path.exists()

    images_dir = db_dir / "images"
    assert images_dir.exists()
    expected_images = [
        "2026-06-13_21-40-15_Frame-0.png",
        "2026-06-13_21-40-15_Frame-0_distMask.tif",
        "2026-06-13_21-40-15_Frame-0_distNormSmlMask.tif",
        "2026-06-13_21-40-15_Frame-0_validMask.png",
    ]
    for filename in expected_images:
        assert (images_dir / filename).exists()

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute("SELECT count(*) FROM scene")
        assert cur.fetchone()[0] == 1

        # Camera is identical across frames, so deduplicated to one.
        cur.execute("SELECT count(*) FROM camera")
        assert cur.fetchone()[0] == 1

        # Fog differs across frames, so one row per frame.
        cur.execute("SELECT count(*) FROM fog")
        assert cur.fetchone()[0] == 6

        # Light luminance differs across the two date directories (20 vs 50).
        cur.execute("SELECT count(*) FROM light")
        assert cur.fetchone()[0] == 2

        cur.execute("SELECT count(*) FROM environment")
        assert cur.fetchone()[0] == 6

        cur.execute("SELECT count(*) FROM image")
        assert cur.fetchone()[0] == 6

        cur.execute(
            "SELECT filePath, rayDistanceFilePath, rayNormalizedDistanceFilePath, rayValidityFilePath FROM image WHERE filePath = ?",
            ("2026-06-13_21-40-15_Frame-0.png",),
        )
        row = cur.fetchone()
        assert row[0] == "2026-06-13_21-40-15_Frame-0.png"
        assert row[1] == "2026-06-13_21-40-15_Frame-0_distMask.tif"
        assert row[2] == "2026-06-13_21-40-15_Frame-0_distNormSmlMask.tif"
        assert row[3] == "2026-06-13_21-40-15_Frame-0_validMask.png"

        cur.execute(
            "SELECT excludingInvalidRaysAverage, excludingInvalidRaysRayCount, includingInvalidRaysAverage, includingInvalidRaysRayCount FROM image WHERE filePath = ?",
            ("2026-06-13_21-40-15_Frame-0.png",),
        )
        row = cur.fetchone()
        assert row[0] is None
        assert row[1] == 0
        assert row[2] == pytest.approx(7197.180718457964)
        assert row[3] == 921600


def test_process_files_imports_multiple_frames_and_deduplicates(tmp_path):
    import_dir = tmp_path / "import"
    _copy_sample_import_tree(import_dir)
    db_dir = tmp_path / "db"

    inputs = collect_input_images(import_dir)
    process_files(inputs, db_dir, max_workers=1)

    db_path = db_dir / "database.sqlite3"
    assert db_path.exists()

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Shared scene and light should be deduplicated.
        cur.execute("SELECT count(*) FROM scene")
        assert cur.fetchone()[0] == 1

        # Camera is identical across frames, so deduplicated to one.
        cur.execute("SELECT count(*) FROM camera")
        assert cur.fetchone()[0] == 1

        # Fog differs across frames, so one row per frame.
        cur.execute("SELECT count(*) FROM fog")
        assert cur.fetchone()[0] == 6

        # Light luminance differs across the two date directories (20 vs 50).
        cur.execute("SELECT count(*) FROM light")
        assert cur.fetchone()[0] == 2

        # Each unique fog gets its own environment.
        cur.execute("SELECT count(*) FROM environment")
        assert cur.fetchone()[0] == 6

        cur.execute("SELECT count(*) FROM environment_light")
        assert cur.fetchone()[0] == 6

        # One image row per frame.
        cur.execute("SELECT count(*) FROM image")
        assert cur.fetchone()[0] == 6

        # Each copied color image should be recorded.
        cur.execute("SELECT filePath FROM image")
        file_paths = {row[0] for row in cur.fetchall()}
        for i in (0, 1):
            assert f"2026-06-13_21-40-15_Frame-{i}.png" in file_paths
        for frame_id in (8, 9):
            assert f"2026-06-13_21-45-51_Frame-{frame_id}.png" in file_paths
        for i in (0, 1):
            assert f"2026-06-13_22-01-00_Frame-{i}.png" in file_paths

        # Verify both fog volume names are present.
        cur.execute("SELECT DISTINCT volumeName FROM fog")
        volume_names = {row[0] for row in cur.fetchall()}
        assert volume_names == {"clouds", "windy_whipped"}
