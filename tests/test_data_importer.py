import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from fogvis.data_importer import collect_input_images, process_files, cleanup_db
from fogvis.db import DatabaseWriter
from fogvis.db.database import Database
from fogvis.db.database_cleanup import DatabaseCleanup
from fogvis.db.entities import ImageEntity, ViewEntity


IMPORT_DIR = Path(__file__).resolve().parent.parent / "media" / "test"
IMPORT_DATE_DIRS = sorted(d for d in IMPORT_DIR.iterdir() if d.is_dir())


def _copy_import_tree(dest_root: Path) -> None:
    """Copy the import directory tree into a temp directory."""
    for src_date_dir in IMPORT_DATE_DIRS:
        dest = dest_root / src_date_dir.name
        dest.mkdir(parents=True, exist_ok=True)
        for src in src_date_dir.iterdir():
            if src.is_file():
                shutil.copy2(src, dest / src.name)


def test_collect_input_images_finds_all_frames(tmp_path):
    import_dir = tmp_path / "import"
    _copy_import_tree(import_dir)

    inputs = collect_input_images(import_dir)
    assert len(inputs) == 6
    assert all(input.image_path.exists() for input in inputs)
    assert all(input.image_data_file_path.exists() for input in inputs)


def test_process_files_imports_sample_frame(tmp_path):
    import_dir = tmp_path / "import"
    _copy_import_tree(import_dir)
    db_dir = tmp_path / "db"

    inputs = collect_input_images(import_dir)
    process_files(inputs, db_dir, max_workers=1)

    db_path = db_dir / "database.sqlite3"
    assert db_path.exists()

    images_dir = db_dir / "images"
    assert images_dir.exists()
    expected_images = [
        ("2026-06-13_21-40-15", "Frame-0.png"),
        ("2026-06-13_21-40-15", "Frame-0_distMask.tif"),
        ("2026-06-13_21-40-15", "Frame-0_distNormSmlMask.tif"),
        ("2026-06-13_21-40-15", "Frame-0_validMask.png"),
    ]
    for date_dir, filename in expected_images:
        assert (images_dir / date_dir / filename).exists()

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute("SELECT count(*) FROM scene")
        assert cur.fetchone()[0] == 1

        cur.execute("SELECT count(*) FROM camera")
        assert cur.fetchone()[0] == 1

        cur.execute("SELECT count(*) FROM fog")
        assert cur.fetchone()[0] == 6

        cur.execute("SELECT count(*) FROM light")
        assert cur.fetchone()[0] == 2

        cur.execute("SELECT count(*) FROM environment")
        assert cur.fetchone()[0] == 6

        cur.execute("SELECT count(*) FROM image")
        assert cur.fetchone()[0] == 24

        cur.execute("SELECT count(*) FROM view")
        assert cur.fetchone()[0] == 6

        cur.execute("SELECT count(*) FROM view_image")
        assert cur.fetchone()[0] == 18

        cur.execute("SELECT count(*) FROM visibility_distance")
        assert cur.fetchone()[0] == 12

        cur.execute(
            "SELECT id, fileName, fileType, width, height FROM image WHERE fileName = ?",
            ("2026-06-13_21-40-15/Frame-0.png",),
        )
        row = cur.fetchone()
        assert row[0] is not None
        assert row[1] == "2026-06-13_21-40-15/Frame-0.png"
        assert row[2] == "color"
        assert row[3] is not None
        assert row[4] is not None

        color_image_id = row[0]

        cur.execute(
            "SELECT count(*) FROM view WHERE colorImageID = ?",
            (color_image_id,),
        )
        assert cur.fetchone()[0] == 1

        cur.execute(
            """SELECT image.fileName, view_image.role
            FROM view_image
            JOIN image ON view_image.imageID = image.id
            JOIN view ON view_image.viewID = view.id
            WHERE view.colorImageID = ?""",
            (color_image_id,),
        )
        linked_images = {role: name for name, role in cur.fetchall()}
        assert linked_images.get("ray_distance") == "2026-06-13_21-40-15/Frame-0_distMask.tif"
        assert linked_images.get("ray_normalized_distance") == "2026-06-13_21-40-15/Frame-0_distNormSmlMask.tif"
        assert linked_images.get("ray_validity") == "2026-06-13_21-40-15/Frame-0_validMask.png"

        cur.execute(
            """SELECT distanceType, average, rayCount
            FROM visibility_distance
            JOIN view ON visibility_distance.viewID = view.id
            WHERE view.colorImageID = ?""",
            (color_image_id,),
        )
        distances = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
        assert distances["ray_excluding_invalid"][1] == 0
        assert distances["ray_including_invalid"][1] == 921600
        assert distances["ray_including_invalid"][0] == pytest.approx(7197.180718457964)


def test_process_files_persists_skybox_info(tmp_path):
    import_dir = tmp_path / "import"
    _copy_import_tree(import_dir)
    json_path = sorted(import_dir.rglob("Frame-0.json"))[0]
    data = json.loads(json_path.read_text())
    data["skybox_info"] = {
        "name": "autumn_field_puresky_4k",
        "layout": "equirect",
        "yaw_degrees": 0.0,
        "face_size": 1024,
        "format_scaling": "full",
    }
    json_path.write_text(json.dumps(data))

    db_dir = tmp_path / "db"
    process_files(collect_input_images(import_dir), db_dir, max_workers=1)

    with sqlite3.connect(db_dir / "database.sqlite3") as conn:
        assert conn.execute("SELECT count(*) FROM skybox").fetchone()[0] == 1
        assert conn.execute(
            "SELECT count(*) FROM view WHERE skyboxID IS NOT NULL"
        ).fetchone()[0] == 1
        row = conn.execute(
            """SELECT skybox.name, view.skyboxID
               FROM view JOIN skybox ON skybox.id = view.skyboxID"""
        ).fetchone()
        assert row[0] == "autumn_field_puresky_4k"
        assert row[1] is not None


def test_process_files_imports_multiple_frames_and_deduplicates(tmp_path):
    import_dir = tmp_path / "import"
    _copy_import_tree(import_dir)
    db_dir = tmp_path / "db"

    inputs = collect_input_images(import_dir)
    process_files(inputs, db_dir, max_workers=1)

    db_path = db_dir / "database.sqlite3"
    assert db_path.exists()

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Shared scene should deduplicate to one row.
        cur.execute("SELECT count(*) FROM scene")
        assert cur.fetchone()[0] == 1

        cur.execute("SELECT count(*) FROM camera")
        assert cur.fetchone()[0] == 1

        cur.execute("SELECT count(*) FROM fog")
        assert cur.fetchone()[0] == 6

        cur.execute("SELECT count(*) FROM light")
        assert cur.fetchone()[0] == 2

        cur.execute("SELECT count(*) FROM environment")
        assert cur.fetchone()[0] == 6

        cur.execute("SELECT count(*) FROM environment_light")
        assert cur.fetchone()[0] == 6

        # One view per frame: each has its own color image, so none dedup.
        cur.execute("SELECT count(*) FROM view")
        assert cur.fetchone()[0] == 6

        cur.execute("SELECT DISTINCT volumeName FROM fog")
        volume_names = {row[0] for row in cur.fetchall()}
        assert volume_names == {"clouds", "windy_whipped"}

        cur.execute("SELECT DISTINCT distanceType FROM visibility_distance")
        distance_types = {row[0] for row in cur.fetchall()}
        assert distance_types == {"ray_excluding_invalid", "ray_including_invalid"}


def test_cleanup_removes_orphaned_image_files(tmp_path):
    """cleanup_db should remove image files left on disk that no longer have
    an image row in the database."""
    import_dir = tmp_path / "import"
    _copy_import_tree(import_dir)
    db_dir = tmp_path / "db"

    inputs = collect_input_images(import_dir)
    process_files(inputs, db_dir, max_workers=1)

    images_dir = db_dir / "images"
    date_dir = images_dir / "2026-06-13_21-40-15"
    orphan_path = date_dir / "Frame-0_orphan.png"
    orphan_path.write_bytes(b"fake")
    assert orphan_path.exists()

    report = cleanup_db(db_dir)

    assert report.files_deleted >= 1
    assert not orphan_path.exists()

    assert (date_dir / "Frame-0.png").exists()


def test_data_importer_main_runs_cleanup(tmp_path):
    """main() should run cleanup automatically after import, so the
    resulting DB is already deduplicated."""
    import_dir = tmp_path / "import"
    _copy_import_tree(import_dir)
    db_dir = tmp_path / "db"

    from fogvis.data_importer import main as import_main

    import_main(import_dir=import_dir, db_dir=db_dir)

    db_path = db_dir / "database.sqlite3"
    with sqlite3.connect(db_path) as conn:
        views = conn.execute("SELECT count(*) FROM view").fetchone()[0]
        dup_groups = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT 1 FROM view
                GROUP BY colorImageID, cameraID, sceneID, environmentID, skyboxID
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

    assert views == 6
    assert dup_groups == 0
