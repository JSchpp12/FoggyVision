import shutil
import sqlite3
from pathlib import Path

import pytest

from fogvis.data_importer import collect_input_images, process_files, cleanup_db
from fogvis.db.database import Database
from fogvis.db.database_cleanup import DatabaseCleanup


IMPORT_DIR = Path(__file__).resolve().parent.parent / "import"
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
    # Two date dirs of 40 frames each.
    assert len(inputs) == 80
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
        "2026-06-21_08-43-32_Frame-0.png",
        "2026-06-21_08-43-32_Frame-0_distMask.tif",
        "2026-06-21_08-43-32_Frame-0_distNormSmlMask.tif",
        "2026-06-21_08-43-32_Frame-0_validMask.png",
    ]
    for filename in expected_images:
        assert (images_dir / filename).exists()

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute("SELECT count(*) FROM scene")
        assert cur.fetchone()[0] == 1

        # 4 distinct camera look directions across the fixture.
        cur.execute("SELECT count(*) FROM camera")
        assert cur.fetchone()[0] == 4

        # 10 distinct fog configurations across the fixture.
        cur.execute("SELECT count(*) FROM fog")
        assert cur.fetchone()[0] == 10

        # Light is shared across import frames.
        cur.execute("SELECT count(*) FROM light")
        assert cur.fetchone()[0] == 1

        # One environment per unique fog.
        cur.execute("SELECT count(*) FROM environment")
        assert cur.fetchone()[0] == 10

        # 80 color images + 240 mask images (3 masks per frame).
        cur.execute("SELECT count(*) FROM image")
        assert cur.fetchone()[0] == 320

        # One view per frame.
        cur.execute("SELECT count(*) FROM view")
        assert cur.fetchone()[0] == 80

        # 240 mask links total (three per frame).
        cur.execute("SELECT count(*) FROM view_image")
        assert cur.fetchone()[0] == 240

        # Each frame has both ray metrics (2 per frame).
        cur.execute("SELECT count(*) FROM visibility_distance")
        assert cur.fetchone()[0] == 160

        cur.execute(
            "SELECT id, fileName, fileType, width, height FROM image WHERE fileName = ?",
            ("2026-06-21_08-43-32_Frame-0.png",),
        )
        row = cur.fetchone()
        assert row[0] is not None
        assert row[1] == "2026-06-21_08-43-32_Frame-0.png"
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
        assert linked_images.get("ray_distance") == "2026-06-21_08-43-32_Frame-0_distMask.tif"
        assert linked_images.get("ray_normalized_distance") == "2026-06-21_08-43-32_Frame-0_distNormSmlMask.tif"
        assert linked_images.get("ray_validity") == "2026-06-21_08-43-32_Frame-0_validMask.png"

        cur.execute(
            """SELECT distanceType, average, rayCount
            FROM visibility_distance
            JOIN view ON visibility_distance.viewID = view.id
            WHERE view.colorImageID = ?""",
            (color_image_id,),
        )
        distances = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
        assert distances["ray_excluding_invalid"][0] == pytest.approx(31845.84600341724)
        assert distances["ray_excluding_invalid"][1] == 50205
        assert distances["ray_including_invalid"][0] == pytest.approx(21210.798309921454)
        assert distances["ray_including_invalid"][1] == 518400


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

        # Shared scene should be deduplicated to one.
        cur.execute("SELECT count(*) FROM scene")
        assert cur.fetchone()[0] == 1

        # 4 distinct camera look directions.
        cur.execute("SELECT count(*) FROM camera")
        assert cur.fetchone()[0] == 4

        # 10 distinct fog configurations.
        cur.execute("SELECT count(*) FROM fog")
        assert cur.fetchone()[0] == 10

        # Light is shared across import frames.
        cur.execute("SELECT count(*) FROM light")
        assert cur.fetchone()[0] == 1

        # Each unique fog gets its own environment.
        cur.execute("SELECT count(*) FROM environment")
        assert cur.fetchone()[0] == 10

        cur.execute("SELECT count(*) FROM environment_light")
        assert cur.fetchone()[0] == 10

        # One view per frame (no dedup at the writer level since each frame
        # has its own color image).
        cur.execute("SELECT count(*) FROM view")
        assert cur.fetchone()[0] == 80

        # All frames in this fixture use the "clouds" fog volume.
        cur.execute("SELECT DISTINCT volumeName FROM fog")
        volume_names = {row[0] for row in cur.fetchall()}
        assert volume_names == {"clouds"}

        # Verify visibility distance label types.
        cur.execute("SELECT DISTINCT distanceType FROM visibility_distance")
        distance_types = {row[0] for row in cur.fetchall()}
        assert distance_types == {"ray_excluding_invalid", "ray_including_invalid"}


def test_cleanup_collapses_re_imported_duplicates(tmp_path):
    """Re-importing the same tree produces duplicate views (same
    camera/scene/environment, different color image); cleanup should
    collapse them back to one view per unique signature, keeping the
    earliest (lowest id) and removing the newer duplicate rows + files."""
    import_dir = tmp_path / "import"
    _copy_import_tree(import_dir)
    db_dir = tmp_path / "db"

    inputs = collect_input_images(import_dir)
    process_files(inputs, db_dir, max_workers=1)

    db_path = db_dir / "database.sqlite3"

    # The 80-frame fixture contains 40 pairs of frames that render the same
    # view content (identical camera/scene/environment). So a single import
    # already has 40 duplicate groups.
    with sqlite3.connect(db_path) as conn:
        before_views = conn.execute("SELECT count(*) FROM view").fetchone()[0]
        before_images = conn.execute("SELECT count(*) FROM image").fetchone()[0]
        # Capture the pre-cleanup duplicate groups: for each group record
        # the keeper (lowest id) and the duplicate ids that should be removed.
        dup_groups_before = conn.execute(
            """
            SELECT cameraID, sceneID, environmentID,
                   MIN(id) AS keeper,
                   (SELECT group_concat(id) FROM view v2
                    WHERE v2.cameraID = v.cameraID
                      AND v2.sceneID = v.sceneID
                      AND v2.environmentID = v.environmentID
                      AND v2.id > MIN(v.id)) AS dups
            FROM view v
            GROUP BY cameraID, sceneID, environmentID
            HAVING COUNT(*) > 1
            """
        ).fetchall()
    assert before_views == 80
    assert before_images == 320
    assert len(dup_groups_before) == 40

    # Remember keepers + duplicates for post-cleanup verification.
    keeper_ids = {row[3] for row in dup_groups_before}
    duplicate_ids = set()
    for row in dup_groups_before:
        if row[4]:
            duplicate_ids.update(int(x) for x in row[4].split(","))

    report = cleanup_db(db_dir)

    assert report.duplicate_views_removed == 40
    assert report.images_removed == 160
    assert report.files_deleted == 160

    with sqlite3.connect(db_path) as conn:
        after_views = conn.execute("SELECT count(*) FROM view").fetchone()[0]
        after_images = conn.execute("SELECT count(*) FROM image").fetchone()[0]
        dup_groups = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT 1 FROM view
                GROUP BY cameraID, sceneID, environmentID
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        # Every keeper must still be present.
        present_keepers = conn.execute(
            f"SELECT COUNT(DISTINCT id) FROM view WHERE id IN ({','.join('?' * len(keeper_ids))})",
            tuple(keeper_ids),
        ).fetchone()[0]
        # Every duplicate must be gone.
        remaining_duplicates = conn.execute(
            f"SELECT COUNT(DISTINCT id) FROM view WHERE id IN ({','.join('?' * len(duplicate_ids))})",
            tuple(duplicate_ids),
        ).fetchone()[0]

    assert after_views == 40
    assert after_images == 160
    assert dup_groups == 0
    assert present_keepers == 40
    assert remaining_duplicates == 0

    # Files for removed images should be gone; files for kept images remain.
    images_dir = db_dir / "images"
    kept = "2026-06-21_08-43-32_Frame-0.png"
    assert (images_dir / kept).exists()


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
                GROUP BY cameraID, sceneID, environmentID
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

    assert views == 40
    assert dup_groups == 0
