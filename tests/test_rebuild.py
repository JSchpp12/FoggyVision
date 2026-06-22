import shutil
import sqlite3
from pathlib import Path

import pytest

from fogvis.cli import main as cli_main
from fogvis.data_importer import (
    collect_input_images,
    collect_input_images_rebuild,
    _derive_prefix,
    main as import_main,
    rebuild_db,
)
from fogvis.data_importer import _PREFIX_RE


IMPORT_DIR = Path(__file__).resolve().parent.parent / "media" / "test"
IMPORT_DATE_DIRS = sorted(d for d in IMPORT_DIR.iterdir() if d.is_dir())


def _copy_import_tree(dest_root: Path) -> None:
    """Copy the import directory tree into a temp directory."""
    dest_root.mkdir(parents=True, exist_ok=True)
    for src_date_dir in IMPORT_DATE_DIRS:
        dest = dest_root / src_date_dir.name
        dest.mkdir(parents=True, exist_ok=True)
        for src in src_date_dir.iterdir():
            if src.is_file():
                shutil.copy2(src, dest / src.name)


def _build_reference_db(db_dir: Path) -> Path:
    """Run a normal import+cleanup into db_dir and return the import dir used."""
    import_dir = db_dir.parent / "import"
    _copy_import_tree(import_dir)
    import_main(import_dir=import_dir, db_dir=db_dir)
    return import_dir


_ALL_TABLES = [
    "scene",
    "camera",
    "fog",
    "fog_type",
    "light",
    "light_type",
    "environment",
    "environment_light",
    "image",
    "view",
    "view_image",
    "visibility_distance",
]


def _snapshot_counts(db_path: Path) -> dict[str, int]:
    with sqlite3.connect(db_path) as conn:
        return {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in _ALL_TABLES}


def _snapshot_files(images_dir: Path) -> dict[str, bytes]:
    if not images_dir.exists():
        return {}
    return {p.name: p.read_bytes() for p in sorted(images_dir.iterdir()) if p.is_file()}


def _sample_view_details(db_path: Path) -> dict:
    """Capture the mask links and visibility distances for the first color
    image in the DB. Values are compared between the reference and rebuilt
    DBs rather than hardcoded so the test stays robust to fixture tweaks."""
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, fileName, fileType, width, height FROM image "
            'WHERE fileType = "color" ORDER BY fileName LIMIT 1'
        )
        color_id, color_name, color_type, width, height = cur.fetchone()

        cur.execute(
            "SELECT count(*) FROM view WHERE colorImageID = ?", (color_id,)
        )
        view_count = cur.fetchone()[0]

        cur.execute(
            """SELECT image.fileName, view_image.role
            FROM view_image
            JOIN image ON view_image.imageID = image.id
            JOIN view ON view_image.viewID = view.id
            WHERE view.colorImageID = ?""",
            (color_id,),
        )
        linked = {role: name for name, role in cur.fetchall()}

        cur.execute(
            """SELECT distanceType, average, rayCount
            FROM visibility_distance
            JOIN view ON visibility_distance.viewID = view.id
            WHERE view.colorImageID = ?""",
            (color_id,),
        )
        distances = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

    return {
        "color_name": color_name,
        "color_type": color_type,
        "width": width,
        "height": height,
        "view_count": view_count,
        "linked_images": linked,
        "distances": distances,
    }


def test_derive_prefix_extracts_prefix_and_rejects_unprefixed():
    prefix = _derive_prefix(Path("2026-06-13_21-40-15_Frame-0.json"))
    assert prefix == "2026-06-13_21-40-15_"

    with pytest.raises(ValueError, match="date-time prefix"):
        _derive_prefix(Path("Frame-0.json"))


def test_collect_input_images_rebuild_finds_all_frames(tmp_path):
    db_dir = tmp_path / "db"
    _build_reference_db(db_dir)

    images_dir = db_dir / "images"
    inputs = collect_input_images_rebuild(images_dir)

    # Current fixture: 6 frames across the date dirs.
    assert len(inputs) == 6
    assert all(inp.image_path.exists() for inp in inputs)
    assert all(inp.image_data_file_path.exists() for inp in inputs)
    # Every collected JSON must be a flattened, prefixed filename.
    assert all(_PREFIX_RE.match(p.name) for p in (i.image_data_file_path for i in inputs))
    # Every color image path must live directly under the images dir and be prefixed.
    for inp in inputs:
        assert inp.image_path.parent == images_dir.resolve()
        assert _PREFIX_RE.match(inp.image_path.name)


def test_rebuild_db_produces_equivalent_database(tmp_path):
    db_dir = tmp_path / "db"
    _build_reference_db(db_dir)
    db_path = db_dir / "database.sqlite3"
    images_dir = db_dir / "images"

    ref_counts = _snapshot_counts(db_path)
    ref_files = _snapshot_files(images_dir)
    ref_details = _sample_view_details(db_path)

    rebuild_db(db_dir)

    assert db_path.exists()
    assert images_dir.exists()

    # Table counts must match the reference.
    assert _snapshot_counts(db_path) == ref_counts

    # The flattened images directory must be untouched byte-for-byte.
    new_files = _snapshot_files(images_dir)
    assert set(new_files.keys()) == set(ref_files.keys())
    for name, content in ref_files.items():
        assert new_files[name] == content

    # The sample view's masks + visibility distances must be preserved.
    new_details = _sample_view_details(db_path)
    assert new_details["color_name"] == ref_details["color_name"]
    assert new_details["color_type"] == ref_details["color_type"]
    assert new_details["width"] == ref_details["width"]
    assert new_details["height"] == ref_details["height"]
    assert new_details["view_count"] == ref_details["view_count"] == 1
    assert new_details["linked_images"] == ref_details["linked_images"]
    assert new_details["distances"] == ref_details["distances"]

    # Rebuild must leave the global flag reset.
    import fogvis.data_importer as di
    assert di._REBUILD_MODE is False


def test_rebuild_db_raises_when_images_dir_missing(tmp_path):
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    with pytest.raises(FileNotFoundError, match="images directory"):
        rebuild_db(db_dir)


def test_rebuild_mode_reset_after_failure(tmp_path, monkeypatch):
    db_dir = tmp_path / "db"
    _build_reference_db(db_dir)

    import fogvis.data_importer as di

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated processing failure")

    monkeypatch.setattr(di, "process_files", _boom)

    with pytest.raises(RuntimeError, match="simulated processing failure"):
        di.rebuild_db(db_dir)

    assert di._REBUILD_MODE is False


def test_cli_rebuild_subcommand(tmp_path, monkeypatch):
    db_dir = tmp_path / "db"
    _build_reference_db(db_dir)
    db_path = db_dir / "database.sqlite3"

    before_views = _snapshot_counts(db_path)["view"]

    monkeypatch.setattr(
        "sys.argv",
        ["fogvis-cli", "rebuild", "--database", str(db_dir)],
    )
    cli_main()

    counts = _snapshot_counts(db_path)
    # Rebuild reproduces the post-cleanup deduplicated state.
    assert counts["view"] == before_views

    with sqlite3.connect(db_path) as conn:
        dup_groups = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT 1 FROM view
                GROUP BY cameraID, sceneID, environmentID
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    assert dup_groups == 0