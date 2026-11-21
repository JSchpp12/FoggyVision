import json
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from fogvis.cli import main as cli_main
from fogvis.data_importer import (
    collect_input_images,
    main as import_main,
    rebuild_db,
)


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
    "skybox",
]


def _snapshot_counts(db_path: Path) -> dict[str, int]:
    # closing() is required: sqlite3's `with` only commits, it does not
    # close the connection - an open handle blocks os.remove() in
    # rebuild_db() on Windows.
    with closing(sqlite3.connect(db_path)) as conn:
        return {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in _ALL_TABLES}


def _snapshot_files(images_dir: Path) -> dict[str, bytes]:
    """Snapshot the images dir keyed by path relative to the images dir."""
    if not images_dir.exists():
        return {}
    return {
        p.relative_to(images_dir).as_posix(): p.read_bytes()
        for p in sorted(images_dir.rglob("*"))
        if p.is_file()
    }


def _sample_view_details(db_path: Path) -> dict:
    """Capture the mask links and visibility distances for the first color
    image in the DB. Values are compared between the reference and rebuilt
    DBs rather than hardcoded so the test stays robust to fixture tweaks."""
    with closing(sqlite3.connect(db_path)) as conn:
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

        cur.execute(
            """SELECT skybox.name
            FROM view
            LEFT JOIN skybox ON skybox.id = view.skyboxID
            WHERE view.colorImageID = ?""",
            (color_id,),
        )
        skybox_names = [row[0] for row in cur.fetchall()]

    return {
        "color_name": color_name,
        "color_type": color_type,
        "width": width,
        "height": height,
        "view_count": view_count,
        "linked_images": linked,
        "distances": distances,
        "skybox_names": skybox_names,
    }


def test_collect_input_images_on_images_dir_finds_all_frames(tmp_path):
    """The db images dir uses the same date-subdirectory layout as an import
    source, so collect_input_images can parse it directly."""
    db_dir = tmp_path / "db"
    _build_reference_db(db_dir)

    images_dir = db_dir / "images"
    inputs = collect_input_images(images_dir)

    assert all(inp.image_path.exists() for inp in inputs)
    assert all(inp.image_data_file_path.exists() for inp in inputs)
    for inp in inputs:
        assert inp.image_path.parent.parent == images_dir.resolve()
        assert inp.image_data_file_path.parent.parent == images_dir.resolve()


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

    assert _snapshot_counts(db_path) == ref_counts

    new_files = _snapshot_files(images_dir)
    assert set(new_files.keys()) == set(ref_files.keys())
    for name, content in ref_files.items():
        assert new_files[name] == content

    new_details = _sample_view_details(db_path)
    assert new_details["color_name"] == ref_details["color_name"]
    assert new_details["color_type"] == ref_details["color_type"]
    assert new_details["width"] == ref_details["width"]
    assert new_details["height"] == ref_details["height"]
    assert new_details["view_count"] == ref_details["view_count"] == 1
    assert new_details["linked_images"] == ref_details["linked_images"]
    assert new_details["distances"] == ref_details["distances"]
    assert new_details["skybox_names"] == ref_details["skybox_names"]


def test_rebuild_preserves_skybox_from_json_sidecar(tmp_path):
    import_dir = tmp_path / "import"
    _copy_import_tree(import_dir)
    json_path = sorted(import_dir.rglob("Frame-0.json"))[0]
    data = json.loads(json_path.read_text())
    data["skybox_info"] = {"name": "autumn_field_puresky_4k"}
    json_path.write_text(json.dumps(data))

    db_dir = tmp_path / "db"
    import_main(import_dir=import_dir, db_dir=db_dir)
    db_path = db_dir / "database.sqlite3"

    def skybox_names():
        with closing(sqlite3.connect(db_path)) as conn:
            return conn.execute(
                """SELECT DISTINCT skybox.name
                   FROM view JOIN skybox ON skybox.id = view.skyboxID"""
            ).fetchall()

    before = skybox_names()
    assert before == [("autumn_field_puresky_4k",)]

    rebuild_db(db_dir)

    assert skybox_names() == before


def test_rebuild_db_raises_when_images_dir_missing(tmp_path):
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    with pytest.raises(FileNotFoundError, match="images directory"):
        rebuild_db(db_dir)


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
    assert counts["view"] == before_views

    with closing(sqlite3.connect(db_path)) as conn:
        dup_groups = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT 1 FROM view
                GROUP BY colorImageID, cameraID, sceneID, environmentID, skyboxID
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    assert dup_groups == 0