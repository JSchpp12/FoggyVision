import sqlite3
from pathlib import Path

from fogvis.common import Latitude, Longitude, VectorContainer3D
from fogvis.db import DatabaseWriter
from fogvis.db.database import Database
from fogvis.db.database_cleanup import DatabaseCleanup, CleanupReport
from fogvis.db.entities import (
    CoordinateEntity,
    SceneEntity,
    CameraEntity,
    FogEntity,
    FogTypeEntity,
    LightEntity,
    LightTypeEntity,
    EnvironmentEntity,
    ImageEntity,
    ViewEntity,
    ViewImageEntity,
)


def _init_db(db_dir: Path) -> Database:
    db = Database(db_dir)
    db.init_tables()
    return db


def _seed_scene_camera_fog_env(db: Database) -> tuple[int, int, int, int]:
    """Seed one of each parent record and return (scene_id, camera_id, fog_id, env_id)."""
    coord = CoordinateEntity(lat=Latitude("39.0"), lon=Longitude("-105.0"))
    coord_id = DatabaseWriter.write_coordinate(db, coord)
    scene = SceneEntity(
        name="test_scene",
        coverage_distance_miles=10,
        upper_right_id=coord_id,
        lower_left_id=coord_id,
        center_id=coord_id,
        terrain_rendering_type="real",
    )
    scene_id = DatabaseWriter.write_scene(db, scene)
    camera = CameraEntity(
        scene_id=scene_id,
        virtual_position=VectorContainer3D.from_json('{"x": 0, "y": 15, "z": 0}'),
        look_dir=VectorContainer3D.from_json('{"x": 0, "y": 0, "z": -1}'),
        fov=60.0,
        near_clip=0.1,
        far_clip=1000.0,
    )
    camera_id = DatabaseWriter.write_camera(db, camera)
    fog_type = FogTypeEntity(name="marched")
    fog_type_id = DatabaseWriter.write_fog_type(db, fog_type)
    fog = FogEntity(
        scene_id=scene_id,
        fog_type_id=fog_type_id,
        exp_fog_density=0.6,
        linear_near_distance=0.01,
        linear_far_distance=16000.0,
        marched_cutoff=None,
        marched_color_transparency_cutoff=0.01,
        marched_distance_transparency_cutoff=0.05,
        marched_light_extinction_scale=0.15,
        marched_default_density=0.0,
        marched_density_multiplier=1.0,
        marched_lightDirG=0.6,
        marched_sigmaAbsorption=1e-5,
        marched_sigmaScattering=1e-4,
        marched_stepSizeDist=80.0,
        marched_stepSizeDist_light=100.0,
    )
    fog_id = DatabaseWriter.write_fog(db, fog)
    environment = EnvironmentEntity(fog_id=fog_id)
    env_id = DatabaseWriter.write_environment(db, environment)
    return scene_id, camera_id, fog_id, env_id


def _add_view_with_image(
    db: Database,
    db_dir: Path,
    scene_id: int,
    camera_id: int,
    env_id: int,
    file_name: str,
) -> int:
    """Insert one image row + file on disk and one view row referencing it."""
    image = ImageEntity(
        file_name=file_name,
        file_type="color",
        width=1920,
        height=1080,
    )
    image_id = DatabaseWriter.write_image(db, image)
    images_dir = db_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    (images_dir / file_name).write_bytes(b"fake")

    view = ViewEntity(
        color_image_id=image_id,
        camera_id=camera_id,
        scene_id=scene_id,
        environment_id=env_id,
    )
    view_id = DatabaseWriter.write_view(db, view)
    return view_id


def test_sweep_orphaned_image_rows_and_files(tmp_path):
    """An image row with no view/view_image referencing it is removed along
    with its file on disk."""
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    images_dir = db_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    (images_dir / "orphan.png").write_bytes(b"fake")

    DatabaseWriter.write_image(
        db,
        ImageEntity(file_name="orphan.png", file_type="color", width=10, height=10),
    )

    report = DatabaseCleanup(db).sweep_orphaned_files()
    assert report.images_removed == 1
    assert report.files_deleted == 1
    assert not (images_dir / "orphan.png").exists()

    with sqlite3.connect(db_dir / "database.sqlite3") as conn:
        assert conn.execute("SELECT count(*) FROM image").fetchone()[0] == 0


def test_sweep_removes_disk_only_orphan_files(tmp_path):
    """A file on disk with no image row at all is removed (the case that
    occurs after image rows have been deleted elsewhere)."""
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    scene_id, camera_id, _, env_id = _seed_scene_camera_fog_env(db)

    # A referenced image/file that must be kept.
    _add_view_with_image(db, db_dir, scene_id, camera_id, env_id, "keep.png")

    images_dir = db_dir / "images"
    (images_dir / "leftover.png").write_bytes(b"fake")
    assert (images_dir / "leftover.png").exists()

    report = DatabaseCleanup(db).sweep_orphaned_files()
    assert report.images_removed == 0
    assert report.disk_orphans_removed == 1
    assert report.files_deleted == 1
    assert (images_dir / "keep.png").exists()
    assert not (images_dir / "leftover.png").exists()


def test_sweep_keeps_json_sidecar_for_referenced_color(tmp_path):
    """A JSON sidecar is kept when its paired color image is still
    referenced, and deleted when the color image is gone."""
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    scene_id, camera_id, _, env_id = _seed_scene_camera_fog_env(db)

    # Referenced color image + its sidecar -> both kept.
    _add_view_with_image(db, db_dir, scene_id, camera_id, env_id, "alive.png")
    images_dir = db_dir / "images"
    (images_dir / "alive.json").write_bytes(b"{}")

    # Orphan sidecar whose color image has no row -> removed.
    (images_dir / "ghost.json").write_bytes(b"{}")

    report = DatabaseCleanup(db).sweep_orphaned_files()
    assert report.disk_orphans_removed == 1
    assert (images_dir / "alive.json").exists()
    assert (images_dir / "alive.png").exists()
    assert not (images_dir / "ghost.json").exists()


def test_sweep_ignores_unknown_extensions(tmp_path):
    """Files with extensions outside the deletable set are left alone even
    when they have no image row."""
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    images_dir = db_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    (images_dir / "README.txt").write_bytes(b"keep me")
    (images_dir / "notes.md").write_bytes(b"keep me too")

    report = DatabaseCleanup(db).sweep_orphaned_files()
    assert report.files_deleted == 0
    assert (images_dir / "README.txt").exists()
    assert (images_dir / "notes.md").exists()


def test_sweep_dry_run_deletes_nothing(tmp_path):
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    images_dir = db_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    (images_dir / "orphan.png").write_bytes(b"fake")
    DatabaseWriter.write_image(
        db,
        ImageEntity(file_name="orphan.png", file_type="color", width=10, height=10),
    )
    (images_dir / "leftover.png").write_bytes(b"fake")

    report = DatabaseCleanup(db).sweep_orphaned_files(dry_run=True)
    assert report.images_removed == 1          # the orphaned row would be removed
    assert report.disk_orphans_removed == 1    # leftover.png would be removed
    assert report.files_deleted == 0           # nothing actually deleted
    assert (images_dir / "orphan.png").exists()
    assert (images_dir / "leftover.png").exists()
    with sqlite3.connect(db_dir / "database.sqlite3") as conn:
        assert conn.execute("SELECT count(*) FROM image").fetchone()[0] == 1


def _add_view_with_color_and_mask(
    db: Database,
    db_dir: Path,
    scene_id: int,
    camera_id: int,
    env_id: int,
    color_name: str,
    mask_name: str,
) -> int:
    """Insert a color image + a ray_distance mask image + files on disk,
    one view referencing the color image, and a view_image link to the mask."""
    images_dir = db_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    color = ImageEntity(file_name=color_name, file_type="color", width=10, height=10)
    mask = ImageEntity(file_name=mask_name, file_type="ray_distance", width=10, height=10)
    color_id = DatabaseWriter.write_image(db, color)
    mask_id = DatabaseWriter.write_image(db, mask)
    (images_dir / color_name).write_bytes(b"fake")
    (images_dir / mask_name).write_bytes(b"fake")

    view_id = DatabaseWriter.write_view(
        db,
        ViewEntity(
            color_image_id=color_id,
            camera_id=camera_id,
            scene_id=scene_id,
            environment_id=env_id,
        ),
    )
    DatabaseWriter.write_view_image(
        db, ViewImageEntity(view_id=view_id, image_id=mask_id, role="ray_distance")
    )
    return view_id


def test_sweep_removes_orphaned_mask_rows_and_files(tmp_path):
    """When a view (and its view_image link) is removed, the color image and
    the ray mask image both become orphaned and are removed along with their
    files."""
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    scene_id, camera_id, _, env_id = _seed_scene_camera_fog_env(db)
    images_dir = db_dir / "images"

    view_id = _add_view_with_color_and_mask(
        db, db_dir, scene_id, camera_id, env_id, "c.png", "c_distMask.tif"
    )
    db_path = db_dir / "database.sqlite3"

    # Simulate the record removal: drop the view and its view_image link.
    with sqlite3.connect(db_path) as con:
        con.execute("DELETE FROM view_image WHERE viewID = ?", (view_id,))
        con.execute("DELETE FROM view WHERE id = ?", (view_id,))

    assert (images_dir / "c.png").exists()
    assert (images_dir / "c_distMask.tif").exists()

    report = DatabaseCleanup(db).sweep_orphaned_files()
    assert report.images_removed == 2          # color + mask
    assert report.files_deleted == 2
    assert not (images_dir / "c.png").exists()
    assert not (images_dir / "c_distMask.tif").exists()
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT count(*) FROM image").fetchone()[0] == 0


def test_sweep_drops_dangling_view_image_and_cleans_mask(tmp_path):
    """If a view was deleted but its view_image link was left behind, the
    dangling link must be dropped so the mask it held is cleaned up (and so
    the image delete is not blocked by the view_image foreign key)."""
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    scene_id, camera_id, _, env_id = _seed_scene_camera_fog_env(db)
    images_dir = db_dir / "images"
    db_path = db_dir / "database.sqlite3"

    view_id = _add_view_with_color_and_mask(
        db, db_dir, scene_id, camera_id, env_id, "c.png", "c_distMask.tif"
    )

    # Delete ONLY the view, leaving the view_image link dangling. Foreign
    # keys must be off for this (no ON DELETE cascade on view_image.viewID).
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = OFF")
    con.execute("DELETE FROM view WHERE id = ?", (view_id,))
    con.commit()
    con.close()

    report = DatabaseCleanup(db).sweep_orphaned_files()
    assert report.view_image_links_removed == 1
    assert report.images_removed == 2          # color (no view) + mask (link dropped)
    assert report.files_deleted == 2
    assert not (images_dir / "c.png").exists()
    assert not (images_dir / "c_distMask.tif").exists()
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT count(*) FROM view_image").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM image").fetchone()[0] == 0
