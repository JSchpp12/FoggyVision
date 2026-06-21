import sqlite3
from pathlib import Path

import pytest

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
    VisibilityDistanceEntity,
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


def test_find_duplicate_views_returns_groups(tmp_path):
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    scene_id, camera_id, _, env_id = _seed_scene_camera_fog_env(db)

    # Two views with identical (camera, scene, env) -> duplicates.
    v1 = _add_view_with_image(db, db_dir, scene_id, camera_id, env_id, "a.png")
    v2 = _add_view_with_image(db, db_dir, scene_id, camera_id, env_id, "b.png")

    # A third view with a different env is its own group.
    fog_type2 = FogTypeEntity(name="linear")
    ft2_id = DatabaseWriter.write_fog_type(db, fog_type2)
    fog2 = FogEntity(
        scene_id=scene_id,
        fog_type_id=ft2_id,
        exp_fog_density=0.1,
        linear_near_distance=0.0,
        linear_far_distance=100.0,
        marched_cutoff=None,
        marched_color_transparency_cutoff=0.0,
        marched_distance_transparency_cutoff=0.0,
        marched_light_extinction_scale=0.0,
        marched_default_density=0.0,
        marched_density_multiplier=1.0,
        marched_lightDirG=0.0,
        marched_sigmaAbsorption=0.0,
        marched_sigmaScattering=0.0,
        marched_stepSizeDist=1.0,
        marched_stepSizeDist_light=1.0,
    )
    fog2_id = DatabaseWriter.write_fog(db, fog2)
    env2_id = DatabaseWriter.write_environment(db, EnvironmentEntity(fog_id=fog2_id))
    _add_view_with_image(db, db_dir, scene_id, camera_id, env2_id, "c.png")

    groups = DatabaseCleanup(db).find_duplicate_views()
    assert len(groups) == 1
    assert groups[0].keeper_id == v1
    assert groups[0].duplicate_ids == [v2]


def test_remove_duplicate_views_keeps_earliest_and_deletes_files(tmp_path):
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    scene_id, camera_id, _, env_id = _seed_scene_camera_fog_env(db)

    v1 = _add_view_with_image(db, db_dir, scene_id, camera_id, env_id, "keeper.png")
    v2 = _add_view_with_image(db, db_dir, scene_id, camera_id, env_id, "dup.png")
    db_path = db_dir / "database.sqlite3"

    images_dir = db_dir / "images"
    assert (images_dir / "keeper.png").exists()
    assert (images_dir / "dup.png").exists()

    report = DatabaseCleanup(db).remove_duplicate_views()
    assert report.duplicate_views_removed == 1
    assert report.images_removed == 1
    assert report.files_deleted == 1

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM view")
        remaining = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT fileName FROM image")
        remaining_files = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT id FROM view WHERE id = ?", (v2,))
        assert cur.fetchone() is None

    assert remaining == [v1]
    assert remaining_files == ["keeper.png"]
    assert (images_dir / "keeper.png").exists()
    assert not (images_dir / "dup.png").exists()


def test_remove_duplicate_views_leaves_distinct_views_untouched(tmp_path):
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    scene_id, camera_id, _, env_id = _seed_scene_camera_fog_env(db)

    v1 = _add_view_with_image(db, db_dir, scene_id, camera_id, env_id, "a.png")

    # Different env -> distinct view.
    fog_type2 = FogTypeEntity(name="linear")
    ft2_id = DatabaseWriter.write_fog_type(db, fog_type2)
    fog2 = FogEntity(
        scene_id=scene_id,
        fog_type_id=ft2_id,
        exp_fog_density=0.1,
        linear_near_distance=0.0,
        linear_far_distance=100.0,
        marched_cutoff=None,
        marched_color_transparency_cutoff=0.0,
        marched_distance_transparency_cutoff=0.0,
        marched_light_extinction_scale=0.0,
        marched_default_density=0.0,
        marched_density_multiplier=1.0,
        marched_lightDirG=0.0,
        marched_sigmaAbsorption=0.0,
        marched_sigmaScattering=0.0,
        marched_stepSizeDist=1.0,
        marched_stepSizeDist_light=1.0,
    )
    fog2_id = DatabaseWriter.write_fog(db, fog2)
    env2_id = DatabaseWriter.write_environment(db, EnvironmentEntity(fog_id=fog2_id))
    v2 = _add_view_with_image(db, db_dir, scene_id, camera_id, env2_id, "b.png")
    db_path = db_dir / "database.sqlite3"

    report = DatabaseCleanup(db).remove_duplicate_views()
    assert report.duplicate_views_removed == 0

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM view")
        remaining = sorted(r[0] for r in cur.fetchall())

    assert remaining == sorted([v1, v2])
    assert (db_dir / "images" / "a.png").exists()
    assert (db_dir / "images" / "b.png").exists()


def test_sweep_orphaned_image_rows_and_files(tmp_path):
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    images_dir = db_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    (images_dir / "orphan.png").write_bytes(b"fake")

    # Image row with no view or view_image referencing it.
    DatabaseWriter.write_image(
        db,
        ImageEntity(file_name="orphan.png", file_type="color", width=10, height=10),
    )

    report = DatabaseCleanup(db).sweep_orphaned_images()
    assert report.images_removed == 1
    assert report.files_deleted == 1
    assert not (images_dir / "orphan.png").exists()

    with sqlite3.connect(db_dir / "database.sqlite3") as conn:
        assert conn.execute("SELECT count(*) FROM image").fetchone()[0] == 0


def test_sweep_orphaned_parent_records(tmp_path):
    """An environment + fog + fog_type with no view referencing them should
    be swept by the parent cleanup."""
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    scene_id, _, _, _ = _seed_scene_camera_fog_env(db)
    db_path = db_dir / "database.sqlite3"

    # The seed created 1 scene, 1 camera, 1 fog_type, 1 fog, 1 environment.
    # No views reference them, so all should be swept.
    report = DatabaseCleanup(db).remove_duplicate_views()
    assert report.duplicate_views_removed == 0
    assert report.environments_removed == 1
    assert report.fog_removed == 1
    assert report.fog_types_removed == 1
    assert report.cameras_removed == 1
    assert report.scenes_removed == 1
    # coordinate is referenced by scene.upperRight/lowerLeft/center; once the
    # scene is gone the coordinate becomes orphaned and is swept too.
    assert report.coordinates_removed == 1

    with sqlite3.connect(db_path) as conn:
        for table in ("scene", "camera", "fog", "fog_type", "environment", "coordinate"):
            assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0


def test_remove_duplicate_views_with_masks_and_visibility_distances(tmp_path):
    """A duplicate view's masks and visibility_distance rows must be deleted
    along with the view, and the mask image files removed from disk."""
    db_dir = tmp_path / "db"
    db = _init_db(db_dir)
    scene_id, camera_id, _, env_id = _seed_scene_camera_fog_env(db)
    db_path = db_dir / "database.sqlite3"

    def _add_full_view(file_name: str, mask_name: str) -> int:
        color = ImageEntity(file_name=file_name, file_type="color", width=10, height=10)
        mask = ImageEntity(file_name=mask_name, file_type="ray_distance", width=10, height=10)
        color_id = DatabaseWriter.write_image(db, color)
        mask_id = DatabaseWriter.write_image(db, mask)
        images_dir = db_dir / "images"
        images_dir.mkdir(parents=True, exist_ok=True)
        (images_dir / file_name).write_bytes(b"fake")
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
        DatabaseWriter.write_visibility_distance(
            db,
            VisibilityDistanceEntity(view_id=view_id, distance_type="simple", value=1000.0),
        )
        return view_id

    v1 = _add_full_view("keep_color.png", "keep_mask.png")
    v2 = _add_full_view("dup_color.png", "dup_mask.png")

    report = DatabaseCleanup(db).remove_duplicate_views()
    assert report.duplicate_views_removed == 1
    assert report.images_removed == 2
    assert report.files_deleted == 2

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM view")
        assert [r[0] for r in cur.fetchall()] == [v1]
        cur.execute("SELECT id FROM view WHERE id = ?", (v2,))
        assert cur.fetchone() is None
        cur.execute("SELECT viewID FROM view_image")
        assert [r[0] for r in cur.fetchall()] == [v1]
        cur.execute("SELECT viewID FROM visibility_distance")
        assert [r[0] for r in cur.fetchall()] == [v1]
        cur.execute("SELECT count(*) FROM view_image")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM visibility_distance")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT fileName FROM image")
        files = sorted(r[0] for r in cur.fetchall())

    assert files == ["keep_color.png", "keep_mask.png"]
    images_dir = db_dir / "images"
    assert (images_dir / "keep_color.png").exists()
    assert (images_dir / "keep_mask.png").exists()
    assert not (images_dir / "dup_color.png").exists()
    assert not (images_dir / "dup_mask.png").exists()