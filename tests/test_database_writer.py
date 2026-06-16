import sqlite3

import pytest

from fogvis.common import Latitude, Longitude, VectorContainer3D, VectorContainer2D
from fogvis.db import DatabaseWriter, ImageImporter
from fogvis.db.database import Database
from fogvis.db.entities import (
    CoordinateEntity,
    SceneEntity,
    CameraEntity,
    FogEntity,
    FogTypeEntity,
    LightEntity,
    LightTypeEntity,
    EnvironmentEntity,
    EnvironmentLightEntity,
    ImageEntity,
    ViewEntity,
    ViewImageEntity,
    VisibilityDistanceEntity,
    DistanceMetricsEntity,
)


def _init_db(db_dir):
    database = Database(db_dir)
    database.init_tables()
    return database


def test_write_scene_includes_terrain(tmp_path):
    db_dir = tmp_path / "test_db"
    db = _init_db(db_dir)
    db_path = db_dir / "database.sqlite3"
    writer = DatabaseWriter(db)

    coord = CoordinateEntity(lat=Latitude("39.0"), lon=Longitude("-105.0"))
    scene = SceneEntity(
        name="berthoud_pass_co",
        coverage_distance_miles=10,
        upper_right_id=DatabaseWriter.write_coordinate(db, coord),
        lower_left_id=DatabaseWriter.write_coordinate(db, coord),
        center_id=DatabaseWriter.write_coordinate(db, coord),
        terrain_rendering_type="real",
        terrain_shape_center='{"x": 39.794502, "y": -105.76389}',
    )
    scene_id = DatabaseWriter.write_scene(db, scene)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT name, coverageDistanceMiles, terrainRenderingType, terrainShapeCenter FROM scene WHERE id = ?",
            (scene_id,),
        )
        row = cur.fetchone()

    assert row[0] == "berthoud_pass_co"
    assert row[1] == 10
    assert row[2] == "real"
    assert "39.794502" in row[3]


def test_write_fog_includes_volume_info(tmp_path):
    db_dir = tmp_path / "test_db"
    db = _init_db(db_dir)
    db_path = db_dir / "database.sqlite3"

    coord = CoordinateEntity(lat=Latitude("39.0"), lon=Longitude("-105.0"))
    scene = SceneEntity(
        name="test_scene",
        coverage_distance_miles=10,
        upper_right_id=DatabaseWriter.write_coordinate(db, coord),
        lower_left_id=DatabaseWriter.write_coordinate(db, coord),
        center_id=DatabaseWriter.write_coordinate(db, coord),
        terrain_rendering_type="real",
    )
    scene_id = DatabaseWriter.write_scene(db, scene)

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
        volume_name="clouds",
        volume_position=VectorContainer3D(5620.0, -3372.5, -20730.0),
        volume_rotation=VectorContainer3D(90.0, 0.0, 180.0),
        volume_scale=VectorContainer3D(201.0, 200.0, 201.0),
    )
    fog_id = DatabaseWriter.write_fog(db, fog)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT volumeName, volumePosition, volumeRotation, volumeScale, marchedColorTransparencyCutoff FROM fog WHERE id = ?",
            (fog_id,),
        )
        row = cur.fetchone()

    assert row[0] == "clouds"
    assert "5620.0" in row[1]
    assert row[4] == pytest.approx(0.01)


def test_write_image_includes_file_metadata(tmp_path):
    db_dir = tmp_path / "test_db"
    db = _init_db(db_dir)
    db_path = db_dir / "database.sqlite3"

    image = ImageEntity(
        file_name="Frame-0.png",
        file_type="color",
        width=1920,
        height=1080,
    )
    image_id = DatabaseWriter.write_image(db, image)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT fileName, fileType, width, height FROM image WHERE id = ?", (image_id,))
        row = cur.fetchone()

    assert row[0] == "Frame-0.png"
    assert row[1] == "color"
    assert row[2] == 1920
    assert row[3] == 1080


def test_write_view_links_image_camera_scene_environment(tmp_path):
    db_dir = tmp_path / "test_db"
    db = _init_db(db_dir)
    db_path = db_dir / "database.sqlite3"

    coord = CoordinateEntity(lat=Latitude("39.0"), lon=Longitude("-105.0"))
    scene = SceneEntity(
        name="test",
        coverage_distance_miles=10,
        upper_right_id=DatabaseWriter.write_coordinate(db, coord),
        lower_left_id=DatabaseWriter.write_coordinate(db, coord),
        center_id=DatabaseWriter.write_coordinate(db, coord),
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
    environment_id = DatabaseWriter.write_environment(db, environment)

    image = ImageEntity(
        file_name="Frame-0.png",
        file_type="color",
        width=1920,
        height=1080,
    )
    image_id = DatabaseWriter.write_image(db, image)

    view = ViewEntity(
        color_image_id=image_id,
        camera_id=camera_id,
        scene_id=scene_id,
        environment_id=environment_id,
    )
    view_id = DatabaseWriter.write_view(db, view)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT colorImageID, cameraID, sceneID, environmentID FROM view WHERE id = ?", (view_id,))
        row = cur.fetchone()

    assert row[0] == image_id
    assert row[1] == camera_id
    assert row[2] == scene_id
    assert row[3] == environment_id


def test_write_visibility_distance(tmp_path):
    db_dir = tmp_path / "test_db"
    db = _init_db(db_dir)
    db_path = db_dir / "database.sqlite3"

    coord = CoordinateEntity(lat=Latitude("39.0"), lon=Longitude("-105.0"))
    scene = SceneEntity(
        name="test",
        coverage_distance_miles=10,
        upper_right_id=DatabaseWriter.write_coordinate(db, coord),
        lower_left_id=DatabaseWriter.write_coordinate(db, coord),
        center_id=DatabaseWriter.write_coordinate(db, coord),
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
    environment_id = DatabaseWriter.write_environment(db, environment)

    image = ImageEntity(
        file_name="Frame-0.png",
        file_type="color",
        width=1920,
        height=1080,
    )
    image_id = DatabaseWriter.write_image(db, image)

    view = ViewEntity(
        color_image_id=image_id,
        camera_id=camera_id,
        scene_id=scene_id,
        environment_id=environment_id,
    )
    view_id = DatabaseWriter.write_view(db, view)

    visibility_distance = VisibilityDistanceEntity(
        view_id=view_id,
        distance_type="simple",
        value=1000.0,
    )
    visibility_distance_id = DatabaseWriter.write_visibility_distance(db, visibility_distance)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT viewID, distanceType, value FROM visibility_distance WHERE id = ?",
            (visibility_distance_id,),
        )
        row = cur.fetchone()

    assert row[0] == view_id
    assert row[1] == "simple"
    assert row[2] == pytest.approx(1000.0)


def test_write_scene_reuses_existing_matching_scene(tmp_path):
    db_dir = tmp_path / "test_db"
    db = _init_db(db_dir)
    db_path = db_dir / "database.sqlite3"

    coord = CoordinateEntity(lat=Latitude("39.0"), lon=Longitude("-105.0"))
    scene = SceneEntity(
        name="berthoud_pass_co",
        coverage_distance_miles=10,
        upper_right_id=DatabaseWriter.write_coordinate(db, coord),
        lower_left_id=DatabaseWriter.write_coordinate(db, coord),
        center_id=DatabaseWriter.write_coordinate(db, coord),
        terrain_rendering_type="real",
        terrain_shape_center='{"x": 39.794502, "y": -105.76389}',
    )
    first_id = DatabaseWriter.write_scene(db, scene)
    second_id = DatabaseWriter.write_scene(db, scene)

    assert first_id == second_id

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM scene")
        assert cur.fetchone()[0] == 1


def test_write_camera_reuses_existing_matching_camera(tmp_path):
    db_dir = tmp_path / "test_db"
    db = _init_db(db_dir)
    db_path = db_dir / "database.sqlite3"

    coord = CoordinateEntity(lat=Latitude("39.0"), lon=Longitude("-105.0"))
    scene = SceneEntity(
        name="test",
        coverage_distance_miles=10,
        upper_right_id=DatabaseWriter.write_coordinate(db, coord),
        lower_left_id=DatabaseWriter.write_coordinate(db, coord),
        center_id=DatabaseWriter.write_coordinate(db, coord),
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
    first_id = DatabaseWriter.write_camera(db, camera)
    second_id = DatabaseWriter.write_camera(db, camera)

    assert first_id == second_id

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM camera")
        assert cur.fetchone()[0] == 1


def test_write_full_scene_end_to_end(tmp_path):
    db_dir = tmp_path / "test_db"
    db = _init_db(db_dir)
    db_path = db_dir / "database.sqlite3"
    writer = DatabaseWriter(db)

    with db:
        writer.write_full_scene(
            scene_name="berthoud_pass_co",
            scene_center=CoordinateEntity(lat=Latitude("39.794502"), lon=Longitude("-105.76389")),
            scene_vis_range=10,
            scene_rendering_type="real",
            camera=CameraEntity(
                scene_id=0,
                virtual_position=VectorContainer3D(0.0, 15.0, 0.0),
                look_dir=VectorContainer3D(0.0, 0.0, -1.0),
                fov=60.0,
                near_clip=0.1,
                far_clip=1000.0,
            ),
            fog=FogEntity(
                scene_id=0,
                fog_type_id=0,
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
                volume_name="clouds",
                volume_position=VectorContainer3D(0.0, 0.0, 0.0),
                volume_rotation=VectorContainer3D(0.0, 0.0, 0.0),
                volume_scale=VectorContainer3D(1.0, 1.0, 1.0),
            ),
            fog_type=FogTypeEntity(name="marched"),
            light=LightEntity(
                ambient=VectorContainer3D(1.0, 1.0, 1.0),
                diffuse=VectorContainer3D(0.5, 0.5, 0.5),
                specular=VectorContainer3D(0.5, 0.5, 0.5),
                virtualDirection=VectorContainer3D(0.0, -1.0, 0.0),
                virtualPosition=VectorContainer3D(0.0, 4.0, 0.0),
                enabled=True,
                innerDiameter=0.0,
                outerDiameter=1.0,
                luminance=50.0,
                type_id=1,
            ),
            light_type=LightTypeEntity(name="Directional"),
            environment=EnvironmentEntity(fog_id=0),
            images=[
                ImageEntity(
                    file_name="Frame-0.png",
                    file_type="color",
                    width=1920,
                    height=1080,
                )
            ],
            views=[
                ViewEntity(
                    color_image_id=0,
                    camera_id=0,
                    scene_id=0,
                    environment_id=0,
                )
            ],
            view_images=[
                ViewImageEntity(
                    view_id=0,
                    image_id=0,
                    role="ray_distance",
                )
            ],
            visibility_distances=[
                VisibilityDistanceEntity(
                    view_id=0,
                    distance_type="simple",
                    value=1000.0,
                )
            ],
            terrain_shape_center='{"x": 39.794502, "y": -105.76389}',
        )

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM image")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM view")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM view_image")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM visibility_distance")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM scene")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT lat, lon FROM coordinate")
        assert cur.fetchone() == pytest.approx((39.794502, -105.76389))
