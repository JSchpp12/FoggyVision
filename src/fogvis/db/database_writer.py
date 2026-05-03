import sqlite3
import os
from contextlib import closing
from typing import Optional
from sqlite3 import Cursor
from .database import Database
from .entities import (
    CoordinateEntity,
    SceneEntity,
    CameraEntity,
    FogTypeEntity,
    FogEntity,
    LightTypeEntity,
    LightEntity,
    EnvironmentEntity,
    EnvironmentLightEntity,
    ImageEntity,
)


class DatabaseWriter:
    def __init__(self, db_path: os.PathLike) -> None:
        self.db: Database = Database(db_path)
        self.cur: Optional[Cursor] = None

    def __enter__(self):
        self.cur = self.db.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur is not None:
            self.cur.close()
            self.cur = None

    @staticmethod
    def write_coordinate(db: Database, coordinate: CoordinateEntity) -> int:
        """Insert a coordinate record and return its row ID."""
        with closing(db.get_connection().cursor()) as cur:
            cmd = f"INSERT INTO coordinate (lat, lon) VALUES (?, ?)"
            cur.execute(
                cmd,
                (coordinate.lat.value, coordinate.lon.value),
            )
            if cur.lastrowid is None:
                raise Exception("Failed to retreive newly inserted row record")

            return cur.lastrowid

    @staticmethod
    def write_scene(db: Database, scene: SceneEntity) -> int | None:
        """Insert a scene record and return its row ID."""
        with closing(db.get_connection().cursor()) as cur:
            cur.execute(
                "INSERT INTO scene (name, coverageDistanceMiles, upperRightPositionID, lowerLeftPositionID, centerPositionID) VALUES (?, ?, ?, ?, ?)",
                (
                    scene.name,
                    scene.coverage_distance_miles,
                    scene.upper_right_id,
                    scene.lower_left_id,
                    scene.center_id,
                ),
            )
            return cur.lastrowid

    @staticmethod
    def write_camera(db: Database, cam: CameraEntity) -> int | None:
        """Insert a camera record and return its row ID."""
        with closing(db.get_connection().cursor()) as cur:
            cur.execute(
                """
                INSERT INTO camera (virtualPosition, sceneID, lookDir, fov, nearClip, farClip)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    cam.virtual_position.to_json(),
                    cam.scene_id,
                    cam.look_dir.to_json(),
                    cam.fov,
                    cam.near_clip,
                    cam.far_clip,
                ),
            )
            return cur.lastrowid

    @staticmethod
    def write_fog_type(db: Database, ft: FogTypeEntity) -> int | None:
        """Insert a fog type record and return its row ID."""
        with closing(db.get_connection().cursor()) as cur:
            cur.execute(
                "INSERT INTO fog_type (name) VALUES (?)",
                (ft.name,),
            )
            return cur.lastrowid

    @staticmethod
    def write_fog(db: Database, fog: FogEntity) -> int | None:
        """Insert a fog record and return its row ID."""
        with closing(db.get_connection().cursor()) as cur:
            if fog.marched_cutoff is not None:
                cur.execute(
                    """
                    INSERT INTO fog (
                        typeID,
                        sceneID,
                        expFogDensity,
                        linearNearDistance,
                        linearFarDistance,
                        marchedCutoff,
                        marchedDefaultDensity,
                        marchedDensityMultiplier,
                        marchedLightG,
                        marchedSigmaAbsorption,
                        marchedSigmaScattering,
                        marchedStepSizeDist,
                        marchedStepSizeDistLight
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        fog.fog_type_id,
                        fog.scene_id,
                        fog.exp_fog_density,
                        fog.linear_near_distance,
                        fog.linear_far_distance,
                        fog.marched_cutoff or "NULL",
                        fog.marched_default_density,
                        fog.marched_density_multiplier,
                        fog.marched_lightDirG,
                        fog.marched_sigmaAbsorption,
                        fog.marched_sigmaScattering,
                        fog.marched_stepSizeDist,
                        fog.marched_stepSizeDist_light,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO fog (
                        typeID,
                        sceneID,
                        expFogDensity,
                        linearNearDistance,
                        linearFarDistance,
                        marchedDefaultDensity,
                        marchedDensityMultiplier,
                        marchedLightG,
                        marchedSigmaAbsorption,
                        marchedSigmaScattering,
                        marchedStepSizeDist,
                        marchedStepSizeDistLight
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        fog.fog_type_id,
                        fog.scene_id,
                        fog.exp_fog_density,
                        fog.linear_near_distance,
                        fog.linear_far_distance,
                        fog.marched_default_density,
                        fog.marched_density_multiplier,
                        fog.marched_lightDirG,
                        fog.marched_sigmaAbsorption,
                        fog.marched_sigmaScattering,
                        fog.marched_stepSizeDist,
                        fog.marched_stepSizeDist_light,
                    ),
                )
            return cur.lastrowid

    @staticmethod
    def write_light_type(db: Database, lt: LightTypeEntity) -> int | None:
        """Insert a light type record and return its row ID."""
        with closing(db.get_connection().cursor()) as cur:
            cur.execute(
                "INSERT INTO light_type (name) VALUES (?)",
                (lt.name,),
            )
            return cur.lastrowid

    @staticmethod
    def write_light(db: Database, light: LightEntity) -> int | None:
        """Insert a light record and return its row ID."""
        with closing(db.get_connection().cursor()) as cur:
            cur.execute(
                """
                INSERT INTO light (
                    ambient,
                    diffuse,
                    specular,
                    virtualDirection,
                    virtualPosition,
                    enabled,
                    innerDiameter,
                    outerDiameter,
                    luminance,
                    typeID
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    light.ambient.to_json(),
                    light.diffuse.to_json(),
                    light.specular.to_json(),
                    light.virtualDirection.to_json(),
                    light.virtualPosition.to_json(),
                    int(light.enabled),
                    light.innerDiameter,
                    light.outerDiameter,
                    light.luminance,
                    light.type_id,
                ),
            )
            return cur.lastrowid

    @staticmethod
    def write_environment(db: Database, environment: EnvironmentEntity) -> int | None:
        """Insert an environment record and return its row ID."""
        with closing(db.get_connection().cursor()) as cur:
            cur.execute(f"""
                INSERT INTO environment (fogID)
                VALUES ({environment.fog_id})
                """)
            return cur.lastrowid

    @staticmethod
    def write_environment_light(
        db: Database, environment_light: EnvironmentLightEntity
    ) -> int | None:
        """Insert an environment–light association and return its row ID."""
        with closing(db.get_connection().cursor()) as cur:
            cur.execute(
                """
                INSERT INTO environment_light (environmentID, lightID)
                VALUES (?, ?)
                """,
                (environment_light.environment_id, environment_light.light_id),
            )
            return cur.lastrowid

    @staticmethod
    def write_image(db: Database, image: ImageEntity) -> int | None:
        """Insert an image record and return its row ID."""
        with closing(db.get_connection().cursor()) as cur:
            cur.execute(
                """
                INSERT INTO image (filePath, visibilityDistance, cameraID, sceneID, environmentID)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    image.file_path,
                    image.visibility_distance,
                    image.camera_id,
                    image.scene_id,
                    image.environment_id,
                ),
            )
            return cur.lastrowid

    def write_full_scene(
        self,
        scene_name: str,
        scene_center: CoordinateEntity,
        scene_vis_range: int,
        camera: CameraEntity,
        fog: Optional[FogEntity] = None,
        fog_type: Optional[FogTypeEntity] = None,
        light: Optional[LightEntity] = None,
        light_type: Optional[LightTypeEntity] = None,
        environment: Optional[EnvironmentEntity] = None,
        images: Optional[list[ImageEntity]] = None,
    ) -> dict[str, int]:
        ids: dict[str, int | None] = {}

        with closing(self.db.get_connection().cursor()) as cur:
            # 1. Scene
            center_coord: int = 0
            if scene_center.get_does_exist(self.db):
                center_coord = scene_center.get_record_id(self.db)
            else:
                center_coord = DatabaseWriter.write_coordinate(self.db, scene_center)

            scene: SceneEntity = SceneEntity(
                name=scene_name,
                coverage_distance_miles=scene_vis_range,
                upper_right_id=center_coord,
                lower_left_id=center_coord,
                center_id=center_coord,
            )

            if not scene.get_does_exist(self.db):
                ids["scene"] = DatabaseWriter.write_scene(self.db, scene)
            else:
                ids["scene"] = scene.get_record_id(self.db)

            # 2. Camera position + camera
            ids["camera_position"] = center_coord
            if camera.virtual_position.x != 0 or camera.virtual_position.z != 0:
                raise Exception(
                    "Assuming that the camera is located at the terrain center"
                )

            camera.scene_id = ids["scene"]
            camera.position_id = ids["camera_position"]
            if camera.get_does_exist(self.db):
                camera_id = camera.get_record_id(self.db)
            else:
                camera_id = DatabaseWriter.write_camera(self.db, camera)
            ids["camera"] = camera_id

            if fog is not None:
                fog.scene_id = ids["scene"] or 0
                fog.fog_type_id = 0
                if fog_type.get_does_exist(self.db):
                    fog.fog_type_id = fog_type.get_record_id(self.db) or 0
                else:
                    fog.fog_type_id = (
                        DatabaseWriter.write_fog_type(self.db, fog_type) or 0
                    )

                if fog.get_does_exist(self.db):
                    ids["fog"] = fog.get_record_id(self.db)
                else:
                    ids["fog"] = DatabaseWriter.write_fog(self.db, fog)

            if light:
                light.type_id = (
                    DatabaseWriter.write_light_type(self.db, light_type) or 0
                )
                ids["light"] = DatabaseWriter.write_light(self.db, light)

            if environment is not None:
                environment.fog_id = ids["fog"]
                if environment.get_does_exist(self.db):
                    env_id = environment.get_record_id(self.db)
                else:
                    env_id = DatabaseWriter.write_environment(self.db, environment)
                ids["environment"] = env_id

                if "light" in ids:
                    light_id = ids["light"]
                    DatabaseWriter.write_environment_light(
                        self.db,
                        EnvironmentLightEntity(
                            environment_id=env_id, light_id=light_id
                        ),
                    )

            if images:
                ids["images"] = []
                for image in images:
                    if not image.get_does_exist(self.db):
                        image.scene_id = ids["scene"]
                        image.camera_id = ids["camera"]
                        image.environment_id = ids["environment"]
                        ids["images"].append(DatabaseWriter.write_image(self.db, image))

        return ids
