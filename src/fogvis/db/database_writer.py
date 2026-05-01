import sqlite3
import os

from typing import Optional

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

    def write_coordinate(self, coordinate: CoordinateEntity) -> int | None:
        """Insert a coordinate record and return its row ID."""
        with self.db as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO coordinate (lat, lon) VALUES (?, ?)",
                (coordinate.lat.value, coordinate.lon.value),
            )
            return cur.lastrowid

    def write_scene(self, scene: SceneEntity) -> int | None:
        """Insert a scene record and return its row ID."""
        with self.db as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO scene (name, upperRightPositionID, lowerLeftPositionID, centerPositionID) VALUES (?, ?, ?, ?)",
                (
                    scene.name,
                    scene.upper_right_id,
                    scene.lower_left_id,
                    scene.center_id,
                ),
            )
            return cur.lastrowid

    def write_camera(self, cam: CameraEntity) -> int | None:
        """Insert a camera record and return its row ID."""
        with self.db as conn:
            cur = conn.cursor()
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

    def write_fog_type(self, ft: FogTypeEntity) -> int | None:
        """Insert a fog type record and return its row ID."""
        with self.db as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO fog_type (name) VALUES (?)",
                (ft.name,),
            )
            return cur.lastrowid

    def write_fog(self, fog: FogEntity) -> int | None:
        """Insert a fog record and return its row ID."""
        with self.db as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO fog (
                    typeID,
                    sceneID,
                    expFogDensity,
                    lienarNearDistance,
                    lienarFarDistance,
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
                    fog.marched_cutoff,
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

    def write_light_type(self, lt: LightTypeEntity) -> int | None:
        """Insert a light type record and return its row ID."""
        with self.db as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO light_type (name) VALUES (?)",
                (lt.name,),
            )
            return cur.lastrowid

    def write_light(self, light: LightEntity) -> int | None:
        """Insert a light record and return its row ID."""
        with self.db as conn:
            cur = conn.cursor()
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

    def write_environment(self, environment: EnvironmentEntity) -> int | None:
        """Insert an environment record and return its row ID."""
        with self.db as conn:
            cur = conn.cursor()
            cur.execute(f"""
                INSERT INTO environment (fogID)
                VALUES ({environment.fog_id})
                """)
            return cur.lastrowid

    def write_environment_light(
        self, environment_light: EnvironmentLightEntity
    ) -> int | None:
        """Insert an environment–light association and return its row ID."""
        with self.db as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO environment_light (environmentID, lightID)
                VALUES (?, ?)
                """,
                (environment_light.environment_id, environment_light.light_id),
            )
            return cur.lastrowid

    def write_image(self, image: ImageEntity) -> int | None:
        """Insert an image record and return its row ID."""
        with self.db as conn:
            cur = conn.cursor()
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
        scene: SceneEntity,
        coordinate: CoordinateEntity,
        camera: CameraEntity,
        fog: Optional[FogEntity] = None,
        fog_type: Optional[FogTypeEntity] = None,
        light: Optional[LightEntity] = None,
        light_type: Optional[LightTypeEntity] = None,
        environment: Optional[EnvironmentEntity] = None,
        images: Optional[list[ImageEntity]] = None,
    ) -> dict[str, int]:
        ids: dict[str, int | None] = {}

        # 1. ScenePosition
        scene.upper_right_id = self.write_coordinate(coordinate) or 0
        scene.lower_left_id = self.write_coordinate(coordinate) or 0
        scene.center_id = self.write_coordinate(coordinate) or 0

        # 1. Scene
        ids["scene"] = self.write_scene(scene)

        # 2. Camera position + camera
        ids["camera_position"] = self.write_coordinate(coordinate)
        camera.scene_id = ids["scene"]
        camera.position_id = ids["camera_position"]
        ids["camera"] = self.write_camera(camera)

        # 3. Optional fog
        if fog is not None:
            fog.scene_id = ids["scene"] or 0
            fog.fog_type_id = self.write_fog_type(fog_type) or 0
            ids["fog"] = self.write_fog(fog)

        # 4. Optional lights (each with its own position)
        if light:
            light.type_id = self.write_light_type(light_type) or 0
            ids["light"] = self.write_light(light)

        # 5. Optional environment + environment–light links
        if environment is not None:
            environment.fog_id = ids["fog"]
            env_id = self.write_environment(environment)
            ids["environment"] = env_id

            light_id = ids["light"]
            self.write_environment_light(
                EnvironmentLightEntity(environment_id=env_id, light_id=light_id)
            )

        # 6. Optional images
        if images:
            ids["images"] = []
            for image in images:
                image.scene_id = ids["scene"]
                image.camera_id = ids["camera"]
                image.environment_id = ids["environment"]
                ids["images"].append(self.write_image(image))

        return ids
