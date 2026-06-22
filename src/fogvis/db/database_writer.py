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
    ViewEntity,
    ViewImageEntity,
    VisibilityDistanceEntity,
)


class DatabaseWriter:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.cur: Optional[Cursor] = None

    def __enter__(self):
        self.cur = self.db.get_connection().cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur is not None:
            self.cur.close()
            self.db.close_connection()
            self.cur = None

    @staticmethod
    def write_coordinate(db: Database, coordinate: CoordinateEntity) -> int:
        """Insert a coordinate record if it does not already exist, and return its row ID."""
        with db as con:
            cur = con.cursor()
            if coordinate.get_does_exist(db):
                return coordinate.get_record_id(db)

            cur.execute(
                "INSERT INTO coordinate (lat, lon) VALUES (?, ?)",
                (coordinate.lat.value, coordinate.lon.value),
            )
            if cur.lastrowid is None:
                raise Exception("Failed to retreive newly inserted row record")

            return cur.lastrowid

    @staticmethod
    def write_scene(db: Database, scene: SceneEntity) -> int:
        """Insert a scene record if it does not already exist, and return its row ID."""
        with db as con:
            cur = con.cursor()
            if scene.get_does_exist(db):
                existing_id = scene.get_record_id(db)
                if existing_id is None:
                    raise Exception("Failed to retrieve existing scene id")
                return existing_id

            cur.execute(
                """
                INSERT INTO scene (
                    name,
                    coverageDistanceMiles,
                    upperRightPositionID,
                    lowerLeftPositionID,
                    centerPositionID,
                    terrainRenderingType,
                    terrainShapeCenter
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    scene.name,
                    scene.coverage_distance_miles,
                    scene.upper_right_id,
                    scene.lower_left_id,
                    scene.center_id,
                    scene.terrain_rendering_type,
                    scene.terrain_shape_center,
                ),
            )
            if cur.lastrowid is None:
                raise Exception("Failed to retrieve newly inserted scene row ID")
            return cur.lastrowid

    @staticmethod
    def write_camera(db: Database, cam: CameraEntity) -> int:
        """Insert a camera record if it does not already exist, and return its row ID."""
        with db as con:
            cur = con.cursor()
            if cam.get_does_exist(db):
                camera_id = cam.get_record_id(db)
                if camera_id is None:
                    raise Exception("Failed to retrieve existing camera id")
                return camera_id

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
            if cur.lastrowid is None:
                raise Exception("Failed to retrieve newly inserted camera row ID")
            return cur.lastrowid

    @staticmethod
    def write_fog_type(db: Database, ft: FogTypeEntity) -> int:
        """Insert a fog type record if it does not already exist, and return its row ID."""
        with db as con:
            cur = con.cursor()
            if ft.get_does_exist(db):
                fog_type_id = ft.get_record_id(db)
                if fog_type_id is None:
                    raise Exception("Failed to retrieve existing fog_type id")
                return fog_type_id

            cur.execute(
                "INSERT INTO fog_type (name) VALUES (?)",
                (ft.name,),
            )
            if cur.lastrowid is None:
                raise Exception("Failed to retrieve newly inserted fog_type row ID")
            return cur.lastrowid

    @staticmethod
    def write_fog(db: Database, fog: FogEntity) -> int:
        """Insert a fog record if it does not already exist, and return its row ID."""
        with db as con:
            cur = con.cursor()
            if fog.get_does_exist(db):
                return fog.get_record_id(db)

            columns = [
                "typeID",
                "sceneID",
                "expFogDensity",
                "linearNearDistance",
                "linearFarDistance",
                "marchedDefaultDensity",
                "marchedDensityMultiplier",
                "marchedLightG",
                "marchedSigmaAbsorption",
                "marchedSigmaScattering",
                "marchedStepSizeDist",
                "marchedStepSizeDistLight",
            ]
            values = [
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
            ]

            for value, column in (
                (fog.marched_cutoff, "marchedCutoff"),
                (fog.marched_color_transparency_cutoff, "marchedColorTransparencyCutoff"),
                (fog.marched_distance_transparency_cutoff, "marchedDistanceTransparencyCutoff"),
                (fog.marched_light_extinction_scale, "marchedLightExtinctionScale"),
                (fog.volume_name, "volumeName"),
            ):
                if value is not None:
                    columns.append(column)
                    values.append(value)

            for vector, column in (
                (fog.volume_position, "volumePosition"),
                (fog.volume_rotation, "volumeRotation"),
                (fog.volume_scale, "volumeScale"),
            ):
                if vector is not None:
                    columns.append(column)
                    values.append(vector.to_json())

            placeholders = ", ".join(["?"] * len(values))
            col_sql = ", ".join(columns)

            cur.execute(
                f"INSERT INTO fog ({col_sql}) VALUES ({placeholders})",
                values,
            )
            if cur.lastrowid is None:
                raise Exception("Failed to retrieve newly inserted fog row ID")
            return cur.lastrowid

    @staticmethod
    def write_light_type(db: Database, lt: LightTypeEntity) -> int:
        """Insert a light type record if it does not already exist, and return its row ID."""
        with db as con:
            cur = con.cursor()
            if lt.get_does_exist(db):
                return lt.get_record_id(db)

            cur.execute(
                "INSERT INTO light_type (name) VALUES (?)",
                (lt.name,),
            )
            if cur.lastrowid is None:
                raise Exception("Failed to retrieve newly inserted light_type row ID")
            return cur.lastrowid

    @staticmethod
    def write_light(db: Database, light: LightEntity) -> int:
        """Insert a light record if it does not already exist, and return its row ID."""
        with db as con:
            cur = con.cursor()
            if light.get_does_exist(db):
                return light.get_record_id(db)

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
            if cur.lastrowid is None:
                raise Exception("Failed to retrieve newly inserted light row ID")
            return cur.lastrowid

    @staticmethod
    def write_environment(db: Database, environment: EnvironmentEntity) -> int:
        """Insert an environment record if it does not already exist, and return its row ID."""
        with db as con:
            cur = con.cursor()
            if environment.get_does_exist(db):
                return environment.get_record_id(db)

            cur.execute(
                "INSERT INTO environment (fogID) VALUES (?)",
                (environment.fog_id,),
            )
            if cur.lastrowid is None:
                raise Exception("Failed to retrieve newly inserted environment row ID")
            return cur.lastrowid

    @staticmethod
    def write_environment_light(
        db: Database, environment_light: EnvironmentLightEntity
    ) -> int:
        """Insert an environment–light association and return its row ID."""
        with db as con:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO environment_light (environmentID, lightID)
                VALUES (?, ?)
                """,
                (environment_light.environment_id, environment_light.light_id),
            )
            if cur.lastrowid is None:
                raise Exception(
                    "Failed to retrieve newly inserted environment_light row ID"
                )
            return cur.lastrowid

    @staticmethod
    def write_image(db: Database, image: ImageEntity) -> int:
        """Insert an image record if it does not already exist, and return its row ID."""
        with db as con:
            cur = con.cursor()
            if image.get_does_exist(db):
                return image.get_record_id(db)

            cur.execute(
                """
                INSERT INTO image (fileName, fileType, width, height, checksum)
                VALUES (?, ?, ?, ?, ?)
                """,
                image._params(),
            )
            if cur.lastrowid is None:
                raise Exception("Failed to retrieve newly inserted image row ID")
            return cur.lastrowid

    @staticmethod
    def write_view(db: Database, view: ViewEntity) -> int:
        """Insert a view record if it does not already exist, and return its row ID."""
        with db as con:
            cur = con.cursor()
            if view.get_does_exist(db):
                return view.get_record_id(db)

            cur.execute(
                """
                INSERT INTO view (colorImageID, cameraID, sceneID, environmentID)
                VALUES (?, ?, ?, ?)
                """,
                view._params(),
            )
            if cur.lastrowid is None:
                raise Exception("Failed to retrieve newly inserted view row ID")
            return cur.lastrowid

    @staticmethod
    def write_view_image(db: Database, view_image: ViewImageEntity) -> int:
        """Insert a view_image record if it does not already exist, and return its row ID."""
        with db as con:
            cur = con.cursor()
            if view_image.get_does_exist(db):
                return view_image.get_record_id(db)

            cur.execute(
                """
                INSERT INTO view_image (viewID, imageID, role)
                VALUES (?, ?, ?)
                """,
                (view_image.view_id, view_image.image_id, view_image.role),
            )
            if cur.lastrowid is None:
                raise Exception("Failed to retrieve newly inserted view_image row ID")
            return cur.lastrowid

    @staticmethod
    def write_visibility_distance(
        db: Database, visibility_distance: VisibilityDistanceEntity
    ) -> int:
        """Insert a visibility_distance record if it does not already exist, and return its row ID."""
        with db as con:
            cur = con.cursor()
            if visibility_distance.get_does_exist(db):
                return visibility_distance.get_record_id(db)

            cur.execute(
                """
                INSERT INTO visibility_distance (
                    viewID,
                    distanceType,
                    value,
                    average,
                    median,
                    minimum,
                    rayCount
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                visibility_distance._params(),
            )
            if cur.lastrowid is None:
                raise Exception(
                    "Failed to retrieve newly inserted visibility_distance row ID"
                )
            return cur.lastrowid

    def write_full_scene(
        self,
        scene_name: str,
        scene_center: CoordinateEntity,
        scene_vis_range: int,
        scene_rendering_type: str,
        camera: CameraEntity,
        fog: Optional[FogEntity] = None,
        fog_type: Optional[FogTypeEntity] = None,
        light: Optional[LightEntity] = None,
        light_type: Optional[LightTypeEntity] = None,
        environment: Optional[EnvironmentEntity] = None,
        images: Optional[list[ImageEntity]] = None,
        views: Optional[list[ViewEntity]] = None,
        view_images: Optional[list[ViewImageEntity]] = None,
        visibility_distances: Optional[list[VisibilityDistanceEntity]] = None,
        terrain_shape_center: str = "",
    ) -> dict[str, int]:
        ids: dict[str, int] = {}

        # 1. Scene
        center_coord: int = DatabaseWriter.write_coordinate(self.db, scene_center)

        scene: SceneEntity = SceneEntity(
            name=scene_name,
            coverage_distance_miles=scene_vis_range,
            upper_right_id=center_coord,
            lower_left_id=center_coord,
            center_id=center_coord,
            terrain_rendering_type=scene_rendering_type,
            terrain_shape_center=terrain_shape_center,
        )

        if scene.get_does_exist(self.db):
            existing_id = scene.get_record_id(self.db)
            if existing_id is None:
                raise Exception("Failed to retrieve existing scene id")
            ids["scene"] = existing_id
        else:
            ids["scene"] = DatabaseWriter.write_scene(self.db, scene)

        # 2. Camera position + camera
        if camera.virtual_position.x != 0 or camera.virtual_position.z != 0:
            raise Exception("Assuming that the camera is located at the terrain center")

        camera.scene_id = ids["scene"]
        if camera.get_does_exist(self.db):
            camera_id = camera.get_record_id(self.db)
            if camera_id is None:
                raise Exception("Failed to retrieve existing camera id")
            ids["camera"] = camera_id
        else:
            ids["camera"] = DatabaseWriter.write_camera(self.db, camera)

        if fog is not None:
            fog.scene_id = ids["scene"] or 0
            fog.fog_type_id = 0
            if fog_type is not None:
                if fog_type.get_does_exist(self.db):
                    fog_type_id = fog_type.get_record_id(self.db)
                    if fog_type_id is None:
                        raise Exception("Failed to retrieve existing fog_type id")
                    fog.fog_type_id = fog_type_id
                else:
                    fog.fog_type_id = DatabaseWriter.write_fog_type(self.db, fog_type)

            if fog.get_does_exist(self.db):
                ids["fog"] = fog.get_record_id(self.db)
            else:
                ids["fog"] = DatabaseWriter.write_fog(self.db, fog)

        if light:
            if light_type:
                if light_type.get_does_exist(self.db):
                    light_type_id = light_type.get_record_id(self.db)
                    if light_type_id is None:
                        raise Exception("Failed to retrieve existing light_type id")
                    light.type_id = light_type_id
                else:
                    light.type_id = DatabaseWriter.write_light_type(self.db, light_type)

            if light.get_does_exist(self.db):
                ids["light"] = light.get_record_id(self.db)
            else:
                ids["light"] = DatabaseWriter.write_light(self.db, light)

        if environment is not None:
            environment.fog_id = ids["fog"]
            # Ensure the environment's light set includes the light that was just written/reused.
            if "light" in ids:
                environment.light_ids.add(ids["light"])
            if environment.get_does_exist(self.db):
                env_id = environment.get_record_id(self.db)
            else:
                env_id = DatabaseWriter.write_environment(self.db, environment)
            ids["environment"] = env_id

            for light_id in environment.light_ids:
                env_light = EnvironmentLightEntity(environment_id=env_id, light_id=light_id)
                if not env_light.get_does_exist(self.db):
                    DatabaseWriter.write_environment_light(
                        self.db,
                        env_light,
                    )

        if images:
            ids["images"] = []
            for image in images:
                image_id = DatabaseWriter.write_image(self.db, image)
                ids["images"].append(image_id)

        if views:
            ids["views"] = []
            for view in views:
                view.scene_id = ids["scene"]
                view.camera_id = ids["camera"]
                if "environment" in ids:
                    view.environment_id = ids["environment"]
                if ids["images"]:
                    view.color_image_id = ids["images"][0]
                view_id = DatabaseWriter.write_view(self.db, view)
                ids["views"].append(view_id)

        if view_images:
            ids["view_images"] = []
            for view_image in view_images:
                if ids["views"]:
                    view_image.view_id = ids["views"][0]
                if view_image.image_id == 0 and ids["images"]:
                    # Map by role to the corresponding image id if available.
                    role_index = {
                        img.file_type: img_id
                        for img_id, img in zip(ids["images"], images or [])
                    }
                    view_image.image_id = role_index.get(view_image.role, ids["images"][0])
                view_image_id = DatabaseWriter.write_view_image(self.db, view_image)
                ids["view_images"].append(view_image_id)

        if visibility_distances:
            ids["visibility_distances"] = []
            for visibility_distance in visibility_distances:
                if ids["views"]:
                    visibility_distance.view_id = ids["views"][0]
                visibility_distance_id = DatabaseWriter.write_visibility_distance(
                    self.db, visibility_distance
                )
                ids["visibility_distances"].append(visibility_distance_id)

        return ids
