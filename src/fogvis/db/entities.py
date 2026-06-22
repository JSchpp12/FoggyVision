from dataclasses import dataclass, field
from sqlite3 import Cursor
from contextlib import closing

import sqlite3

from typing import Optional, Set

from fogvis.common import Latitude, Longitude, VectorContainer3D
from .database import Database


@dataclass
class CoordinateEntity:
    lat: Latitude
    lon: Longitude

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM coordinate WHERE lat = ? AND lon = ?)",
                (self.lat.value, self.lon.value),
            )
            return cur.fetchone()[0]

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cur.execute(
                "SELECT id FROM coordinate WHERE lat = ? AND lon = ?",
                (self.lat.value, self.lon.value),
            )
            result = cur.fetchone()
            if result is None:
                raise Exception("Failed to retreive record")
            return result[0]


@dataclass
class SceneEntity:
    name: str
    coverage_distance_miles: int
    upper_right_id: int
    lower_left_id: int
    center_id: int
    terrain_rendering_type: str
    terrain_shape_center: str = ""

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cur.execute(
                """SELECT EXISTS (
                    SELECT 1 FROM scene
                    WHERE name = ? AND terrainRenderingType = ?
                )""",
                (self.name, self.terrain_rendering_type),
            )
            return cur.fetchone()[0]

    def get_record_id(self, db: Database) -> Optional[int]:
        with db as con:
            cur = con.cursor()
            cur.execute(
                """SELECT id FROM scene
                WHERE name = ? AND terrainRenderingType = ?""",
                (self.name, self.terrain_rendering_type),
            )
            result = cur.fetchone()
            return None if result is None else result[0]

    @classmethod
    def create_from_db(cls, db: Database, id: int) -> "SceneEntity":
        with db as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM scene WHERE id = ?", (id,))
            result = cur.fetchone()
            cur.close()

            return cls(
                name=result[1],
                coverage_distance_miles=result[2],
                upper_right_id=result[3],
                lower_left_id=result[4],
                center_id=result[5],
                terrain_rendering_type=result[6],
                terrain_shape_center=result[7] if len(result) > 7 else "",
            )


@dataclass
class CameraEntity:
    scene_id: int
    virtual_position: VectorContainer3D
    look_dir: VectorContainer3D
    fov: float
    near_clip: float
    far_clip: float

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cmd = """
                SELECT EXISTS (
                    SELECT 1 FROM camera
                    WHERE virtualPosition = ?
                    AND lookDir = ?
                    AND sceneID = ?
                    AND fov = ?
                    AND nearClip = ?
                    AND farClip = ?
                )
            """
            params = (
                self.virtual_position.to_json(),
                self.look_dir.to_json(),
                self.scene_id,
                self.fov,
                self.near_clip,
                self.far_clip,
            )
            cur.execute(cmd, params)
            return bool(cur.fetchone()[0])

    def get_record_id(self, db: Database) -> Optional[int]:
        with db as con:
            cur = con.cursor()
            cmd = """
                SELECT id FROM camera
                WHERE virtualPosition = ?
                AND lookDir = ?
                AND sceneID = ?
                AND fov = ?
                AND nearClip = ?
                AND farClip = ?
            """
            params = (
                self.virtual_position.to_json(),
                self.look_dir.to_json(),
                self.scene_id,
                self.fov,
                self.near_clip,
                self.far_clip,
            )
            cur.execute(cmd, params)
            result = cur.fetchone()
            return None if result is None else result[0]


@dataclass
class FogTypeEntity:
    name: str

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM fog_type WHERE name = ?)",
                (self.name,),
            )
            return bool(cur.fetchone()[0])

    def get_record_id(self, db: Database) -> Optional[int]:
        with db as con:
            cur = con.cursor()
            cur.execute("SELECT id FROM fog_type WHERE name = ?", (self.name,))
            result = cur.fetchone()
            return None if result is None else result[0]


@dataclass
class FogEntity:
    scene_id: int
    fog_type_id: int
    exp_fog_density: float
    linear_near_distance: float
    linear_far_distance: float
    marched_cutoff: Optional[float]
    marched_color_transparency_cutoff: Optional[float]
    marched_distance_transparency_cutoff: Optional[float]
    marched_light_extinction_scale: Optional[float]
    marched_default_density: float
    marched_density_multiplier: float
    marched_lightDirG: float
    marched_sigmaAbsorption: float
    marched_sigmaScattering: float
    marched_stepSizeDist: float
    marched_stepSizeDist_light: float
    volume_name: Optional[str] = None
    volume_position: Optional[VectorContainer3D] = None
    volume_rotation: Optional[VectorContainer3D] = None
    volume_scale: Optional[VectorContainer3D] = None

    def _build_where(self) -> tuple[str, tuple]:
        conditions = [
            "sceneID = ?",
            "typeID = ?",
            "expFogDensity = ?",
            "linearNearDistance = ?",
            "linearFarDistance = ?",
            "marchedDefaultDensity = ?",
            "marchedDensityMultiplier = ?",
            "marchedLightG = ?",
            "marchedSigmaAbsorption = ?",
            "marchedSigmaScattering = ?",
            "marchedStepSizeDist = ?",
            "marchedStepSizeDistLight = ?",
        ]
        params: list = [
            self.scene_id,
            self.fog_type_id,
            self.exp_fog_density,
            self.linear_near_distance,
            self.linear_far_distance,
            self.marched_default_density,
            self.marched_density_multiplier,
            self.marched_lightDirG,
            self.marched_sigmaAbsorption,
            self.marched_sigmaScattering,
            self.marched_stepSizeDist,
            self.marched_stepSizeDist_light,
        ]

        for value, column in (
            (self.marched_cutoff, "marchedCutoff"),
            (self.marched_color_transparency_cutoff, "marchedColorTransparencyCutoff"),
            (
                self.marched_distance_transparency_cutoff,
                "marchedDistanceTransparencyCutoff",
            ),
            (self.marched_light_extinction_scale, "marchedLightExtinctionScale"),
            (self.volume_name, "volumeName"),
        ):
            if value is None:
                conditions.append(f"{column} IS NULL")
            else:
                conditions.append(f"{column} = ?")
                params.append(value)

        for vector, column in (
            (self.volume_position, "volumePosition"),
            (self.volume_rotation, "volumeRotation"),
            (self.volume_scale, "volumeScale"),
        ):
            if vector is None:
                conditions.append(f"{column} IS NULL")
            else:
                conditions.append(f"{column} = ?")
                params.append(vector.to_json())

        where = " AND ".join(conditions)
        return where, tuple(params)

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            where, params = self._build_where()
            cur.execute(
                f"SELECT EXISTS (SELECT 1 FROM fog WHERE {where})",
                params,
            )
            return bool(cur.fetchone()[0])

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            where, params = self._build_where()
            cur.execute(f"SELECT id FROM fog WHERE {where}", params)
            result = cur.fetchone()
            if result is None:
                raise Exception("Failed to get record id")
            return result[0]


@dataclass
class LightTypeEntity:
    name: str

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM light_type WHERE name = ?)",
                (self.name,),
            )
            return bool(cur.fetchone()[0])

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cur.execute("SELECT id FROM light_type WHERE name = ?", (self.name,))
            res = cur.fetchone()
            return int(res[0])


@dataclass
class LightEntity:
    ambient: VectorContainer3D
    diffuse: VectorContainer3D
    specular: VectorContainer3D
    virtualDirection: VectorContainer3D
    virtualPosition: VectorContainer3D
    enabled: bool
    innerDiameter: float
    outerDiameter: float
    luminance: float
    type_id: int

    def _params(self) -> tuple:
        return (
            self.ambient.to_json(),
            self.diffuse.to_json(),
            self.specular.to_json(),
            self.virtualDirection.to_json(),
            self.virtualPosition.to_json(),
            int(self.enabled),
            self.innerDiameter,
            self.outerDiameter,
            self.luminance,
            self.type_id,
        )

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cmd = """SELECT id FROM light WHERE 
                ambient = ? AND
                diffuse = ?  AND 
                specular = ? AND
                virtualDirection = ? AND
                virtualPosition = ? AND
                enabled = ? AND
                innerDiameter = ? AND
                outerDiameter = ? AND
                luminance = ? AND
                typeID = ?"""
            result = cur.execute(cmd, self._params()).fetchone()
            if result is None:
                raise Exception("Failed to get record id for light")
            return int(result[0])

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cmd = """SELECT EXISTS (SELECT 1 FROM light WHERE
                ambient = ? AND
                diffuse = ?  AND 
                specular = ? AND
                virtualDirection = ? AND
                virtualPosition = ? AND
                enabled = ? AND
                innerDiameter = ? AND
                outerDiameter = ? AND
                luminance = ? AND
                typeID = ?
                )"""
            result = cur.execute(cmd, self._params()).fetchone()
            return bool(result[0])


@dataclass
class EnvironmentEntity:
    fog_id: int
    light_ids: Set[int] = field(default_factory=set)

    def _params(self) -> tuple:
        return (self.fog_id,)

    @staticmethod
    def _find_environment_id(
        cur: Cursor, fog_id: int, light_ids: Set[int]
    ) -> Optional[int]:
        """Find an environment for the given fog that is linked to exactly these lights."""
        if not light_ids:
            cur.execute(
                "SELECT id FROM environment WHERE fogID = ? AND id NOT IN (SELECT environmentID FROM environment_light)",
                (fog_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None

        placeholders = ", ".join(["?"] * len(light_ids))
        cur.execute(
            f"""
            SELECT e.id
            FROM environment e
            WHERE e.fogID = ?
              AND NOT EXISTS (
                  SELECT 1 FROM environment_light el
                  WHERE el.environmentID = e.id
                    AND el.lightID NOT IN ({placeholders})
              )
              AND (
                  SELECT COUNT(DISTINCT lightID)
                  FROM environment_light
                  WHERE environmentID = e.id
              ) = ?
            """,
            (fog_id, *light_ids, len(light_ids)),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            return (
                self._find_environment_id(cur, self.fog_id, self.light_ids) is not None
            )

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            env_id = self._find_environment_id(cur, self.fog_id, self.light_ids)
            if env_id is None:
                raise Exception("Failed to get record id for environment")
            return env_id


@dataclass
class EnvironmentLightEntity:
    light_id: int
    environment_id: int

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cur.execute(
                """SELECT EXISTS (
                    SELECT 1 FROM environment_light
                    WHERE lightID = ? AND environmentID = ?
                )""",
                (self.light_id, self.environment_id),
            )
            return bool(cur.fetchone()[0])

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cur.execute(
                """SELECT rowid FROM environment_light
                WHERE lightID = ? AND environmentID = ?""",
                (self.light_id, self.environment_id),
            )
            result = cur.fetchone()
            if result is None:
                raise Exception("Failed to get record id for environment_light")
            return result[0]


@dataclass
class DistanceMetricsEntity:
    excluding_invalid_rays_average: Optional[float]
    excluding_invalid_rays_median: Optional[float]
    excluding_invalid_rays_minimum: Optional[float]
    excluding_invalid_rays_ray_count: int
    including_invalid_rays_average: float
    including_invalid_rays_median: float
    including_invalid_rays_minimum: float
    including_invalid_rays_ray_count: int


@dataclass
class ImageEntity:
    file_name: str
    file_type: str
    width: Optional[int] = None
    height: Optional[int] = None
    checksum: Optional[str] = None

    def _params(self) -> tuple:
        return (
            self.file_name,
            self.file_type,
            self.width,
            self.height,
            self.checksum,
        )

    @classmethod
    def from_row(cls, row: tuple) -> "ImageEntity":
        return cls(
            file_name=row[0],
            file_type=row[1],
            width=row[2],
            height=row[3],
            checksum=row[4],
        )

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM image WHERE fileName = ?)",
                (self.file_name,),
            )
            return cur.fetchone()[0]

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cur.execute("SELECT id FROM image WHERE fileName = ?", (self.file_name,))
            result = cur.fetchone()
            if result is None:
                raise Exception("Failed to get record id for image")
            return result[0]


@dataclass
class ViewEntity:
    color_image_id: int
    camera_id: int
    scene_id: int
    environment_id: int

    def _params(self) -> tuple:
        return (
            self.color_image_id,
            self.camera_id,
            self.scene_id,
            self.environment_id,
        )

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cur.execute(
                """SELECT EXISTS(
                    SELECT 1 FROM view
                    WHERE colorImageID = ? AND cameraID = ? AND sceneID = ? AND environmentID = ?
                )""",
                self._params(),
            )
            return cur.fetchone()[0]

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cur.execute(
                """SELECT id FROM view
                WHERE colorImageID = ? AND cameraID = ? AND sceneID = ? AND environmentID = ?""",
                self._params(),
            )
            result = cur.fetchone()
            if result is None:
                raise Exception("Failed to get record id for view")
            return result[0]


@dataclass
class ViewImageEntity:
    view_id: int
    image_id: int
    role: str

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM view_image WHERE viewID = ? AND role = ?)",
                (self.view_id, self.role),
            )
            return cur.fetchone()[0]

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cur.execute(
                "SELECT rowid FROM view_image WHERE viewID = ? AND role = ?",
                (self.view_id, self.role),
            )
            result = cur.fetchone()
            if result is None:
                raise Exception("Failed to get record id for view_image")
            return result[0]


@dataclass
class VisibilityDistanceEntity:
    view_id: int
    distance_type: str
    value: Optional[float] = None
    average: Optional[float] = None
    median: Optional[float] = None
    minimum: Optional[float] = None
    ray_count: Optional[int] = None

    def _params(self) -> tuple:
        return (
            self.view_id,
            self.distance_type,
            self.value,
            self.average,
            self.median,
            self.minimum,
            self.ray_count,
        )

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM visibility_distance WHERE viewID = ? AND distanceType = ?)",
                (self.view_id, self.distance_type),
            )
            return cur.fetchone()[0]

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cur.execute(
                "SELECT id FROM visibility_distance WHERE viewID = ? AND distanceType = ?",
                (self.view_id, self.distance_type),
            )
            result = cur.fetchone()
            if result is None:
                raise Exception("Failed to get record id for visibility_distance")
            return result[0]
