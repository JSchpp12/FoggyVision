from dataclasses import dataclass
from sqlite3 import Cursor
from contextlib import closing

import sqlite3

from typing import Optional

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
    scene_id: int
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
            (self.marched_distance_transparency_cutoff, "marchedDistanceTransparencyCutoff"),
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

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM environment WHERE fogID = ?)",
                (self.fog_id,),
            )
            return bool(cur.fetchone()[0])

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cur.execute("SELECT id FROM environment WHERE fogID = ?", (self.fog_id,))
            result = cur.fetchone()
            if result is None:
                raise Exception("Failed to get record id for environment")
            return result[0]


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
    file_path: str
    ray_distance_file_path: str
    ray_normalized_distance_file_path: str
    ray_validity_file_path: str
    distance_metrics: DistanceMetricsEntity
    camera_id: int
    scene_id: int
    environment_id: int
    resolution_x: int
    resolution_y: int

    def _params(self) -> tuple:
        return (
            self.file_path,
            self.ray_distance_file_path,
            self.ray_normalized_distance_file_path,
            self.ray_validity_file_path,
            self.distance_metrics.excluding_invalid_rays_average,
            self.distance_metrics.excluding_invalid_rays_median,
            self.distance_metrics.excluding_invalid_rays_minimum,
            self.distance_metrics.excluding_invalid_rays_ray_count,
            self.distance_metrics.including_invalid_rays_average,
            self.distance_metrics.including_invalid_rays_median,
            self.distance_metrics.including_invalid_rays_minimum,
            self.distance_metrics.including_invalid_rays_ray_count,
            self.camera_id,
            self.scene_id,
            self.environment_id,
            self.resolution_x,
            self.resolution_y,
        )

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cur.execute(
                """SELECT EXISTS(SELECT 1 FROM image WHERE 
                    filePath = ? AND 
                    cameraID = ? AND
                    sceneID = ? AND
                    environmentID = ? AND
                    resolution_x = ? AND
                    resolution_y = ?
                    )""",
                (
                    self.file_path,
                    self.camera_id,
                    self.scene_id,
                    self.environment_id,
                    self.resolution_x,
                    self.resolution_y,
                ),
            )
            return cur.fetchone()[0]
