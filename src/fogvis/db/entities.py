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
                f"SELECT EXISTS(SELECT 1 FROM coordinate WHERE lat = {self.lat.value} AND lon = {self.lon.value})"
            )
            return cur.fetchone()[0]

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cmd = f"SELECT id FROM coordinate WHERE lat = {self.lat.value} AND lon = {self.lon.value}"
            cur.execute(cmd)

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

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cmd = f"SELECT EXISTS(SELECT 1 FROM scene WHERE name = '{self.name}' AND terrainRenderingType = '{self.terrain_rendering_type}')"
            cur.execute(cmd)
            exists: bool = cur.fetchone()[0]
            return exists

    def get_record_id(self, db: Database) -> Optional[int]:
        with db as con:
            cur = con.cursor()
            cmd = f"""SELECT id FROM scene 
            WHERE name = '{self.name}' 
            AND terrainRenderingType = '{self.terrain_rendering_type}'
            """
            cur.execute(cmd)

            result = cur.fetchone()
            if result is None:
                return None

            return result[0]

    @classmethod
    def create_from_db(cls, db: Database, id: int) -> "SceneEntity":
        with db as con:
            cur = con.cursor()
            cmd: str = f"SELECT * FROM scene WHERE id = {id}"
            cur.execute(cmd)

            result = cur.fetchone()

            cur.close()

            return cls(
                name=result[1],
                coverage_distance_miles=result[2],
                upper_right_id=result[3],
                lower_left_id=result[4],
                center_id=result[5],
                terrain_rendering_type=result[6],
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
            cmd: str = (
                f"SELECT EXISTS (SELECT 1 FROM fog_type WHERE name = '{self.name}')"
            )
            return cur.execute(cmd).fetchone()[0]

    def get_record_id(self, db: Database) -> Optional[int]:
        with db as con:
            cur = con.cursor()
            cmd: str = f"SELECT id FROM fog_type WHERE name = '{self.name}'"
            result = cur.execute(cmd).fetchone()

            if result is None:
                return None
            return result[0]


@dataclass
class FogEntity:
    scene_id: int
    fog_type_id: int
    exp_fog_density: float
    linear_near_distance: float
    linear_far_distance: float
    marched_cutoff: Optional[float]
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

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cmd = f"""
                SELECT EXISTS (
                SELECT 1
                FROM fog
                WHERE sceneID = ?
                    AND typeID = ?
                    AND expFogDensity = ?
                    AND linearNearDistance = ?
                    AND linearFarDistance = ?
                    AND marchedDefaultDensity = ?
                    AND marchedDensityMultiplier = ?
                    AND marchedLightG = ?
                    AND marchedSigmaAbsorption = ?
                    AND marchedSigmaScattering = ?
                    AND marchedStepSizeDist = ?
                    AND marchedStepSizeDistLight = ?\n
            """
            if self.marched_cutoff is None:
                cmd += f"AND marchedCutoff IS NULL\n"
            else:
                cmd += f"AND marchedCutoff = {self.marched_cutoff}\n"

            if self.volume_name is None:
                cmd += "AND volumeName IS NULL\n"
            else:
                cmd += f"AND volumeName = '{self.volume_name}'\n"

            if self.volume_position is None:
                cmd += "AND volumePosition IS NULL\n"
            else:
                cmd += f"AND volumePosition = '{self.volume_position.to_json()}'\n"

            if self.volume_rotation is None:
                cmd += "AND volumeRotation IS NULL\n"
            else:
                cmd += f"AND volumeRotation = '{self.volume_rotation.to_json()}'\n"

            if self.volume_scale is None:
                cmd += "AND volumeScale IS NULL\n"
            else:
                cmd += f"AND volumeScale = '{self.volume_scale.to_json()}'\n"

            cmd += ")"
            params = (
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
            )

            result = cur.execute(cmd, params).fetchone()
            return bool(result[0])

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cmd = f"""
                SELECT id
                FROM fog
                WHERE sceneID = ?
                    AND typeID = ?
                    AND expFogDensity = ?
                    AND linearNearDistance = ?
                    AND linearFarDistance = ?
                    AND marchedDefaultDensity = ?
                    AND marchedDensityMultiplier = ?
                    AND marchedLightG = ?
                    AND marchedSigmaAbsorption = ?
                    AND marchedSigmaScattering = ?
                    AND marchedStepSizeDist = ?
                    AND marchedStepSizeDistLight = ?\n
            """
            if self.marched_cutoff is None:
                cmd += f"AND marchedCutoff IS NULL\n"
            else:
                cmd += f"AND marchedCutoff = {self.marched_cutoff}\n"

            if self.volume_name is None:
                cmd += "AND volumeName IS NULL\n"
            else:
                cmd += f"AND volumeName = '{self.volume_name}'\n"

            params = (
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
            )
            result = cur.execute(cmd, params).fetchone()
            if result is None:
                raise Exception("Failed to get record id")
            return result[0]


@dataclass
class LightTypeEntity:
    name: str

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cmd: str = """SELECT EXISTS (SELECT 1 FROM light_type WHERE name = ?)"""
            params = [self.name]

            res = cur.execute(cmd, params).fetchone()
            return bool(res[0])

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cmd: str = """SELECT id FROM light_type WHERE name = ?"""
            params = [self.name]

            res = cur.execute(cmd, params).fetchone()
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

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cmd: str = """SELECT id FROM light WHERE 
                ambient = ? AND
                diffuse = ?  AND 
                specular = ? AND
                virtualDirection = ? AND
                virtualPosition = ? AND
                innerDiameter = ? AND
                outerDiameter = ? AND
                luminance = ? AND
                typeID = ?"""
            parms = (
                self.ambient.to_json(),
                self.diffuse.to_json(),
                self.specular.to_json(),
                self.virtualDirection.to_json(),
                self.virtualPosition.to_json(),
                self.innerDiameter,
                self.outerDiameter,
                self.luminance,
                self.type_id,
            )
            result = cur.execute(cmd, parms).fetchone()
            return int(result[0])

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cmd: str = """SELECT EXISTS (SELECT 1 FROM light WHERE
                ambient = ? AND
                diffuse = ?  AND 
                specular = ? AND
                virtualDirection = ? AND
                virtualPosition = ? AND
                innerDiameter = ? AND
                outerDiameter = ? AND
                luminance = ? AND
                typeID = ?
                )"""

            parms = (
                self.ambient.to_json(),
                self.diffuse.to_json(),
                self.specular.to_json(),
                self.virtualDirection.to_json(),
                self.virtualPosition.to_json(),
                self.innerDiameter,
                self.outerDiameter,
                self.luminance,
                self.type_id,
            )
            result = cur.execute(cmd, parms).fetchone()
            return bool(result[0])


@dataclass
class EnvironmentEntity:
    fog_id: int

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cmd: str = f"""SELECT EXISTS (
                SELECT 1 FROM environment
                WHERE fogID = {self.fog_id}
            )"""

            result = cur.execute(cmd).fetchone()
            return bool(result[0])

    def get_record_id(self, db: Database) -> int:
        with db as con:
            cur = con.cursor()
            cmd: str = f"""
                SELECT id FROM environment
                WHERE fogID = {self.fog_id}"""

            result = cur.execute(cmd).fetchone()
            if result is None:
                raise Exception("Failed to get record id for environment")
            return result[0]


@dataclass
class EnvironmentLightEntity:
    light_id: int
    environment_id: int


@dataclass
class ImageEntity:
    file_path: str
    visibility_distance: str
    camera_id: int
    scene_id: int
    environment_id: int
    resolution_x: int
    resolution_y: int

    def get_does_exist(self, db: Database) -> bool:
        with db as con:
            cur = con.cursor()
            cmd: str = """SELECT EXISTS(SELECT 1 FROM image WHERE 
                    filePath = ? AND 
                    visibilityDistance = ? AND
                    cameraID = ? AND
                    sceneID = ? AND
                    environmentID = ? AND
                    resolution_x = ? AND
                    resolution_y = ?
                    )"""

            parms = (
                self.file_path,
                self.visibility_distance,
                self.camera_id,
                self.scene_id,
                self.environment_id,
                self.resolution_x,
                self.resolution_y,
            )
            cur.execute(cmd, parms)
            return cur.fetchone()[0]
