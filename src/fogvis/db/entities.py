from dataclasses import dataclass

from fogvis.common import Latitude, Longitude, VectorContainer


@dataclass
class CoordinateEntity:
    lat: Latitude
    lon: Longitude


@dataclass
class SceneEntity:
    name: str
    upper_right_id: int
    lower_left_id: int
    center_id: int


@dataclass
class CameraEntity:
    scene_id: int
    virtual_position: VectorContainer
    scene_id: int
    look_dir: VectorContainer
    fov: float
    near_clip: float
    far_clip: float


@dataclass
class FogTypeEntity:
    name: str


@dataclass
class FogEntity:
    scene_id : int
    fog_type_id: int
    exp_fog_density: float
    linear_near_distance: float
    linear_far_distance: float
    marched_cutoff: float
    marched_default_density: float
    marched_density_multiplier: float
    marched_lightDirG: float
    marched_sigmaAbsorption: float
    marched_sigmaScattering: float
    marched_stepSizeDist: float
    marched_stepSizeDist_light: float


@dataclass
class LightTypeEntity:
    name: str


@dataclass
class LightEntity:
    ambient: VectorContainer
    diffuse: VectorContainer
    specular: VectorContainer
    virtualDirection: VectorContainer
    virtualPosition: VectorContainer
    enabled: bool
    innerDiameter: float
    outerDiameter: float
    luminance: float
    type_id: int


@dataclass
class EnvironmentEntity:
    fog_id: int


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
