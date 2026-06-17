import json
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from fogvis.common import WorldCoordinates, VectorContainer3D, VectorContainer2D
from .entities import (
    FogEntity,
    LightEntity,
    FogTypeEntity,
    LightTypeEntity,
    CameraEntity,
    ImageEntity,
    ViewEntity,
    ViewImageEntity,
    VisibilityDistanceEntity,
    EnvironmentEntity,
    EnvironmentLightEntity,
    SceneEntity,
    DistanceMetricsEntity,
)


@dataclass
class SceneData:
    name: str
    rendering_type: str
    center: VectorContainer2D
    vis_range: int


@dataclass
class FrameData:
    """All dataclasses parsed from a single Frame JSON file."""

    fog_type: FogTypeEntity
    fog: FogEntity
    light_type: LightTypeEntity
    light: LightEntity
    camera: CameraEntity
    environment: EnvironmentEntity
    environment_light: EnvironmentLightEntity
    image: ImageEntity
    view: ViewEntity
    view_images: list[ViewImageEntity]
    visibility_distances: list[VisibilityDistanceEntity]


LIGHT_TYPE_NAMES = {0: "Point", 1: "Directional", 2: "Spot"}


class ImageImporter:
    def __init__(self, path: Path) -> None:
        self.path = path
        with open(path, "r", encoding="utf-8") as fh:
            self._data: dict = json.load(fh)

    def read_fog_type(self) -> FogTypeEntity:
        return FogTypeEntity(name=self._data["fog_type"])

    def read_fog(self, *, fog_type_id: int = 0, scene_id: int = 0) -> FogEntity:
        fp = self._data["fog_params"]
        vi = self._data.get("volume_info", {})

        marched_cutoff: Optional[float] = None
        if (
            "cutoffValue" in fp["marchedInfo"]
            and fp["marchedInfo"]["cutoffValue"] is not None
        ):
            marched_cutoff = float(fp["marchedInfo"]["cutoffValue"])

        return FogEntity(
            scene_id=scene_id,
            fog_type_id=fog_type_id,
            # exponential
            exp_fog_density=float(fp["expFogInfo"]["density"]),
            # linear
            linear_near_distance=float(fp["linearInfo"]["nearDist"]),
            linear_far_distance=float(fp["linearInfo"]["farDist"]),
            # ray-marched
            marched_cutoff=marched_cutoff,
            marched_color_transparency_cutoff=float(
                fp["marchedInfo"].get("colorTransparencyCutoff", 0.0)
            ),
            marched_distance_transparency_cutoff=float(
                fp["marchedInfo"].get("distanceTransparencyCutoff", 0.0)
            ),
            marched_light_extinction_scale=float(
                fp["marchedInfo"].get("lightExtinctionScale", 0.0)
            ),
            marched_default_density=float(fp["marchedInfo"]["defaultDensity"]),
            marched_density_multiplier=float(fp["marchedInfo"]["densityMultiplier"]),
            marched_lightDirG=float(fp["marchedInfo"]["lightPropertyDirG"]),
            marched_sigmaAbsorption=float(fp["marchedInfo"]["sigmaAbsorption"]),
            marched_sigmaScattering=float(fp["marchedInfo"]["sigmaScattering"]),
            marched_stepSizeDist=float(fp["marchedInfo"]["stepSizeDist"]),
            marched_stepSizeDist_light=float(fp["marchedInfo"]["stepSizeDist_light"]),
            volume_name=self._data.get("fog_volume_name"),
            volume_position=VectorContainer3D(**vi.get("position", {}))
            if "position" in vi
            else None,
            volume_rotation=VectorContainer3D(**vi.get("rotation", {}))
            if "rotation" in vi
            else None,
            volume_scale=VectorContainer3D(**vi.get("scale", {}))
            if "scale" in vi
            else None,
        )

    def read_environment(self, *, fog_id: int = 0, light_id: int = 0) -> EnvironmentEntity:
        light_ids = {light_id} if light_id else set()
        return EnvironmentEntity(fog_id=fog_id, light_ids=light_ids)

    def read_environment_light(
        self, *, light_id: int = 0, environment_id: int = 0
    ) -> EnvironmentLightEntity:
        return EnvironmentLightEntity(light_id=light_id, environment_id=environment_id)

    def read_light_type(self) -> Optional[LightTypeEntity]:
        if "light" in self._data:
            type_int = int(self._data["light"]["type"])
            name: str = LIGHT_TYPE_NAMES.get(type_int, f"unknown_{type_int}")
            return LightTypeEntity(name=name)

        return None

    def read_light(self, *, type_id: int = 0) -> Optional[LightEntity]:
        """Maps the light block to a Light. Pass type_id once you have the DB id."""
        if "light" in self._data:
            l = self._data["light"]
            a_str: str = json.dumps(l["ambient"])
            d_str: str = json.dumps(l["diffuse"])
            s_str: str = json.dumps(l["specular"])
            dir_str: str = json.dumps(l["direction"])
            pos_str: str = json.dumps(l["position"])

            return LightEntity(
                ambient=VectorContainer3D.from_json(a_str),
                diffuse=VectorContainer3D.from_json(d_str),
                specular=VectorContainer3D.from_json(s_str),
                virtualDirection=VectorContainer3D.from_json(dir_str),
                virtualPosition=VectorContainer3D.from_json(pos_str),
                enabled=bool(l["enabled"]),
                innerDiameter=float(l["innerDiameter"]),
                outerDiameter=float(l["outerDiameter"]),
                luminance=float(l["luminance"]),
                type_id=type_id,
            )

        return None

    def read_distance_metrics(self) -> DistanceMetricsEntity:
        distance_metrics = self._data.get("distance_metrics", {})

        # Support both legacy flat format and newer ray_metrics wrapped format
        if "ray_metrics" in distance_metrics:
            excluding = distance_metrics["ray_metrics"]["excludingInvalidRays"]
            including = distance_metrics["ray_metrics"]["includingInvalidRays"]
        else:
            excluding = distance_metrics.get("excludingInvalidRays", {})
            including = distance_metrics.get("includingInvalidRays", {})

        def _float(value):
            return float(value) if value is not None else None

        def _int(value):
            return int(value) if value is not None else 0

        return DistanceMetricsEntity(
            excluding_invalid_rays_average=_float(excluding.get("average")),
            excluding_invalid_rays_median=_float(excluding.get("median")),
            excluding_invalid_rays_minimum=_float(excluding.get("minimum")),
            excluding_invalid_rays_ray_count=_int(excluding.get("rayCount")),
            including_invalid_rays_average=float(including["average"]),
            including_invalid_rays_median=float(including["median"]),
            including_invalid_rays_minimum=float(including["minimum"]),
            including_invalid_rays_ray_count=int(including["rayCount"]),
        )

    def read_scene(self) -> SceneData:
        center_str: str = json.dumps(self._data["terrain_shape"]["center"])
        return SceneData(
            name=self._data["terrain_name"],
            rendering_type=self._data["terrain_shape_type"],
            center=VectorContainer2D.from_json(center_str),
            vis_range=self._data["terrain_shape"]["view_distance"],
        )

    def read_camera(
        self,
        *,
        scene_id=0,
        fov: float = 0.0,
        near_clip: float = 0.0,
        far_clip: float = 0.0,
    ) -> CameraEntity:
        pos_str: str = str(json.dumps(self._data["camera_position"]))
        dir_str: str = json.dumps(self._data["camera_look_dir"])

        return CameraEntity(
            scene_id=scene_id,
            virtual_position=VectorContainer3D.from_json(pos_str),
            look_dir=VectorContainer3D.from_json(dir_str),
            fov=fov,
            near_clip=near_clip,
            far_clip=far_clip,
        )

    def read_image(self, *, file_name: str = "", file_type: str = "color") -> ImageEntity:
        return ImageEntity(
            file_name=Path(file_name).name if file_name else "",
            file_type=file_type,
        )

    def read_view(
        self,
        *,
        color_image_id: int = 0,
        camera_id: int = 0,
        scene_id: int = 0,
        environment_id: int = 0,
    ) -> ViewEntity:
        return ViewEntity(
            color_image_id=color_image_id,
            camera_id=camera_id,
            scene_id=scene_id,
            environment_id=environment_id,
        )

    def read_view_images(
        self,
        *,
        view_id: int = 0,
        color_image_id: int = 0,
        ray_distance_image_id: int = 0,
        ray_normalized_distance_image_id: int = 0,
        ray_validity_image_id: int = 0,
    ) -> list[ViewImageEntity]:
        view_images: list[ViewImageEntity] = []

        view_images.append(
            ViewImageEntity(
                view_id=view_id, image_id=color_image_id, role="color"
            )
        )

        if ray_distance_image_id != 0:
            view_images.append(
                ViewImageEntity(
                    view_id=view_id,
                    image_id=ray_distance_image_id,
                    role="ray_distance",
                )
            )

        if ray_normalized_distance_image_id != 0:
            view_images.append(
                ViewImageEntity(
                    view_id=view_id,
                    image_id=ray_normalized_distance_image_id,
                    role="ray_normalized_distance",
                )
            )

        if ray_validity_image_id != 0:
            view_images.append(
                ViewImageEntity(
                    view_id=view_id,
                    image_id=ray_validity_image_id,
                    role="ray_validity",
                )
            )

        return view_images

    def read_visibility_distances(
        self, *, view_id: int = 0
    ) -> list[VisibilityDistanceEntity]:
        visibility_distances: list[VisibilityDistanceEntity] = []
        distance_metrics = self._data.get("distance_metrics", {})

        if "simple_distance" in distance_metrics:
            visibility_distances.append(
                VisibilityDistanceEntity(
                    view_id=view_id,
                    distance_type="simple",
                    value=float(distance_metrics["simple_distance"]),
                )
            )

        # Support both old flat format and newer ray_metrics wrapper.
        if "ray_metrics" in distance_metrics:
            ray_metrics = distance_metrics["ray_metrics"]
        else:
            ray_metrics = distance_metrics

        excluding = ray_metrics.get("excludingInvalidRays", {})
        including = ray_metrics.get("includingInvalidRays", {})

        def _float(value):
            return float(value) if value is not None else None

        def _int(value):
            return int(value) if value is not None else 0

        if excluding:
            visibility_distances.append(
                VisibilityDistanceEntity(
                    view_id=view_id,
                    distance_type="ray_excluding_invalid",
                    average=_float(excluding.get("average")),
                    median=_float(excluding.get("median")),
                    minimum=_float(excluding.get("minimum")),
                    ray_count=_int(excluding.get("rayCount")),
                )
            )

        if including:
            visibility_distances.append(
                VisibilityDistanceEntity(
                    view_id=view_id,
                    distance_type="ray_including_invalid",
                    average=float(including["average"]),
                    median=float(including["median"]),
                    minimum=float(including["minimum"]),
                    ray_count=int(including["rayCount"]),
                )
            )

        return visibility_distances

    def read_all(
        self,
        *,
        scene_id: int = 0,
        fog_type_id: int = 0,
        fog_id: int = 0,
        light_type_id: int = 0,
        light_id: int = 0,
        camera_id: int = 0,
        environment_id: int = 0,
        # Camera-only extras
        fov: float = 0.0,
        near_clip: float = 0.0,
        far_clip: float = 0.0,
    ) -> FrameData:
        """
        Parse the entire JSON file and return a FrameData bundle.

        All *_id keyword arguments let the caller inject real database IDs
        after the entities have been persisted.
        """
        fog_type = self.read_fog_type()
        fog = self.read_fog(fog_type_id=fog_type_id)
        light_type = self.read_light_type()
        light = self.read_light(type_id=light_type_id)
        camera = self.read_camera(
            scene_id=scene_id,
            fov=fov,
            near_clip=near_clip,
            far_clip=far_clip,
        )
        environment = self.read_environment(fog_id=fog_id, light_id=light_id)
        environment_light = self.read_environment_light(
            light_id=light_id,
            environment_id=environment_id,
        )
        image = self.read_image()
        view = self.read_view(
            camera_id=camera_id,
            scene_id=scene_id,
            environment_id=environment_id,
        )
        view_images: list[ViewImageEntity] = []
        visibility_distances: list[VisibilityDistanceEntity] = []

        return FrameData(
            fog_type=fog_type,
            fog=fog,
            light_type=light_type,
            light=light,
            camera=camera,
            environment=environment,
            environment_light=environment_light,
            image=image,
            view=view,
            view_images=view_images,
            visibility_distances=visibility_distances,
        )
