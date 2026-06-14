import json

import pytest

from fogvis.db.image_importer import ImageImporter


def test_read_distance_metrics(sample_json_path):
    reader = ImageImporter(sample_json_path)
    metrics = reader.read_distance_metrics()

    assert metrics.including_invalid_rays_average == pytest.approx(7197.180718457964)
    assert metrics.including_invalid_rays_median == pytest.approx(7346.6640625)
    assert metrics.including_invalid_rays_minimum == pytest.approx(5198.962890625)
    assert metrics.including_invalid_rays_ray_count == 921600


def test_distance_metrics_vary_across_frames(sample_json_paths):
    readers = [ImageImporter(p) for p in sample_json_paths]
    ray_counts = [r.read_distance_metrics().excluding_invalid_rays_ray_count for r in readers]

    # All frames should not share the exact same excluding ray count.
    assert len(set(ray_counts)) > 1


def test_read_scene(sample_json_path):
    reader = ImageImporter(sample_json_path)
    scene_data = reader.read_scene()

    assert scene_data.name == "berthoud_pass_co"
    assert scene_data.rendering_type == "real"
    assert scene_data.center.x == pytest.approx(39.794502)
    assert scene_data.center.y == pytest.approx(-105.76389)
    assert scene_data.vis_range == 10


def test_read_scene_consistent_across_frames(sample_json_paths):
    readers = [ImageImporter(p) for p in sample_json_paths]
    scenes = [r.read_scene() for r in readers]

    first = scenes[0]
    for scene in scenes[1:]:
        assert scene.name == first.name
        assert scene.rendering_type == first.rendering_type
        assert scene.center.x == pytest.approx(first.center.x)
        assert scene.center.y == pytest.approx(first.center.y)
        assert scene.vis_range == first.vis_range


def test_read_fog_volume_info(sample_json_path):
    reader = ImageImporter(sample_json_path)
    fog = reader.read_fog()

    assert fog.volume_name in {"clouds", "windy_whipped"}
    assert fog.volume_position is not None
    assert fog.volume_rotation is not None
    assert fog.volume_scale is not None


def test_fog_volume_names_differ(sample_json_paths):
    volume_names = {ImageImporter(p).read_fog().volume_name for p in sample_json_paths}
    assert volume_names == {"clouds", "windy_whipped"}


def test_camera_positions_differ(sample_json_paths):
    readers = [ImageImporter(p) for p in sample_json_paths]
    volumes = [r.read_fog().volume_position for r in readers]
    positions = {(v.x, v.y, v.z) for v in volumes}
    assert len(positions) > 1


def test_read_image_paths(sample_json_path):
    reader = ImageImporter(sample_json_path)
    image = reader.read_image()

    assert image.file_path == "Frame-0.png"
    assert image.ray_distance_file_path == "Frame-0_distMask.tif"
    assert image.ray_normalized_distance_file_path == "Frame-0_distNormSmlMask.tif"
    assert image.ray_validity_file_path == "Frame-0_validMask.png"
