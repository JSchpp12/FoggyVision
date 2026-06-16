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


def test_read_image_entity(sample_json_path):
    reader = ImageImporter(sample_json_path)
    image = reader.read_image(file_name="images/Frame-0.png", file_type="color")

    assert image.file_name == "Frame-0.png"
    assert image.file_type == "color"


def test_read_visibility_distances_simple(tmp_path):
    simple_json = {
        "distance_metrics": {
            "simple_distance": 1234.5
        }
    }
    json_path = tmp_path / "simple.json"
    json_path.write_text(json.dumps(simple_json))
    reader = ImageImporter(json_path)
    distances = reader.read_visibility_distances(view_id=1)

    assert len(distances) == 1
    assert distances[0].distance_type == "simple"
    assert distances[0].value == pytest.approx(1234.5)
    assert distances[0].view_id == 1


def test_read_visibility_distances_ray_metrics(tmp_path):
    ray_json = {
        "distance_metrics": {
            "ray_metrics": {
                "excludingInvalidRays": {
                    "average": 100.0,
                    "median": 200.0,
                    "minimum": 50.0,
                    "rayCount": 1000
                },
                "includingInvalidRays": {
                    "average": 150.0,
                    "median": 250.0,
                    "minimum": 75.0,
                    "rayCount": 2000
                }
            }
        }
    }
    json_path = tmp_path / "ray.json"
    json_path.write_text(json.dumps(ray_json))
    reader = ImageImporter(json_path)
    distances = reader.read_visibility_distances(view_id=2)

    assert len(distances) == 2
    
    excluding = next(d for d in distances if d.distance_type == "ray_excluding_invalid")
    assert excluding.average == pytest.approx(100.0)
    assert excluding.median == pytest.approx(200.0)
    assert excluding.minimum == pytest.approx(50.0)
    assert excluding.ray_count == 1000
    
    including = next(d for d in distances if d.distance_type == "ray_including_invalid")
    assert including.average == pytest.approx(150.0)
    assert including.median == pytest.approx(250.0)
    assert including.minimum == pytest.approx(75.0)
    assert including.ray_count == 2000
