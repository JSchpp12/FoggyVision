from pathlib import Path

import pytest


MEDIA_TEST_DIR = Path(__file__).resolve().parent.parent / "media" / "test"
SAMPLE_DATE_DIRS = sorted(MEDIA_TEST_DIR.iterdir())


@pytest.fixture
def sample_json_path() -> Path:
    """Path to a single sample Frame-0.json file."""
    return SAMPLE_DATE_DIRS[0] / "Frame-0.json"


@pytest.fixture
def sample_json_paths() -> list[Path]:
    """Paths to all sample frame JSON files."""
    paths: list[Path] = []
    for date_dir in SAMPLE_DATE_DIRS:
        paths.extend(sorted(date_dir.glob("Frame-*.json")))
    return paths
