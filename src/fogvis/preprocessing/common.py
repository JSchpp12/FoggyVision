import json
from matplotlib import pyplot as plt
from pathlib import Path
from dataclasses import dataclass, asdict

@dataclass
class Subregion: 
    x0 : str
    y0 : str
    x1 : str
    y1 : str

    def to_dict(self) -> dict: 
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data : dict) -> "Subregion": 
        return cls(**data)
    
class CameraSubregions:
    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.subregions: dict[int, list[Subregion]] = {}

    def add_subregion(self, camera_id: int, subregion: Subregion) -> None:
        self.subregions.setdefault(camera_id, []).append(subregion)

    def write(self) -> None:
        result = {
            camera_id: [sr.to_dict() for sr in subregion_list]
            for camera_id, subregion_list in self.subregions.items()
        }

        with open(self.file_path, "w", encoding="utf-8") as file:
            json.dump(result, file, indent=4)