from dataclasses import dataclass, asdict
import json

@dataclass
class VectorContainer2D:
    x : float
    y : float

    def to_json(self) -> str: 
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str : str): 
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise ValueError(f'Invalid JSON: {exc.msg}') from exc

        if not isinstance(data, dict):
            raise ValueError('JSON must represent an object (dictionary)')

        # Ensure the three required coordinates are present
        required_keys = {'x', 'y'}
        missing = required_keys - data.keys()
        if missing:
            raise ValueError(f'Missing required keys: {", ".join(sorted(missing))}')

        try:
            # Explicitly convert to float to guard against bad types
            x = float(data['x'])
            y = float(data['y'])
        except (TypeError, ValueError) as exc:
            raise TypeError('All coordinates must be numeric values') from exc

        return cls(x, y)


@dataclass
class VectorContainer3D:
    x : float
    y : float
    z : float

    def to_json(self) -> str: 
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str : str): 
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise ValueError(f'Invalid JSON: {exc.msg}') from exc

        if not isinstance(data, dict):
            raise ValueError('JSON must represent an object (dictionary)')

        # Ensure the three required coordinates are present
        required_keys = {'x', 'y', 'z'}
        missing = required_keys - data.keys()
        if missing:
            raise ValueError(f'Missing required keys: {", ".join(sorted(missing))}')

        try:
            # Explicitly convert to float to guard against bad types
            x = float(data['x'])
            y = float(data['y'])
            z = float(data['z'])
        except (TypeError, ValueError) as exc:
            raise TypeError('All coordinates must be numeric values') from exc

        return cls(x, y, z)