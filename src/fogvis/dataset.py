"""PyTorch Dataset for loading foggy image data from the SQLite database."""

import sqlite3
from pathlib import Path
from typing import Any, Callable, Optional
from fogvis.db import Database

import torch
from PIL import Image
from torch.utils.data import DataLoader, Dataset


class FoggyVisionDataset(Dataset):
    """PyTorch Dataset of foggy images and visibility-distance labels.

    Each sample is ``(image_tensor, log_visibility_label)`` loaded from the
    project's SQLite database.
    """

    def __init__(
        self,
        db_path: str | Path,
        image_root: str | Path,
        transform: Optional[Callable] = None,
        label_transform: Optional[Callable[[float], torch.Tensor]] = None,
    ) -> None:
        self._db_path = Path(db_path)
        self._image_root = Path(image_root)
        self._transform = transform
        self._label_transform = label_transform or (lambda v: v)

        # (id, fileName, visibility_distance) lookups; fileName is the image's
        # path relative to image_root (including its date-named subdirectory),
        # linked from view.colorImageID.
        self._samples: list[tuple[int, str, float]] = []
        if self._db_path.exists():
            with Database(self._db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT i.id, i.fileName, vd.value
                    FROM image i
                    JOIN view v ON v.colorImageID = i.id
                    JOIN visibility_distance vd ON vd.viewID = v.id
                    ORDER BY i.id
                    """
                )
                self._samples = list(cur.fetchall())

    def __len__(self) -> int:
        return len(self._samples)

    def __getitem__(self, index: int) -> tuple[torch.Tensor, float]:
        _id, file_name, raw_vis = self._samples[index]

        image_path = self._image_root / file_name
        if not image_path.exists():
            raise FileNotFoundError(
                f"Image not found: {image_path}  (database references '{file_name}')"
            )

        image = Image.open(image_path).convert("RGB")

        if self._transform is not None:
            image = self._transform(image)
        else:
            raise Exception("Transform must be provided")

        label = self._label_transform(raw_vis)

        return image, label

    @property
    def labels(self) -> torch.Tensor:
        """Return all labels as a 1-D float Tensor (convenience for plotting)."""
        return torch.tensor(
            [self._label_transform(vis) for _id, _name, vis in self._samples],
            dtype=torch.float,
        )

    @property
    def file_names(self) -> list[str]:
        return [name for _id, name, _vis in self._samples]

    @property
    def raw_visibilities(self) -> list[float]:
        """Return the raw (non-transformed) visibility distances."""
        return [vis for _id, _name, vis in self._samples]


def get_dataloader(
    db_path: str | Path = "media/db/database.sqlite3",
    image_root: str | Path = "media/db/images",
    batch_size: int = 4,
    shuffle: bool = False,
    transform: Optional[Callable] = None,
    label_transform: Optional[Callable[[float], float]] = None,
    num_workers: int = 0,
    pin_memory: bool = False,
) -> DataLoader:
    """
    Convenience factory that creates a :class:`FoggyVisionDataset` and wraps it
    in a :class:`torch.utils.data.DataLoader`.
    """
    dataset = FoggyVisionDataset(
        db_path=db_path,
        image_root=image_root,
        transform=transform,
        label_transform=label_transform,
    )

    return DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=num_workers,
        pin_memory=pin_memory,
    )
