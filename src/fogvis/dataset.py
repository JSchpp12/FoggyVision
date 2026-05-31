# dataset.py
"""PyTorch Dataset for loading foggy image data from the SQLite database."""

import sqlite3
from pathlib import Path
from typing import Any, Callable, Optional
from fogvis.db import Database

import torch
from PIL import Image
from torch.utils.data import DataLoader, Dataset


class FoggyVisionDataset(Dataset):
    """
    PyTorch Dataset that loads foggy images and their visibility-distance
    labels from the project's SQLite database.

    Each sample is a tuple of ``(image_tensor, log_visibility_label)``.

    Parameters
    ----------
    db_path : str | Path
        Path to ``database.sqlite3``.
    image_root : str | Path
        Root directory where per-scene image PNGs are stored on disk.
    transform : callable, optional
        A torchvision.transforms-compatible callable applied to every image
        *after* loading.
    label_transform : callable, optional
        A callable applied to the raw ``visibilityDistance`` value so that
        the caller can normalise / log-transform labels as desired.
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

        # Build an internal lookup: (id, file_path, visibility_distance)
        self._samples: list[tuple[int, str, float]] = []
        if self._db_path.exists():
            with Database(self._db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id, filePath, visibilityDistance FROM image ORDER BY id"
                )
                self._samples = list(cur.fetchall())

    def __len__(self) -> int:
        return len(self._samples)

    def __getitem__(self, index: int) -> tuple[torch.Tensor, float]:
        _id, file_path, raw_vis = self._samples[index]

        # --- image ---------------------------------------------------------
        image_path = self._image_root / file_path
        if not image_path.exists():
            raise FileNotFoundError(
                f"Image not found: {image_path}  (database references '{file_path}')"
            )

        image = Image.open(image_path).convert("RGB")

        if self._transform is not None:
            image = self._transform(image)
        else:
            raise Exception("Transform must be provided")

        # --- label ---------------------------------------------------------
        label = self._label_transform(raw_vis)

        return image, label

    @property
    def labels(self) -> torch.Tensor:
        """Return all labels as a 1-D float Tensor (convenience for plotting)."""
        return torch.tensor(
            [self._label_transform(vis) for _id, _path, vis in self._samples],
            dtype=torch.float,
        )

    @property
    def file_paths(self) -> list[str]:
        return [fp for _id, fp, _vis in self._samples]

    @property
    def raw_visibilities(self) -> list[float]:
        """Return the raw (non-transformed) visibility distances."""
        return [vis for _id, _path, vis in self._samples]


# ---------------------------------------------------------------------------
# Module-level factory for quick setup.
# ---------------------------------------------------------------------------


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

    Parameters
    ----------
    db_path : str | Path
        Path to the SQLite database.
    image_root : str | Path
        Root directory where images live on disk.
    batch_size : int
        Batch size for the dataloader.
    shuffle : bool
        Whether to shuffle the dataset each epoch.
    transform : callable, optional
        Image transform applied per-item.
    label_transform : callable, optional
        Callable applied to each ``visibilityDistance`` label.
    num_workers : int
        Workers for the DataLoader (set >0 for faster I/O).
    pin_memory : bool
        Pin memory for faster GPU transfers.

    Returns
    -------
    DataLoader
        A ready-to-iterate PyTorch DataLoader.
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
