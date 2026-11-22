# FoggyVision

Synthetic fog imagery and renderer metadata for atmospheric-visibility research.

FoggyVision imports rendered frames and their JSON sidecars into a SQLite
database and exposes the raw metadata plus PyTorch dataset helpers. Browse and
query an imported dataset with the separate, dependency-free
`foggy_vision_loader` package (`F:\repos\Foggy_Vision_Loader`).

Python 3.10+ is required.

## Install

```powershell
cd F:\repos\FoggyVision
python -m pip install -e ".[dev]"
```

This installs the runtime stack used by the importer and ML helpers (`torch`,
`Pillow`, `numpy`, `scipy`, `opencv-python`, `matplotlib`, `tqdm`) plus pytest.
Importing the top-level `fogvis` package is lightweight; only
`fogvis.dataset` imports torch.

## Data layout

An imported dataset is a directory containing the database and the copied
media:

```
<db_dir>/
├── database.sqlite3
└── images/
    └── 2026-06-13_21-40-15/
        ├── Frame-0.png
        ├── Frame-0.json
        ├── Frame-0_distMask.tif
        ├── Frame-0_distNormSmlMask.tif
        └── Frame-0_validMask.png
```

`image.fileName` stores a POSIX path relative to `images/`, including the
session folder. Session folders are named either `YYYY-MM-DD_HH-MM-SS`
(legacy) or `YYYY-MM-DD_HH-MM-SS-<millis>_pid<pid>` (current) and are the only
capture-time source in the database. The loader preserves the milliseconds and
the PID suffix in `record.session`.

The importer's `--database` is the database **directory** (it creates
`database.sqlite3` and `images/` inside). The loader and viewer, by contrast,
take the `database.sqlite3` **file** itself.

## Importing

```powershell
# Small bundled sample (3 sessions, 6 frames)
fogvis-cli import --database media/db --images media/test

# A rendered session directory (80 frames, ~400 MB copied)
fogvis-cli import --database media/db --images F:\repos\2026-11-08_14-05-50
```

Other commands:

```powershell
fogvis-cli cleanup --database media/db   # delete orphaned files on disk
fogvis-cli rebuild --database media/db   # wipe and rebuild from images/
```

The importer accepts both JSON layouts used by the renderer (`image_files` /
`distance_mask_images` wrappers and the older flat `file_name` / `ray_masks`
keys). When a frame JSON contains a `skybox_info` block, its `name` is stored
per view in the `skybox` table and linked from `view.skyboxID`; older frame
JSONs without the block import as `NULL`. Importing into an existing database
adds the new table/column in place, but only `fogvis-cli rebuild` re-reads the
preserved Frame JSON sidecars and backfills skybox names for old views.

## Querying

Install the standalone loader (standard library only):

```powershell
python -m pip install -e F:\repos\Foggy_Vision_Loader
```

```python
from foggy_vision_loader import FoggyVisionLibrary

lib = FoggyVisionLibrary("media/db/database.sqlite3")
print(lib.stats())

q = lib.query()
q.scene_name("berthoud").count()
q.fog_type("marched").fog_volume("clouds").count()
q.date_range("2026-06-13", "2026-06-13").count()
q.visibility(min_meters=3000, max_meters=12000).paths()

# Immutable, chainable selections
foggy = (
    lib.query()
    .scene_name("berthoud")
    .fog_type("marched")
    .visibility(max_meters=8000)
    .order_by("visibility_value")
)
for record in foggy.limit(20).records():
    print(record.path, record.visibility_value, record.visibility_metric)

lib.close()
```

`ImageRecord.path` is relative to `lib.data_dir` (the `images/` directory).
Each record also exposes its masks through `record.mask_paths`
(`ray_distance`, `ray_normalized_distance`, `ray_validity`).

### Visibility metrics

The database stores up to three metric rows per view:

| `distanceType` | usable field |
|---|---|
| `simple` | `value` |
| `ray_including_invalid` | `average`, `median`, `minimum`, `rayCount` |
| `ray_excluding_invalid` | `average`, `median`, `minimum`, `rayCount` |

`record.visibility_value` uses the first available metric in the order
`simple` → `ray_including_invalid` → `ray_excluding_invalid`;
`record.visibility_metric` names the one used. Raw values for every metric
remain available on the record. Values are in the renderer's world units,
treated as meters; `visibility_miles` converts with 1609.344.

## Dataset helpers

```python
from fogvis.dataset import FoggyVisionDataset, get_dataloader

dataset = FoggyVisionDataset(
    db_path="media/db/database.sqlite3",
    image_root="media/db/images",
    transform=my_transform,
)
loader = get_dataloader(batch_size=4, transform=my_transform)
```

`fogvis.dataset` imports torch. For browsing or inspecting labels without
torch, prefer `foggy_vision_loader`.

## Notes

- The repository's notebooks predate the current database schema and are not
  part of the supported API.
- `FoggyVisionDataset` reads `visibility_distance.value`, which is only set for
  `simple` rows; use `foggy_vision_loader` when working with ray metrics.
