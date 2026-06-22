import shutil
import sqlite3
from pathlib import Path

from fogvis.data_importer import collect_input_images, process_files

src = Path("media/test")
t = Path(__file__).resolve().parent / "_calibrate_tmp_db"
if t.exists():
    shutil.rmtree(t, ignore_errors=True)
t.mkdir(parents=True)
import_dir = t / "import"
for d in sorted(src.iterdir()):
    if d.is_dir():
        dest = import_dir / d.name
        dest.mkdir(parents=True, exist_ok=True)
        for f in d.iterdir():
            if f.is_file():
                shutil.copy2(f, dest / f.name)
db_dir = t / "db"
inputs = collect_input_images(import_dir)
print("inputs:", len(inputs))
process_files(inputs, db_dir, max_workers=1)
conn = sqlite3.connect(db_dir / "database.sqlite3")
for tbl in [
    "scene",
    "camera",
    "fog",
    "fog_type",
    "light",
    "light_type",
    "environment",
    "environment_light",
    "image",
    "view",
    "view_image",
    "visibility_distance",
]:
    c = conn.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
    print(f"{tbl}: {c}")
print("---")
rows = conn.execute(
    'SELECT fileName FROM image WHERE fileType="color" ORDER BY fileName'
).fetchall()
for r in rows:
    print("color:", r[0])
print("---")
dup = conn.execute(
    """
    SELECT cameraID, sceneID, environmentID, COUNT(*) AS n
    FROM view
    GROUP BY cameraID, sceneID, environmentID
    HAVING COUNT(*) > 1
    """
).fetchall()
print("pre-cleanup dup groups:", len(dup))
conn.close()
shutil.rmtree(t, ignore_errors=True)