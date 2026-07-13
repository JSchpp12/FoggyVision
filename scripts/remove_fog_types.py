#!/usr/bin/env python
"""Remove all records for one or more fog types from the database.

This is a one-off maintenance script, intentionally kept OUT of the fogvis
package (it lives under scripts/). It deletes every database row that
belongs to the given fog type(s) - visibility_distance, view_image, view,
image, environment_light, environment, and fog - while leaving the shared
infrastructure (scene, camera rows still in use, lights, fog_type rows) and
all other fog types untouched.

It does NOT delete image files on disk. That is handled separately by the
DatabaseCleanup disk sweep (run on the machine that actually holds the
files), so this script only mutates the database.

Usage
-----
    # preview what would be removed (no changes)
    python scripts/remove_fog_types.py --dry-run

    # remove linear + exponential (the defaults), keeping the fog_type rows
    python scripts/remove_fog_types.py

    # target different fog types / a different database
    python scripts/remove_fog_types.py --fog-types linear exponential --db /path/to/db

Back up database.sqlite3 before running this for real.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from fogvis.db.database import Database

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_DIR = REPO_ROOT / "media" / "db"
DEFAULT_FOG_TYPES = ["linear", "exponential"]


def _resolve_fog_type_ids(con: sqlite3.Connection, names: list[str]) -> list[int]:
    placeholders = ",".join("?" * len(names))
    rows = con.execute(
        f"SELECT name, id FROM fog_type WHERE name IN ({placeholders})", names
    ).fetchall()
    found = {name: fid for name, fid in rows}
    missing = [n for n in names if n not in found]
    if missing:
        raise SystemExit(f"foam_type row(s) not found: {missing}")
    return [found[n] for n in names]


def _ensure_indexes(con: sqlite3.Connection) -> None:
    """Create foreign-key indexes used by the deletes if they don't exist.

    These are cheap, non-destructive, and make the removal fast on large
    databases (the schema ships with almost no FK indexes). They are left in
    place afterwards because they help normal queries too.
    """
    con.executescript("""
        CREATE INDEX IF NOT EXISTS idx_fog_typeID        ON fog(typeID);
        CREATE INDEX IF NOT EXISTS idx_env_fogID         ON environment(fogID);
        CREATE INDEX IF NOT EXISTS idx_envlight_env      ON environment_light(environmentID);
        CREATE INDEX IF NOT EXISTS idx_view_env          ON view(environmentID);
        CREATE INDEX IF NOT EXISTS idx_view_cam          ON view(cameraID);
        CREATE INDEX IF NOT EXISTS idx_view_color        ON view(colorImageID);
        CREATE INDEX IF NOT EXISTS idx_viewimage_view    ON view_image(viewID);
        CREATE INDEX IF NOT EXISTS idx_viewimage_image   ON view_image(imageID);
        CREATE INDEX IF NOT EXISTS idx_visdist_view      ON visibility_distance(viewID);
        """)


def _build_scope_tables(con: sqlite3.Connection, ft_ids: list[int]) -> None:
    """Populate temp tables for the fog rows / environments / views / images
    that are in scope. Must run before any deletes."""
    ph = ",".join("?" * len(ft_ids))

    con.execute("DROP TABLE IF EXISTS _scope_fogs")
    con.execute("CREATE TEMP TABLE _scope_fogs (id INTEGER PRIMARY KEY)")
    con.execute(
        f"INSERT INTO _scope_fogs SELECT id FROM fog WHERE typeID IN ({ph})", ft_ids
    )

    con.execute("DROP TABLE IF EXISTS _scope_envs")
    con.execute("CREATE TEMP TABLE _scope_envs (id INTEGER PRIMARY KEY)")
    con.execute(
        "INSERT INTO _scope_envs SELECT id FROM environment "
        "WHERE fogID IN (SELECT id FROM _scope_fogs)"
    )

    con.execute("DROP TABLE IF EXISTS _scope_views")
    con.execute("CREATE TEMP TABLE _scope_views (id INTEGER PRIMARY KEY)")
    con.execute(
        "INSERT INTO _scope_views SELECT id FROM view "
        "WHERE environmentID IN (SELECT id FROM _scope_envs)"
    )

    con.execute("DROP TABLE IF EXISTS _scope_images")
    con.execute("CREATE TEMP TABLE _scope_images (id INTEGER PRIMARY KEY)")
    con.execute("""INSERT INTO _scope_images
           SELECT colorImageID FROM view WHERE id IN (SELECT id FROM _scope_views)
           UNION
           SELECT imageID FROM view_image
           WHERE viewID IN (SELECT id FROM _scope_views)""")


def _drop_scope_tables(con: sqlite3.Connection) -> None:
    for t in ("_scope_images", "_scope_views", "_scope_envs", "_scope_fogs"):
        con.execute(f"DROP TABLE IF EXISTS {t}")


def _counts(con: sqlite3.Connection) -> dict[str, int]:
    tables = [
        "fog",
        "environment",
        "environment_light",
        "view",
        "visibility_distance",
        "view_image",
        "image",
        "camera",
    ]
    out: dict[str, int] = {}
    for t in tables:
        out[t] = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    return out


def remove_fog_types(
    db_dir: Path,
    fog_types: list[str],
    dry_run: bool = False,
    drop_type_rows: bool = False,
    delete_orphan_cameras: bool = True,
) -> None:
    db = Database(db_dir)
    with db as con:
        con.execute("PRAGMA foreign_keys = ON")
        ft_ids = _resolve_fog_type_ids(con, fog_types)
        ph = ",".join("?" * len(ft_ids))

        _ensure_indexes(con)
        before = _counts(con)
        _build_scope_tables(con, ft_ids)

        scope_counts = {
            "fog": con.execute("SELECT COUNT(*) FROM _scope_fogs").fetchone()[0],
            "environment": con.execute("SELECT COUNT(*) FROM _scope_envs").fetchone()[
                0
            ],
            "view": con.execute("SELECT COUNT(*) FROM _scope_views").fetchone()[0],
            "visibility_distance": con.execute(
                "SELECT COUNT(*) FROM visibility_distance "
                "WHERE viewID IN (SELECT id FROM _scope_views)"
            ).fetchone()[0],
            "view_image": con.execute(
                "SELECT COUNT(*) FROM view_image "
                "WHERE viewID IN (SELECT id FROM _scope_views)"
            ).fetchone()[0],
            "image": con.execute("SELECT COUNT(*) FROM _scope_images").fetchone()[0],
            "environment_light": con.execute(
                "SELECT COUNT(*) FROM environment_light "
                "WHERE environmentID IN (SELECT id FROM _scope_envs)"
            ).fetchone()[0],
        }

        print(f"Fog types to remove: {fog_types} (type ids {ft_ids})")
        print(f"{'table':22s} {'in_scope':>10s}")
        for k, v in scope_counts.items():
            print(f"  {k:20s} {v:>10d}")

        if dry_run:
            print("\n[dry-run] no changes made.")
            _drop_scope_tables(con)
            return

        # Delete child-first to satisfy foreign keys.
        con.execute(
            "DELETE FROM visibility_distance "
            "WHERE viewID IN (SELECT id FROM _scope_views)"
        )
        con.execute(
            "DELETE FROM view_image WHERE viewID IN (SELECT id FROM _scope_views)"
        )
        con.execute("DELETE FROM view WHERE id IN (SELECT id FROM _scope_views)")
        con.execute("""DELETE FROM image
               WHERE id IN (SELECT id FROM _scope_images)
                 AND NOT EXISTS (SELECT 1 FROM view WHERE colorImageID = image.id)
                 AND NOT EXISTS (SELECT 1 FROM view_image WHERE imageID = image.id)""")
        con.execute(
            "DELETE FROM environment_light "
            "WHERE environmentID IN (SELECT id FROM _scope_envs)"
        )
        con.execute("DELETE FROM environment WHERE id IN (SELECT id FROM _scope_envs)")
        con.execute("DELETE FROM fog WHERE id IN (SELECT id FROM _scope_fogs)")

        if drop_type_rows:
            con.execute(f"DELETE FROM fog_type WHERE id IN ({ph})", ft_ids)

        if delete_orphan_cameras:
            # Cameras no longer referenced by any view (e.g. the 170 cameras
            # only the removed fog types used). Marched keeps its 180.
            con.execute("""DELETE FROM camera
                   WHERE NOT EXISTS (
                       SELECT 1 FROM view WHERE view.cameraID = camera.id
                   )""")

        _drop_scope_tables(con)

        after = _counts(con)
        print("\nRemoved:")
        for k in before:
            delta = before[k] - after[k]
            if delta:
                print(f"  {k:20s} {before[k]:>8d} -> {after[k]:>8d}  (-{delta})")

        # Distinguish the type *definition* rows (kept by default) from the
        # actual fog bodies still present. The removed types show 0 bodies but
        # their fog_type row is intentionally retained for ID reuse.
        type_rows = [r[0] for r in con.execute("SELECT name FROM fog_type ORDER BY id")]
        bodies = con.execute(
            """SELECT ft.name, COUNT(f.id) AS n
               FROM fog_type ft LEFT JOIN fog f ON f.typeID = ft.id
               GROUP BY ft.id ORDER BY ft.id"""
        ).fetchall()
        kept = [name for name, n in bodies if n > 0]
        emptied = [name for name, n in bodies if n == 0]
        print(f"\nfog_type rows kept (definitions): {type_rows}")
        print("fog bodies remaining by type:")
        for name, n in bodies:
            print(f"  {name:14s} {n}")
        print(f"types with data left: {kept}")
        if emptied:
            print(f"types emptied (data removed, definition row kept): {emptied}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_DIR,
        help=f"Database directory (contains database.sqlite3). Default: {DEFAULT_DB_DIR}",
    )
    parser.add_argument(
        "--fog-types",
        nargs="+",
        default=DEFAULT_FOG_TYPES,
        help=f"Fog type names to remove. Default: {DEFAULT_FOG_TYPES}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be removed and make no changes.",
    )
    parser.add_argument(
        "--drop-type-rows",
        action="store_true",
        help="Also delete the fog_type rows themselves (default: keep them so a "
        "re-run reuses the same type IDs).",
    )
    parser.add_argument(
        "--keep-orphan-cameras",
        action="store_true",
        help="Do not delete cameras that become unreferenced after removal.",
    )
    args = parser.parse_args(argv)

    remove_fog_types(
        db_dir=args.db,
        fog_types=args.fog_types,
        dry_run=args.dry_run,
        drop_type_rows=args.drop_type_rows,
        delete_orphan_cameras=not args.keep_orphan_cameras,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
