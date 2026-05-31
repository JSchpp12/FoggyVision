import sqlite3
from sqlite3 import Connection
from typing import Optional
from pathlib import Path
import os

from .schema import (
    create_scene_table,
    create_coordinate_table,
    create_camera_table,
    create_fog_type_table,
    create_fog_table,
    create_light_type_table,
    create_light_table,
    create_environment_light_table,
    create_environment_table,
    create_image_table,
)

DB_FILE_NAME: str = "database.sqlite3"


class Database:

    @staticmethod
    def Prep_DB_Path(db_path: os.PathLike) -> os.PathLike:
        if os.path.isfile(db_path):
            return db_path
        else:
            if not os.path.exists(db_path):
                os.makedirs(db_path)
            return Path(os.path.join(db_path, DB_FILE_NAME))

    def __init__(self, db_path: os.PathLike) -> None:
        self.db_path: os.PathLike = self.Prep_DB_Path(db_path)
        self.import_dir : Path = Path(os.path.join(Path(self.db_path).parent), "images")
        self.conn: Optional[sqlite3.Connection] = None

    def __enter__(self) -> sqlite3.Connection:
        return self.get_connection()

    def __exit__(self, exc_type, exc, tb):
        self.close_connection()

    def close_connection(self):
        con = self.get_connection()
        con.commit()
        con.close()
        self.conn = None

    def get_connection(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")

        return self.conn

    def init_tables(self) -> None:
        with self as conn:
            cur = conn.cursor()
            create_coordinate_table(cur)
            create_scene_table(cur)
            create_camera_table(cur)
            create_fog_type_table(cur)
            create_fog_table(cur)
            create_light_type_table(cur)
            create_light_table(cur)
            create_environment_table(cur)
            create_environment_light_table(cur)
            create_image_table(cur)

            create_image_index = (
                "CREATE INDEX IF NOT EXISTS idx_image_filepath ON image(filePath)"
            )
            cur.execute(create_image_index)
