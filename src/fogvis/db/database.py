import sqlite3
from sqlite3 import Connection
from typing import Optional
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


class Database:
    def __init__(self, db_path: os.PathLike) -> None:
        self.db_path: os.PathLike = db_path
        self.conn : Optional[sqlite3.Connection] = None

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
            
            create_image_index = "CREATE INDEX IF NOT EXISTS idx_image_filepath ON image(filePath)"
            cur.execute(create_image_index)
