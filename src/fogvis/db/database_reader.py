import sqlite3
from typing import Optional, List
from pathlib import Path
import os
from dataclasses import asdict
from fogvis.db.entities import ImageEntity, CoordinateEntity, SceneEntity
from fogvis.db.database import Database

class DatabaseReader:
    """
    Handles reading entities from the database, mirroring the logic 
    of DatabaseWriter but for retrieval operations.
    """
    def __init__(self, db_path: Path) -> None:
        self.db: Database = Database(db_path)

    def read_image_by_file_path(self, file_path: str) -> Optional[ImageEntity]:
        """
        Reads a single ImageEntity record based on its unique file_path.

        Args:
            file_path: The unique file path of the image.

        Returns:
            An ImageEntity object if found, otherwise None.
        """
        try:
            with self.db as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT file_path, visibility_distance, camera_id, scene_id, environment_id FROM image WHERE file_path = ?",
                    (file_path,)
                )
                row = cur.fetchone()
                
                if row is None:
                    return None

                return ImageEntity(
                    file_path=row[0],
                    visibility_distance=row[1],
                    camera_id=row[2],
                    scene_id=row[3],
                    environment_id=row[4]
                )
        except sqlite3.Error as e:
            print(f"Database error reading image by file path: {e}")
            return None

    # Add other read methods here later, e.g., read_scene, read_camera, etc.