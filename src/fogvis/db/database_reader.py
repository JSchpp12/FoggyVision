import sqlite3
from typing import Optional, List
from pathlib import Path
import os
from dataclasses import asdict
from fogvis.db.entities import ImageEntity, CoordinateEntity, SceneEntity, ViewEntity
from fogvis.db.database import Database


class DatabaseReader:
    """
    Handles reading entities from the database, mirroring the logic
    of DatabaseWriter but for retrieval operations.
    """

    def __init__(self, db_path: Path) -> None:
        self.db: Database = Database(db_path)

    def read_image_by_file_name(self, file_name: str) -> Optional[ImageEntity]:
        """
        Reads a single ImageEntity record based on its unique file_name.

        Args:
            file_name: The unique file name of the image.

        Returns:
            An ImageEntity object if found, otherwise None.
        """
        try:
            with self.db as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT fileName, fileType, width, height, checksum FROM image WHERE fileName = ?",
                    (file_name,),
                )
                row = cur.fetchone()

                if row is None:
                    return None

                return ImageEntity.from_row(row)
        except sqlite3.Error as e:
            print(f"Database error reading image by file name: {e}")
            return None

    def read_view_by_id(self, view_id: int) -> Optional[ViewEntity]:
        """
        Reads a single ViewEntity record based on its id.

        Args:
            view_id: The unique id of the view.

        Returns:
            A ViewEntity object if found, otherwise None.
        """
        try:
            with self.db as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT colorImageID, cameraID, sceneID, environmentID FROM view WHERE id = ?",
                    (view_id,),
                )
                row = cur.fetchone()

                if row is None:
                    return None

                return ViewEntity(
                    color_image_id=row[0],
                    camera_id=row[1],
                    scene_id=row[2],
                    environment_id=row[3],
                )
        except sqlite3.Error as e:
            print(f"Database error reading view by id: {e}")
            return None

    # Add other read methods here later, e.g., read_scene, read_camera, etc.
