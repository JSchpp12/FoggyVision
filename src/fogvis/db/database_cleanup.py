import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .database import Database

logger = logging.getLogger(__name__)


@dataclass
class DuplicateGroup:
    keeper_id: int
    duplicate_ids: list[int]


@dataclass
class CleanupReport:
    duplicate_views_removed: int = 0
    images_removed: int = 0
    files_deleted: int = 0
    environments_removed: int = 0
    environment_lights_removed: int = 0
    fog_removed: int = 0
    fog_types_removed: int = 0
    lights_removed: int = 0
    light_types_removed: int = 0
    cameras_removed: int = 0
    scenes_removed: int = 0
    coordinates_removed: int = 0
    duplicate_groups: list[DuplicateGroup] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"Cleanup: removed {self.duplicate_views_removed} duplicate views, "
            f"{self.images_removed} images, {self.files_deleted} files, "
            f"{self.environments_removed} environments, "
            f"{self.environment_lights_removed} environment_lights, "
            f"{self.fog_removed} fog, "
            f"{self.lights_removed} lights, {self.cameras_removed} cameras, "
            f"{self.scenes_removed} scenes, {self.coordinates_removed} coordinates"
        )


class DatabaseCleanup:
    """Removes duplicate view records and sweeps orphaned rows/files.

    A view is considered a duplicate of another when it shares the same
    (cameraID, sceneID, environmentID) tuple — i.e. the same rendered scene
    content. The colorImageID is intentionally ignored since a re-import
    produces a new image row and file even when the depicted view is
    identical.

    When duplicates are found the view with the lowest id (the earliest
    imported) is kept; newer duplicates are removed along with their
    associated view_image, visibility_distance, and image rows, and the
    image files are deleted from disk. Parent records (environment, fog,
    light, camera, scene, coordinate) that become unreferenced after the
    removal are swept as well.
    """

    def __init__(self, db: Database) -> None:
        self.db = db

    @property
    def images_dir(self) -> Path:
        return Path(self.db.import_dir)

    def find_duplicate_views(self) -> list[DuplicateGroup]:
        """Return one DuplicateGroup per (cameraID, sceneID, environmentID)
        group that has more than one view row. The lowest id in each group
        is the keeper; the rest are duplicates."""
        with self.db as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT id, cameraID, sceneID, environmentID
                FROM view
                ORDER BY cameraID, sceneID, environmentID, id ASC
                """
            )
            rows = cur.fetchall()

        groups: dict[tuple, list[int]] = {}
        for view_id, cam_id, scene_id, env_id in rows:
            groups.setdefault((cam_id, scene_id, env_id), []).append(view_id)

        result: list[DuplicateGroup] = []
        for ids in groups.values():
            if len(ids) > 1:
                result.append(DuplicateGroup(keeper_id=ids[0], duplicate_ids=ids[1:]))
        return result

    def remove_duplicate_views(self) -> CleanupReport:
        """Remove duplicate views and sweep orphaned rows/files.

        Runs in a single transaction. Parent-record sweep (environment, fog,
        light, camera, scene, coordinate) is always performed.
        """
        report = CleanupReport()
        groups = self.find_duplicate_views()
        report.duplicate_groups = groups

        if not groups:
            # Still sweep orphaned images/parents that may exist from prior
            # partial runs or manual edits.
            with self.db as con:
                self._sweep_orphaned_images(con, report)
                self._sweep_parents(con, report)
            return report

        duplicate_ids: list[int] = []
        for g in groups:
            duplicate_ids.extend(g.duplicate_ids)

        with self.db as con:
            cur = con.cursor()
            placeholders = ",".join(["?"] * len(duplicate_ids))

            # 1. Collect image file names for the duplicate views (color +
            #    masks) so we can delete the files afterwards. Only images
            #    that will become unreferenced should be deleted; we resolve
            #    that in sweep_orphaned_images after the rows are gone.
            cur.execute(
                f"""
                SELECT image.fileName
                FROM view
                JOIN image ON view.colorImageID = image.id
                WHERE view.id IN ({placeholders})
                """,
                duplicate_ids,
            )
            color_files = [row[0] for row in cur.fetchall() if row[0]]

            cur.execute(
                f"""
                SELECT image.fileName
                FROM view_image
                JOIN image ON view_image.imageID = image.id
                WHERE view_image.viewID IN ({placeholders})
                """,
                duplicate_ids,
            )
            mask_files = [row[0] for row in cur.fetchall() if row[0]]
            candidate_files = set(color_files) | set(mask_files)

            # 2. Delete child rows for duplicate views.
            cur.execute(
                f"DELETE FROM visibility_distance WHERE viewID IN ({placeholders})",
                duplicate_ids,
            )
            cur.execute(
                f"DELETE FROM view_image WHERE viewID IN ({placeholders})",
                duplicate_ids,
            )
            cur.execute(
                f"DELETE FROM view WHERE id IN ({placeholders})",
                duplicate_ids,
            )
            report.duplicate_views_removed = len(duplicate_ids)

            # 3. Sweep orphaned image rows (no longer referenced by any view
            #    or view_image). Delete the on-disk files for those rows.
            self._sweep_orphaned_images(con, report, candidate_files=candidate_files)

            # 4. Sweep orphaned parent records.
            self._sweep_parents(con, report)

        return report

    def sweep_orphaned_images(self) -> CleanupReport:
        """Standalone sweep of image rows not referenced by any view or
        view_image, deleting both the rows and their files on disk."""
        report = CleanupReport()
        with self.db as con:
            self._sweep_orphaned_images(con, report)
        return report

    def _sweep_orphaned_images(
        self,
        con,
        report: CleanupReport,
        candidate_files: Optional[set] = None,
    ) -> None:
        """Delete image rows not referenced by any view.colorImageID or
        view_image.imageID, along with their files on disk.

        If candidate_files is provided, only rows whose fileName is in
        that set are considered — this limits the sweep to images owned
        by the duplicate views just removed, leaving pre-existing
        orphans for a standalone sweep."""
        cur = con.cursor()
        if candidate_files is not None:
            if not candidate_files:
                return
            placeholders = ",".join(["?"] * len(candidate_files))
            cur.execute(
                f"""
                SELECT id, fileName FROM image
                WHERE fileName IN ({placeholders})
                  AND id NOT IN (SELECT colorImageID FROM view)
                  AND id NOT IN (SELECT imageID FROM view_image)
                """,
                tuple(candidate_files),
            )
        else:
            cur.execute(
                """
                SELECT id, fileName FROM image
                WHERE id NOT IN (SELECT colorImageID FROM view)
                  AND id NOT IN (SELECT imageID FROM view_image)
                """
            )
        orphan_rows = cur.fetchall()

        for image_id, file_name in orphan_rows:
            self._delete_image_file(file_name, report)
            cur.execute("DELETE FROM image WHERE id = ?", (image_id,))
            report.images_removed += 1

    def _delete_image_file(self, file_name: str, report: CleanupReport) -> None:
        if not file_name:
            return
        path = self.images_dir / file_name
        try:
            path.unlink(missing_ok=True)
            report.files_deleted += 1
        except OSError as exc:
            logger.warning("Failed to delete image file %s: %s", path, exc)

    def _sweep_parents(self, con, report: CleanupReport) -> None:
        """Delete unreferenced environment/fog/light/camera/scene/coordinate
        rows. Must be called after view/view_image/visibility_distance
        deletions for the current pass are complete."""
        cur = con.cursor()

        # environment not referenced by any view.
        cur.execute(
            """
            DELETE FROM environment
            WHERE id NOT IN (SELECT environmentID FROM view)
            """
        )
        report.environments_removed += cur.rowcount

        # environment_light rows for environments that no longer exist.
        cur.execute(
            """
            DELETE FROM environment_light
            WHERE environmentID NOT IN (SELECT id FROM environment)
            """
        )
        report.environment_lights_removed += cur.rowcount

        # fog not referenced by any environment.
        cur.execute(
            """
            DELETE FROM fog
            WHERE id NOT IN (SELECT fogID FROM environment)
            """
        )
        report.fog_removed += cur.rowcount

        # fog_type not referenced by any fog.
        cur.execute(
            """
            DELETE FROM fog_type
            WHERE id NOT IN (SELECT typeID FROM fog)
            """
        )
        report.fog_types_removed += cur.rowcount

        # light not referenced by any environment_light.
        cur.execute(
            """
            DELETE FROM light
            WHERE id NOT IN (SELECT lightID FROM environment_light)
            """
        )
        report.lights_removed += cur.rowcount

        # light_type not referenced by any light.
        cur.execute(
            """
            DELETE FROM light_type
            WHERE id NOT IN (SELECT typeID FROM light)
            """
        )
        report.light_types_removed += cur.rowcount

        # camera not referenced by any view.
        cur.execute(
            """
            DELETE FROM camera
            WHERE id NOT IN (SELECT cameraID FROM view)
            """
        )
        report.cameras_removed += cur.rowcount

        # scene not referenced by any view or camera.
        cur.execute(
            """
            DELETE FROM scene
            WHERE id NOT IN (SELECT sceneID FROM view)
              AND id NOT IN (SELECT sceneID FROM camera)
            """
        )
        report.scenes_removed += cur.rowcount

        # coordinate not referenced by any scene.
        cur.execute(
            """
            DELETE FROM coordinate
            WHERE id NOT IN (SELECT upperRightPositionID FROM scene)
              AND id NOT IN (SELECT lowerLeftPositionID FROM scene)
              AND id NOT IN (SELECT centerPositionID FROM scene)
            """
        )
        report.coordinates_removed += cur.rowcount