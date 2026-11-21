import logging
from dataclasses import dataclass, field
from pathlib import Path

from .database import Database

logger = logging.getLogger(__name__)


@dataclass
class CleanupReport:
    images_removed: int = 0
    files_deleted: int = 0
    disk_orphans_removed: int = 0
    view_image_links_removed: int = 0
    errors: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"Cleanup: removed {self.images_removed} orphaned image rows "
            f"(color + masks), {self.view_image_links_removed} dangling "
            f"view_image links, {self.files_deleted} files on disk "
            f"({self.disk_orphans_removed} disk-only orphans), "
            f"{len(self.errors)} error(s)"
        )


class DatabaseCleanup:
    """Delete image rows/files and disk files no longer referenced by the database.

    The images directory holds date-named subdirectories
    (``images/<date>/<file>``) and the ``image`` table stores each file's
    path relative to that directory.

    Orphaned image rows (referenced by no ``view.colorImageID`` or
    ``view_image.imageID``) and their files are removed; dangling
    ``view_image`` links are dropped first so mask rows held only by a dead
    view count as orphaned. Files on disk with no image row are removed,
    along with date subdirectories the sweep empties. JSON sidecars are not
    tracked in the ``image`` table: a sidecar is deleted only when its paired
    color image (same stem, ``.png``) is also gone.
    """

    # Anything else in the images directory is never deleted, image row or not.
    DELETABLE_EXTENSIONS: frozenset[str] = frozenset({".png", ".tif", ".tiff", ".json"})

    def __init__(self, db: Database) -> None:
        self.db = db

    @property
    def images_dir(self) -> Path:
        return Path(self.db.import_dir)

    def sweep_orphaned_files(self, dry_run: bool = False) -> CleanupReport:
        """Remove orphaned image rows/files and disk-only orphan files.

        When ``dry_run`` is True nothing is deleted; the report counts what
        would be removed.
        """
        report = CleanupReport()

        with self.db as con:
            cur = con.cursor()
            # Drop dangling view_image links first, so mask rows held only by
            # a dead view count as orphaned (and don't block the image delete).
            if dry_run:
                report.view_image_links_removed = cur.execute(
                    """SELECT COUNT(*) FROM view_image vi
                       WHERE NOT EXISTS (
                           SELECT 1 FROM view v WHERE v.id = vi.viewID
                       )"""
                ).fetchone()[0]
            else:
                cur.execute(
                    """DELETE FROM view_image
                       WHERE NOT EXISTS (
                           SELECT 1 FROM view v WHERE v.id = view_image.viewID
                       )"""
                )
                report.view_image_links_removed = cur.rowcount

            orphan_rows = self._find_orphaned_image_rows(con)
            for image_id, file_name in orphan_rows:
                if dry_run:
                    report.images_removed += 1
                    continue
                self._delete_image_file(file_name, report)
                con.execute("DELETE FROM image WHERE id = ?", (image_id,))
                report.images_removed += 1

        # Read referenced names after the row deletions have committed, so
        # files for rows just removed count as orphaned.
        referenced = self._referenced_filenames()
        self._sweep_disk_orphans(referenced, report, dry_run)

        return report

    def sweep_orphaned_disk_files(self, dry_run: bool = False) -> CleanupReport:
        """Delete image files on disk that have no row in the ``image`` table.

        No database rows are modified; use this once the database has been
        cleaned up by other means and only the leftover files need sweeping.
        """
        report = CleanupReport()
        referenced = self._referenced_filenames()
        self._sweep_disk_orphans(referenced, report, dry_run)
        return report

    # Backwards-compatible alias for the previous public name.
    def sweep_orphaned_images(self) -> CleanupReport:
        return self.sweep_orphaned_files()

    def _find_orphaned_image_rows(self, con) -> list[tuple[int, str]]:
        """Return (id, fileName) for image rows referenced by no view or
        view_image (color or ray mask). Call after dangling view_image rows
        have been removed."""
        cur = con.cursor()
        cur.execute(
            """
            SELECT img.id, img.fileName
            FROM image AS img
            WHERE NOT EXISTS (
                SELECT 1 FROM view v WHERE v.colorImageID = img.id
            )
            AND NOT EXISTS (
                SELECT 1 FROM view_image vi WHERE vi.imageID = img.id
            )
            """
        )
        return cur.fetchall()

    def _referenced_filenames(self) -> set[str]:
        with self.db as con:
            return {row[0] for row in con.execute("SELECT fileName FROM image")}

    def _sweep_disk_orphans(
        self,
        referenced: set[str],
        report: CleanupReport,
        dry_run: bool,
    ) -> None:
        """Delete files under the images dir (including date-named
        subdirectories) not referenced by the ``image`` table; sidecars are
        deleted only when their paired color image is also gone."""
        if not self.images_dir.is_dir():
            return

        for path in sorted(self.images_dir.rglob("*")):
            if not path.is_file():
                continue
            if path.suffix.lower() not in self.DELETABLE_EXTENSIONS:
                continue
            rel_name = path.relative_to(self.images_dir).as_posix()
            if rel_name in referenced:
                continue

            if path.suffix.lower() == ".json":
                color_name = path.with_suffix(".png").relative_to(
                    self.images_dir
                ).as_posix()
                if color_name in referenced:
                    continue

            if dry_run:
                report.disk_orphans_removed += 1
            else:
                self._delete_image_file(rel_name, report)
                report.disk_orphans_removed += 1

        if not dry_run:
            self._remove_empty_date_dirs()

    def _remove_empty_date_dirs(self) -> None:
        """Best-effort removal of subdirectories emptied by the sweep."""
        for sub in self.images_dir.iterdir():
            if not sub.is_dir():
                continue
            try:
                sub.rmdir()  # only succeeds when the directory is empty
            except OSError:
                pass

    def _delete_image_file(self, file_name: str, report: CleanupReport) -> None:
        if not file_name:
            return
        path = self.images_dir / file_name
        try:
            path.unlink(missing_ok=True)
            report.files_deleted += 1
        except OSError as exc:
            report.errors.append(str(path))
            logger.warning("Failed to delete image file %s: %s", path, exc)
