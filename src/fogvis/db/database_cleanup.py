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
    """Delete image files on disk that are no longer referenced by the database.

    Two kinds of orphans are handled:

    1. **Orphaned image rows** - rows in the ``image`` table (color images
       *and* ray mask images: ray_distance / ray_normalized_distance /
       ray_validity) that are not referenced by any ``view.colorImageID``
       or ``view_image.imageID``. Dangling ``view_image`` links (whose view
       no longer exists) are dropped first so mask rows held only by a dead
       view's link are recognized as orphaned. The row and its on-disk file
       are removed.
    2. **Disk-only orphans** - files that exist in the images directory but
       have no corresponding row in the ``image`` table (for example, files
       left behind after image rows were deleted elsewhere). These files are
       removed.

    JSON sidecar files are not tracked in the ``image`` table. A sidecar is
    only considered orphaned when its paired color image (same stem,
    ``.png``) is also gone; sidecars whose color image is still referenced
    are kept.

    Only files with a known image/sidecar extension (``.png``, ``.tif``,
    ``.tiff``, ``.json``) are ever deleted, so unrelated files in the
    images directory are left untouched.
    """

    # Extensions that are safe to delete when orphaned. Anything else in the
    # images directory is ignored even if it has no image row.
    DELETABLE_EXTENSIONS: frozenset[str] = frozenset({".png", ".tif", ".tiff", ".json"})

    def __init__(self, db: Database) -> None:
        self.db = db

    @property
    def images_dir(self) -> Path:
        return Path(self.db.import_dir)

    def sweep_orphaned_files(self, dry_run: bool = False) -> CleanupReport:
        """Remove orphaned image rows/files and disk-only orphan files.

        When ``dry_run`` is True nothing is deleted; the returned report
        counts what *would* be removed.
        """
        report = CleanupReport()

        # 1. Orphaned image rows -> delete the row and its file.
        with self.db as con:
            cur = con.cursor()
            # Drop view_image links whose view no longer exists. Without this
            # a mask image row held only by a dead view's view_image link
            # would still look "referenced", escape cleanup, and block the
            # image delete via the view_image.imageID foreign key.
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

        # 2. Files on disk with no image row -> delete the file. The set of
        #    referenced filenames is read after the row deletions above have
        #    committed, so files for the rows just removed are now orphans.
        referenced = self._referenced_filenames()
        self._sweep_disk_orphans(referenced, report, dry_run)

        return report

    def sweep_orphaned_disk_files(self, dry_run: bool = False) -> CleanupReport:
        """Delete image files on disk that have no row in the ``image`` table.

        This does NOT modify any database rows - it only removes orphaned
        files from disk. Use this when the database has already been cleaned
        up by other means (e.g. records removed by a maintenance script) and
        the leftover files just need to be swept.
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
        view_image.

        Covers both color images (linked via view.colorImageID) and ray mask
        images (linked via view_image.imageID). Call after dangling
        view_image rows have been removed so masks held only by a dead view
        are included."""
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
        """Delete files in the images directory that are not referenced by
        the ``image`` table. JSON sidecars are deleted only when their
        paired color image is also gone."""
        if not self.images_dir.is_dir():
            return

        for path in sorted(self.images_dir.iterdir()):
            if not path.is_file():
                continue
            if path.suffix.lower() not in self.DELETABLE_EXTENSIONS:
                continue
            if path.name in referenced:
                continue

            # JSON sidecars are not tracked in image. Treat one as orphaned
            # only when its paired color image (.png) is also unreferenced.
            if path.suffix.lower() == ".json":
                color_name = path.with_suffix(".png").name
                if color_name in referenced:
                    continue

            if dry_run:
                report.disk_orphans_removed += 1
            else:
                self._delete_image_file(path.name, report)
                report.disk_orphans_removed += 1

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
