import argparse
from pathlib import Path
from .data_importer import main as import_main
from .data_importer import cleanup_db


def main():
    parser = argparse.ArgumentParser(
        prog="FoggyVision-Cli", description="Cli interface for foggy vision"
    )
    subparsers = parser.add_subparsers(title="Commands", dest="command")
    subparsers.required = True

    importer_parser = subparsers.add_parser(
        "import", help="Import images into database"
    )
    importer_parser.add_argument("--database", required=True)
    importer_parser.add_argument("--images", required=True)
    importer_parser.add_argument(
        "--format",
        choices=("png", "jpg"),
        default="png",
        help="Color image format in the database (default: png).",
    )
    importer_parser.add_argument(
        "--jpeg-quality",
        type=int,
        default=95,
        help="JPEG quality (1-100) used when --format=jpg. Default: 95.",
    )

    cleanup_parser = subparsers.add_parser(
        "cleanup",
        help="Remove duplicate view records and orphaned files from the database",
    )
    cleanup_parser.add_argument("--database", required=True)

    args = parser.parse_args()

    if args.command == "cleanup":
        database_dir: Path = Path(args.database)
        cleanup_db(database_dir)
        return

    image_dir: Path = Path(args.images)
    database_dir = Path(args.database)
    import_main(
        import_dir=image_dir,
        db_dir=database_dir,
        image_format=args.format,
        jpeg_quality=args.jpeg_quality,
    )


if __name__ == "__main__":
    main()
