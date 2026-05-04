import argparse
from pathlib import Path
from .data_importer import main as import_main


def main():
    parser = argparse.ArgumentParser(
        prog="FoggyVision-Cli", description="Cli interface for foggy vision"
    )
    subparsers = parser.add_subparsers(title="Commands", dest="command")
    subparsers.required = True

    importer_parser = subparsers.add_parser(
        "import", help="Import images into database"
    )
    importer_parser.add_argument("--database")
    importer_parser.add_argument("--images")

    args = parser.parse_args()
    image_dir: Path = Path(args.images)
    database_dir: Path = Path(args.database)
    import_main(import_dir=image_dir, db_dir=database_dir)


if __name__ == "__main__":
    main()
