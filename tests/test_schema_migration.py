import sqlite3

from fogvis.db.database import Database


def _create_legacy_view_database(db_dir):
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "database.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE view(
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                colorImageID   INTEGER NOT NULL,
                cameraID       INTEGER NOT NULL,
                sceneID        INTEGER NOT NULL,
                environmentID  INTEGER NOT NULL
            );
            INSERT INTO view (colorImageID, cameraID, sceneID, environmentID)
            VALUES (1, 2, 3, 4);
            """
        )
    return db_path


def _columns(db_path, table):
    with sqlite3.connect(db_path) as connection:
        return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}


def _tables(db_path):
    with sqlite3.connect(db_path) as connection:
        return {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }


def test_init_tables_adds_skybox_to_legacy_database(tmp_path):
    db_dir = tmp_path / "legacy"
    db_path = _create_legacy_view_database(db_dir)

    Database(db_dir).init_tables()

    assert "skybox" in _tables(db_path)
    assert "skyboxID" in _columns(db_path, "view")
    with sqlite3.connect(db_path) as connection:
        assert connection.execute("SELECT skyboxID FROM view").fetchone() == (None,)


def test_init_tables_migration_is_idempotent(tmp_path):
    db_dir = tmp_path / "legacy"
    db_path = _create_legacy_view_database(db_dir)

    Database(db_dir).init_tables()
    Database(db_dir).init_tables()

    assert "skyboxID" in _columns(db_path, "view")
    with sqlite3.connect(db_path) as connection:
        assert connection.execute("SELECT count(*) FROM view").fetchone()[0] == 1
