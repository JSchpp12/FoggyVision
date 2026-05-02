import sqlite3


def create_coordinate_table(cur: sqlite3.Cursor) -> None:
    cmd = """
    CREATE TABLE IF NOT EXISTS coordinate(
        id      INTEGER PRIMARY KEY AUTOINCREMENT, 
        lat     REAL NOT NULL, 
        lon     REAL NOT NULL
    )"""
    cur.execute(cmd)


def create_scene_table(cur: sqlite3.Cursor) -> None:
    cmd = """
    CREATE TABLE IF NOT EXISTS scene(
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        name                    TEXT NOT NULL UNIQUE, 
        coverageDistanceMiles       REAL NOT NULL,
        upperRightPositionID    INTEGER NOT NULL REFERENCES coordinate(id), 
        lowerLeftPositionID     INTEGER NOT NULL REFERENCES coordinate(id),
        centerPositionID        INTEGER NOT NULL REFERENCES coordinate(id) 
    )
    """
    cur.execute(cmd)


def create_camera_table(cur: sqlite3.Cursor) -> None:
    cmd = """
    CREATE TABLE IF NOT EXISTS camera (
        id              INTEGER PRIMARY KEY AUTOINCREMENT, 
        virtualPosition     TEXT NOT NULL, 
        sceneID        INTEGER REFERENCES scene(id) NOT NULL,
        lookDir        TEXT NOT NULL,
        fov REAL,
        nearClip REAL, 
        farClip REAL
    )"""
    cur.execute(cmd)


def create_fog_type_table(cur: sqlite3.Cursor) -> None:
    cmd = """
    CREATE TABLE IF NOT EXISTS fog_type (
        id              INTEGER PRIMARY KEY AUTOINCREMENT, 
        name            TEXT NOT NULL
    )"""
    cur.execute(cmd)


def create_fog_table(cur: sqlite3.Cursor) -> None:
    cmd = """
    CREATE TABLE IF NOT EXISTS fog (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        typeID INTEGER NOT NULL REFERENCES fog_type(id), 
        sceneID INTEGER NOT NULL REFERENCES scene(id),
        expFogDensity REAL,
        linearNearDistance REAL,
        linearFarDistance REAL, 
        marchedCutoff        REAL,
        marchedDefaultDensity REAL,
        marchedDensityMultiplier REAL,
        marchedLightG     REAL,
        marchedSigmaAbsorption REAL,
        marchedSigmaScattering REAL,
        marchedStepSizeDist  REAL,
        marchedStepSizeDistLight REAL
    )"""
    cur.execute(cmd)


def create_light_type_table(cur: sqlite3.Cursor) -> None:
    cmd = """
    CREATE TABLE IF NOT EXISTS light_type(
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        name TEXT NOT NULL
    )
    """
    cur.execute(cmd)


def create_light_table(cur: sqlite3.Cursor) -> None:
    cmd = """
    CREATE TABLE IF NOT EXISTS light(
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ambient         TEXT NOT NULL,
        diffuse         TEXT NOT NULL,
        specular        TEXT NOT NULL,
        virtualDirection       TEXT NOT NULL,
        virtualPosition        TEXT NOT NULL,
        enabled       BOOLEAN NOT NULL,
        innerDiameter REAL NOT NULL,
        outerDiameter REAL NOT NULL,
        luminance     INTEGER NOT NULL,
        typeID          INTEGER NOT NULL REFERENCES light_type(id)
    )"""
    cur.execute(cmd)


def create_environment_table(cur: sqlite3.Cursor) -> None:
    cmd = """
    CREATE TABLE IF NOT EXISTS environment(
        id  INTEGER PRIMARY KEY AUTOINCREMENT,
        fogID INTEGER NOT NULL REFERENCES fog(id)
    )"""
    cur.execute(cmd)


def create_environment_light_table(cur: sqlite3.Cursor) -> None:
    cmd = """
    CREATE TABLE IF NOT EXISTS environment_light(
        lightID        INTEGER NOT NULL REFERENCES light(id),
        environmentID  INTEGER NOT NULL REFERENCES environment(id)
    )"""
    cur.execute(cmd)


def create_image_table(cur: sqlite3.Cursor) -> None:
    cmd = """
    CREATE TABLE IF NOT EXISTS image(
        id                      INTEGER PRIMARY KEY AUTOINCREMENT, 
        filePath               TEXT NOT NULL UNIQUE, 
        visibilityDistance     REAL NOT NULL,
        cameraID               INTEGER NOT NULL REFERENCES camera(id),
        sceneID                INTEGER NOT NULL REFERENCES scene(id),
        environmentID          INTEGER NOT NULL REFERENCES environment(id) 
    )"""
    cur.execute(cmd)
