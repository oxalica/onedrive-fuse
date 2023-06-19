PRAGMA foreign_keys = TRUE;
PRAGMA temp_store = MEMORY;

PRAGMA synchronous = NORMAL;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS item (
    ino             INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    id              TEXT NULL UNIQUE, -- NULL when dirty.
    parent_ino      INTEGER NULL CHECK ((parent_ino IS NULL) == (ino == 1)),
    name            TEXT NOT NULL,

    is_directory    INTEGER NOT NULL,
    size            INTEGER NOT NULL, -- Zero for directories.
    created_time    INTEGER NOT NULL,
    modified_time   INTEGER NOT NULL
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS item_children_by_ino ON item (parent_ino ASC, ino ASC);
CREATE UNIQUE INDEX IF NOT EXISTS item_children_by_name ON item (parent_ino ASC, name ASC);

CREATE TABLE IF NOT EXISTS sync (
    time            INTEGER NOT NULL,
    delta_url       TEXT NOT NULL
) STRICT;
