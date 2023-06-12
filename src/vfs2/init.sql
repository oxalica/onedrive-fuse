PRAGMA foreign_keys = TRUE;

CREATE TABLE IF NOT EXISTS item (
    ino             INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    id              TEXT NULL UNIQUE, -- NULL when dirty.
    is_directory    INTEGER NOT NULL,
    parent_ino      INTEGER NULL,
    name            TEXT NULL, -- NULL for root.
    size            INTEGER NOT NULL, -- Zero for directories.
    created_time    INTEGER NOT NULL,
    modified_time   INTEGER NOT NULL,

    UNIQUE (parent_ino, name)
) STRICT;

CREATE INDEX IF NOT EXISTS item_children_by_ino ON item (parent_ino ASC, ino ASC);

CREATE TABLE IF NOT EXISTS sync (
    time            INTEGER NOT NULL,
    delta_url       TEXT NOT NULL
) STRICT;
