PRAGMA foreign_keys = TRUE;
PRAGMA temp_store = MEMORY;

PRAGMA synchronous = NORMAL;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS item (
    ino             INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    id              TEXT NULL, -- NULL for new items.
    parent_ino      INTEGER NULL CHECK ((parent_ino IS NULL) == (ino == 1)),
    name            TEXT NOT NULL,

    is_directory    INTEGER NOT NULL,
    size            INTEGER NOT NULL, -- Zero for directories.
    created_time    INTEGER NOT NULL,
    modified_time   INTEGER NOT NULL
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS item_id ON item (id ASC);
CREATE UNIQUE INDEX IF NOT EXISTS item_children_by_ino ON item (parent_ino ASC, ino ASC);
CREATE UNIQUE INDEX IF NOT EXISTS item_children_by_name ON item (parent_ino ASC, name ASC);

-- Reserve low inodes.
INSERT OR IGNORE INTO sqlite_sequence VALUES ('item', 100);

-- Local changed items.
-- NB. New directories without `id` are not necessary included here.
-- They will always be pushed.
CREATE TABLE IF NOT EXISTS dirty_item (
    ino             INTEGER NOT NULL PRIMARY KEY
                        REFERENCES item (ino) ON DELETE RESTRICT
) STRICT;

CREATE TABLE IF NOT EXISTS sync (
    time            INTEGER NOT NULL,
    delta_url       TEXT NOT NULL
) STRICT;
