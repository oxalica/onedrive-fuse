PRAGMA foreign_keys = TRUE;
PRAGMA temp_store = MEMORY;

PRAGMA synchronous = NORMAL;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS item (
    ino             INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- `id` is NULL iff this is a locally created item that needs an upload.
    id              TEXT NULL,
    parent_ino      INTEGER NULL CHECK ((parent_ino IS NULL) == (ino == 1))
                                 REFERENCES item (ino)
                                 ON UPDATE RESTRICT
                                 ON DELETE NO ACTION, -- Allow batch delete but no auto-recursion.
    name            TEXT NOT NULL,

    is_directory    INTEGER NOT NULL,
    -- Directories are reported as 0-sized.
    size            INTEGER NOT NULL CHECK (NOT is_directory OR size == 0),
    created_time    INTEGER NOT NULL,
    modified_time   INTEGER NOT NULL,

    -- 's': Synchronized.
    -- 't': Time is updated locally, or this is a new item.
    -- 'c': The current time values are in committing progress.
    state           TEXT NOT NULL
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS item_id ON item (id ASC);
CREATE UNIQUE INDEX IF NOT EXISTS item_children_by_ino ON item (parent_ino ASC, ino ASC);
CREATE UNIQUE INDEX IF NOT EXISTS item_children_by_name ON item (parent_ino ASC, name ASC);
-- NB. The condition must be written as OR-expr so that sqlite can recognize it
-- when either side matches a query.
CREATE        INDEX IF NOT EXISTS item_state ON item (state) WHERE state == 't' OR state == 'c';

-- Reserve low inodes.
INSERT OR IGNORE INTO sqlite_sequence VALUES ('item', 100);

CREATE TABLE IF NOT EXISTS sync (
    time            INTEGER NOT NULL,
    delta_url       TEXT NOT NULL
) STRICT;

-- Local directories to be uploaded, in depth first order.
CREATE VIEW IF NOT EXISTS new_directories AS
    WITH RECURSIVE
        new_dir(ino, parent_id, child_path, created_time, modified_time) AS (
            -- Initial cases: lowest new directories with their parents existing on the remote.
            SELECT
                item.ino,
                parent.id,
                '/' || item.name,
                item.created_time,
                item.modified_time
            FROM item
            JOIN item AS parent ON
                parent.ino = item.parent_ino AND
                parent.id IS NOT NULL
            WHERE item.id IS NULL

            UNION ALL
            SELECT
                item.ino,
                parent_id,
                child_path || '/' || item.name,
                item.created_time,
                item.modified_time
            FROM new_dir
            JOIN item ON item.parent_ino = new_dir.ino
            WHERE item.id IS NULL
        )
    SELECT *
    FROM new_dir;

CREATE VIEW IF NOT EXISTS time_updates AS
    SELECT *
    FROM item
    WHERE state == 't' AND id IS NOT NULL;

-- TODO: Review the time complexity.
CREATE VIEW IF NOT EXISTS updates(ino, id, child_path, created_time, modified_time) AS
    SELECT * FROM new_directories
    UNION ALL
    SELECT ino, id, NULL, created_time, modified_time FROM time_updates;


-- Recover from last abort during uploading, if exists.
UPDATE item SET state = 't' WHERE state = 'c';
