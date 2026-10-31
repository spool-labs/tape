-- The block store's whole schema. Each statement is safe to run again on open.

CREATE TABLE IF NOT EXISTS meta (
    name  TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

-- A filtered confirmed block's result body in each encoding. A skipped slot
-- has neither body. The slot is the rowid, and rows run to hundreds of bytes,
-- so this table keeps its rowid.
CREATE TABLE IF NOT EXISTS blocks (
    slot   INTEGER PRIMARY KEY,
    base64 BLOB,
    json   BLOB,
    CHECK ((base64 IS NULL) = (json IS NULL))
);
