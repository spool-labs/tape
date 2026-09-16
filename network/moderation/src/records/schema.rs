//! Tables of the records database

/// Created on open; every statement is idempotent
pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS reports (
    id INTEGER PRIMARY KEY,
    received_at INTEGER NOT NULL,
    target_kind TEXT NOT NULL,
    target TEXT NOT NULL,
    reason TEXT NOT NULL,
    note TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    reporter_hash TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reports_target ON reports (target_kind, target);

CREATE TABLE IF NOT EXISTS targets (
    kind TEXT NOT NULL,
    address TEXT NOT NULL,
    tape TEXT,
    state TEXT NOT NULL,
    decided_by TEXT,
    report_count INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (kind, address)
);
CREATE INDEX IF NOT EXISTS targets_tape ON targets (tape, state);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    target_kind TEXT NOT NULL,
    target TEXT NOT NULL,
    tape TEXT,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_due ON jobs (state, next_attempt_at);
CREATE INDEX IF NOT EXISTS jobs_target ON jobs (target_kind, target, state);

CREATE TABLE IF NOT EXISTS checks (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES jobs (id),
    track TEXT NOT NULL,
    checked_at INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    detail TEXT,
    content_type TEXT,
    input_type TEXT,
    read_bytes INTEGER,
    total_bytes INTEGER,
    is_complete INTEGER,
    response_id TEXT,
    model TEXT
);
CREATE INDEX IF NOT EXISTS checks_track ON checks (track);

CREATE TABLE IF NOT EXISTS check_categories (
    check_id INTEGER NOT NULL REFERENCES checks (id),
    result_index INTEGER NOT NULL,
    category TEXT NOT NULL,
    flagged INTEGER,
    score REAL,
    applied_input_types TEXT,
    PRIMARY KEY (check_id, result_index, category)
);

CREATE TABLE IF NOT EXISTS tapes (
    id INTEGER PRIMARY KEY,
    address TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL,
    key_path TEXT NOT NULL,
    state TEXT NOT NULL,
    next_track INTEGER NOT NULL DEFAULT 0,
    expiry_epoch INTEGER,
    capacity INTEGER,
    used INTEGER,
    observed_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS tapes_one_rack ON tapes (role) WHERE role = 'rack';

-- Every track the producer planned or found on its tapes; bytes are the text written
CREATE TABLE IF NOT EXISTS tracks (
    id INTEGER PRIMARY KEY,
    tape TEXT NOT NULL REFERENCES tapes (address),
    kind TEXT NOT NULL,
    bytes BLOB NOT NULL,
    key BLOB NOT NULL,
    state TEXT NOT NULL,
    track_number INTEGER,
    address TEXT UNIQUE,
    delete_sent_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (tape, track_number),
    CHECK ((track_number IS NULL) = (state IN ('writing', 'abandoned')))
);
CREATE INDEX IF NOT EXISTS tracks_state ON tracks (state, tape);
CREATE INDEX IF NOT EXISTS tracks_key ON tracks (tape, key);

-- track: the writing or present track holding the decision, NULL while it waits for one
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY,
    action TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    target TEXT NOT NULL,
    source TEXT NOT NULL,
    check_id INTEGER REFERENCES checks (id),
    created_at INTEGER NOT NULL,
    track INTEGER REFERENCES tracks (id),
    undone_at INTEGER
);
CREATE INDEX IF NOT EXISTS decisions_target ON decisions (target_kind, target);
CREATE INDEX IF NOT EXISTS decisions_track ON decisions (track);
CREATE INDEX IF NOT EXISTS decisions_queued ON decisions (id) WHERE track IS NULL AND undone_at IS NULL;
CREATE INDEX IF NOT EXISTS decisions_undone ON decisions (id) WHERE undone_at IS NOT NULL;

-- Every track that names a decision
CREATE TABLE IF NOT EXISTS entries (
    track INTEGER NOT NULL REFERENCES tracks (id),
    decision INTEGER NOT NULL REFERENCES decisions (id),
    PRIMARY KEY (track, decision)
);
CREATE INDEX IF NOT EXISTS entries_decision ON entries (decision, track);

-- Every rack track that names a blacklist tape
CREATE TABLE IF NOT EXISTS rack_records (
    track INTEGER NOT NULL REFERENCES tracks (id),
    tape TEXT NOT NULL REFERENCES tapes (address),
    PRIMARY KEY (track, tape)
);
CREATE INDEX IF NOT EXISTS rack_records_tape ON rack_records (tape, track);
";
