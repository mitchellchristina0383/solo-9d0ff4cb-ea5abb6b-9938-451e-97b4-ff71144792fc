"""SQLite schema and connection helpers.

All service state lives in a single SQLite database.  Every mutation runs in a
``BEGIN IMMEDIATE`` transaction, so concurrent writers serialise at the
database level instead of racing on Python objects.  The database uses WAL mode
plus ``synchronous=FULL``; a committed transaction therefore survives a process
kill and SQLite replays/recoveres the WAL on the next startup.

Storage model
-------------
``payloads`` holds the actual block bytes exactly once, keyed by
``(block_size, sha256(data))``.  Both base blocks and volume blocks reference a
payload row, and ``payloads.refcount`` counts every reference:

* ``base_blocks`` maps a read-only base image block to a payload.  A base
  contributes one reference for each of its blocks.
* ``volume_blocks`` maps every block of a writable volume to a payload.  When a
  volume is created it copies *references* to the base's payloads, not bytes,
  so N volumes inheriting the same base block still share one payload row.
* ``origin`` records whether a volume block is still inherited from the base,
  was explicitly zeroed, or was written by the client.

When a reference is dropped (overwrite, volume deletion, base retirement) the
refcount is decremented and, if it reaches zero, the payload row is deleted.
That is the reclamation step: bytes that no volume inherits become reusable
for later writes, and SQLite reuses the freed pages.

Bases move ``active -> retiring -> retired``.  Retirement is split into
*prepare* (volumes keep serving and accepting writes) and *commit*, which drops
the base's references and marks the base retired in one atomic transaction.  A
crash during commit leaves either the old state or the new state, never a
half-retired base.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA_VERSION = 1

_SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bases (
    id                     TEXT PRIMARY KEY,
    name                   TEXT NOT NULL UNIQUE,
    block_size             INTEGER NOT NULL CHECK (block_size > 0),
    block_count            INTEGER NOT NULL CHECK (block_count > 0),
    status                 TEXT NOT NULL
                           CHECK (status IN ('active', 'retiring', 'retired')),
    created_at             TEXT NOT NULL,
    retirement_prepared_at TEXT,
    retired_at             TEXT,
    CHECK ((status = 'retired') = (retired_at IS NOT NULL)),
    CHECK (status <> 'active' OR retirement_prepared_at IS NULL)
);

CREATE TABLE IF NOT EXISTS payloads (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    block_size   INTEGER NOT NULL CHECK (block_size > 0),
    -- Content address of the live payload; NULL while the slot is on the free
    -- list (free slots are zero-filled so retired data is not recoverable).
    content_hash TEXT,
    data         BLOB NOT NULL,
    refcount     INTEGER NOT NULL DEFAULT 0 CHECK (refcount >= 0),
    free         INTEGER NOT NULL DEFAULT 0 CHECK (free IN (0, 1)),
    reuse_count  INTEGER NOT NULL DEFAULT 0 CHECK (reuse_count >= 0),
    created_at   TEXT NOT NULL,
    freed_at     TEXT,
    CHECK (length(data) = block_size),
    CHECK ((free = 1) = (content_hash IS NULL))
);

-- Only live payloads participate in content addressing, so identical bytes
-- written by two volumes share one row.  Freed slots keep a NULL hash and are
-- therefore never matched by this index.
CREATE UNIQUE INDEX IF NOT EXISTS idx_payload_content
    ON payloads(block_size, content_hash) WHERE free = 0;
CREATE INDEX IF NOT EXISTS idx_payload_free ON payloads(block_size, free);

CREATE TABLE IF NOT EXISTS base_blocks (
    base_id     TEXT NOT NULL REFERENCES bases(id) ON DELETE CASCADE,
    block_index INTEGER NOT NULL CHECK (block_index >= 0),
    payload_id  INTEGER NOT NULL REFERENCES payloads(id),
    PRIMARY KEY (base_id, block_index)
);

CREATE TABLE IF NOT EXISTS volumes (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    -- Provenance.  ``base_id`` is cleared only when a *retired* base's
    -- catalogue row is explicitly deleted; ``base_name`` keeps the origin
    -- useful for operators and tests after that.
    base_id     TEXT REFERENCES bases(id) ON DELETE SET NULL,
    base_name   TEXT NOT NULL,
    block_size  INTEGER NOT NULL CHECK (block_size > 0),
    block_count INTEGER NOT NULL CHECK (block_count > 0),
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS volume_blocks (
    volume_id   TEXT NOT NULL REFERENCES volumes(id) ON DELETE CASCADE,
    block_index INTEGER NOT NULL CHECK (block_index >= 0),
    payload_id  INTEGER NOT NULL REFERENCES payloads(id),
    origin      TEXT NOT NULL CHECK (origin IN ('base', 'written', 'zeroed')),
    PRIMARY KEY (volume_id, block_index)
);

-- Audit trail of payload slots that were released.  ``reused_at`` shows when
-- the slot's bytes were later handed out again by a write.
CREATE TABLE IF NOT EXISTS reclaim_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    payload_id   INTEGER NOT NULL,
    block_size   INTEGER NOT NULL,
    data_bytes   INTEGER NOT NULL,
    reason       TEXT NOT NULL,
    reclaimed_at TEXT NOT NULL,
    reused_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_volumes_base ON volumes(base_id);
CREATE INDEX IF NOT EXISTS idx_base_blocks_payload ON base_blocks(payload_id);
CREATE INDEX IF NOT EXISTS idx_volume_blocks_payload ON volume_blocks(payload_id);
CREATE INDEX IF NOT EXISTS idx_reclaim_events_time ON reclaim_events(reclaimed_at);
"""


class Database:
    """Owns one SQLite connection per thread.

    ``sqlite3`` connections cannot be shared between threads and FastAPI runs
    synchronous endpoints in a thread pool, so a connection is created lazily
    per thread.  The lifespan shutdown hook closes the connection owned by the
    shutdown thread; connections owned by pool workers are closed by the OS at
    process exit after their last committed transaction.
    """

    def __init__(self, path: str | Path, *, busy_timeout: float = 30.0) -> None:
        self.path = str(path)
        self.busy_timeout = busy_timeout
        self._local = threading.local()
        self._init_lock = threading.Lock()

    # -- connections -----------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout,
            isolation_level=None,  # explicit transactions only
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"PRAGMA busy_timeout = {int(self.busy_timeout * 1000)}")
        return conn

    def connection(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            self._local.conn = conn
        return conn

    def close_thread_connection(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # -- transactions ----------------------------------------------------------
    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        """Serialised write transaction backed by ``BEGIN IMMEDIATE``."""
        conn = self.connection()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        conn = self.connection()
        conn.execute("BEGIN")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    # -- lifecycle -------------------------------------------------------------
    def initialise(self) -> None:
        with self._init_lock:
            conn = self._connect()
            try:
                existing = conn.execute(
                    "SELECT value FROM meta WHERE key = 'schema_version'"
                ).fetchone() if self._has_meta(conn) else None
                if existing is not None and int(existing["value"]) > SCHEMA_VERSION:
                    raise RuntimeError(
                        f"database schema version {existing['value']} is newer than "
                        f"this service supports ({SCHEMA_VERSION}); refusing to open"
                    )
                conn.executescript(_SCHEMA)
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (str(SCHEMA_VERSION),),
                )
                # Crash recovery.  ``retiring`` is a transient state committed
                # by the prepare endpoint.  Commit itself is one transaction
                # that either removed the base's references completely or did
                # not run at all, so a prepared-but-not-committed base is still
                # fully usable after a restart.  Volumes never depend on the
                # base row for reads, so restoring ``active`` affects no volume.
                conn.execute(
                    "UPDATE bases SET status = 'active', "
                    "retirement_prepared_at = NULL "
                    "WHERE status = 'retiring'"
                )
                conn.commit()
            finally:
                conn.close()

    @staticmethod
    def _has_meta(conn: sqlite3.Connection) -> bool:
        return (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'"
            ).fetchone()
            is not None
        )

    def schema_version(self) -> int | None:
        with self.read() as conn:
            row = conn.execute(
                "SELECT value FROM meta WHERE key = 'schema_version'"
            ).fetchone()
            return int(row["value"]) if row else None


def database_path_from_env() -> str:
    return os.environ.get("BLOCKSTORE_DB", "blockstore.db")


def busy_timeout_from_env() -> float:
    raw = os.environ.get("BLOCKSTORE_BUSY_TIMEOUT_SECONDS", "30")
    try:
        return float(raw)
    except ValueError:
        return 30.0
