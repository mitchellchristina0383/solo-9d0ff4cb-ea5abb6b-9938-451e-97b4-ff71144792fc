# Block Store API

Copy-on-write block storage with writable volumes on top of read-only base
images, exposed as a FastAPI service and persisted in a single SQLite database.

* A **base** is an immutable image made of fixed-size blocks. Blocks are stored
  by content (SHA-256 per block), so duplicates inside a base are stored once.
* A **volume** is a writable snapshot of a base. Creating one copies *block
  references*, not bytes: several volumes inheriting the same base block share a
  single stored copy (reference counted).
* Reading an unwritten volume block returns the base bytes. Writing copies the
  block into the volume. Zeroing makes the block read as zeros until it is
  written again. Neither operation touches the base or other volumes.
* **Retirement** removes the base at runtime without interrupting volumes:
  `prepare` marks the base retiring (reads and writes keep working), `commit`
  atomically drops every base block reference. Volumes still read their
  inherited content and every acknowledged write is preserved. Base blocks no
  volume inherits any more are released and recycled into a free-slot pool that
  later writes reuse.
* All state is committed in SQLite WAL transactions with `synchronous=FULL`, so
  after a crash or `SIGKILL` a volume remains readable and writable, and a base
  is either fully usable or fully retired (never half-removed).

## Run with Docker Compose

```bash
docker compose up --build -d
docker compose logs -f blockstore      # until healthy
```

The API is on <http://localhost:8000>, interactive docs on
<http://localhost:8000/docs>. The SQLite file lives in the `blockstore-data`
named volume at `/data/blockstore.db`, so `docker compose down` (without `-v`)
keeps all data and `docker compose up -d` restores the exact state.

```bash
docker compose down          # stop, keep data
docker compose down -v       # stop and wipe the database volume
```

Override the published port with `BLOCKSTORE_PORT=9000 docker compose up -d`.

## Demonstration (`/docs`)

Use the Swagger UI, or run the scripted version against the running service:

```bash
python scripts/demo.py --url http://localhost:8000
```

The script performs the whole workflow and asserts each step:

1. import a base of three 8-byte blocks (two of them identical → stored once),
2. create two volumes from it (both share the base payloads),
3. read an unwritten block → base content,
4. write a block on one volume and zero a block on the other,
5. `prepare` retirement and write again (volumes stay writable),
6. `commit` retirement, then read back: late write, earlier write, zeroed block
   and untouched inherited block all survive; direct base read → `410`,
7. delete the volumes, observe reclaimed slots and a later write reusing them,
8. run `/verify`.

## API summary

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/bases` | Import a base (`blocks_base64` or an all-zero `block_count`) |
| GET | `/bases?status=` | List bases |
| GET | `/bases/{id}` | Base details, stored block count, inheriting volumes |
| GET | `/bases/{id}/blocks/{i}` | Read a base block directly |
| POST | `/bases/{id}/retirement/prepare` | Phase 1: base `active → retiring` |
| POST | `/bases/{id}/retirement/cancel` | Abort a prepared retirement |
| GET | `/bases/{id}/retirement` | Retirement status and affected volumes |
| POST | `/bases/{id}/retirement/commit` | Phase 2: atomic removal + reclamation |
| DELETE | `/bases/{id}` | Delete a retired base's catalogue row |
| POST | `/volumes` | Create a writable volume from an active base |
| GET | `/volumes` | List volumes |
| GET | `/volumes/{id}` | Volume details and per-origin block counts |
| GET | `/volumes/{id}/blocks` | List blocks (origin, hash, optional data) |
| GET | `/volumes/{id}/blocks/{i}` | Read a block |
| PUT | `/volumes/{id}/blocks/{i}` | Write a block (JSON base64) |
| PUT | `/volumes/{id}/blocks/{i}/raw` | Write a block (raw octet-stream) |
| POST | `/volumes/{id}/blocks/{i}/zero` | Zero a block |
| DELETE | `/volumes/{id}` | Delete a volume and release its blocks |
| GET | `/stats` | Storage, reference and reclamation stats |
| GET | `/verify` | Full consistency audit (`ok` plus issue list) |
| GET | `/reclaim-events` | Reclamation audit trail (including slot reuse) |
| GET | `/healthz` | Liveness probe |

Blocks are base64 in JSON responses; sizes are small by design (limit 1 MiB per
block, 65 536 blocks and 256 MiB per base).

## Data model

* `payloads` — one row per stored block payload, keyed by
  `(block_size, sha256)`, with a `refcount`. Released rows keep their slot,
  become zero-filled and unaddressable (`free = 1`), and are recycled by later
  writes (`reuse_count`, `reclaim_events.reused_at`).
* `bases` / `base_blocks` — the read-only catalogue and its block → payload map.
* `volumes` / `volume_blocks` — each volume block maps to a payload with an
  `origin` of `base`, `written` or `zeroed`.
* `reclaim_events` — audit trail for every released payload slot.

Every mutation is a `BEGIN IMMEDIATE` transaction, so concurrent writers queue
on SQLite's write lock (bounded by `BLOCKSTORE_BUSY_TIMEOUT_SECONDS`, default
30 s) rather than corrupting reference counts.

## Local development

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt pytest httpx
uvicorn app.main:app --reload        # BLOCKSTORE_DB defaults to ./blockstore.db
```

## Tests and checks

```bash
pytest -q                            # 40 tests: blocks, dedup, retirement,
                                     # concurrency, restart/crash recovery
python scripts/check.py              # static review checks
python scripts/crash_check.py        # SIGKILL during write bursts and commits
python scripts/demo.py --url ...     # end-to-end demo against a live service
```

`tests/test_restart.py` and `scripts/crash_check.py` start a real uvicorn
process, `SIGKILL` it and restart on the same SQLite file. They require a free
local port and the current Python environment (uvicorn installed).

## Operational notes

* Single writer process is the intended deployment
  (`--workers 1`, the Compose default): SQLite serialises writers, and a single
  process keeps the connection pool small. Multiple processes would still be
  correct but heavily contended.
* The container runs as an unprivileged user; `/data` is owned by it so SQLite
  can create `-wal`/`-shm` files.
* The database file grows but SQLite reuses freed pages; `/stats` reports
  `freelist_pages` and reclaimed slot reuse.
