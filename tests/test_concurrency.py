"""Concurrent writes must be serialised and never lose acknowledged data."""

from __future__ import annotations

import base64
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from app.db import Database
from app.service import BlockStoreService
from conftest import b64, make_volume

BLOCK = 8


# ---------------------------------------------------------------------------
# service-level concurrency: many threads writing through one SQLite database
# ---------------------------------------------------------------------------
def test_concurrent_writes_to_distinct_blocks(tmp_path):
    db_path = tmp_path / "concurrent.db"
    db = Database(str(db_path), busy_timeout=30)
    db.initialise()
    service = BlockStoreService(db)

    base = service.import_base("base", BLOCK, [b"BASE0000"] * 32)
    volume = service.create_volume("vol", base["id"])
    volume_id = volume["id"]

    def write(index: int) -> bytes:
        payload = f"T{index:03d}DATA".encode()[:BLOCK].ljust(BLOCK, b".")
        service.write_volume_block(volume_id, index, payload)
        return payload

    with ThreadPoolExecutor(max_workers=16) as pool:
        written = list(pool.map(write, range(32)))

    for index, expected in enumerate(written):
        assert service.read_volume_block(volume_id, index)["data"] == expected

    report = service.verify()
    assert report["ok"], report
    assert report["counts"]["volume_blocks"] == 32


def test_concurrent_writes_to_same_block_are_consistent(tmp_path):
    db_path = tmp_path / "contended.db"
    db = Database(str(db_path), busy_timeout=30)
    db.initialise()
    service = BlockStoreService(db)

    base = service.import_base("base", BLOCK, [b"BASE0000"])
    volume = service.create_volume("vol", base["id"])
    volume_id = volume["id"]

    payloads = [f"W{i:07d}".encode() for i in range(40)]

    def write(payload: bytes) -> None:
        service.write_volume_block(volume_id, 0, payload)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(write, payloads))

    final = service.read_volume_block(volume_id, 0)["data"]
    assert final in payloads
    report = service.verify()
    assert report["ok"], report
    # Exactly one payload is referenced by the single block; the other 39
    # overwritten payloads were released.
    assert report["counts"]["payloads"] == 2  # base payload + final write


def test_concurrent_writes_and_retirement_commit(tmp_path):
    """Writes racing retirement must all survive; the switch must be atomic.

    A thread prepares retirement while 8 writers keep writing different blocks.
    Every write that returned without error must be readable afterwards, and
    the base must end up fully retired with intact refcounts.
    """
    db_path = tmp_path / "retire-race.db"
    db = Database(str(db_path), busy_timeout=30)
    db.initialise()
    service = BlockStoreService(db)

    base = service.import_base("base", BLOCK, [b"BASE0000"] * 8)
    volume = service.create_volume("vol", base["id"])
    volume_id = volume["id"]
    service.prepare_retirement(base["id"])

    acknowledged: dict[int, bytes] = {}
    errors: list[str] = []

    def write(index: int) -> None:
        payload = f"RACE{index:04d}".encode()
        try:
            service.write_volume_block(volume_id, index, payload)
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(f"block {index}: {exc}")
            return
        acknowledged[index] = payload

    # Start the switch concurrently with the writers.
    with ThreadPoolExecutor(max_workers=9) as pool:
        futures = [pool.submit(write, index) for index in range(8)]
        futures.append(pool.submit(service.commit_retirement, base["id"]))
        results = [future.result() for future in futures]

    assert not errors, errors
    assert results[-1]["status"] == "retired"
    for index, payload in acknowledged.items():
        assert service.read_volume_block(volume_id, index)["data"] == payload
    report = service.verify()
    assert report["ok"], report


def test_concurrent_volumes_inherit_same_payload(tmp_path):
    db_path = tmp_path / "inherit.db"
    db = Database(str(db_path), busy_timeout=30)
    db.initialise()
    service = BlockStoreService(db)
    base = service.import_base("base", BLOCK, [b"SHARED00", b"SHARED00"])

    def make_volume(index: int) -> str:
        return service.create_volume(f"vol-{index}", base["id"])["id"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        volume_ids = list(pool.map(make_volume, range(8)))

    assert len(volume_ids) == 8
    # One base payload plus one zero? No: both base blocks are identical ->
    # exactly one payload for all 16 inherited block references.
    payloads = service.stats()["payloads"]
    assert payloads["distinct"] == 1
    assert payloads["total_references"] == 2 + 8 * 2
    assert service.verify()["ok"] is True


# ---------------------------------------------------------------------------
# HTTP-level concurrency against a real uvicorn server
# ---------------------------------------------------------------------------
def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def live_server(db_path: Path):
    port = _free_port()
    env = {
        "PATH": "/usr/bin:/bin",
        "BLOCKSTORE_DB": str(db_path),
        "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
    }
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base_url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if httpx.get(f"{base_url}/healthz", timeout=0.5).status_code == 200:
                break
        except httpx.HTTPError:
            time.sleep(0.1)
    else:
        process.kill()
        raise RuntimeError("server did not start")
    yield base_url
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()


def test_http_concurrent_writes(live_server):
    with httpx.Client(base_url=live_server, timeout=30) as client:
        base = client.post(
            "/bases",
            json={
                "name": "base",
                "block_size": BLOCK,
                "blocks_base64": [b64(b"BASE0000")] * 16,
            },
        ).json()
        volume = client.post(
            "/volumes", json={"name": "vol", "base_id": base["id"]}
        ).json()
        volume_id = volume["id"]

        def write(index: int) -> tuple[int, int]:
            response = client.put(
                f"/volumes/{volume_id}/blocks/{index}",
                json={"data_base64": b64(f"T{index:03d}DATA".encode())},
            )
            return index, response.status_code

        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(write, range(16)))

        assert all(code == 200 for _, code in results), results
        for index in range(16):
            data = client.get(f"/volumes/{volume_id}/blocks/{index}").json()
            assert base64.b64decode(data["data_base64"]) == f"T{index:03d}DATA".encode()
        assert client.get("/verify").json()["ok"] is True
