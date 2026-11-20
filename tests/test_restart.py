"""Crash / restart recovery.

Each test runs the API in a subprocess and kills it with ``SIGKILL`` so SQLite's
WAL recovery, not a clean shutdown, is exercised on the next start.
"""

from __future__ import annotations

import base64
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

BLOCK = 8


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.port = _free_port()
        self.process: subprocess.Popen | None = None
        self.url = f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        env = {
            "PATH": "/usr/bin:/bin",
            "BLOCKSTORE_DB": str(self.db_path),
            "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
        }
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-level",
                "warning",
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                if httpx.get(f"{self.url}/healthz", timeout=0.5).status_code == 200:
                    return
            except httpx.HTTPError:
                time.sleep(0.1)
        self.kill()
        raise RuntimeError("server did not start")

    def kill(self) -> None:
        if self.process and self.process.poll() is None:
            os.kill(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=10)
        self.process = None

    def client(self) -> httpx.Client:
        return httpx.Client(base_url=self.url, timeout=30)


@pytest.fixture()
def server_factory(db_path: Path):
    servers: list[Server] = []

    def make() -> Server:
        server = Server(db_path)
        server.start()
        servers.append(server)
        return server

    yield make
    for server in servers:
        server.kill()


def _seed(server: Server) -> dict:
    with server.client() as client:
        base = client.post(
            "/bases",
            json={
                "name": "base",
                "block_size": BLOCK,
                "blocks_base64": [base64.b64encode(b"AAAAAAAA").decode(),
                                  base64.b64encode(b"BBBBBBBB").decode()],
            },
        ).json()
        volume = client.post(
            "/volumes", json={"name": "vol", "base_id": base["id"]}
        ).json()
        client.put(
            f"/volumes/{volume['id']}/blocks/0",
            json={"data_base64": base64.b64encode(b"CONFIRM1").decode()},
        )
        return {"base_id": base["id"], "volume_id": volume["id"]}


def _read(client: httpx.Client, volume_id: str, index: int) -> bytes:
    response = client.get(f"/volumes/{volume_id}/blocks/{index}")
    assert response.status_code == 200, response.text
    return base64.b64decode(response.json()["data_base64"])


def test_restart_preserves_volume_reads_and_writes(server_factory):
    first = server_factory()
    ids = _seed(first)
    first.kill()

    second = server_factory()
    with second.client() as client:
        assert _read(client, ids["volume_id"], 0) == b"CONFIRM1"
        assert _read(client, ids["volume_id"], 1) == b"BBBBBBBB"
        # Still writable after restart.
        response = client.put(
            f"/volumes/{ids['volume_id']}/blocks/1",
            json={"data_base64": base64.b64encode(b"AFTRBOOT").decode()},
        )
        assert response.status_code == 200
        assert client.get("/verify").json()["ok"] is True
    second.kill()

    third = server_factory()
    with third.client() as client:
        assert _read(client, ids["volume_id"], 1) == b"AFTRBOOT"
        assert client.get("/verify").json()["ok"] is True


def test_restart_between_prepare_and_commit_leaves_base_usable(server_factory):
    first = server_factory()
    ids = _seed(first)
    with first.client() as client:
        assert client.post(f"/bases/{ids['base_id']}/retirement/prepare").status_code == 200
    first.kill()

    second = server_factory()
    with second.client() as client:
        base = client.get(f"/bases/{ids['base_id']}").json()
        # Recovery restores a prepared-but-uncommitted base to active.
        assert base["status"] == "active"
        assert base["blocks_still_stored"] == 2
        assert client.get(f"/bases/{ids['base_id']}/blocks/0").status_code == 200
        # Volumes were readable and writable in the interim.
        assert _read(client, ids["volume_id"], 0) == b"CONFIRM1"
        # The base can still be used for new volumes.
        assert (
            client.post("/volumes", json={"name": "after", "base_id": ids["base_id"]}).status_code
            == 201
        )
        # And retirement can be completed cleanly after recovery.
        client.post(f"/bases/{ids['base_id']}/retirement/prepare")
        assert client.post(f"/bases/{ids['base_id']}/retirement/commit").status_code == 200
        assert _read(client, ids["volume_id"], 0) == b"CONFIRM1"
        assert client.get("/verify").json()["ok"] is True
    second.kill()


def test_restart_after_commit_keeps_volumes_working(server_factory):
    first = server_factory()
    ids = _seed(first)
    with first.client() as client:
        client.post(f"/bases/{ids['base_id']}/retirement/prepare")
        commit = client.post(f"/bases/{ids['base_id']}/retirement/commit")
        assert commit.status_code == 200
        before = client.get("/stats").json()
    first.kill()

    second = server_factory()
    with second.client() as client:
        base = client.get(f"/bases/{ids['base_id']}").json()
        assert base["status"] == "retired"
        assert base["blocks_still_stored"] == 0
        assert _read(client, ids["volume_id"], 0) == b"CONFIRM1"
        assert _read(client, ids["volume_id"], 1) == b"BBBBBBBB"
        assert (
            client.put(
                f"/volumes/{ids['volume_id']}/blocks/1",
                json={"data_base64": base64.b64encode(b"POSTRTRD").decode()},
            ).status_code
            == 200
        )
        after = client.get("/stats").json()
        assert after["payloads"]["distinct"] == before["payloads"]["distinct"]
        assert client.get("/verify").json()["ok"] is True
    second.kill()


def test_restart_without_clean_shutdown_keeps_acknowledged_writes(server_factory):
    first = server_factory()
    with first.client() as client:
        base = client.post(
            "/bases", json={"name": "base", "block_size": BLOCK, "block_count": 1}
        ).json()
        volume = client.post(
            "/volumes", json={"name": "vol", "base_id": base["id"]}
        ).json()
        acknowledged = []
        for index_byte in range(10):
            payload = f"ACK{index_byte:05d}".encode()
            response = client.put(
                f"/volumes/{volume['id']}/blocks/0",
                json={"data_base64": base64.b64encode(payload).decode()},
            )
            assert response.status_code == 200
            acknowledged.append(payload)
    first.kill()

    second = server_factory()
    with second.client() as client:
        final = _read(client, volume["id"], 0)
        assert final == acknowledged[-1]
        assert client.get("/verify").json()["ok"] is True
    second.kill()
