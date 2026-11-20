"""Shared fixtures: every test gets its own database and app instance."""

from __future__ import annotations

import base64
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import create_app  # noqa: E402


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "blockstore.db"


@pytest.fixture()
def app(db_path: Path):
    return create_app(str(db_path), busy_timeout=10)


@pytest.fixture()
def client(app):
    with TestClient(app) as test_client:
        yield test_client


def b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def unb64(value: str) -> bytes:
    return base64.b64decode(value)


def read_block(client: TestClient, volume_id: str, index: int) -> bytes:
    response = client.get(f"/volumes/{volume_id}/blocks/{index}")
    assert response.status_code == 200, response.text
    return unb64(response.json()["data_base64"])


def make_base(client: TestClient, name: str, blocks: list[bytes]) -> dict:
    response = client.post(
        "/bases",
        json={
            "name": name,
            "block_size": len(blocks[0]),
            "blocks_base64": [b64(b) for b in blocks],
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def make_volume(client: TestClient, name: str, base_id: str) -> dict:
    response = client.post("/volumes", json={"name": name, "base_id": base_id})
    assert response.status_code == 201, response.text
    return response.json()
