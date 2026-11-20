"""Block read/write/zero semantics and input validation."""

from __future__ import annotations

import pytest

from conftest import b64, make_base, make_volume, read_block

BLOCK = 8


def test_create_base_and_volume(client):
    base = make_base(client, "base-a", [b"AAAAAAAA", b"BBBBBBBB"])
    assert base["status"] == "active"
    assert base["block_count"] == 2

    volume = make_volume(client, "vol-a", base["id"])
    assert volume["base_id"] == base["id"]
    assert volume["block_count"] == 2
    assert volume["blocks_by_origin"] == {"base": 2}


def test_unwritten_blocks_read_from_base(client):
    base = make_base(client, "base-a", [b"AAAAAAAA", b"BBBBBBBB"])
    volume = make_volume(client, "vol-a", base["id"])

    assert read_block(client, volume["id"], 0) == b"AAAAAAAA"
    assert read_block(client, volume["id"], 1) == b"BBBBBBBB"


def test_written_block_shadows_base(client):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    volume = make_volume(client, "vol-a", base["id"])

    response = client.put(
        f"/volumes/{volume['id']}/blocks/0", json={"data_base64": b64(b"ZZZZZZZZ")}
    )
    assert response.status_code == 200
    assert read_block(client, volume["id"], 0) == b"ZZZZZZZZ"
    # The base itself is untouched.
    assert read_block(client, volume["id"], 0) == b"ZZZZZZZZ"
    base_read = client.get(f"/bases/{base['id']}/blocks/0").json()
    assert base_read["data_base64"] == b64(b"AAAAAAAA")


def test_zero_block_reads_zero_until_written(client):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    volume = make_volume(client, "vol-a", base["id"])
    url = f"/volumes/{volume['id']}/blocks/0"

    response = client.post(f"{url}/zero")
    assert response.status_code == 200
    assert response.json()["origin"] == "zeroed"
    assert read_block(client, volume["id"], 0) == b"\x00" * BLOCK

    # Reading again still returns zeros: the zeroed payload is persistent.
    assert read_block(client, volume["id"], 0) == b"\x00" * BLOCK

    client.put(url, json={"data_base64": b64(b"REVIVED!")})
    assert read_block(client, volume["id"], 0) == b"REVIVED!"


def test_zero_after_write_clears_written_data(client):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    volume = make_volume(client, "vol-a", base["id"])
    url = f"/volumes/{volume['id']}/blocks/0"

    client.put(url, json={"data_base64": b64(b"WRITTEN!")})
    assert read_block(client, volume["id"], 0) == b"WRITTEN!"
    client.post(f"{url}/zero")
    assert read_block(client, volume["id"], 0) == b"\x00" * BLOCK


def test_write_wrong_size_rejected(client):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    volume = make_volume(client, "vol-a", base["id"])

    response = client.put(
        f"/volumes/{volume['id']}/blocks/0", json={"data_base64": b64(b"short")}
    )
    assert response.status_code == 422
    assert response.json()["code"] == "invalid_block_length"
    # Nothing changed.
    assert read_block(client, volume["id"], 0) == b"AAAAAAAA"


def test_block_index_bounds(client):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    volume = make_volume(client, "vol-a", base["id"])

    assert client.get(f"/volumes/{volume['id']}/blocks/1").status_code == 404
    assert client.get(f"/volumes/{volume['id']}/blocks/-1").status_code == 422


def test_raw_write_endpoint(client):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    volume = make_volume(client, "vol-a", base["id"])

    response = client.put(
        f"/volumes/{volume['id']}/blocks/0/raw",
        content=b"RAWDATA!",
        headers={"content-type": "application/octet-stream"},
    )
    assert response.status_code == 200, response.text
    assert read_block(client, volume["id"], 0) == b"RAWDATA!"


def test_invalid_base_inputs(client):
    assert (
        client.post(
            "/bases", json={"name": "b", "block_size": 4, "blocks_base64": [b64(b"toolong!")]}
        ).status_code
        == 422
    )
    assert (
        client.post(
            "/bases", json={"name": "b", "block_size": 0, "block_count": 2}
        ).status_code
        == 422
    )
    assert (
        client.post(
            "/bases", json={"name": "b", "block_size": 4, "block_count": 0}
        ).status_code
        == 422
    )
    assert (
        client.post(
            "/bases", json={"name": "bad name!", "block_size": 4, "block_count": 1}
        ).status_code
        == 422
    )


def test_duplicate_names_conflict(client):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    assert (
        client.post(
            "/bases", json={"name": "base-a", "block_size": 8, "block_count": 1}
        ).status_code
        == 409
    )
    make_volume(client, "vol-a", base["id"])
    response = client.post("/volumes", json={"name": "vol-a", "base_id": base["id"]})
    assert response.status_code == 409


def test_zero_base_without_explicit_blocks(client):
    response = client.post(
        "/bases", json={"name": "zero-base", "block_size": 4, "block_count": 3}
    )
    assert response.status_code == 201
    base = response.json()
    assert base["block_count"] == 3
    volume = make_volume(client, "vol-z", base["id"])
    for index in range(3):
        assert read_block(client, volume["id"], index) == b"\x00" * 4


def test_delete_volume(client):
    base = make_base(client, "base-a", [b"AAAAAAAA", b"BBBBBBBB"])
    volume = make_volume(client, "vol-a", base["id"])
    response = client.delete(f"/volumes/{volume['id']}")
    assert response.status_code == 200
    assert client.get(f"/volumes/{volume['id']}").status_code == 404
    assert client.get("/verify").json()["ok"] is True


@pytest.mark.parametrize("bad", ["not-base64!!", "YWJj"])
def test_invalid_base64_rejected(client, bad):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    volume = make_volume(client, "vol-a", base["id"])
    response = client.put(
        f"/volumes/{volume['id']}/blocks/0", json={"data_base64": bad}
    )
    # "YWJj" decodes to 3 bytes -> wrong length; the other is not base64.
    assert response.status_code == 422
