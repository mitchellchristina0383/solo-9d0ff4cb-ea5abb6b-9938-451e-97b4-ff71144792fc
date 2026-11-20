"""Deduplication, refcounting, reclamation and base retirement."""

from __future__ import annotations

import pytest

from conftest import b64, make_base, make_volume, read_block


def stats(client):
    return client.get("/stats").json()


def test_duplicate_blocks_stored_once(client):
    base = make_base(client, "base-a", [b"AAAAAAAA", b"AAAAAAAA", b"BBBBBBBB"])
    assert base["distinct_payloads"] == 2
    assert base["block_count"] == 3


def test_volumes_sharing_base_block_share_one_payload(client):
    base = make_base(client, "base-a", [b"AAAAAAAA", b"BBBBBBBB"])
    make_volume(client, "vol-1", base["id"])
    make_volume(client, "vol-2", base["id"])
    payloads = stats(client)["payloads"]
    assert payloads["distinct"] == 2
    # 2 base blocks + 2 volumes * 2 blocks reference the two payloads.
    assert payloads["total_references"] == 6


def test_identical_writes_deduplicate(client):
    base = make_base(client, "base-a", [b"AAAAAAAA", b"AAAAAAAA"])
    v1 = make_volume(client, "vol-1", base["id"])
    v2 = make_volume(client, "vol-2", base["id"])
    for volume in (v1, v2):
        client.put(
            f"/volumes/{volume['id']}/blocks/0",
            json={"data_base64": b64(b"SAMEDATA")},
        )
    payloads = stats(client)["payloads"]
    assert payloads["distinct"] == 2  # base block + one shared SAME block
    assert read_block(client, v1["id"], 0) == b"SAMEDATA"
    assert read_block(client, v2["id"], 0) == b"SAMEDATA"


def test_retirement_preserves_writes_and_returns_zeros_for_unwritten(client):
    base = make_base(client, "base-a", [b"AAAAAAAA", b"BBBBBBBB", b"CCCCCCCC"])
    v1 = make_volume(client, "vol-1", base["id"])
    v2 = make_volume(client, "vol-2", base["id"])
    client.put(f"/volumes/{v1['id']}/blocks/1", json={"data_base64": b64(b"WRITTEN1")})
    client.put(f"/volumes/{v2['id']}/blocks/2", json={"data_base64": b64(b"WRITTEN2")})

    # Writes are still accepted while the base is in its retirement phase.
    assert client.post(f"/bases/{base['id']}/retirement/prepare").status_code == 200
    late = client.put(
        f"/volumes/{v1['id']}/blocks/0", json={"data_base64": b64(b"LATEWRIT")}
    )
    assert late.status_code == 200

    commit = client.post(f"/bases/{base['id']}/retirement/commit")
    assert commit.status_code == 200, commit.text
    assert commit.json()["status"] == "retired"

    # Confirmed writes survive the switch.
    assert read_block(client, v1["id"], 0) == b"LATEWRIT"
    assert read_block(client, v1["id"], 1) == b"WRITTEN1"
    assert read_block(client, v2["id"], 2) == b"WRITTEN2"

    # Blocks that were never rewritten still read their base content, because
    # each volume owns its payload references.
    assert read_block(client, v2["id"], 0) == b"AAAAAAAA"
    assert read_block(client, v2["id"], 1) == b"BBBBBBBB"

    # Direct base access is gone.
    assert client.get(f"/bases/{base['id']}/blocks/0").status_code == 410
    assert client.get(f"/bases/{base['id']}").json()["blocks_still_stored"] == 0
    assert client.get("/verify").json()["ok"] is True


def test_new_volumes_rejected_from_retiring_base(client):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    client.post(f"/bases/{base['id']}/retirement/prepare")
    response = client.post("/volumes", json={"name": "late", "base_id": base["id"]})
    assert response.status_code == 409
    assert response.json()["code"] == "base_not_active"


def test_retirement_requires_prepare_then_is_idempotent_guard(client):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    assert (
        client.post(f"/bases/{base['id']}/retirement/commit").status_code == 409
    )
    client.post(f"/bases/{base['id']}/retirement/prepare")
    assert client.post(f"/bases/{base['id']}/retirement/commit").status_code == 200
    assert client.post(f"/bases/{base['id']}/retirement/commit").status_code == 409
    assert (
        client.post(f"/bases/{base['id']}/retirement/prepare").status_code == 409
    )


def test_cancel_retirement_restores_active_base(client):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    client.post(f"/bases/{base['id']}/retirement/prepare")
    cancel = client.post(f"/bases/{base['id']}/retirement/cancel")
    assert cancel.status_code == 200
    assert cancel.json()["status"] == "active"
    make_volume(client, "still-ok", base["id"])


def test_uninherited_blocks_reclaimed_at_commit(client):
    # base has two distinct blocks; a volume is created then deleted, so no
    # volume inherits the base any more.
    base = make_base(client, "base-a", [b"AAAAAAAA", b"BBBBBBBB"])
    volume = make_volume(client, "vol-1", base["id"])
    client.delete(f"/volumes/{volume['id']}")
    before = stats(client)
    assert before["free_slots"]["count"] == 0  # base still holds both blocks

    client.post(f"/bases/{base['id']}/retirement/prepare")
    commit = client.post(f"/bases/{base['id']}/retirement/commit").json()
    assert commit["base_blocks_removed"] == 2
    assert commit["payloads_reclaimed"] == 2
    assert commit["bytes_reclaimed"] == 16

    after = stats(client)
    assert after["payloads"]["distinct"] == 0
    assert after["free_slots"]["count"] == 2


def test_reclaimed_slots_reused_by_later_writes(client):
    base = make_base(client, "base-a", [b"AAAAAAAA", b"BBBBBBBB"])
    volume = make_volume(client, "vol-1", base["id"])
    client.delete(f"/volumes/{volume['id']}")
    client.post(f"/bases/{base['id']}/retirement/prepare")
    client.post(f"/bases/{base['id']}/retirement/commit")
    assert stats(client)["free_slots"]["count"] == 2

    base2 = make_base(client, "base-b", [b"CCCCCCCC"])
    # Importing the new base already consumed one released slot.
    assert stats(client)["free_slots"]["count"] == 1
    volume2 = make_volume(client, "vol-2", base2["id"])

    client.put(
        f"/volumes/{volume2['id']}/blocks/0", json={"data_base64": b64(b"NEWDATA!")}
    )
    after = stats(client)
    # The write consumed the second released slot instead of growing the file.
    assert after["free_slots"]["count"] == 0
    assert after["reclaim"]["slots_reused"] == 2
    assert read_block(client, volume2["id"], 0) == b"NEWDATA!"

    events = client.get("/reclaim-events").json()
    assert any(event["reused_at"] for event in events)


def test_inherited_blocks_not_reclaimed_at_commit(client):
    base = make_base(client, "base-a", [b"AAAAAAAA", b"BBBBBBBB"])
    volume = make_volume(client, "vol-1", base["id"])
    client.post(f"/bases/{base['id']}/retirement/prepare")
    commit = client.post(f"/bases/{base['id']}/retirement/commit").json()
    # Both blocks are still referenced by the volume, so nothing is freed.
    assert commit["payloads_reclaimed"] == 0
    assert read_block(client, volume["id"], 0) == b"AAAAAAAA"
    assert read_block(client, volume["id"], 1) == b"BBBBBBBB"

    # Deleting the volume releases them.
    client.delete(f"/volumes/{volume['id']}")
    after = stats(client)
    assert after["payloads"]["distinct"] == 0
    assert after["free_slots"]["count"] == 2


def test_blocks_overwritten_by_all_volumes_reclaimed_at_commit(client):
    # Every base block is overwritten by every volume before the switch, so no
    # volume inherits any base block: the switch must release the base's bytes
    # while keeping each confirmed write.
    base = make_base(client, "base-a", [b"AAAAAAAA", b"BBBBBBBB"])
    v1 = make_volume(client, "vol-1", base["id"])
    v2 = make_volume(client, "vol-2", base["id"])
    for volume, tag in ((v1, b"V1"), (v2, b"V2")):
        for index in range(2):
            payload = tag + bytes([48 + index]) * (8 - len(tag))
            response = client.put(
                f"/volumes/{volume['id']}/blocks/{index}",
                json={"data_base64": b64(payload)},
            )
            assert response.status_code == 200, response.text

    client.post(f"/bases/{base['id']}/retirement/prepare")
    commit = client.post(f"/bases/{base['id']}/retirement/commit").json()
    assert commit["base_blocks_removed"] == 2
    assert commit["payloads_reclaimed"] == 2  # AAAAAAAA and BBBBBBBB are inherited by nobody
    assert commit["bytes_reclaimed"] == 16

    after = stats(client)
    assert after["payloads"]["distinct"] == 4  # two distinct writes per volume
    assert read_block(client, v1["id"], 0) == b"V1000000"
    assert read_block(client, v2["id"], 1) == b"V2111111"
    assert client.get("/verify").json()["ok"] is True


def test_delete_retired_base_keeps_volume_working(client):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    volume = make_volume(client, "vol-1", base["id"])
    client.post(f"/bases/{base['id']}/retirement/prepare")
    client.post(f"/bases/{base['id']}/retirement/commit")
    assert client.delete(f"/bases/{base['id']}").status_code == 200
    assert client.get(f"/bases/{base['id']}").status_code == 404

    assert read_block(client, volume["id"], 0) == b"AAAAAAAA"
    assert (
        client.put(
            f"/volumes/{volume['id']}/blocks/0", json={"data_base64": b64(b"AFTERDEL")}
        ).status_code
        == 200
    )
    assert read_block(client, volume["id"], 0) == b"AFTERDEL"
    assert client.get("/verify").json()["ok"] is True


def test_cannot_delete_active_base(client):
    base = make_base(client, "base-a", [b"AAAAAAAA"])
    assert client.delete(f"/bases/{base['id']}").status_code == 409


@pytest.mark.parametrize("blocks", [1, 5])
def test_retirement_any_block_count(client, blocks):
    content = [bytes([65 + (i % 26)]) * 8 for i in range(blocks)]
    base = make_base(client, "base-x", content)
    volume = make_volume(client, "vol-x", base["id"])
    client.post(f"/bases/{base['id']}/retirement/prepare")
    client.post(f"/bases/{base['id']}/retirement/commit")
    for index in range(blocks):
        assert read_block(client, volume["id"], index) == content[index]
