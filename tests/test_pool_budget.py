"""Configurable pool byte budget: accounting, rejection, rollback and races.

The fixture starts the same FastAPI app as the rest of the suite but with a
small ``BLOCKSTORE_POOL_BUDGET_BYTES``-style cap passed straight to the service,
so the budget behaviour can be exercised deterministically.  Blocks are 8
bytes, which keeps the arithmetic obvious.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.db import Database
from app.main import create_app
from app.service import BlockStoreService
from conftest import b64, make_base, make_volume, read_block

BLOCK = 8


@pytest.fixture()
def budget_client(db_path: Path):
    # 32 bytes == four 8-byte slots at most (live or recycled).
    app = create_app(str(db_path), busy_timeout=10, pool_budget_bytes=32)
    with TestClient(app) as test_client:
        yield test_client


def budget_stats(client: TestClient) -> dict:
    return client.get("/stats").json()["pool_budget"]


def test_stats_reports_budget_and_unlimited_default(db_path: Path):
    unlimited = create_app(str(db_path), busy_timeout=10, pool_budget_bytes=None)
    with TestClient(unlimited) as client:
        pool = budget_stats(client)
        assert pool["limited"] is False
        assert pool["budget_bytes"] is None
        assert pool["available_bytes"] is None
        assert pool["utilization"] is None


def test_import_within_budget_succeeds_and_accounts_slots(budget_client):
    # Two distinct 8-byte blocks -> 16 of 32 bytes used.
    base = make_base(budget_client, "base-a", [b"AAAAAAAA", b"BBBBBBBB"])
    assert base["distinct_payloads"] == 2
    pool = budget_stats(budget_client)
    assert pool["used_bytes"] == 16
    assert pool["available_bytes"] == 16
    assert pool["slot_count"] == 2


def test_import_overflow_is_rejected_and_fully_rolled_back(budget_client):
    make_base(budget_client, "base-a", [b"AAAAAAAA", b"BBBBBBBB", b"CCCCCCCC"])
    before = budget_stats(budget_client)
    assert before["used_bytes"] == 24

    # Four distinct new blocks would need 32 fresh bytes; only 8 fit.
    response = budget_client.post(
        "/bases",
        json={
            "name": "too-big",
            "block_size": BLOCK,
            "blocks_base64": [b64(b"DDDDDDDD"), b64(b"EEEEEEEE"),
                              b64(b"FFFFFFFF"), b64(b"GGGGGGGG")],
        },
    )
    assert response.status_code == 507
    body = response.json()
    assert body["code"] == "pool_budget_exceeded"
    assert "budget" in body["detail"]

    # Whole-request rollback: no base row, no slots, no refcounts, no events.
    names = [b["name"] for b in budget_client.get("/bases").json()]
    assert names == ["base-a"]
    after = budget_stats(budget_client)
    assert after["used_bytes"] == before["used_bytes"] == 24
    assert after["slot_count"] == 3
    stats = budget_client.get("/stats").json()
    assert stats["reclaim"]["events"] == 0
    assert budget_client.get("/verify").json()["ok"] is True


def test_recycled_free_slots_still_count_against_budget(budget_client):
    base = make_base(budget_client, "base-a", [b"AAAAAAAA", b"BBBBBBBB"])
    volume = make_volume(budget_client, "vol-a", base["id"])
    budget_client.delete(f"/volumes/{volume['id']}")
    budget_client.post(f"/bases/{base['id']}/retirement/prepare")
    budget_client.post(f"/bases/{base['id']}/retirement/commit")

    stats = budget_client.get("/stats").json()
    # Both payloads are now free slots: they remain allocated and still count.
    assert stats["free_slots"]["count"] == 2
    pool = stats["pool_budget"]
    assert pool["used_bytes"] == 16
    assert pool["available_bytes"] == 16

    # Reusing both free slots needs no new budget and succeeds.
    base2 = make_base(budget_client, "base-b", [b"HHHHHHHH"])
    vol2 = make_volume(budget_client, "vol-b", base2["id"])
    response = budget_client.put(
        f"/volumes/{vol2['id']}/blocks/0", json={"data_base64": b64(b"IIIIIIII")}
    )
    assert response.status_code == 200
    stats = budget_client.get("/stats").json()
    assert stats["free_slots"]["count"] == 0
    assert stats["pool_budget"]["used_bytes"] == 16

    # Pool is now full of live slots: a third distinct payload is refused.
    overflow = budget_client.post(
        "/bases",
        json={"name": "no-room", "block_size": BLOCK,
              "blocks_base64": [b64(b"JJJJJJJJ"), b64(b"KKKKKKKK"),
                                b64(b"LLLLLLLL")]},
    )
    assert overflow.status_code == 507
    assert budget_client.get("/verify").json()["ok"] is True


def test_identical_content_sharing_consumes_no_extra_budget(budget_client):
    base = make_base(budget_client, "base-a", [b"AAAAAAAA", b"AAAAAAAA", b"AAAAAAAA"])
    # All three blocks are one payload: 8 bytes used, not 24.
    pool = budget_stats(budget_client)
    assert pool["used_bytes"] == 8
    assert pool["slot_count"] == 1

    # Writing the same content from a volume shares the payload again.
    volume = make_volume(budget_client, "vol-a", base["id"])
    response = budget_client.put(
        f"/volumes/{volume['id']}/blocks/0", json={"data_base64": b64(b"AAAAAAAA")}
    )
    assert response.status_code == 200
    assert budget_stats(budget_client)["used_bytes"] == 8


def test_full_pool_overwrite_succeeds_when_old_slot_is_sole_reference(budget_client):
    # Fill the 32-byte pool with four distinct payloads via one base.
    base = make_base(
        budget_client,
        "base-a",
        [b"AAAAAAAA", b"BBBBBBBB", b"CCCCCCCC", b"DDDDDDDD"],
    )
    volume = make_volume(budget_client, "vol-a", base["id"])
    assert budget_stats(budget_client)["available_bytes"] == 0

    # The base still references every payload, so the old slot is NOT sole:
    # a fresh-content write must be rejected and leave everything intact.
    blocked = budget_client.put(
        f"/volumes/{volume['id']}/blocks/0", json={"data_base64": b64(b"NEW00000")}
    )
    assert blocked.status_code == 507
    assert read_block(budget_client, volume["id"], 0) == b"AAAAAAAA"
    assert budget_stats(budget_client)["used_bytes"] == 32

    # Retire the base: the volume becomes the only reference holder.
    budget_client.post(f"/bases/{base['id']}/retirement/prepare")
    budget_client.post(f"/bases/{base['id']}/retirement/commit")

    # Now the old payload is the volume block's sole reference: the overwrite
    # releases that slot and recycles it, so the pool never exceeds 32 bytes.
    response = budget_client.put(
        f"/volumes/{volume['id']}/blocks/0", json={"data_base64": b64(b"NEW00000")}
    )
    assert response.status_code == 200
    assert read_block(budget_client, volume["id"], 0) == b"NEW00000"
    stats = budget_client.get("/stats").json()
    assert stats["pool_budget"]["used_bytes"] == 32
    assert stats["pool_budget"]["slot_count"] == 4
    # The swap was recorded as a reclamation followed by a slot reuse.
    assert stats["reclaim"]["events"] >= 1
    assert stats["reclaim"]["slots_reused"] >= 1
    assert budget_client.get("/verify").json()["ok"] is True


def test_full_pool_zero_succeeds_for_sole_reference_and_is_rejected_otherwise(
    budget_client,
):
    base = make_base(
        budget_client,
        "base-a",
        [b"AAAAAAAA", b"BBBBBBBB", b"CCCCCCCC", b"DDDDDDDD"],
    )
    volume = make_volume(budget_client, "vol-a", base["id"])

    # A zero payload does not exist yet and the pool is full; the base shares
    # the old reference, so zeroing must be refused with a full rollback.
    blocked = budget_client.post(f"/volumes/{volume['id']}/blocks/1/zero")
    assert blocked.status_code == 507
    assert read_block(budget_client, volume["id"], 1) == b"BBBBBBBB"

    budget_client.post(f"/bases/{base['id']}/retirement/prepare")
    budget_client.post(f"/bases/{base['id']}/retirement/commit")

    # Sole reference now: zeroing recycles the old slot in place.
    response = budget_client.post(f"/volumes/{volume['id']}/blocks/1/zero")
    assert response.status_code == 200
    assert read_block(budget_client, volume["id"], 1) == b"\x00" * BLOCK
    stats = budget_client.get("/stats").json()
    assert stats["pool_budget"]["used_bytes"] == 32
    assert budget_client.get("/verify").json()["ok"] is True

    # A second zeroed block wants the existing zero payload, which is already
    # live: sharing it needs no new slot, but the block's own old slot must be
    # released first. That only works because it is a sole reference, and the
    # pool keeps its 32-byte footprint throughout.
    response = budget_client.post(f"/volumes/{volume['id']}/blocks/2/zero")
    assert response.status_code == 200
    assert read_block(budget_client, volume["id"], 2) == b"\x00" * BLOCK
    stats = budget_client.get("/stats").json()
    assert stats["pool_budget"]["used_bytes"] == 32
    # Two volume blocks now point at one shared zero payload.
    zero_rows = [
        b for b in budget_client.get(
            f"/volumes/{volume['id']}/blocks", params={"include_data": "false"}
        ).json()["blocks"]
        if b["origin"] == "zeroed"
    ]
    assert len(zero_rows) == 2
    assert len({b["sha256"] for b in zero_rows}) == 1
    assert budget_client.get("/verify").json()["ok"] is True


def test_rejected_write_rolls_back_refcounts_and_reclaim_records(budget_client):
    base = make_base(
        budget_client,
        "base-a",
        [b"AAAAAAAA", b"BBBBBBBB", b"CCCCCCCC", b"DDDDDDDD"],
    )
    volume = make_volume(budget_client, "vol-a", base["id"])
    before = budget_client.get("/stats").json()

    response = budget_client.put(
        f"/volumes/{volume['id']}/blocks/0", json={"data_base64": b64(b"NEW00000")}
    )
    assert response.status_code == 507

    after = budget_client.get("/stats").json()
    # No slot appeared, no refcount moved, no reclaim event was written.
    assert after["payloads"] == before["payloads"]
    assert after["reclaim"] == before["reclaim"]
    assert after["pool_budget"]["used_bytes"] == 32
    assert read_block(budget_client, volume["id"], 0) == b"AAAAAAAA"
    assert after["payloads"]["distinct"] == 4
    assert budget_client.get("/verify").json()["ok"] is True


def test_concurrent_imports_never_exceed_the_budget(tmp_path):
    """Service-level race: parallel requests serialise on BEGIN IMMEDIATE."""
    db = Database(str(tmp_path / "race.db"), busy_timeout=30)
    db.initialise()
    service = BlockStoreService(db, pool_budget_bytes=32)

    # Pre-fill half the budget (16 bytes, two slots).
    service.import_base("seed", BLOCK, [b"AAAAAAAA", b"BBBBBBBB"])

    def import_base(index: int) -> tuple[int, str]:
        try:
            service.import_base(
                f"base-{index}", BLOCK, [f"{index:08d}".encode()]
            )
            return 201, "created"
        except Exception as exc:  # noqa: BLE001 - assert status below
            return getattr(exc, "status_code", 500), getattr(exc, "code", "error")

    # Eight writers each want one fresh 8-byte slot; only two can fit.
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(import_base, range(8)))

    created = [r for r in results if r[0] == 201]
    refused = [r for r in results if r[0] == 507]
    assert len(created) == 2
    assert len(refused) == 6
    assert all(code == "pool_budget_exceeded" for _, code in refused)

    report = service.verify()
    assert report["ok"], report
    stats = service.stats()
    assert stats["pool_budget"]["used_bytes"] == 32
    assert stats["pool_budget"]["used_bytes"] <= stats["pool_budget"]["budget_bytes"]


def test_concurrent_writes_at_full_pool_never_over_allocate(tmp_path):
    db = Database(str(tmp_path / "writes.db"), busy_timeout=30)
    db.initialise()
    service = BlockStoreService(db, pool_budget_bytes=16)

    base = service.import_base("base", BLOCK, [b"AAAAAAAA", b"BBBBBBBB"])
    volume = service.create_volume("vol", base["id"])
    service.prepare_retirement(base["id"])
    service.commit_retirement(base["id"])
    # Pool full (16 bytes); the volume is sole holder of both payloads.

    outcomes: list[str] = []

    def write(index: int) -> None:
        try:
            service.write_volume_block(
                volume["id"], index, f"W{index:07d}".encode()
            )
            outcomes.append("ok")
        except Exception as exc:  # noqa: BLE001
            outcomes.append(getattr(exc, "code", "error"))

    # Every write either recycles a sole-reference slot or is rejected; the
    # total footprint can never pass 16 bytes regardless of interleaving.
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, [0, 1, 0, 1, 0, 1, 0, 1]))

    stats = service.stats()
    assert stats["pool_budget"]["used_bytes"] <= 16
    assert stats["pool_budget"]["slot_count"] == 2
    assert service.verify()["ok"] is True
    assert set(outcomes) <= {"ok", "pool_budget_exceeded"}


def test_budget_allows_mixed_block_sizes_with_shared_free_slots(budget_client):
    base = make_base(budget_client, "base-a", [b"AAAAAAAA", b"BBBBBBBB"])  # 16 B
    volume = make_volume(budget_client, "vol-a", base["id"])

    # A 4-byte block needs its own slot; 16 bytes remain, so it fits twice.
    small = budget_client.post(
        "/bases",
        json={"name": "small", "block_size": 4,
              "blocks_base64": [b64(b"abcd"), b64(b"efgh")]},
    )
    assert small.status_code == 201, small.text
    assert budget_stats(budget_client)["used_bytes"] == 24

    # One 8-byte slot would bring the pool to 32 bytes exactly: allowed.
    exact = budget_client.post(
        "/bases", json={"name": "exact", "block_size": BLOCK,
                        "blocks_base64": [b64(b"ZZZZZZZZ")]}
    )
    assert exact.status_code == 201, exact.text
    assert budget_stats(budget_client)["available_bytes"] == 0

    # One more byte of slot would overflow.
    over = budget_client.post(
        "/bases", json={"name": "over", "block_size": 4,
                        "blocks_base64": [b64(b"wxyz")]}
    )
    assert over.status_code == 507
    assert budget_client.get("/verify").json()["ok"] is True
