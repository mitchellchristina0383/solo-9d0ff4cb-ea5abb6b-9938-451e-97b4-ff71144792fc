#!/usr/bin/env python3
"""Pool byte-budget demonstration against a running Block Store API.

Uses only the standard library so it can run from the container or anywhere
with network access.  The script assumes a small cap -- the Compose default
``BLOCKSTORE_POOL_BUDGET_BYTES=128`` works out of the box -- and demonstrates:

1. the budget accounting in ``GET /stats`` (live slots *and* recycled free
   slots both count),
2. a request that would overflow the pool being refused with
   ``507 pool_budget_exceeded`` and rolled back completely,
3. an overwrite at a full pool succeeding when the old block is the payload's
   sole reference (its slot is recycled in place, footprint unchanged),
4. concurrent writers never exceeding the cap: requests either fit or fail,
   and ``/verify`` stays clean.

Usage::

    python scripts/budget_demo.py --url http://localhost:8000
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


class Api:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def call(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={"content-type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def get(self, path: str):
        return self.call("GET", path)

    def post(self, path: str, body: dict | None = None):
        return self.call("POST", path, body)

    def put(self, path: str, body: dict):
        return self.call("PUT", path, body)


def check(condition: bool, message: str) -> None:
    if not condition:
        print(f"  FAIL  {message}")
        raise SystemExit(1)
    print(f"  ok    {message}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--prefix", default="budget-demo")
    args = parser.parse_args()
    api = Api(args.url)

    status, _ = api.get("/healthz")
    check(status == 200, f"service healthy at {args.url}")

    status, stats = api.get("/stats")
    budget = stats["pool_budget"]
    check(budget["limited"], "service is running with a byte budget "
                             f"(budget_bytes={budget['budget_bytes']})")
    cap = budget["budget_bytes"]
    block = 8
    slots = cap // block
    check(slots >= 4, f"budget of {cap} bytes fits at least four {block}-byte slots")

    print("\n1. Fill the pool with one base of distinct 8-byte blocks")
    filler = [f"{i:08d}".encode() for i in range(slots)]
    status, base = api.post(
        "/bases",
        {"name": f"{args.prefix}-filler", "block_size": block,
         "blocks_base64": [b64(b) for b in filler]},
    )
    check(status == 201, f"imported {slots} blocks filling the budget")
    status, stats = api.get("/stats")
    check(stats["pool_budget"]["available_bytes"] == 0,
          f"pool full: used {stats['pool_budget']['used_bytes']}/{cap} bytes")
    check(stats["pool_budget"]["slot_count"] == slots, "every block is one slot")

    print("\n2. Overflow is refused with 507 and rolled back")
    before = stats
    status, body = api.post(
        "/bases",
        {"name": f"{args.prefix}-overflow", "block_size": block,
         "blocks_base64": [b64(b"OVERFLOW")]},
    )
    check(status == 507 and body["code"] == "pool_budget_exceeded",
          f"overflow rejected: {body.get('detail')}")
    status, after = api.get("/stats")
    check(after["payloads"] == before["payloads"], "no payload slot was created")
    check(after["reclaim"] == before["reclaim"], "no reclaim event was written")
    check(after["pool_budget"]["used_bytes"] == cap, "footprint unchanged after rollback")

    print("\n3. Full-pool overwrite: shared reference is refused first")
    status, volume = api.post(
        "/volumes", {"name": f"{args.prefix}-vol", "base_id": base["id"]}
    )
    check(status == 201, "volume created from the full-pool base")
    status, body = api.put(
        f"/volumes/{volume['id']}/blocks/0",
        {"data_base64": b64(b"FRESH000")},
    )
    check(status == 507, "overwrite refused while the base still shares the slot "
                         f"({body['code']})")

    print("\n4. Retire the base, then the sole-reference overwrite succeeds")
    check(api.post(f"/bases/{base['id']}/retirement/prepare")[0] == 200, "retirement prepared")
    check(api.post(f"/bases/{base['id']}/retirement/commit")[0] == 200, "retirement committed")
    status, _ = api.put(
        f"/volumes/{volume['id']}/blocks/0",
        {"data_base64": b64(b"FRESH000")},
    )
    check(status == 200, "full-pool overwrite succeeded via in-place slot recycling")
    status, readback = api.get(f"/volumes/{volume['id']}/blocks/0")
    check(base64.b64decode(readback["data_base64"]) == b"FRESH000",
          "overwritten block reads back the new bytes")
    status, stats = api.get("/stats")
    check(stats["pool_budget"]["used_bytes"] == cap,
          "pool still exactly at its budget after the swap")
    check(stats["reclaim"]["slots_reused"] >= 1,
          f"reclaim audit shows slot reuse ({stats['reclaim']['slots_reused']})")

    print("\n5. Concurrent writers never exceed the cap")
    # The pool is fully held by the first (now retired) base's slots, which the
    # sole surviving volume references.  Hammer several of its blocks in
    # parallel: every response must be 200 (in-place slot swap) or 507
    # (rejected), and the footprint must never pass the cap.
    race_volume = volume["id"]
    race_indices = list(range(min(4, base["block_count"])))

    def write(task: tuple[int, int]) -> int:
        index, seq = task
        status, _ = api.put(
            f"/volumes/{race_volume}/blocks/{index}",
            {"data_base64": b64(f"C{seq:07d}".encode())},
        )
        return status

    tasks = [(race_indices[i % len(race_indices)], seq)
             for seq, i in enumerate(range(16))]
    with ThreadPoolExecutor(max_workers=8) as pool:
        codes = list(pool.map(write, tasks))
    check(all(code in (200, 507) for code in codes),
          f"all {len(codes)} concurrent writes answered 200 or 507: {sorted(set(codes))}")
    status, stats = api.get("/stats")
    check(stats["pool_budget"]["used_bytes"] <= cap,
          f"footprint never exceeded the cap ({stats['pool_budget']['used_bytes']} <= {cap})")
    status, verify = api.get("/verify")
    check(status == 200 and verify["ok"], f"consistency audit clean: {verify['counts']}")

    print("\nAll budget demo checks passed.")
    print(f"Interactive API documentation: {args.url}/docs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
