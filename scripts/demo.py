#!/usr/bin/env python3
"""End-to-end demonstration against a running Block Store API.

Usage::

    python scripts/demo.py --url http://localhost:8000

The script walks through the workflow shown in ``/docs``: import a base,
branch two volumes, read through to the base, write and zero blocks, retire the
base while writing, then prove the volumes still work and that the base is gone.
It prints a compact before/after table and exits non-zero if any check fails.

Only the standard library is used so it can run anywhere the container runs.
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
import urllib.error
import urllib.request


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def unb64(value: str) -> bytes:
    return base64.b64decode(value)


class Api:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def call(self, method: str, path: str, body: dict | bytes | None = None,
             content_type: str = "application/json"):
        headers = {}
        data = None
        if body is not None:
            if isinstance(body, bytes):
                data = body
                headers["content-type"] = content_type
            else:
                data = json.dumps(body).encode()
                headers["content-type"] = "application/json"
        request = urllib.request.Request(
            self.base_url + path, data=data, method=method, headers=headers
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = response.read()
                if response.headers.get_content_type() == "application/json":
                    return response.status, json.loads(payload)
                return response.status, payload
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            try:
                return exc.code, json.loads(payload)
            except json.JSONDecodeError:
                return exc.code, payload

    def get(self, path: str):
        return self.call("GET", path)

    def post(self, path: str, body: dict | None = None):
        return self.call("POST", path, body)

    def put(self, path: str, body: dict):
        return self.call("PUT", path, body)

    def delete(self, path: str):
        return self.call("DELETE", path)


def check(condition: bool, message: str) -> None:
    if not condition:
        print(f"  FAIL  {message}")
        raise SystemExit(1)
    print(f"  ok    {message}")


def show_block(api: Api, volume_id: str, index: int) -> tuple[str, bytes]:
    status, body = api.get(f"/volumes/{volume_id}/blocks/{index}")
    check(status == 200, f"read block {index} -> HTTP {status}")
    return body["origin"], unb64(body["data_base64"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument(
        "--prefix",
        default="demo",
        help="Name prefix so repeated runs do not collide",
    )
    args = parser.parse_args()
    api = Api(args.url)

    status, health = api.get("/healthz")
    check(status == 200, f"service healthy at {args.url}")

    print("\n1. Import a read-only base of three fixed-size blocks")
    blocks = [b"AAAAAAAA", b"BBBBBBBB", b"AAAAAAAA"]
    status, base = api.post(
        "/bases",
        {
            "name": f"{args.prefix}-base",
            "block_size": 8,
            "blocks_base64": [b64(b) for b in blocks],
        },
    )
    check(status == 201, f"base imported (distinct payloads={base['distinct_payloads']}, "
                          f"blocks={base['block_count']})")
    check(base["distinct_payloads"] == 2, "duplicate block stored only once")
    base_id = base["id"]

    print("\n2. Create two writable volumes from the same base")
    volumes = []
    for index in (1, 2):
        status, volume = api.post(
            "/volumes", {"name": f"{args.prefix}-vol{index}", "base_id": base_id}
        )
        check(status == 201, f"volume {volume['name']} created")
        volumes.append(volume["id"])
    v1, v2 = volumes
    status, stats = api.get("/stats")
    check(
        stats["payloads"]["distinct"] == 2,
        f"both volumes share the base payloads (distinct={stats['payloads']['distinct']})",
    )

    print("\n3. Unwritten blocks read the base")
    origin, data = show_block(api, v1, 0)
    check(origin == "base" and data == b"AAAAAAAA", f"v1 block 0 -> {data!r} from base")

    print("\n4. Write and zero on one volume; the other is unaffected")
    status, _ = api.put(
        f"/volumes/{v1}/blocks/0", {"data_base64": b64(b"WRITTEN1")}
    )
    check(status == 200, "write v1 block 0")
    origin, data = show_block(api, v1, 0)
    check(data == b"WRITTEN1", f"v1 block 0 -> {data!r}")
    status, _ = api.post(f"/volumes/{v2}/blocks/2/zero")
    check(status == 200, "zero v2 block 2")
    origin, data = show_block(api, v2, 2)
    check(origin == "zeroed" and data == b"\x00" * 8, f"v2 block 2 -> {data!r} (zeroed)")
    status, _ = api.put(
        f"/volumes/{v1}/blocks/1", {"data_base64": b64(b"WRITTEN2")}
    )
    check(status == 200, "write v1 block 1")

    print("\n5. Prepare retirement: volumes stay writable")
    status, report = api.post(f"/bases/{base_id}/retirement/prepare")
    check(status == 200 and report["status"] == "retiring", "base -> retiring")
    check(report["writable_volumes"] == [f"{args.prefix}-vol1", f"{args.prefix}-vol2"],
          "both volumes reported writable")
    status, _ = api.put(
        f"/volumes/{v1}/blocks/0", {"data_base64": b64(b"LATEWRIT")}
    )
    check(status == 200, "write accepted during retirement")

    print("\n6. Commit retirement: base removed, acknowledged writes kept")
    status, commit = api.post(f"/bases/{base_id}/retirement/commit")
    check(status == 200 and commit["status"] == "retired", "base -> retired")
    check(commit["base_blocks_removed"] == 3, "all base blocks removed")
    for volume_id, index, expected in (
        (v1, 0, b"LATEWRIT"),
        (v1, 1, b"WRITTEN2"),
        (v2, 2, b"\x00" * 8),
        (v2, 0, b"AAAAAAAA"),  # never rewritten -> still served from inherited block
    ):
        _, data = show_block(api, volume_id, index)
        check(data == expected, f"volume {volume_id[:8]} block {index} -> {data!r}")

    status, _ = api.get(f"/bases/{base_id}/blocks/0")
    check(status == 410, "direct base read is gone (410)")
    status, _ = api.post("/volumes", {"name": f"{args.prefix}-late", "base_id": base_id})
    check(status == 409, "no new volumes from a retired base")

    print("\n7. Uninherited blocks reclaimed and reused by later writes")
    status, freed = api.delete(f"/volumes/{v1}")
    check(status == 200, f"delete v1 -> {freed['payloads_reclaimed']} payloads reclaimed")
    status, freed = api.delete(f"/volumes/{v2}")
    check(status == 200, f"delete v2 -> {freed['payloads_reclaimed']} payloads reclaimed")
    status, stats_before = api.get("/stats")
    free_before = stats_before["free_slots"]["count"]
    check(free_before > 0, f"{free_before} reclaimed slots available for reuse")

    status, base2 = api.post(
        "/bases", {"name": f"{args.prefix}-base2", "block_size": 8, "block_count": 1}
    )
    check(status == 201, "import a follow-up base")
    status, vol3 = api.post(
        "/volumes", {"name": f"{args.prefix}-vol3", "base_id": base2["id"]}
    )
    check(status == 201, "create a follow-up volume")
    api.put(f"/volumes/{vol3['id']}/blocks/0", {"data_base64": b64(b"REUSED00")})
    status, stats_after = api.get("/stats")
    check(
        stats_after["reclaim"]["slots_reused"] > 0
        and stats_after["free_slots"]["count"] < free_before,
        f"later write reused a reclaimed slot "
        f"(free {free_before} -> {stats_after['free_slots']['count']}, "
        f"reused={stats_after['reclaim']['slots_reused']})",
    )

    print("\n8. Consistency audit")
    status, verify = api.get("/verify")
    check(status == 200 and verify["ok"], f"verify ok ({verify['counts']})")

    print("\nAll demo checks passed.")
    print(f"Interactive API documentation: {args.url}/docs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
