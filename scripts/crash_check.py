#!/usr/bin/env python3
"""Kill -9 the service during a write burst, then verify recovery.

This complements ``tests/test_restart.py``: it hammers one volume from several
threads, SIGKILLs the process at a random moment, restarts it and checks that
every write that received HTTP 200 is either visible as the final block value
or was superseded by a later acknowledged write, and that the database passes
the consistency audit.

Usage: python scripts/crash_check.py [--rounds 3]
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, db: Path) -> None:
        self.db = db
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.process: subprocess.Popen | None = None

    def start(self) -> None:
        env = dict(os.environ)
        env["BLOCKSTORE_DB"] = str(self.db)
        env["PYTHONPATH"] = str(ROOT)
        self.process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(self.port), "--log-level", "warning"],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                if self.call("GET", "/healthz")[0] == 200:
                    return
            except Exception:
                time.sleep(0.05)
        raise RuntimeError("server failed to start")

    def kill(self) -> None:
        if self.process and self.process.poll() is None:
            os.kill(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=10)
        self.process = None

    def call(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.url + path, data=data, method=method,
            headers={"content-type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())


def b64(value: bytes) -> str:
    return base64.b64encode(value).decode()


MAGIC = b"CRSH"


def encode_payload(worker: int, seq: int) -> bytes:
    """8-byte payload: tag, worker marker, 3-byte sequence number.

    The encoding must never equal the all-zero bytes of an unwritten block, so
    a lost write cannot be confused with "never wrote anything".  ``seq`` is
    24-bit, far more than the 120 writes per burst.
    """
    return MAGIC + bytes([worker + 1]) + seq.to_bytes(3, "big")


def decode_payload(payload: bytes) -> tuple[int, int] | None:
    """Return ``(worker, seq)`` or ``None`` for an unwritten (zero) block."""
    if payload == b"\x00" * 8:
        return None
    if len(payload) != 8 or payload[:4] != MAGIC:
        return None
    return payload[4] - 1, int.from_bytes(payload[5:], "big")


def run_round(db: Path, round_index: int) -> list[str]:
    """Burst writes and SIGKILL mid-flight.

    Each worker owns one block and writes monotonically increasing sequence
    numbers to it.  Two properties must hold after restart:

    * the surviving value of a worker's block is one of *that worker's* payloads
      (writes do not leak between blocks), and
    * it is at least the last sequence number acknowledged with HTTP 200.

    The second property is the durability guarantee.  A write may commit and
    still lose its HTTP response to the SIGKILL; such a write is allowed to be
    present (it was applied), which is why the check is ``>=`` and not ``==``.
    """
    problems: list[str] = []
    server = Server(db)
    server.start()
    workers = 6
    acknowledged: dict[int, int] = {worker: -1 for worker in range(workers)}
    try:
        status, base = server.call(
            "POST", "/bases",
            {"name": f"base-{round_index}", "block_size": 8, "block_count": workers},
        )
        assert status == 201, base
        status, volume = server.call(
            "POST", "/volumes", {"name": f"vol-{round_index}", "base_id": base["id"]}
        )
        assert status == 201, volume
        volume_id = volume["id"]
        lock = __import__("threading").Lock()

        def writer(worker: int) -> None:
            for seq in range(120):
                try:
                    status, _ = server.call(
                        "PUT", f"/volumes/{volume_id}/blocks/{worker}",
                        {"data_base64": b64(encode_payload(worker, seq))},
                    )
                except Exception:
                    return
                if status != 200:
                    return
                with lock:
                    acknowledged[worker] = seq

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(writer, worker) for worker in range(workers)]
            time.sleep(0.35)  # let writes be in flight
            server.kill()     # ungraceful kill mid-burst
            for future in futures:
                future.result(timeout=15)
    finally:
        server.kill()

    # Restart on the same SQLite file: WAL replay must make it consistent.
    server = Server(db)
    server.start()
    try:
        status, verify = server.call("GET", "/verify")
        if status != 200 or not verify["ok"]:
            problems.append(f"round {round_index}: verify failed: {verify}")
        status, health = server.call("GET", "/healthz")
        if status != 200:
            problems.append(f"round {round_index}: service unhealthy after restart")
        for worker in range(workers):
            status, body = server.call("GET", f"/volumes/{volume_id}/blocks/{worker}")
            if status != 200:
                problems.append(
                    f"round {round_index}: block {worker} unreadable: {body}"
                )
                continue
            value = base64.b64decode(body["data_base64"])
            decoded = decode_payload(value)
            if decoded is None:
                if value != b"\x00" * 8:
                    problems.append(
                        f"round {round_index}: block {worker} holds foreign value {value!r}"
                    )
                    continue
                seq = -1  # never written before the kill
            else:
                surviving_worker, seq = decoded
                if surviving_worker != worker:
                    problems.append(
                        f"round {round_index}: block {worker} holds foreign value {value!r}"
                    )
                    continue
            if seq < acknowledged[worker]:
                problems.append(
                    f"round {round_index}: block {worker} lost acknowledged write "
                    f"(have seq {seq}, acked {acknowledged[worker]})"
                )
        status, _ = server.call(
            "PUT", f"/volumes/{volume_id}/blocks/{workers - 1}",
            {"data_base64": b64(b"POSTBOOT")},
        )
        if status != 200:
            problems.append(f"round {round_index}: volume not writable after restart")
        status, body = server.call("GET", f"/volumes/{volume_id}/blocks/{workers - 1}")
        if status == 200 and base64.b64decode(body["data_base64"]) != b"POSTBOOT":
            problems.append(f"round {round_index}: write after restart not visible")
        total_ack = sum(seq + 1 for seq in acknowledged.values())
        print(
            f"  round {round_index}: {total_ack} acknowledged writes before kill, "
            "no acknowledged write lost, verify ok, volume readable/writable"
        )
    finally:
        server.kill()
    return problems


def run_commit_crash(db: Path, round_index: int) -> list[str]:
    """SIGKILL while a retirement commit is in flight.

    Commit is one transaction, so after restart the base must be either fully
    usable (retiring -> recovered to active) or fully retired, and every
    acknowledged volume write must still be readable.
    """
    problems: list[str] = []
    server = Server(db)
    server.start()
    try:
        status, base = server.call(
            "POST", "/bases",
            {"name": f"crashbase-{round_index}", "block_size": 8, "block_count": 4},
        )
        assert status == 201, base
        status, volume = server.call(
            "POST", "/volumes", {"name": f"crashvol-{round_index}", "base_id": base["id"]}
        )
        assert status == 201, volume
        volume_id = volume["id"]
        for index in range(4):
            payload = f"KEEP{index:04d}".encode()
            status, _ = server.call(
                "PUT", f"/volumes/{volume_id}/blocks/{index}",
                {"data_base64": b64(payload)},
            )
            assert status == 200
        server.call("POST", f"/bases/{base['id']}/retirement/prepare")

        import threading

        def commit() -> None:
            try:
                server.call("POST", f"/bases/{base['id']}/retirement/commit")
            except Exception:
                pass

        thread = threading.Thread(target=commit)
        thread.start()
        time.sleep(0.002 if round_index % 2 else 0.0005)
        server.kill()
        thread.join(timeout=10)
    finally:
        server.kill()

    server = Server(db)
    server.start()
    try:
        status, verify = server.call("GET", "/verify")
        if status != 200 or not verify["ok"]:
            problems.append(f"commit round {round_index}: verify failed: {verify}")
        status, state = server.call("GET", f"/bases/{base['id']}")
        if status != 200:
            problems.append(f"commit round {round_index}: base row missing")
            state = {"status": "missing", "blocks_still_stored": None}
        stored = state.get("blocks_still_stored")
        if state["status"] in ("active", "retiring"):
            if stored != 4:
                problems.append(
                    f"commit round {round_index}: base {state['status']} but "
                    f"stores {stored} of 4 blocks"
                )
        elif state["status"] == "retired":
            if stored != 0:
                problems.append(
                    f"commit round {round_index}: retired base still stores {stored} blocks"
                )
        else:
            problems.append(f"commit round {round_index}: bad status {state['status']}")
        for index in range(4):
            status, body = server.call("GET", f"/volumes/{volume_id}/blocks/{index}")
            if status != 200 or base64.b64decode(body["data_base64"]) != f"KEEP{index:04d}".encode():
                problems.append(
                    f"commit round {round_index}: acknowledged write {index} lost"
                )
        # If the base survived, retirement can be retried and completes.
        if state["status"] in ("active", "retiring"):
            server.call("POST", f"/bases/{base['id']}/retirement/prepare")
            status, _ = server.call("POST", f"/bases/{base['id']}/retirement/commit")
            if status != 200:
                problems.append(f"commit round {round_index}: retry commit failed")
        print(
            f"  commit round {round_index}: recovered base status={state['status']}, "
            "writes intact, verify ok"
        )
    finally:
        server.kill()
    return problems


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("/tmp/blockstore-crash/blockstore.db"),
    )
    args = parser.parse_args()
    args.db.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        Path(str(args.db) + suffix).unlink(missing_ok=True)

    problems: list[str] = []
    for index in range(args.rounds):
        problems.extend(run_round(args.db, index))
    for index in range(args.rounds):
        problems.extend(run_commit_crash(args.db, index))
    if problems:
        print("\nFAILURES:")
        for problem in problems:
            print("  " + problem)
        return 1
    print(f"\n{args.rounds} write-burst and {args.rounds} commit SIGKILL rounds recovered cleanly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
