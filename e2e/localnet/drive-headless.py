#!/usr/bin/env python3
"""Run localnet without a person at the keyboard.

The orchestrator's front end is a crossterm TUI, which refuses a pipe and reads
its keys from a terminal, so a headless run gives it a pty and nothing else:
the run itself is driven through the orchestrator's own HTTP API, and the only
keystroke sent is the `q` that shuts every node down cleanly.

    make run-solana                       # in another terminal, first
    cargo build --release -p tape-node --features metrics
    cargo build --release -p tape-e2e-localnet --bin localnet
    ulimit -n 65536
    python3 e2e/localnet/drive-headless.py

Environment, all optional:

    LOCALNET_NODES          nodes to start, default 20
    LOCALNET_RUN_SECONDS    seconds to hold the fleet up, default 600
    LOCALNET_BOOT_TIMEOUT   seconds to wait for the api, default 900
    LOCALNET_UPLOADS        uploads to start during the run, default 1
    LOCALNET_UPLOAD_EVERY   seconds between uploads, default 60
    LOCALNET_UPLOAD_SIZES   comma separated byte sizes to draw from, read by
                            the orchestrator; unset keeps the random spread
    LOCALNET_LOG_DIR        where the pty capture and snapshots land,
                            default target/localnet-run

Disk: every node reserves its tails up front, which is 8 GiB per node on a
14-core box, so twenty nodes take 160 GiB before a byte is uploaded.
"""

import fcntl
import json
import os
import pty
import select
import signal
import struct
import subprocess
import sys
import termios
import time
import urllib.request

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
API = f"http://127.0.0.1:{os.environ.get('LOCALNET_API_PORT', '9000')}"

NODES = int(os.environ.get("LOCALNET_NODES", "20"))
RUN_SECONDS = int(os.environ.get("LOCALNET_RUN_SECONDS", "600"))
BOOT_TIMEOUT = int(os.environ.get("LOCALNET_BOOT_TIMEOUT", "900"))
UPLOADS = int(os.environ.get("LOCALNET_UPLOADS", "1"))
UPLOAD_EVERY = float(os.environ.get("LOCALNET_UPLOAD_EVERY", "60"))
LOG_DIR = os.environ.get("LOCALNET_LOG_DIR", os.path.join(ROOT, "target/localnet-run"))


def say(message):
    print(f"[driver {time.strftime('%H:%M:%S')}] {message}", flush=True)


def get(path, timeout=10):
    with urllib.request.urlopen(API + path, timeout=timeout) as response:
        return json.loads(response.read())


def post(path, timeout=30):
    request = urllib.request.Request(API + path, method="POST", data=b"")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    master, slave = pty.openpty()
    # A window wide enough that the TUI lays its grid out rather than clipping it.
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 50, 200, 0, 0))

    argv = [
        os.path.join(ROOT, "target/release/localnet"),
        "--node-binary", os.path.join(ROOT, "target/release/tape-node"),
        "--rpc-url", os.environ.get("LOCALNET_RPC_URL", "http://127.0.0.1:8899"),
        "--api-port", os.environ.get("LOCALNET_API_PORT", "9000"),
        "--init-nodes", str(NODES),
    ]
    say(f"spawning: {' '.join(argv)}")
    # Its own process group, so a stuck run can be killed without taking the
    # shell with it, and so the nodes it spawned go with it.
    child = subprocess.Popen(
        argv, cwd=ROOT, stdin=slave, stdout=slave, stderr=slave,
        close_fds=True, start_new_session=True,
    )
    os.close(slave)
    say(f"localnet pid {child.pid}")

    raw = open(os.path.join(LOG_DIR, "localnet-pty.log"), "wb")
    snaps = open(os.path.join(LOG_DIR, "localnet-snapshots.jsonl"), "w")

    def pump():
        """Drain the pty, so the TUI never blocks on a full buffer."""
        while True:
            ready, _, _ = select.select([master], [], [], 0)
            if not ready:
                return
            try:
                chunk = os.read(master, 65536)
            except OSError:
                return
            if not chunk:
                return
            raw.write(chunk)
            raw.flush()

    started = time.time()
    while True:
        pump()
        try:
            get("/api/health", timeout=3)
            break
        except Exception:
            pass
        if child.poll() is not None:
            say(f"localnet exited early, code {child.returncode}")
            return 1
        if time.time() - started > BOOT_TIMEOUT:
            say("api never came up")
            os.killpg(os.getpgid(child.pid), signal.SIGKILL)
            return 1
        time.sleep(2)
    say(f"api healthy after {time.time() - started:.0f}s")

    deadline = time.time() + RUN_SECONDS
    next_snapshot = 0.0
    next_upload = time.time() + 30
    uploads_started = 0
    last_uploads = []
    while time.time() < deadline:
        pump()
        if child.poll() is not None:
            say(f"localnet exited early, code {child.returncode}")
            return 1
        now = time.time()
        if uploads_started < UPLOADS and now >= next_upload:
            try:
                upload = post("/api/uploads")
                uploads_started += 1
                say(f"upload started: {upload['size_bytes']} bytes to {upload['tape_address']}")
                next_upload = now + UPLOAD_EVERY
            except Exception as error:
                say(f"upload request failed: {error}")
                next_upload = now + 30
        if now >= next_snapshot:
            next_snapshot = now + 10
            try:
                snapshot = get("/api/snapshot")
            except Exception as error:
                say(f"snapshot failed: {error}")
                continue
            cluster = snapshot["cluster"]
            healthy = sum(1 for node in snapshot["nodes"] if node["healthy"])
            snaps.write(json.dumps({"at": now, **snapshot}) + "\n")
            snaps.flush()
            last_uploads = snapshot["uploads"]
            say(
                f"epoch {cluster['epoch']} phase {cluster['phase']} slot {cluster['slot']} "
                f"committee {cluster['committee_size']} healthy {healthy}/{len(snapshot['nodes'])} "
                f"spools {len(snapshot['spools'])} "
                f"uploads {[u['cert_status'] for u in snapshot['uploads']]}"
            )
        time.sleep(1)

    say("sending q to the tui")
    os.write(master, b"q")
    stopped = time.time()
    while child.poll() is None:
        pump()
        if time.time() - stopped > 300:
            say("clean shutdown timed out, killing the process group")
            os.killpg(os.getpgid(child.pid), signal.SIGKILL)
            break
        time.sleep(1)
    pump()
    say(f"localnet exit code {child.returncode}, shutdown took {time.time() - stopped:.0f}s")
    raw.close()
    snaps.close()
    report(last_uploads, uploads_started)
    return 0


# Size buckets a row is reported under, by upper bound in bytes.
BUCKETS = [
    ("<=1 KiB", 1024),
    ("<=1 MiB", 1024 * 1024),
    ("<=16 MiB", 16 * 1024 * 1024),
    ("<=64 MiB", 64 * 1024 * 1024),
    ("stream", float("inf")),
]


def bucket_of(size):
    for name, ceiling in BUCKETS:
        if size <= ceiling:
            return name
    return BUCKETS[-1][0]


def median(sorted_times):
    """The middle time, averaging the two middle ones on an even count."""
    middle = len(sorted_times) // 2
    if len(sorted_times) % 2:
        return sorted_times[middle]
    return (sorted_times[middle - 1] + sorted_times[middle]) / 2


def report(uploads, started):
    """What the run uploaded, how long each size took, and what went wrong."""
    path = os.path.join(LOG_DIR, "uploads.json")
    with open(path, "w") as handle:
        json.dump(uploads, handle, indent=2)

    settled = [u for u in uploads if u.get("settled_ms") is not None]
    certified = [u for u in settled if u["cert_status"] == "yes"]
    refused = [u for u in settled if u["cert_status"] != "yes"]
    pending = [u for u in uploads if u.get("settled_ms") is None]

    say(f"uploads started {started}, seen {len(uploads)}, "
        f"certified {len(certified)}, refused {len(refused)}, pending {len(pending)}")

    by_bucket = {}
    for upload in certified:
        by_bucket.setdefault(bucket_of(upload["size_bytes"]), []).append(upload["settled_ms"])
    for name, _ in BUCKETS:
        times = sorted(by_bucket.get(name, []))
        if not times:
            continue
        say(f"  {name:>9}  n={len(times):<4} "
            f"min={times[0] / 1000:.1f}s  "
            f"median={median(times) / 1000:.1f}s  "
            f"max={times[-1] / 1000:.1f}s")

    faults = len(refused) + len(pending)
    for upload in refused + pending:
        say(f"  FAULT {upload['size_bytes']}B {upload['cert_status']} "
            f"{upload.get('last_error') or ''}")
    say(f"faults: {faults}")


if __name__ == "__main__":
    sys.exit(main())
