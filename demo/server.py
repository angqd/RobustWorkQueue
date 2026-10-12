"""Visual demo server for WorkQueue (stdlib only).

Run with:  venv/bin/python demo/server.py   (from the project root)
Then open: http://127.0.0.1:8765
"""

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import UUID

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from work_queue import JobState, QueueFull, WorkQueue  # noqa: E402

HOST = "127.0.0.1"
PORT = 8765
MAX_RETRIES = 2
TIMEOUT = 30
CAPACITY = 5
LOG_LINES = 50


class ManualClock:
    """Callable clock that only moves when advance() is called."""

    def __init__(self, start: float = 0.0):
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("cannot move the clock backwards")
        self.now += seconds


# One lock guards every API request, so the demo state is never touched concurrently.
STATE_LOCK = threading.Lock()
clock = ManualClock()
queue = WorkQueue(max_retries=MAX_RETRIES, timeout=TIMEOUT, capacity=CAPACITY, clock=clock)
completed_count = 0
log_lines: list[str] = []


def fmt_t(x: float) -> str:
    return f"{x:g}"


def short(job_id) -> str:
    return str(job_id)[:8]


def log(msg: str) -> None:
    log_lines.append(f"t={fmt_t(clock.now)} {msg}")
    del log_lines[:-LOG_LINES]


def reset_all() -> None:
    global clock, queue, completed_count
    clock = ManualClock()
    queue = WorkQueue(max_retries=MAX_RETRIES, timeout=TIMEOUT, capacity=CAPACITY, clock=clock)
    completed_count = 0
    log_lines.clear()
    log("reset: fresh queue, clock t=0")


def job_view(job) -> dict:
    return {
        "id": short(job.id),
        "full_id": str(job.id),
        "payload": job.payload,
        "state": job.state.value,
        "attempts": job.attempts,
        "reserved_until": job.reserved_until,
    }


def snapshot() -> dict:
    # Reads internals (_jobs, _pending, _dlq) because the queue has no public read API.
    # Caller must hold STATE_LOCK.
    jobs = queue._jobs
    pending = [
        job_view(jobs[j])
        for j in queue._pending
        if j in jobs and jobs[j].state is JobState.PENDING
    ]
    reserved = [job_view(j) for j in jobs.values() if j.state is JobState.RESERVED]
    dead = [job_view(j) for j in queue._dlq]
    return {
        "now": clock.now,
        "timeout": queue.timeout,
        "max_retries": queue.max_retries,
        "capacity": queue.capacity,
        "active": len(pending) + len(reserved),
        "pending": pending,
        "reserved": reserved,
        "dead": dead,
        "completed_count": completed_count,
        "log": list(log_lines),
    }


def handle_submit(body: dict):
    payload = body.get("payload")
    if not isinstance(payload, str) or not payload.strip():
        return 400, {"error": "payload must be a non-empty string"}
    try:
        job_id = queue.submit(payload)
    except QueueFull:
        log(f'submit "{payload}" -> rejected (QueueFull, capacity {CAPACITY})')
        return 200, {**snapshot(), "error": "QueueFull"}
    log(f'submit "{payload}" -> job {short(job_id)}')
    return 200, snapshot()


def handle_reserve(_body: dict):
    now = clock.now
    # Expired leases are only moved by the sweep inside reserve(); note them first.
    expired = [
        j.id
        for j in queue._jobs.values()
        if j.state is JobState.RESERVED and j.reserved_until is not None and j.reserved_until <= now
    ]
    lease = queue.reserve()
    for job_id in expired:
        if lease is not None and job_id == lease.job_id:
            continue
        log(f"lease expired: job {short(job_id)} -> {queue._jobs[job_id].state.value} (sweep)")
    if lease is None:
        log("reserve -> none available")
        out = snapshot()
        out["lease"] = None
        return 200, out
    note = " (expired lease swept, re-reserved)" if lease.job_id in expired else ""
    log(f"reserve -> job {short(lease.job_id)} token={lease.token}{note}")
    out = snapshot()
    out["lease"] = {
        "job_id": str(lease.job_id),
        "payload": lease.payload,
        "token": lease.token,
    }
    return 200, out


def _parse_uuid(value):
    try:
        return UUID(str(value))
    except ValueError:
        return None


def _reject_reason(job, token: int, now: float) -> str:
    # Mirrors the guard order in WorkQueue._check_lease_locked, for log messages only.
    if job is None:
        return "unknown job"
    if job.state is not JobState.RESERVED:
        return f"job is {job.state.value}"
    if token != job.attempts:
        return "stale token"
    if now >= job.reserved_until:
        return "lease expired"
    return "rejected"


def handle_lease_op(body: dict, op: str):
    global completed_count
    token = body.get("token")
    if not isinstance(token, int) or isinstance(token, bool):
        return 400, {"error": "token must be an integer"}
    job_id = _parse_uuid(body.get("job_id"))
    if job_id is None:
        log(f"{op} job {str(body.get('job_id'))[:8]} token={token} -> rejected (unknown job)")
        return 200, {**snapshot(), "ok": False}

    job = queue._jobs.get(job_id)
    reason = _reject_reason(job, token, clock.now)
    if op == "complete":
        ok = queue.complete(job_id, token)
    else:
        ok = queue.fail(job_id, token)

    if not ok:
        log(f"{op} job {short(job_id)} token={token} -> rejected ({reason})")
    elif op == "complete":
        completed_count += 1
        log(f"complete job {short(job_id)} token={token} -> ok (evicted)")
    else:
        state = queue._jobs[job_id].state.value
        log(f"fail job {short(job_id)} token={token} -> ok, job now {state}")

    out = snapshot()
    out["ok"] = ok
    return 200, out


def handle_complete(body: dict):
    return handle_lease_op(body, "complete")


def handle_fail(body: dict):
    return handle_lease_op(body, "fail")


def handle_advance(body: dict):
    try:
        seconds = float(body.get("seconds"))
    except (TypeError, ValueError):
        return 400, {"error": "seconds must be a number"}
    if seconds < 0:
        return 400, {"error": "seconds must be >= 0"}
    clock.advance(seconds)
    log(f"clock +{fmt_t(seconds)}s")
    return 200, snapshot()


def handle_reset(_body: dict):
    reset_all()
    return 200, snapshot()


POST_ROUTES = {
    "/api/submit": handle_submit,
    "/api/reserve": handle_reserve,
    "/api/complete": handle_complete,
    "/api/fail": handle_fail,
    "/api/advance": handle_advance,
    "/api/reset": handle_reset,
}


class Handler(BaseHTTPRequestHandler):
    server_version = "WorkQueueDemo/1.0"

    def log_message(self, format, *args):  # keep server.log quiet
        pass

    def _send(self, status: int, payload: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _json(self, status: int, obj: dict) -> None:
        self._send(status, json.dumps(obj).encode("utf-8"), "application/json; charset=utf-8")

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            with open(os.path.join(HERE, "index.html"), "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
            return
        if path == "/api/state":
            with STATE_LOCK:
                body = snapshot()
            self._json(200, body)
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        handler = POST_ROUTES.get(path)
        if handler is None:
            self._json(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError:
            self._json(400, {"error": "invalid JSON"})
            return
        if not isinstance(body, dict):
            self._json(400, {"error": "body must be a JSON object"})
            return
        with STATE_LOCK:
            try:
                status, out = handler(body)
            except ValueError as exc:
                status, out = 400, {"error": str(exc)}
        self._json(status, out)


class DemoServer(ThreadingHTTPServer):
    daemon_threads = True


if __name__ == "__main__":
    httpd = DemoServer((HOST, PORT), Handler)
    print(f"Work queue demo on http://{HOST}:{PORT}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
