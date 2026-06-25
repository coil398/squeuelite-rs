"""Minimal SqueueLite client — Unix Domain Socket, JSON Lines.

SqueueLite is *write-only*. Send writes through this client; read straight from
the SQLite file with a read-only connection (WAL allows concurrent readers).

Example
-------
    from squeue import Squeue

    with Squeue("/run/app/squeuelite.sock", actor_id="agent-py") as db:
        resp = db.execute(
            "INSERT INTO events(agent_id, kind, payload) VALUES (?, ?, ?)",
            ["agent-py", "started", '{"ok": true}'],
            idempotency_key="run-7:step-1",   # safe to retry
            run_id="run-7",
        )
        # resp == {"request_id": "...", "status": "committed", "commit_seq": 12}

No third-party dependencies; standard library only.
"""

import json
import socket
import uuid


def _request_id() -> str:
    # uuid7 is time-ordered (nice for request_id); fall back to uuid4 on <3.13.
    factory = getattr(uuid, "uuid7", uuid.uuid4)
    return str(factory())


class SqueueError(RuntimeError):
    """Raised when the gateway closes the connection unexpectedly."""


class Squeue:
    def __init__(self, socket_path: str, actor_id: str):
        self.actor_id = actor_id
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.connect(socket_path)
        # Buffered, newline-aware IO in both directions.
        self._io = self._sock.makefile("rwb")

    # -- write API ---------------------------------------------------------

    def execute(self, sql, params=None, *, idempotency_key=None, run_id=None):
        """Run one statement as a single atomic transaction."""
        return self.transaction(
            [(sql, params or [])],
            idempotency_key=idempotency_key,
            run_id=run_id,
        )

    def transaction(self, ops, *, idempotency_key=None, run_id=None):
        """Run several (sql, params) ops as ONE transaction (all-or-nothing)."""
        req = {
            "request_id": _request_id(),
            "actor_id": self.actor_id,
            "operations": [{"sql": s, "params": list(p)} for (s, p) in ops],
        }
        if run_id is not None:
            req["run_id"] = run_id
        if idempotency_key is not None:
            req["idempotency_key"] = idempotency_key
        return self._roundtrip(json.dumps(req))

    # -- admin API ---------------------------------------------------------

    def stats(self):
        return self._roundtrip('{"type":"stats"}')

    def health(self):
        return self._roundtrip('{"type":"health"}')

    def checkpoint(self):
        return self._roundtrip('{"type":"checkpoint"}')

    # -- internals ---------------------------------------------------------

    def _roundtrip(self, line: str):
        self._io.write(line.encode() + b"\n")
        self._io.flush()
        reply = self._io.readline()
        if not reply:
            raise SqueueError("gateway closed the connection")
        return json.loads(reply)

    def close(self):
        try:
            self._io.close()
        finally:
            self._sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


if __name__ == "__main__":
    import sys

    sock_path = sys.argv[1] if len(sys.argv) > 1 else "./squeuelite.sock"
    with Squeue(sock_path, actor_id="demo-py") as db:
        print("health:", db.health())
        print(
            "write :",
            db.execute(
                "INSERT INTO events(agent_id, kind) VALUES (?, ?)",
                ["demo-py", "started"],
                idempotency_key="demo:1",
            ),
        )
        print("stats :", db.stats())
