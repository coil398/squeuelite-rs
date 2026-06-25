"""Minimal SqueueLite client — Unix Domain Socket, JSON-RPC 2.0.

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
        # resp == {"jsonrpc": "2.0", "id": 1, "result": {"status": "committed", "commit_seq": 12}}
        # Check resp["result"] for success, resp["error"] for failure.

No third-party dependencies; standard library only.
"""

import base64
import json
import socket


def blob(data: bytes) -> dict:
    """Wrap binary data for a BLOB column parameter: {"$blob": "<base64>"}.

    Example:
        db.execute("INSERT INTO files(data) VALUES (?)", [blob(b"\\x00\\x01\\x02")])
    """
    return {"$blob": base64.b64encode(data).decode("ascii")}


class SqueueError(RuntimeError):
    """Raised when the gateway closes the connection unexpectedly."""


class Squeue:
    def __init__(self, socket_path: str, actor_id: str):
        self.actor_id = actor_id
        self._counter = 0
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.connect(socket_path)
        # Buffered, newline-aware IO in both directions.
        self._io = self._sock.makefile("rwb")

    def _next_id(self) -> int:
        self._counter += 1
        return self._counter

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
        rpc_params = {
            "actor_id": self.actor_id,
            "operations": [{"sql": s, "params": list(p)} for (s, p) in ops],
        }
        if run_id is not None:
            rpc_params["run_id"] = run_id
        if idempotency_key is not None:
            rpc_params["idempotency_key"] = idempotency_key
        req = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "execute",
            "params": rpc_params,
        }
        return self._roundtrip(json.dumps(req))

    # -- admin API ---------------------------------------------------------

    def stats(self):
        req = {"jsonrpc": "2.0", "id": self._next_id(), "method": "stats"}
        return self._roundtrip(json.dumps(req))

    def health(self):
        req = {"jsonrpc": "2.0", "id": self._next_id(), "method": "health"}
        return self._roundtrip(json.dumps(req))

    def checkpoint(self):
        req = {"jsonrpc": "2.0", "id": self._next_id(), "method": "checkpoint"}
        return self._roundtrip(json.dumps(req))

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
        resp = db.health()
        print("health:", resp)
        resp = db.execute(
            "INSERT INTO events(agent_id, kind) VALUES (?, ?)",
            ["demo-py", "started"],
            idempotency_key="demo:1",
        )
        print("write :", resp)
        print("stats :", db.stats())
