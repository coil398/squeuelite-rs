// Minimal SqueueLite client — Unix Domain Socket, JSON-RPC 2.0.
//
// SqueueLite is *write-only*. Send writes through this client; read straight
// from the SQLite file with a read-only connection (WAL allows readers).
//
// Example:
//   import { Squeue } from "./squeue.mjs";
//
//   const db = await Squeue.connect("/run/app/squeuelite.sock", "agent-node");
//   const r = await db.execute(
//     "INSERT INTO events(agent_id, kind) VALUES (?, ?)",
//     ["agent-node", "started"],
//     { idempotencyKey: "run-7:step-1", runId: "run-7" });
//   // r == { jsonrpc: "2.0", id: 1, result: { status: "committed", commit_seq: 12 } }
//   // Check r.result for success, r.error for failure.
//   db.close();
//
// No third-party dependencies; Node >= 16.

import net from "node:net";

// Wrap binary data for a BLOB column parameter: {"$blob": "<base64>"}.
// `data` may be a Buffer, Uint8Array, or ArrayBuffer.
//   db.execute("INSERT INTO files(data) VALUES (?)", [blob(buf)])
export function blob(data) {
  return { $blob: Buffer.from(data).toString("base64") };
}

export class Squeue {
  #sock;
  #buf = "";
  #waiters = [];
  #counter = 0;

  static connect(socketPath, actorId) {
    return new Promise((resolve, reject) => {
      const sock = net.createConnection(socketPath);
      const self = new Squeue();
      self.#sock = sock;
      self.actorId = actorId;
      sock.setEncoding("utf8");
      sock.once("connect", () => resolve(self));
      sock.once("error", reject);
      sock.on("data", (chunk) => self.#onData(chunk));
      sock.on("close", () => {
        while (self.#waiters.length) {
          self.#waiters.shift().reject(new Error("gateway closed the connection"));
        }
      });
    });
  }

  #onData(chunk) {
    this.#buf += chunk;
    let i;
    while ((i = this.#buf.indexOf("\n")) >= 0) {
      const line = this.#buf.slice(0, i);
      this.#buf = this.#buf.slice(i + 1);
      const w = this.#waiters.shift();
      if (!w) continue;
      try {
        w.resolve(JSON.parse(line));
      } catch (e) {
        w.reject(e);
      }
    }
  }

  #nextId() {
    return ++this.#counter;
  }

  #roundtrip(obj) {
    return new Promise((resolve, reject) => {
      this.#waiters.push({ resolve, reject });
      this.#sock.write(JSON.stringify(obj) + "\n");
    });
  }

  // Run one statement as a single atomic transaction.
  execute(sql, params = [], opts = {}) {
    return this.transaction([[sql, params]], opts);
  }

  // Run several [sql, params] ops as ONE transaction (all-or-nothing).
  transaction(ops, { idempotencyKey, runId } = {}) {
    const rpcParams = {
      actor_id: this.actorId,
      operations: ops.map(([sql, params]) => ({ sql, params })),
    };
    if (runId != null) rpcParams.run_id = runId;
    if (idempotencyKey != null) rpcParams.idempotency_key = idempotencyKey;
    return this.#roundtrip({
      jsonrpc: "2.0",
      id: this.#nextId(),
      method: "execute",
      params: rpcParams,
    });
  }

  stats() {
    return this.#roundtrip({ jsonrpc: "2.0", id: this.#nextId(), method: "stats" });
  }
  health() {
    return this.#roundtrip({ jsonrpc: "2.0", id: this.#nextId(), method: "health" });
  }
  checkpoint() {
    return this.#roundtrip({ jsonrpc: "2.0", id: this.#nextId(), method: "checkpoint" });
  }

  close() {
    this.#sock.end();
  }
}

// Tiny demo:  node squeue.mjs ./squeuelite.sock
if (import.meta.url === `file://${process.argv[1]}`) {
  const path = process.argv[2] ?? "./squeuelite.sock";
  const db = await Squeue.connect(path, "demo-node");
  console.log("health:", await db.health());
  console.log(
    "write :",
    await db.execute("INSERT INTO events(agent_id, kind) VALUES (?, ?)", ["demo-node", "started"], {
      idempotencyKey: "demo:1",
    }),
  );
  console.log("stats :", await db.stats());
  db.close();
}
