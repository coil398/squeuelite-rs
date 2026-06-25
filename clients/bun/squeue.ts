// Minimal SqueueLite client for Bun — Unix Domain Socket, JSON Lines.
//
// Uses Bun's native `Bun.connect`. (Bun is also node:net compatible, so
// clients/node/squeue.mjs works under Bun too — this is the Bun-native variant.)
//
// SqueueLite is *write-only*; read the SQLite file directly (read-only, WAL).
//
// Run the demo:
//   bun clients/bun/squeue.ts ./squeuelite.sock
//
// Example:
//   import { Squeue } from "./squeue.ts";
//   const db = await Squeue.connect("/run/app/squeuelite.sock", "agent-bun");
//   const r = await db.execute(
//     "INSERT INTO events(agent_id, kind) VALUES (?, ?)",
//     ["agent-bun", "started"],
//     { idempotencyKey: "run-7:step-1", runId: "run-7" });
//   db.close();

import type { Socket } from "bun";

export interface WriteOpts {
  idempotencyKey?: string;
  runId?: string;
}

export class Squeue {
  #socket!: Socket;
  #buf = "";
  #waiters: Array<{ resolve: (v: unknown) => void; reject: (e: unknown) => void }> = [];

  private constructor(public actorId: string) {}

  static async connect(path: string, actorId: string): Promise<Squeue> {
    const self = new Squeue(actorId);
    self.#socket = await Bun.connect({
      unix: path,
      socket: {
        data(_sock, data) {
          self.#onData(data.toString());
        },
        close() {
          self.#failAll("gateway closed the connection");
        },
        error(_sock, err) {
          self.#failAll(String(err));
        },
      },
    });
    return self;
  }

  #onData(chunk: string) {
    this.#buf += chunk;
    let i: number;
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

  #failAll(msg: string) {
    while (this.#waiters.length) this.#waiters.shift()!.reject(new Error(msg));
  }

  #roundtrip(obj: unknown): Promise<unknown> {
    return new Promise((resolve, reject) => {
      this.#waiters.push({ resolve, reject });
      this.#socket.write(JSON.stringify(obj) + "\n");
    });
  }

  execute(sql: string, params: unknown[] = [], opts: WriteOpts = {}) {
    return this.transaction([[sql, params]], opts);
  }

  transaction(ops: Array<[string, unknown[]]>, { idempotencyKey, runId }: WriteOpts = {}) {
    const req: Record<string, unknown> = {
      request_id: crypto.randomUUID(),
      actor_id: this.actorId,
      operations: ops.map(([sql, params]) => ({ sql, params })),
    };
    if (runId != null) req.run_id = runId;
    if (idempotencyKey != null) req.idempotency_key = idempotencyKey;
    return this.#roundtrip(req);
  }

  stats() {
    return this.#roundtrip({ type: "stats" });
  }
  health() {
    return this.#roundtrip({ type: "health" });
  }
  checkpoint() {
    return this.#roundtrip({ type: "checkpoint" });
  }

  close() {
    this.#socket.end();
  }
}

if (import.meta.main) {
  const path = Bun.argv[2] ?? "./squeuelite.sock";
  const db = await Squeue.connect(path, "demo-bun");
  console.log("health:", await db.health());
  console.log(
    "write :",
    await db.execute("INSERT INTO events(agent_id, kind) VALUES (?, ?)", ["demo-bun", "started"], {
      idempotencyKey: "demo:1",
    }),
  );
  console.log("stats :", await db.stats());
  db.close();
}
