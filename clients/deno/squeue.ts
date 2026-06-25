// Minimal SqueueLite client for Deno — Unix Domain Socket, JSON Lines.
//
// SqueueLite is *write-only*; read the SQLite file directly (read-only, WAL).
//
// Run the demo (Unix socket connect needs read+write permission on the path):
//   deno run --allow-read --allow-write clients/deno/squeue.ts ./squeuelite.sock
//
// Example:
//   import { Squeue } from "./squeue.ts";
//   const db = await Squeue.connect("/run/app/squeuelite.sock", "agent-deno");
//   const r = await db.execute(
//     "INSERT INTO events(agent_id, kind) VALUES (?, ?)",
//     ["agent-deno", "started"],
//     { idempotencyKey: "run-7:step-1", runId: "run-7" });
//   db.close();

export interface WriteOpts {
  idempotencyKey?: string;
  runId?: string;
}

export class Squeue {
  #conn: Deno.UnixConn;
  #buf = "";
  #enc = new TextEncoder();
  #dec = new TextDecoder();

  private constructor(conn: Deno.UnixConn, public actorId: string) {
    this.#conn = conn;
  }

  static async connect(path: string, actorId: string): Promise<Squeue> {
    const conn = await Deno.connect({ transport: "unix", path });
    return new Squeue(conn, actorId);
  }

  // Run one statement as a single atomic transaction.
  execute(sql: string, params: unknown[] = [], opts: WriteOpts = {}) {
    return this.transaction([[sql, params]], opts);
  }

  // Run several [sql, params] ops as ONE transaction (all-or-nothing).
  transaction(
    ops: Array<[string, unknown[]]>,
    { idempotencyKey, runId }: WriteOpts = {},
  ) {
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

  async #roundtrip(obj: unknown): Promise<Record<string, unknown>> {
    await this.#conn.write(this.#enc.encode(JSON.stringify(obj) + "\n"));
    return await this.#readLine();
  }

  async #readLine(): Promise<Record<string, unknown>> {
    while (!this.#buf.includes("\n")) {
      const chunk = new Uint8Array(4096);
      const n = await this.#conn.read(chunk);
      if (n === null) throw new Error("gateway closed the connection");
      this.#buf += this.#dec.decode(chunk.subarray(0, n));
    }
    const i = this.#buf.indexOf("\n");
    const line = this.#buf.slice(0, i);
    this.#buf = this.#buf.slice(i + 1);
    return JSON.parse(line);
  }

  close() {
    this.#conn.close();
  }
}

if (import.meta.main) {
  const path = Deno.args[0] ?? "./squeuelite.sock";
  const db = await Squeue.connect(path, "demo-deno");
  console.log("health:", await db.health());
  console.log(
    "write :",
    await db.execute(
      "INSERT INTO events(agent_id, kind) VALUES (?, ?)",
      ["demo-deno", "started"],
      { idempotencyKey: "demo:1" },
    ),
  );
  console.log("stats :", await db.stats());
  db.close();
}
