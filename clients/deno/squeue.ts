// Minimal SqueueLite client for Deno — Unix Domain Socket, JSON-RPC 2.0.
//
// SqueueLite is *write-only*; read the SQLite file directly (read-only, WAL).
//
// Run the demo. A Unix-socket connect needs read+write permission on the socket
// file AND net access to the unix address:
//   deno run --allow-read --allow-write --allow-net clients/deno/squeue.ts ./squeuelite.sock
//
// Example:
//   import { Squeue } from "./squeue.ts";
//   const db = await Squeue.connect("/run/app/squeuelite.sock", "agent-deno");
//   const r = await db.execute(
//     "INSERT INTO events(agent_id, kind) VALUES (?, ?)",
//     ["agent-deno", "started"],
//     { idempotencyKey: "run-7:step-1", runId: "run-7" });
//   // r == { jsonrpc: "2.0", id: 1, result: { status: "committed", commit_seq: 12 } }
//   // Check r.result for success, r.error for failure.
//   db.close();

// Wrap binary data for a BLOB column parameter: {"$blob": "<base64>"}.
//   db.execute("INSERT INTO files(data) VALUES (?)", [blob(bytes)])
export function blob(data: Uint8Array): { $blob: string } {
  let bin = "";
  for (const b of data) bin += String.fromCharCode(b);
  return { $blob: btoa(bin) };
}

export interface WriteOpts {
  idempotencyKey?: string;
  runId?: string;
}

export class Squeue {
  #conn: Deno.UnixConn;
  #buf = "";
  #enc = new TextEncoder();
  #dec = new TextDecoder();
  #counter = 0;

  private constructor(conn: Deno.UnixConn, public actorId: string) {
    this.#conn = conn;
  }

  static async connect(path: string, actorId: string): Promise<Squeue> {
    const conn = await Deno.connect({ transport: "unix", path });
    return new Squeue(conn, actorId);
  }

  #nextId(): number {
    return ++this.#counter;
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
    const rpcParams: Record<string, unknown> = {
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
