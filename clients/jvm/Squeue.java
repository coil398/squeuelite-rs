// Minimal SqueueLite client for the JVM (Java 16+) — dependency-free.
//
// Uses java.nio Unix Domain Socket support (Java 16+). Kotlin / Scala / Clojure
// can call it directly. SqueueLite is *write-only*; read the SQLite file
// directly (read-only, WAL).
//
// Methods return the raw JSON response line as a String so you can parse it with
// whatever JSON library you already use (Jackson, Gson, …) — or none.
//
// Demo:  java clients/jvm/Squeue.java ./squeuelite.sock

package squeue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

public final class Squeue implements AutoCloseable {
    private final String actorId;
    private final SocketChannel ch;

    public Squeue(String socketPath, String actorId) throws IOException {
        this.actorId = actorId;
        this.ch = SocketChannel.open(StandardProtocolFamily.UNIX);
        this.ch.connect(UnixDomainSocketAddress.of(socketPath));
    }

    /** Run one statement as a single transaction. Returns the raw JSON response line. */
    public String execute(String sql, List<Object> params) throws IOException {
        return execute(sql, params, null, null);
    }

    /** Run one statement, optionally with idempotency_key / run_id. */
    public String execute(String sql, List<Object> params, String idempotencyKey, String runId)
            throws IOException {
        String ops = "[{\"sql\":" + jsonStr(sql) + ",\"params\":" + jsonArray(params) + "}]";
        return sendRequest(ops, idempotencyKey, runId);
    }

    /** Admin: {"type":"stats"} etc. Returns the raw JSON response line. */
    public String admin(String type) throws IOException {
        writeLine("{\"type\":" + jsonStr(type) + "}");
        return readLine();
    }

    private String sendRequest(String operationsJson, String idempotencyKey, String runId)
            throws IOException {
        StringBuilder b = new StringBuilder();
        b.append("{\"request_id\":").append(jsonStr(UUID.randomUUID().toString()));
        b.append(",\"actor_id\":").append(jsonStr(actorId));
        if (runId != null) b.append(",\"run_id\":").append(jsonStr(runId));
        if (idempotencyKey != null) b.append(",\"idempotency_key\":").append(jsonStr(idempotencyKey));
        b.append(",\"operations\":").append(operationsJson).append("}");
        writeLine(b.toString());
        return readLine();
    }

    private void writeLine(String json) throws IOException {
        ByteBuffer out = ByteBuffer.wrap((json + "\n").getBytes(StandardCharsets.UTF_8));
        while (out.hasRemaining()) {
            ch.write(out);
        }
    }

    private String readLine() throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        ByteBuffer one = ByteBuffer.allocate(1);
        while (true) {
            one.clear();
            int n = ch.read(one);
            if (n < 0) throw new IOException("gateway closed the connection");
            if (n == 0) continue;
            byte b = one.get(0);
            if (b == '\n') break;
            line.write(b);
        }
        return line.toString(StandardCharsets.UTF_8);
    }

    // -- tiny JSON encoder (strings + scalar params only) ---------------------

    private static String jsonArray(List<Object> params) {
        StringBuilder b = new StringBuilder("[");
        for (int i = 0; i < params.size(); i++) {
            if (i > 0) b.append(",");
            b.append(jsonValue(params.get(i)));
        }
        return b.append("]").toString();
    }

    private static String jsonValue(Object v) {
        if (v == null) return "null";
        if (v instanceof Number || v instanceof Boolean) return v.toString();
        return jsonStr(v.toString());
    }

    private static String jsonStr(String s) {
        StringBuilder b = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n");  break;
                case '\r': b.append("\\r");  break;
                case '\t': b.append("\\t");  break;
                default:
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
            }
        }
        return b.append("\"").toString();
    }

    @Override
    public void close() throws IOException {
        ch.close();
    }

    public static void main(String[] args) throws IOException {
        String path = args.length > 0 ? args[0] : "./squeuelite.sock";
        try (Squeue db = new Squeue(path, "demo-jvm")) {
            System.out.println("health: " + db.admin("health"));
            System.out.println("write : " + db.execute(
                    "INSERT INTO events(agent_id, kind) VALUES (?, ?)",
                    List.of("demo-jvm", "started"), "demo:1", null));
            System.out.println("stats : " + db.admin("stats"));
        }
    }
}
