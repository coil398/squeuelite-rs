// Package squeue is a minimal SqueueLite client — Unix Domain Socket, JSON-RPC 2.0.
//
// SqueueLite is *write-only*. Send writes through this client; read straight
// from the SQLite file with a read-only connection (WAL allows readers).
//
// Example:
//
//	c, err := squeue.Connect("/run/app/squeuelite.sock", "agent-go")
//	if err != nil { log.Fatal(err) }
//	defer c.Close()
//
//	resp, err := c.Execute(
//		"INSERT INTO events(agent_id, kind) VALUES (?, ?)",
//		[]any{"agent-go", "started"},
//		squeue.With{IdempotencyKey: "run-7:step-1", RunID: "run-7"},
//	)
//	// resp["result"] == map[status:committed commit_seq:12]
//	// Check resp["result"] for success, resp["error"] for failure.
//
// No extra dependencies needed beyond the standard library.
package squeue

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
)

// Op is a single parameterised statement. Params are positional (`?`).
type Op struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params"`
}

// Blob wraps binary data for a BLOB column parameter: {"$blob": "<base64>"}.
//
//	c.Execute("INSERT INTO files(data) VALUES (?)", []any{squeue.Blob(bytes)})
func Blob(data []byte) map[string]string {
	return map[string]string{"$blob": base64.StdEncoding.EncodeToString(data)}
}

// With carries optional request metadata.
type With struct {
	IdempotencyKey string // dedup retried writes (e.g. "run-7:step-1")
	RunID          string // group related writes
}

// Client owns one Unix Domain Socket connection (one in-flight request at a time).
type Client struct {
	actorID string
	conn    net.Conn
	r       *bufio.Reader
	counter atomic.Int64
}

// Connect dials the gateway socket as actorID.
func Connect(socketPath, actorID string) (*Client, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, err
	}
	return &Client{actorID: actorID, conn: conn, r: bufio.NewReader(conn)}, nil
}

func (c *Client) nextID() int64 {
	return c.counter.Add(1)
}

// Execute runs one statement as a single atomic transaction.
func (c *Client) Execute(sql string, params []any, opts ...With) (map[string]any, error) {
	return c.Transaction([]Op{{SQL: sql, Params: params}}, opts...)
}

// Transaction runs several ops as ONE transaction (all-or-nothing).
func (c *Client) Transaction(ops []Op, opts ...With) (map[string]any, error) {
	rpcParams := map[string]any{
		"actor_id":   c.actorID,
		"operations": ops,
	}
	if len(opts) > 0 {
		if opts[0].RunID != "" {
			rpcParams["run_id"] = opts[0].RunID
		}
		if opts[0].IdempotencyKey != "" {
			rpcParams["idempotency_key"] = opts[0].IdempotencyKey
		}
	}
	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      c.nextID(),
		"method":  "execute",
		"params":  rpcParams,
	}
	return c.roundtrip(req)
}

// Stats returns a JSON-RPC response with server stats in "result".
func (c *Client) Stats() (map[string]any, error) {
	return c.roundtrip(map[string]any{
		"jsonrpc": "2.0",
		"id":      c.nextID(),
		"method":  "stats",
	})
}

// Health returns a JSON-RPC response with {"status":"ok"} in "result".
func (c *Client) Health() (map[string]any, error) {
	return c.roundtrip(map[string]any{
		"jsonrpc": "2.0",
		"id":      c.nextID(),
		"method":  "health",
	})
}

// Checkpoint triggers a WAL checkpoint and returns a JSON-RPC response.
func (c *Client) Checkpoint() (map[string]any, error) {
	return c.roundtrip(map[string]any{
		"jsonrpc": "2.0",
		"id":      c.nextID(),
		"method":  "checkpoint",
	})
}

func (c *Client) roundtrip(req any) (map[string]any, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if _, err := c.conn.Write(append(b, '\n')); err != nil {
		return nil, err
	}
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("gateway closed the connection: %w", err)
	}
	var resp map[string]any
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }
