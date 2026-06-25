// Package squeue is a minimal SqueueLite client — Unix Domain Socket, JSON Lines.
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
//	// resp.Status == "committed"; resp.CommitSeq != nil
//
// Dependency: github.com/google/uuid  (go get github.com/google/uuid)
package squeue

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"

	"github.com/google/uuid"
)

// Op is a single parameterised statement. Params are positional (`?`).
type Op struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params"`
}

// Response is the gateway's reply to a write request.
type Response struct {
	RequestID string  `json:"request_id"`
	Status    string  `json:"status"` // "committed" | "failed"
	CommitSeq *int64  `json:"commit_seq,omitempty"`
	Error     *string `json:"error,omitempty"`
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
}

// Connect dials the gateway socket as actorID.
func Connect(socketPath, actorID string) (*Client, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, err
	}
	return &Client{actorID: actorID, conn: conn, r: bufio.NewReader(conn)}, nil
}

// Execute runs one statement as a single atomic transaction.
func (c *Client) Execute(sql string, params []any, opts ...With) (*Response, error) {
	return c.Transaction([]Op{{SQL: sql, Params: params}}, opts...)
}

// Transaction runs several ops as ONE transaction (all-or-nothing).
func (c *Client) Transaction(ops []Op, opts ...With) (*Response, error) {
	req := map[string]any{
		"request_id": uuid.NewString(),
		"actor_id":   c.actorID,
		"operations": ops,
	}
	if len(opts) > 0 {
		if opts[0].RunID != "" {
			req["run_id"] = opts[0].RunID
		}
		if opts[0].IdempotencyKey != "" {
			req["idempotency_key"] = opts[0].IdempotencyKey
		}
	}
	return c.roundtrip(req)
}

func (c *Client) roundtrip(req any) (*Response, error) {
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
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }
