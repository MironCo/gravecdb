// Package bolt implements a Neo4j Bolt protocol server
package bolt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"

	"github.com/MironCo/gravecdb/bolt/messages"
	"github.com/MironCo/gravecdb/bolt/packstream"
	"github.com/MironCo/gravecdb/graph"
)

// Bolt protocol constants
var (
	MagicPreamble = []byte{0x60, 0x60, 0xB0, 0x17} // "GoGoBolt"

	// We'll claim to support Bolt 4.4 - widely compatible
	// The actual message format is similar enough across versions
	Bolt44Version = []byte{0x00, 0x00, 0x04, 0x04}
	NoVersion     = []byte{0x00, 0x00, 0x00, 0x00}
)

// Server is a Bolt protocol server
type Server struct {
	listener net.Listener
	db       *graph.DiskGraph
	addr     string
	mu       sync.RWMutex
	running  bool
}

// NewServer creates a new Bolt server
func NewServer(addr string, db *graph.DiskGraph) *Server {
	return &Server{
		addr: addr,
		db:   db,
	}
}

// Start starts the Bolt server
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}

	s.mu.Lock()
	s.listener = listener
	s.running = true
	s.mu.Unlock()

	fmt.Printf("Bolt server listening on %s\n", s.addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.RLock()
			running := s.running
			s.mu.RUnlock()

			if !running {
				return nil // Server was stopped
			}
			fmt.Printf("Accept error: %v\n", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

// Stop stops the Bolt server
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.running = false
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// Connection represents a client connection
type Connection struct {
	conn    net.Conn
	db      *graph.DiskGraph
	encoder *packstream.Encoder
	decoder *packstream.Decoder
	version []byte
	failed  bool // Track if we're in a failed state

	// Transaction state
	inTransaction bool
	tx            graph.GraphTransaction
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	c := &Connection{
		conn:    conn,
		db:      s.db,
		encoder: packstream.NewEncoder(conn, math.MaxUint16),
		decoder: packstream.NewDecoder(conn),
		version: make([]byte, 4),
	}

	// Cleanup on disconnect
	defer func() {
		// Rollback any active transaction
		if c.inTransaction && c.tx != nil {
			c.tx.Rollback()
			fmt.Printf("Transaction rolled back due to disconnect\n")
		}
		// Clean up pending results
		pendingMu.Lock()
		delete(pendingResults, c)
		pendingMu.Unlock()
	}()

	// Perform handshake
	if err := c.handshake(); err != nil {
		fmt.Printf("Handshake failed: %v\n", err)
		return
	}

	fmt.Printf("Client connected from %s\n", conn.RemoteAddr())

	// Handle messages
	for {
		if err := c.handleMessage(); err != nil {
			if err == io.EOF {
				fmt.Printf("Client disconnected: %s\n", conn.RemoteAddr())
				return
			}
			fmt.Printf("Error handling message: %v\n", err)
			return
		}
	}
}

func (c *Connection) handshake() error {
	// Read magic preamble + 4 version proposals (20 bytes total)
	handshakeData := make([]byte, 20)
	if _, err := io.ReadFull(c.conn, handshakeData); err != nil {
		return fmt.Errorf("failed to read handshake: %w", err)
	}

	// Verify magic preamble
	if !bytes.Equal(handshakeData[:4], MagicPreamble) {
		return fmt.Errorf("invalid magic preamble: %x", handshakeData[:4])
	}

	// Check client's proposed versions (4 x 4 bytes)
	// We just accept any version >= 3.0 and respond with 4.4
	// The message format is similar enough that it works
	selectedVersion := NoVersion
	for i := 0; i < 4; i++ {
		proposedVersion := handshakeData[4+i*4 : 4+(i+1)*4]
		major := proposedVersion[3]
		if major >= 3 {
			// Accept anything Bolt 3.0+, respond with 4.4
			selectedVersion = Bolt44Version
			break
		}
	}

	// Send selected version
	if _, err := c.conn.Write(selectedVersion); err != nil {
		return fmt.Errorf("failed to send version: %w", err)
	}

	if bytes.Equal(selectedVersion, NoVersion) {
		return fmt.Errorf("no supported protocol version")
	}

	copy(c.version, selectedVersion)
	return nil
}

func (c *Connection) handleMessage() error {
	// Read and decode message
	msg, err := c.decoder.Decode()
	if err != nil {
		return err
	}

	raw, ok := msg.(*packstream.RawStruct)
	if !ok {
		return fmt.Errorf("expected struct message, got %T", msg)
	}

	switch raw.Sig {
	case messages.InitSignature: // Also HelloSignature (same value)
		return c.handleInit(raw)
	case messages.GoodbyeSignature:
		return io.EOF // Clean disconnect
	case messages.RunSignature:
		return c.handleRun(raw)
	case messages.PullAllSignature: // Also PullSignature
		return c.handlePullAll()
	case messages.DiscardAllSignature: // Also DiscardSignature
		return c.handleDiscardAll()
	case messages.ResetSignature:
		return c.handleReset()
	case messages.AckFailureSignature:
		return c.handleAckFailure()
	case messages.BeginSignature:
		return c.handleBegin()
	case messages.CommitSignature:
		return c.handleCommit()
	case messages.RollbackSignature:
		return c.handleRollback()
	default:
		return fmt.Errorf("unknown message signature: 0x%02X", raw.Sig)
	}
}

func (c *Connection) handleInit(raw *packstream.RawStruct) error {
	init, err := messages.ParseInit(raw)
	if err != nil {
		return c.sendFailure("Neo.ClientError.Request.Invalid", err.Error())
	}

	fmt.Printf("Client init: %s\n", init.ClientName)

	// TODO: Handle authentication from init.AuthToken
	// For now, accept all connections

	return c.sendSuccess(map[string]interface{}{
		"server": "GravecDB/1.0",
	})
}

// pendingResult stores query results between RUN and PULL_ALL
type pendingResult struct {
	columns []string
	rows    []map[string]interface{}
	index   int
}

var pendingResults = make(map[*Connection]*pendingResult)
var pendingMu sync.Mutex

func (c *Connection) handleRun(raw *packstream.RawStruct) error {
	if c.failed {
		return c.sendIgnored()
	}

	// Recover from panics during query execution to avoid killing the connection
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Panic during query execution: %v\n", r)
			c.failed = true
			c.sendFailure("Neo.DatabaseError.General.UnknownError", fmt.Sprintf("internal error: %v", r))
		}
	}()

	run, err := messages.ParseRun(raw)
	if err != nil {
		c.failed = true
		return c.sendFailure("Neo.ClientError.Request.Invalid", err.Error())
	}

	fmt.Printf("Query: %s\n", run.Statement)

	// Parse and execute the query
	query, err := graph.ParseQuery(run.Statement)
	if err != nil {
		c.failed = true
		return c.sendFailure("Neo.ClientError.Statement.SyntaxError", err.Error())
	}

	// Attach parameters from the Bolt protocol to the query
	if len(run.Parameters) > 0 {
		query.Parameters = run.Parameters
		query.ResolveParams() // substitute $param refs in property maps
	}

	var result *graph.QueryResult

	// If we're in a transaction, execute within it
	if c.inTransaction && c.tx != nil {
		result, err = c.tx.ExecuteQuery(query, nil)
	} else {
		result, err = c.db.ExecuteQueryWithEmbedder(query, nil)
	}

	if err != nil {
		c.failed = true
		return c.sendFailure("Neo.ClientError.Statement.ExecutionFailed", err.Error())
	}

	// Store result for PULL_ALL
	pendingMu.Lock()
	pendingResults[c] = &pendingResult{
		columns: result.Columns,
		rows:    result.Rows,
		index:   0,
	}
	pendingMu.Unlock()

	// Convert columns to interface slice
	fields := make([]interface{}, len(result.Columns))
	for i, col := range result.Columns {
		fields[i] = col
	}

	return c.sendSuccess(map[string]interface{}{
		"fields": fields,
	})
}

func (c *Connection) handlePullAll() error {
	if c.failed {
		return c.sendIgnored()
	}

	pendingMu.Lock()
	result, ok := pendingResults[c]
	if ok {
		delete(pendingResults, c)
	}
	pendingMu.Unlock()

	if !ok || result == nil {
		return c.sendSuccess(map[string]interface{}{})
	}

	// Send each row as a RECORD message
	for _, row := range result.rows {
		values := make([]interface{}, len(result.columns))
		for i, col := range result.columns {
			val := row[col]
			// Convert graph types to Bolt types
			values[i] = convertToBoltValue(val)
		}

		record := &messages.Record{Values: values}
		if err := c.encoder.Encode(record); err != nil {
			return err
		}
	}

	// Send SUCCESS with summary
	return c.sendSuccess(map[string]interface{}{
		"type": "r", // read
	})
}

func (c *Connection) handleDiscardAll() error {
	if c.failed {
		return c.sendIgnored()
	}

	pendingMu.Lock()
	delete(pendingResults, c)
	pendingMu.Unlock()

	return c.sendSuccess(map[string]interface{}{})
}

func (c *Connection) handleReset() error {
	c.failed = false

	pendingMu.Lock()
	delete(pendingResults, c)
	pendingMu.Unlock()

	// Rollback any active transaction
	if c.inTransaction && c.tx != nil {
		c.tx.Rollback()
		c.tx = nil
		c.inTransaction = false
		fmt.Printf("Transaction rolled back due to RESET\n")
	}

	return c.sendSuccess(map[string]interface{}{})
}

func (c *Connection) handleAckFailure() error {
	c.failed = false
	return c.sendSuccess(map[string]interface{}{})
}

// Transaction handling - real ACID transactions if the database supports it
func (c *Connection) handleBegin() error {
	if c.inTransaction {
		return c.sendFailure("Neo.ClientError.Transaction.TransactionStartFailed", "Already in a transaction")
	}

	// Start transaction
	tx, err := c.db.BeginTransaction()
	if err != nil {
		return c.sendFailure("Neo.ClientError.Transaction.TransactionStartFailed", err.Error())
	}
	c.tx = tx
	c.inTransaction = true
	fmt.Printf("Transaction started\n")

	return c.sendSuccess(map[string]interface{}{})
}

func (c *Connection) handleCommit() error {
	if !c.inTransaction {
		return c.sendFailure("Neo.ClientError.Transaction.TransactionNotFound", "No transaction to commit")
	}

	if c.tx != nil {
		if err := c.tx.Commit(); err != nil {
			c.inTransaction = false
			c.tx = nil
			return c.sendFailure("Neo.ClientError.Transaction.TransactionCommitFailed", err.Error())
		}
		fmt.Printf("Transaction committed\n")
	}

	c.inTransaction = false
	c.tx = nil
	return c.sendSuccess(map[string]interface{}{})
}

func (c *Connection) handleRollback() error {
	if !c.inTransaction {
		return c.sendFailure("Neo.ClientError.Transaction.TransactionNotFound", "No transaction to rollback")
	}

	if c.tx != nil {
		if err := c.tx.Rollback(); err != nil {
			c.inTransaction = false
			c.tx = nil
			return c.sendFailure("Neo.ClientError.Transaction.TransactionRollbackFailed", err.Error())
		}
		fmt.Printf("Transaction rolled back\n")
	}

	c.inTransaction = false
	c.tx = nil
	return c.sendSuccess(map[string]interface{}{})
}

func (c *Connection) sendSuccess(metadata map[string]interface{}) error {
	msg := &messages.Success{Metadata: metadata}
	return c.encoder.Encode(msg)
}

func (c *Connection) sendFailure(code, message string) error {
	msg := messages.NewFailure(code, message)
	return c.encoder.Encode(msg)
}

func (c *Connection) sendIgnored() error {
	msg := &messages.Ignored{}
	return c.encoder.Encode(msg)
}

// convertToBoltValue converts internal graph types to Bolt wire types
func convertToBoltValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case *graph.Node:
		return convertNode(v)
	case *graph.Relationship:
		return convertRelationship(v)
	case *graph.Path:
		return convertPath(v)
	case map[string]interface{}:
		// Check if it's a node-like map
		if _, hasID := v["ID"]; hasID {
			if _, hasLabels := v["Labels"]; hasLabels {
				return convertNodeMap(v)
			}
			if _, hasType := v["Type"]; hasType {
				return convertRelationshipMap(v)
			}
		}
		return v
	default:
		return val
	}
}

func convertNode(n *graph.Node) *messages.Node {
	// Generate a numeric ID from the string ID (hash it)
	id := hashStringID(n.ID)
	return &messages.Node{
		ID:         id,
		Labels:     n.Labels,
		Properties: n.Properties,
	}
}

func convertRelationship(r *graph.Relationship) *messages.Relationship {
	return &messages.Relationship{
		ID:         hashStringID(r.ID),
		StartID:    hashStringID(r.FromNodeID),
		EndID:      hashStringID(r.ToNodeID),
		Type:       r.Type,
		Properties: r.Properties,
	}
}

func convertPath(p *graph.Path) *messages.Path {
	boltPath := &messages.Path{}
	for _, n := range p.Nodes {
		boltPath.Nodes = append(boltPath.Nodes, convertNode(n))
	}
	for _, r := range p.Relationships {
		boltPath.Relationships = append(boltPath.Relationships, &messages.UnboundRelationship{
			ID:         hashStringID(r.ID),
			Type:       r.Type,
			Properties: r.Properties,
		})
	}
	// Build sequence: pairs of (relIndex, nodeIndex) for each step
	// relIndex is 1-based, nodeIndex is 1-based (index into Nodes after first)
	for i := range p.Relationships {
		boltPath.Sequence = append(boltPath.Sequence, int64(i+1), int64(i+1))
	}
	return boltPath
}

func convertNodeMap(m map[string]interface{}) *messages.Node {
	node := &messages.Node{
		Properties: make(map[string]interface{}),
	}

	if id, ok := m["ID"].(string); ok {
		node.ID = hashStringID(id)
	}
	if labels, ok := m["Labels"].([]string); ok {
		node.Labels = labels
	} else if labels, ok := m["Labels"].([]interface{}); ok {
		for _, l := range labels {
			if s, ok := l.(string); ok {
				node.Labels = append(node.Labels, s)
			}
		}
	}
	if props, ok := m["Properties"].(map[string]interface{}); ok {
		node.Properties = props
	}

	return node
}

func convertRelationshipMap(m map[string]interface{}) *messages.Relationship {
	rel := &messages.Relationship{
		Properties: make(map[string]interface{}),
	}

	if id, ok := m["ID"].(string); ok {
		rel.ID = hashStringID(id)
	}
	if t, ok := m["Type"].(string); ok {
		rel.Type = t
	}
	if from, ok := m["FromNodeID"].(string); ok {
		rel.StartID = hashStringID(from)
	}
	if to, ok := m["ToNodeID"].(string); ok {
		rel.EndID = hashStringID(to)
	}
	if props, ok := m["Properties"].(map[string]interface{}); ok {
		rel.Properties = props
	}

	return rel
}

// hashStringID converts a string ID to an int64
// Uses a simple hash to maintain consistency
func hashStringID(s string) int64 {
	var h int64 = 0
	for _, c := range s {
		h = 31*h + int64(c)
	}
	if h < 0 {
		h = -h
	}
	return h
}

// Helper to read exactly n bytes
func readFull(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

// Helper to write uint16 in big endian
func writeUint16(w io.Writer, v uint16) error {
	return binary.Write(w, binary.BigEndian, v)
}
