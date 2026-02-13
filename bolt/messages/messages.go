// Package messages defines Bolt protocol message types
package messages

import "github.com/MironCo/gravecdb/bolt/packstream"

// Message signatures (Bolt v1-v4)
const (
	// Client messages
	InitSignature       = 0x01
	HelloSignature      = 0x01 // Bolt 3+ renamed INIT to HELLO
	GoodbyeSignature    = 0x02 // Bolt 3+ clean disconnect
	RunSignature        = 0x10
	PullAllSignature    = 0x3F
	PullSignature       = 0x3F // Bolt 4+ (same as PULL_ALL for compatibility)
	DiscardAllSignature = 0x2F
	DiscardSignature    = 0x2F // Bolt 4+ (same as DISCARD_ALL for compatibility)
	ResetSignature      = 0x0F
	AckFailureSignature = 0x0E
	BeginSignature      = 0x11 // Bolt 3+ transactions
	CommitSignature     = 0x12
	RollbackSignature   = 0x13

	// Server messages
	SuccessSignature = 0x70
	FailureSignature = 0x7F
	IgnoredSignature = 0x7E
	RecordSignature  = 0x71

	// Graph structure signatures
	NodeSignature                = 0x4E
	RelationshipSignature        = 0x52
	UnboundRelationshipSignature = 0x72
	PathSignature                = 0x50
)

// Init message (client -> server)
type Init struct {
	ClientName string
	AuthToken  map[string]interface{}
}

// ParseInit parses an Init message from raw struct
func ParseInit(raw *packstream.RawStruct) (*Init, error) {
	if len(raw.Fields) < 2 {
		return &Init{}, nil
	}

	init := &Init{}
	if name, ok := raw.Fields[0].(string); ok {
		init.ClientName = name
	}
	if auth, ok := raw.Fields[1].(map[string]interface{}); ok {
		init.AuthToken = auth
	}
	return init, nil
}

// Run message (client -> server)
type Run struct {
	Statement  string
	Parameters map[string]interface{}
}

// ParseRun parses a Run message from raw struct
func ParseRun(raw *packstream.RawStruct) (*Run, error) {
	run := &Run{
		Parameters: make(map[string]interface{}),
	}

	if len(raw.Fields) >= 1 {
		if stmt, ok := raw.Fields[0].(string); ok {
			run.Statement = stmt
		}
	}
	if len(raw.Fields) >= 2 {
		if params, ok := raw.Fields[1].(map[string]interface{}); ok {
			run.Parameters = params
		}
	}
	return run, nil
}

// Success message (server -> client)
type Success struct {
	Metadata map[string]interface{}
}

func (s *Success) Signature() byte { return SuccessSignature }
func (s *Success) Fields() []interface{} {
	if s.Metadata == nil {
		return []interface{}{map[string]interface{}{}}
	}
	return []interface{}{s.Metadata}
}

// Failure message (server -> client)
type Failure struct {
	Metadata map[string]interface{}
}

func (f *Failure) Signature() byte { return FailureSignature }
func (f *Failure) Fields() []interface{} {
	return []interface{}{f.Metadata}
}

// NewFailure creates a failure message with code and message
func NewFailure(code, message string) *Failure {
	return &Failure{
		Metadata: map[string]interface{}{
			"code":    code,
			"message": message,
		},
	}
}

// Ignored message (server -> client)
type Ignored struct{}

func (i *Ignored) Signature() byte        { return IgnoredSignature }
func (i *Ignored) Fields() []interface{} { return []interface{}{} }

// Record message (server -> client)
type Record struct {
	Values []interface{}
}

func (r *Record) Signature() byte        { return RecordSignature }
func (r *Record) Fields() []interface{} { return []interface{}{r.Values} }

// Node represents a graph node in Bolt format
type Node struct {
	ID         int64
	Labels     []string
	Properties map[string]interface{}
}

func (n *Node) Signature() byte { return NodeSignature }
func (n *Node) Fields() []interface{} {
	labels := make([]interface{}, len(n.Labels))
	for i, l := range n.Labels {
		labels[i] = l
	}
	return []interface{}{n.ID, labels, n.Properties}
}

// Relationship represents a graph relationship in Bolt format
type Relationship struct {
	ID         int64
	StartID    int64
	EndID      int64
	Type       string
	Properties map[string]interface{}
}

func (r *Relationship) Signature() byte { return RelationshipSignature }
func (r *Relationship) Fields() []interface{} {
	return []interface{}{r.ID, r.StartID, r.EndID, r.Type, r.Properties}
}

// UnboundRelationship is a relationship without start/end nodes (used in paths)
type UnboundRelationship struct {
	ID         int64
	Type       string
	Properties map[string]interface{}
}

func (r *UnboundRelationship) Signature() byte { return UnboundRelationshipSignature }
func (r *UnboundRelationship) Fields() []interface{} {
	return []interface{}{r.ID, r.Type, r.Properties}
}

// Path represents a graph path
type Path struct {
	Nodes         []*Node
	Relationships []*UnboundRelationship
	Sequence      []int64
}

func (p *Path) Signature() byte { return PathSignature }
func (p *Path) Fields() []interface{} {
	nodes := make([]interface{}, len(p.Nodes))
	for i, n := range p.Nodes {
		nodes[i] = n
	}
	rels := make([]interface{}, len(p.Relationships))
	for i, r := range p.Relationships {
		rels[i] = r
	}
	seq := make([]interface{}, len(p.Sequence))
	for i, s := range p.Sequence {
		seq[i] = s
	}
	return []interface{}{nodes, rels, seq}
}
