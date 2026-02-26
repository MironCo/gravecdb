package cypher

// Node represents a node in the AST
type Node interface {
	node()
}

// Statement represents a complete Cypher statement
type Statement interface {
	Node
	statement()
}

// Expression represents an expression (value, comparison, function call, etc.)
type Expression interface {
	Node
	expression()
}

// Clause represents a query clause (MATCH, WHERE, RETURN, etc.)
type Clause interface {
	Node
	clause()
}

// ============================================================================
// Statements
// ============================================================================

// Query represents a complete Cypher query with multiple clauses
type Query struct {
	Clauses    []Clause
	IsUnion    bool     // true if this is a UNION query
	UnionAll   bool     // true for UNION ALL (keep duplicates)
	SubQueries []*Query // sub-queries joined by UNION
}

func (q *Query) node()      {}
func (q *Query) statement() {}

// ============================================================================
// Clauses
// ============================================================================

// MatchClause represents a MATCH clause
type MatchClause struct {
	Optional bool
	Pattern  *Pattern
}

func (m *MatchClause) node()   {}
func (m *MatchClause) clause() {}

// CreateClause represents a CREATE clause
type CreateClause struct {
	Pattern *Pattern
}

func (c *CreateClause) node()   {}
func (c *CreateClause) clause() {}

// MergeClause represents a MERGE clause
type MergeClause struct {
	Pattern  *Pattern
	OnCreate []Expression // SET expressions for ON CREATE
	OnMatch  []Expression // SET expressions for ON MATCH
}

func (m *MergeClause) node()   {}
func (m *MergeClause) clause() {}

// WhereClause represents a WHERE clause
type WhereClause struct {
	Condition Expression
}

func (w *WhereClause) node()   {}
func (w *WhereClause) clause() {}

// ReturnClause represents a RETURN clause
type ReturnClause struct {
	Distinct bool
	Items    []*ReturnItem
	OrderBy  []*OrderItem
	Skip     Expression
	Limit    Expression
}

func (r *ReturnClause) node()   {}
func (r *ReturnClause) clause() {}

// ReturnItem represents a single item in RETURN
type ReturnItem struct {
	Expression Expression
	Alias      string // AS alias (empty if not specified)
}

func (r *ReturnItem) node()       {}
func (r *ReturnItem) expression() {}

// OrderItem represents ORDER BY item
type OrderItem struct {
	Expression Expression
	Descending bool
}

func (o *OrderItem) node()       {}
func (o *OrderItem) expression() {}

// WithClause represents a WITH clause
type WithClause struct {
	Distinct bool
	Items    []*ReturnItem
	Where    Expression
	OrderBy  []*OrderItem
	Skip     Expression
	Limit    Expression
}

func (w *WithClause) node()   {}
func (w *WithClause) clause() {}

// SetClause represents a SET clause
type SetClause struct {
	Items []*SetItem
}

func (s *SetClause) node()   {}
func (s *SetClause) clause() {}

// SetItem represents a single SET operation
type SetItem struct {
	Property   *PropertyAccess // For n.prop = value
	Variable   string          // For n = {props} or n += {props}
	Expression Expression
	Append     bool // += vs =
}

func (s *SetItem) node()       {}
func (s *SetItem) expression() {}

// DeleteClause represents a DELETE clause
type DeleteClause struct {
	Detach      bool // DETACH DELETE
	Expressions []Expression
}

func (d *DeleteClause) node()   {}
func (d *DeleteClause) clause() {}

// RemoveClause represents a REMOVE clause
type RemoveClause struct {
	Items []*RemoveItem
}

func (r *RemoveClause) node()   {}
func (r *RemoveClause) clause() {}

// RemoveItem represents a single REMOVE operation: n.property or n:Label
type RemoveItem struct {
	Variable string
	Property string // set for REMOVE n.property
	Label    string // set for REMOVE n:Label
}

func (r *RemoveItem) node()       {}
func (r *RemoveItem) expression() {}

// UnwindClause represents an UNWIND clause
type UnwindClause struct {
	Expression Expression
	Variable   string // AS variable
}

func (u *UnwindClause) node()   {}
func (u *UnwindClause) clause() {}

// ============================================================================
// Custom Extension Clauses
// ============================================================================

// TimeClause represents AT TIME clause (custom extension)
type TimeClause struct {
	Mode      string     // "EARLIEST" or "TIMESTAMP"
	Timestamp Expression // Unix timestamp (when Mode == "TIMESTAMP")
}

func (t *TimeClause) node()   {}
func (t *TimeClause) clause() {}

// EmbedClause represents EMBED clause (custom extension)
type EmbedClause struct {
	Variable string     // Node variable to embed
	Mode     string     // "AUTO", "TEXT", "PROPERTY"
	Text     string     // Literal text (when Mode == "TEXT")
	Property string     // Property name (when Mode == "PROPERTY")
	Source   Expression // The expression to embed
}

func (e *EmbedClause) node()   {}
func (e *EmbedClause) clause() {}

// SimilarToClause represents SIMILAR TO clause (custom extension)
type SimilarToClause struct {
	Query       Expression // Search query text
	Limit       Expression // Max results
	Threshold   Expression // Min similarity threshold
	ThroughTime bool       // If true, search all historical versions
	DriftMode   bool       // If true (with ThroughTime), calculate semantic drift metrics
}

func (s *SimilarToClause) node()   {}
func (s *SimilarToClause) clause() {}

// ============================================================================
// Patterns
// ============================================================================

// Pattern represents a graph pattern (nodes and relationships)
type Pattern struct {
	Parts []*PatternPart // Multiple pattern parts separated by commas
}

func (p *Pattern) node()       {}
func (p *Pattern) expression() {}

// PatternPart represents a single connected pattern part
type PatternPart struct {
	Variable string          // Optional variable for entire path
	Elements []PatternElement // Alternating nodes and relationships
}

func (p *PatternPart) node()       {}
func (p *PatternPart) expression() {}

// PatternElement is either a node or relationship in a pattern
type PatternElement interface {
	Node
	patternElement()
}

// NodePattern represents a node in a pattern: (n:Label {props})
type NodePattern struct {
	Variable   string
	Labels     []string
	Properties Expression // MapLiteral or Parameter
}

func (n *NodePattern) node()           {}
func (n *NodePattern) patternElement() {}
func (n *NodePattern) expression()     {}

// RelationshipPattern represents a relationship: -[r:TYPE {props}]->
type RelationshipPattern struct {
	Variable    string
	Types       []string // Relationship types (can use |)
	Properties  Expression
	Direction   RelationshipDirection
	MinHops     *int // For variable-length: *min..max
	MaxHops     *int
	VarLength   bool // True if variable-length pattern
}

func (r *RelationshipPattern) node()           {}
func (r *RelationshipPattern) patternElement() {}

// RelationshipDirection represents arrow direction
type RelationshipDirection int

const (
	DirectionNone  RelationshipDirection = iota // -[]-
	DirectionRight                              // -[]->
	DirectionLeft                               // <-[]-
	DirectionBoth                               // <-[]->
)

// ShortestPathPattern represents shortestPath() or allShortestPaths()
type ShortestPathPattern struct {
	Function string       // "shortestPath" or "allShortestPaths"
	Variable string       // Optional variable name
	Pattern  *PatternPart // The inner pattern
}

func (s *ShortestPathPattern) node()           {}
func (s *ShortestPathPattern) patternElement() {}
func (s *ShortestPathPattern) expression()     {}

// ============================================================================
// Expressions
// ============================================================================

// Identifier represents a variable reference
type Identifier struct {
	Name string
}

func (i *Identifier) node()       {}
func (i *Identifier) expression() {}

// IntegerLiteral represents an integer value
type IntegerLiteral struct {
	Value int64
}

func (i *IntegerLiteral) node()       {}
func (i *IntegerLiteral) expression() {}

// FloatLiteral represents a floating-point value
type FloatLiteral struct {
	Value float64
}

func (f *FloatLiteral) node()       {}
func (f *FloatLiteral) expression() {}

// StringLiteral represents a string value
type StringLiteral struct {
	Value string
}

func (s *StringLiteral) node()       {}
func (s *StringLiteral) expression() {}

// BooleanLiteral represents a boolean value
type BooleanLiteral struct {
	Value bool
}

func (b *BooleanLiteral) node()       {}
func (b *BooleanLiteral) expression() {}

// NullLiteral represents NULL
type NullLiteral struct{}

func (n *NullLiteral) node()       {}
func (n *NullLiteral) expression() {}

// ListLiteral represents a list: [1, 2, 3]
type ListLiteral struct {
	Elements []Expression
}

func (l *ListLiteral) node()       {}
func (l *ListLiteral) expression() {}

// MapLiteral represents a map: {key: value, ...}
type MapLiteral struct {
	Pairs []*MapPair
}

func (m *MapLiteral) node()       {}
func (m *MapLiteral) expression() {}

// MapPair represents a key-value pair in a map
type MapPair struct {
	Key   string
	Value Expression
}

func (m *MapPair) node()       {}
func (m *MapPair) expression() {}

// PropertyAccess represents property access: n.name
type PropertyAccess struct {
	Object   Expression
	Property string
}

func (p *PropertyAccess) node()       {}
func (p *PropertyAccess) expression() {}

// IndexAccess represents index access: list[0] or map["key"]
type IndexAccess struct {
	Object Expression
	Index  Expression
}

func (i *IndexAccess) node()       {}
func (i *IndexAccess) expression() {}

// SliceAccess represents slice access: list[0..5]
type SliceAccess struct {
	Object Expression
	Start  Expression
	End    Expression
}

func (s *SliceAccess) node()       {}
func (s *SliceAccess) expression() {}

// BinaryExpression represents binary operations: a + b, a AND b, etc.
type BinaryExpression struct {
	Left     Expression
	Operator string
	Right    Expression
}

func (b *BinaryExpression) node()       {}
func (b *BinaryExpression) expression() {}

// UnaryExpression represents unary operations: NOT a, -x
type UnaryExpression struct {
	Operator string
	Operand  Expression
}

func (u *UnaryExpression) node()       {}
func (u *UnaryExpression) expression() {}

// ComparisonExpression represents comparisons: a = b, a > b, etc.
type ComparisonExpression struct {
	Left     Expression
	Operator string // =, <>, <, >, <=, >=, IN, STARTS WITH, ENDS WITH, CONTAINS
	Right    Expression
}

func (c *ComparisonExpression) node()       {}
func (c *ComparisonExpression) expression() {}

// SimilarToExpression represents `variable SIMILAR TO "query"` inside a WHERE clause
type SimilarToExpression struct {
	Variable  string
	Query     string
	Threshold float32
}

func (s *SimilarToExpression) node()       {}
func (s *SimilarToExpression) expression() {}

// FunctionCall represents a function call: count(n), toUpper(s)
type FunctionCall struct {
	Name      string
	Distinct  bool // For aggregation functions: COUNT(DISTINCT n)
	Arguments []Expression
}

func (f *FunctionCall) node()       {}
func (f *FunctionCall) expression() {}

// CaseExpression represents CASE WHEN ... THEN ... ELSE ... END
type CaseExpression struct {
	Test        Expression   // For simple CASE (can be nil)
	Whens       []*CaseWhen
	ElseResult  Expression   // Can be nil
}

func (c *CaseExpression) node()       {}
func (c *CaseExpression) expression() {}

// CaseWhen represents a WHEN ... THEN ... branch
type CaseWhen struct {
	When Expression
	Then Expression
}

func (c *CaseWhen) node()       {}
func (c *CaseWhen) expression() {}

// ExistsExpression represents EXISTS { pattern }
type ExistsExpression struct {
	Pattern *Pattern
}

func (e *ExistsExpression) node()       {}
func (e *ExistsExpression) expression() {}

// ListPredicateExpression represents ANY/ALL/NONE/SINGLE(variable IN list WHERE condition)
type ListPredicateExpression struct {
	Function  string     // "ANY", "ALL", "NONE", "SINGLE"
	Variable  string     // iteration variable
	List      Expression // the list to iterate
	Condition Expression // WHERE condition
}

func (l *ListPredicateExpression) node()       {}
func (l *ListPredicateExpression) expression() {}

// InExpression represents x IN [1, 2, 3]
type InExpression struct {
	Expression Expression
	List       Expression
	Not        bool // NOT IN
}

func (i *InExpression) node()       {}
func (i *InExpression) expression() {}

// IsNullExpression represents x IS NULL or x IS NOT NULL
type IsNullExpression struct {
	Expression Expression
	Not        bool // IS NOT NULL
}

func (i *IsNullExpression) node()       {}
func (i *IsNullExpression) expression() {}

// Parameter represents a parameter: $param
type Parameter struct {
	Name string
}

func (p *Parameter) node()       {}
func (p *Parameter) expression() {}

// Star represents * in RETURN *
type Star struct{}

func (s *Star) node()       {}
func (s *Star) expression() {}
