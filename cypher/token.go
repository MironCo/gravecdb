package cypher

// TokenType represents the type of a token
type TokenType int

const (
	// Special tokens
	TOKEN_ILLEGAL TokenType = iota
	TOKEN_EOF
	TOKEN_WHITESPACE

	// Literals
	TOKEN_IDENT      // variable names, labels, etc.
	TOKEN_INT        // 123
	TOKEN_FLOAT      // 123.456
	TOKEN_STRING     // "hello" or 'hello'
	TOKEN_TRUE       // true
	TOKEN_FALSE      // false
	TOKEN_NULL       // null

	// Operators
	TOKEN_ASSIGN      // =
	TOKEN_EQ          // =
	TOKEN_NEQ         // <> or !=
	TOKEN_LT          // <
	TOKEN_GT          // >
	TOKEN_LTE         // <=
	TOKEN_GTE         // >=
	TOKEN_PLUS        // +
	TOKEN_MINUS       // -
	TOKEN_STAR        // *
	TOKEN_SLASH       // /
	TOKEN_PERCENT     // %
	TOKEN_CARET       // ^
	TOKEN_DOT         // .
	TOKEN_DOTDOT      // ..
	TOKEN_COMMA       // ,
	TOKEN_COLON       // :
	TOKEN_SEMICOLON   // ;
	TOKEN_PIPE        // |

	// Delimiters
	TOKEN_LPAREN   // (
	TOKEN_RPAREN   // )
	TOKEN_LBRACKET // [
	TOKEN_RBRACKET // ]
	TOKEN_LBRACE   // {
	TOKEN_RBRACE   // }

	// Arrow patterns
	TOKEN_ARROW_LEFT  // <-
	TOKEN_ARROW_RIGHT // ->
	TOKEN_DASH        // -

	// Keywords (Cypher)
	TOKEN_MATCH
	TOKEN_OPTIONAL
	TOKEN_WHERE
	TOKEN_RETURN
	TOKEN_CREATE
	TOKEN_DELETE
	TOKEN_DETACH
	TOKEN_SET
	TOKEN_REMOVE
	TOKEN_MERGE
	TOKEN_ON
	TOKEN_WITH
	TOKEN_UNWIND
	TOKEN_ORDER
	TOKEN_BY
	TOKEN_ASC
	TOKEN_DESC
	TOKEN_SKIP
	TOKEN_LIMIT
	TOKEN_UNION
	TOKEN_ALL
	TOKEN_AS
	TOKEN_AND
	TOKEN_OR
	TOKEN_XOR
	TOKEN_NOT
	TOKEN_IN
	TOKEN_STARTS
	TOKEN_ENDS
	TOKEN_CONTAINS
	TOKEN_IS
	TOKEN_DISTINCT
	TOKEN_CASE
	TOKEN_WHEN
	TOKEN_THEN
	TOKEN_ELSE
	TOKEN_END
	TOKEN_COUNT
	TOKEN_SUM
	TOKEN_AVG
	TOKEN_MIN
	TOKEN_MAX
	TOKEN_COLLECT
	TOKEN_CALL
	TOKEN_YIELD
	TOKEN_FOREACH
	TOKEN_LOAD
	TOKEN_CSV
	TOKEN_FROM
	TOKEN_HEADERS
	TOKEN_FIELDTERMINATOR
	TOKEN_USING
	TOKEN_INDEX
	TOKEN_SCAN
	TOKEN_JOIN
	TOKEN_CONSTRAINT
	TOKEN_ASSERT
	TOKEN_UNIQUE
	TOKEN_EXISTS
	TOKEN_NODE
	TOKEN_RELATIONSHIP
	TOKEN_REL
	TOKEN_DROP
	TOKEN_EXPLAIN
	TOKEN_PROFILE

	// Path functions
	TOKEN_SHORTESTPATH
	TOKEN_ALLSHORTESTPATHS

	// Custom extensions
	TOKEN_AT
	TOKEN_TIME
	TOKEN_EARLIEST
	TOKEN_EMBED
	TOKEN_SIMILAR
	TOKEN_TO
	TOKEN_THRESHOLD
	TOKEN_THROUGH
	TOKEN_DRIFT
	TOKEN_VERSIONS
)

// Token represents a lexical token
type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Column  int
}

var keywords = map[string]TokenType{
	// Cypher keywords
	"MATCH":            TOKEN_MATCH,
	"OPTIONAL":         TOKEN_OPTIONAL,
	"WHERE":            TOKEN_WHERE,
	"RETURN":           TOKEN_RETURN,
	"CREATE":           TOKEN_CREATE,
	"DELETE":           TOKEN_DELETE,
	"DETACH":           TOKEN_DETACH,
	"SET":              TOKEN_SET,
	"REMOVE":           TOKEN_REMOVE,
	"MERGE":            TOKEN_MERGE,
	"ON":               TOKEN_ON,
	"WITH":             TOKEN_WITH,
	"UNWIND":           TOKEN_UNWIND,
	"ORDER":            TOKEN_ORDER,
	"BY":               TOKEN_BY,
	"ASC":              TOKEN_ASC,
	"ASCENDING":        TOKEN_ASC,
	"DESC":             TOKEN_DESC,
	"DESCENDING":       TOKEN_DESC,
	"SKIP":             TOKEN_SKIP,
	"LIMIT":            TOKEN_LIMIT,
	"UNION":            TOKEN_UNION,
	"ALL":              TOKEN_ALL,
	"AS":               TOKEN_AS,
	"AND":              TOKEN_AND,
	"OR":               TOKEN_OR,
	"XOR":              TOKEN_XOR,
	"NOT":              TOKEN_NOT,
	"IN":               TOKEN_IN,
	"STARTS":           TOKEN_STARTS,
	"ENDS":             TOKEN_ENDS,
	"CONTAINS":         TOKEN_CONTAINS,
	"IS":               TOKEN_IS,
	"DISTINCT":         TOKEN_DISTINCT,
	"CASE":             TOKEN_CASE,
	"WHEN":             TOKEN_WHEN,
	"THEN":             TOKEN_THEN,
	"ELSE":             TOKEN_ELSE,
	"END":              TOKEN_END,
	"COUNT":            TOKEN_COUNT,
	"SUM":              TOKEN_SUM,
	"AVG":              TOKEN_AVG,
	"MIN":              TOKEN_MIN,
	"MAX":              TOKEN_MAX,
	"COLLECT":          TOKEN_COLLECT,
	"CALL":             TOKEN_CALL,
	"YIELD":            TOKEN_YIELD,
	"FOREACH":          TOKEN_FOREACH,
	"LOAD":             TOKEN_LOAD,
	"CSV":              TOKEN_CSV,
	"FROM":             TOKEN_FROM,
	"HEADERS":          TOKEN_HEADERS,
	"FIELDTERMINATOR":  TOKEN_FIELDTERMINATOR,
	"USING":            TOKEN_USING,
	"INDEX":            TOKEN_INDEX,
	"SCAN":             TOKEN_SCAN,
	"JOIN":             TOKEN_JOIN,
	"CONSTRAINT":       TOKEN_CONSTRAINT,
	"ASSERT":           TOKEN_ASSERT,
	"UNIQUE":           TOKEN_UNIQUE,
	"EXISTS":           TOKEN_EXISTS,
	"NODE":             TOKEN_NODE,
	"RELATIONSHIP":     TOKEN_RELATIONSHIP,
	"REL":              TOKEN_REL,
	"DROP":             TOKEN_DROP,
	"EXPLAIN":          TOKEN_EXPLAIN,
	"PROFILE":          TOKEN_PROFILE,
	"TRUE":             TOKEN_TRUE,
	"FALSE":            TOKEN_FALSE,
	"NULL":             TOKEN_NULL,
	"SHORTESTPATH":     TOKEN_SHORTESTPATH,
	"ALLSHORTESTPATHS": TOKEN_ALLSHORTESTPATHS,

	// Custom extensions
	"AT":        TOKEN_AT,
	"TIME":      TOKEN_TIME,
	"EARLIEST":  TOKEN_EARLIEST,
	"EMBED":     TOKEN_EMBED,
	"SIMILAR":   TOKEN_SIMILAR,
	"TO":        TOKEN_TO,
	"THRESHOLD": TOKEN_THRESHOLD,
	"THROUGH":   TOKEN_THROUGH,
	"DRIFT":     TOKEN_DRIFT,
	"VERSIONS":  TOKEN_VERSIONS,
}

// LookupIdent checks if an identifier is a keyword
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return TOKEN_IDENT
}

// String returns a string representation of the token type
func (t TokenType) String() string {
	switch t {
	case TOKEN_ILLEGAL:
		return "ILLEGAL"
	case TOKEN_EOF:
		return "EOF"
	case TOKEN_IDENT:
		return "IDENT"
	case TOKEN_INT:
		return "INT"
	case TOKEN_FLOAT:
		return "FLOAT"
	case TOKEN_STRING:
		return "STRING"
	case TOKEN_MATCH:
		return "MATCH"
	case TOKEN_WHERE:
		return "WHERE"
	case TOKEN_RETURN:
		return "RETURN"
	case TOKEN_CREATE:
		return "CREATE"
	case TOKEN_DELETE:
		return "DELETE"
	case TOKEN_SET:
		return "SET"
	default:
		return "UNKNOWN"
	}
}
