package cypher

import (
	"fmt"
	"strconv"
	"strings"
)

// Parser parses Cypher queries into an AST
type Parser struct {
	lexer     *Lexer
	curToken  Token
	peekToken Token
	errors    []string
}

// NewParser creates a new Parser
func NewParser(input string) *Parser {
	p := &Parser{
		lexer:  NewLexer(input),
		errors: []string{},
	}
	// Read two tokens to initialize curToken and peekToken
	p.nextToken()
	p.nextToken()
	return p
}

// nextToken advances to the next token
func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

// Errors returns parsing errors
func (p *Parser) Errors() []string {
	return p.errors
}

// addError adds a parsing error
func (p *Parser) addError(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	p.errors = append(p.errors, fmt.Sprintf("line %d, column %d: %s", p.curToken.Line, p.curToken.Column, msg))
}

// curTokenIs checks if current token matches type
func (p *Parser) curTokenIs(t TokenType) bool {
	return p.curToken.Type == t
}

// peekTokenIs checks if peek token matches type
func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.Type == t
}

// expectPeek advances if peek matches, otherwise errors
func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.addError("expected %v, got %v", t, p.peekToken.Type)
	return false
}

// Parse parses a complete Cypher query, handling UNION between sub-queries
func (p *Parser) Parse() (*Query, error) {
	query := p.parseQueryClauses()
	if len(p.errors) > 0 {
		return nil, fmt.Errorf("parse errors: %s", strings.Join(p.errors, "; "))
	}

	// Check for UNION
	for p.curTokenIs(TOKEN_UNION) {
		p.nextToken() // consume UNION

		// Check for ALL
		unionAll := false
		if p.curTokenIs(TOKEN_ALL) {
			unionAll = true
			p.nextToken() // consume ALL
		}

		// Parse next sub-query
		right := p.parseQueryClauses()
		if len(p.errors) > 0 {
			return nil, fmt.Errorf("parse errors: %s", strings.Join(p.errors, "; "))
		}

		// Wrap into a union query: first time, wrap the left side too
		union := &Query{
			IsUnion:  true,
			UnionAll: unionAll,
		}
		if query.IsUnion {
			// Already a union — append sub-query
			union = query
			union.SubQueries = append(union.SubQueries, right)
		} else {
			union.SubQueries = []*Query{query, right}
			query = union
		}
	}

	return query, nil
}

// parseQueryClauses parses clauses until EOF or UNION
func (p *Parser) parseQueryClauses() *Query {
	query := &Query{
		Clauses: []Clause{},
	}

	for !p.curTokenIs(TOKEN_EOF) && !p.curTokenIs(TOKEN_UNION) {
		clause := p.parseClause()
		if clause != nil {
			query.Clauses = append(query.Clauses, clause)
		}
		if len(p.errors) > 0 {
			return query
		}
	}

	return query
}

// parseClause parses a single clause
func (p *Parser) parseClause() Clause {
	switch p.curToken.Type {
	case TOKEN_MATCH:
		return p.parseMatchClause()
	case TOKEN_OPTIONAL:
		if p.peekTokenIs(TOKEN_MATCH) {
			p.nextToken()
			mc := p.parseMatchClause()
			if mc != nil {
				mc.Optional = true
			}
			return mc
		}
		p.addError("expected MATCH after OPTIONAL")
		return nil
	case TOKEN_CREATE:
		return p.parseCreateClause()
	case TOKEN_MERGE:
		return p.parseMergeClause()
	case TOKEN_WHERE:
		return p.parseWhereClause()
	case TOKEN_RETURN:
		return p.parseReturnClause()
	case TOKEN_WITH:
		return p.parseWithClause()
	case TOKEN_SET:
		return p.parseSetClause()
	case TOKEN_DELETE:
		return p.parseDeleteClause(false)
	case TOKEN_DETACH:
		if p.peekTokenIs(TOKEN_DELETE) {
			p.nextToken()
			return p.parseDeleteClause(true)
		}
		p.addError("expected DELETE after DETACH")
		return nil
	case TOKEN_REMOVE:
		return p.parseRemoveClause()
	case TOKEN_UNWIND:
		return p.parseUnwindClause()
	case TOKEN_AT:
		return p.parseTimeClause()
	case TOKEN_FOREACH:
		return p.parseForeachClause()
	case TOKEN_EMBED:
		return p.parseEmbedClause()
	case TOKEN_SIMILAR:
		return p.parseSimilarToClause()
	case TOKEN_CALL:
		return p.parseCallClause()
	case TOKEN_LOAD:
		return p.parseLoadCSVClause()
	default:
		p.addError("unexpected token: %s", p.curToken.Literal)
		p.nextToken()
		return nil
	}
}

// parseMatchClause parses MATCH pattern
func (p *Parser) parseMatchClause() *MatchClause {
	clause := &MatchClause{}
	p.nextToken() // consume MATCH

	pattern := p.parsePattern()
	if pattern == nil {
		return nil
	}
	clause.Pattern = pattern

	return clause
}

// parseCreateClause parses CREATE pattern
func (p *Parser) parseCreateClause() *CreateClause {
	clause := &CreateClause{}
	p.nextToken() // consume CREATE

	pattern := p.parsePattern()
	if pattern == nil {
		return nil
	}
	clause.Pattern = pattern

	return clause
}

// parseMergeClause parses MERGE pattern [ON CREATE SET ...] [ON MATCH SET ...]
func (p *Parser) parseMergeClause() *MergeClause {
	clause := &MergeClause{}
	p.nextToken() // consume MERGE

	pattern := p.parsePattern()
	if pattern == nil {
		return nil
	}
	clause.Pattern = pattern

	// Parse optional ON CREATE SET and ON MATCH SET
	for p.curTokenIs(TOKEN_ON) {
		p.nextToken()
		if p.curTokenIs(TOKEN_CREATE) {
			p.nextToken()
			if !p.curTokenIs(TOKEN_SET) {
				p.addError("expected SET after ON CREATE")
				return nil
			}
			p.nextToken()
			// Parse SET items
			for {
				item := p.parseSetItem()
				if item == nil {
					break
				}
				clause.OnCreate = append(clause.OnCreate, item)
				if !p.curTokenIs(TOKEN_COMMA) {
					break
				}
				p.nextToken()
			}
		} else if p.curTokenIs(TOKEN_MATCH) {
			p.nextToken()
			if !p.curTokenIs(TOKEN_SET) {
				p.addError("expected SET after ON MATCH")
				return nil
			}
			p.nextToken()
			// Parse SET items
			for {
				item := p.parseSetItem()
				if item == nil {
					break
				}
				clause.OnMatch = append(clause.OnMatch, item)
				if !p.curTokenIs(TOKEN_COMMA) {
					break
				}
				p.nextToken()
			}
		}
	}

	return clause
}

// parseWhereClause parses WHERE condition
func (p *Parser) parseWhereClause() *WhereClause {
	clause := &WhereClause{}
	p.nextToken() // consume WHERE

	clause.Condition = p.parseExpression(LOWEST)
	return clause
}

// parseReturnClause parses RETURN [DISTINCT] items [ORDER BY ...] [SKIP n] [LIMIT n]
func (p *Parser) parseReturnClause() *ReturnClause {
	clause := &ReturnClause{}
	p.nextToken() // consume RETURN

	// Check for DISTINCT
	if p.curTokenIs(TOKEN_DISTINCT) {
		clause.Distinct = true
		p.nextToken()
	}

	// Parse return items
	clause.Items = p.parseReturnItems()

	// Parse ORDER BY
	if p.curTokenIs(TOKEN_ORDER) {
		p.nextToken()
		if !p.curTokenIs(TOKEN_BY) {
			p.addError("expected BY after ORDER")
			return clause
		}
		p.nextToken()
		clause.OrderBy = p.parseOrderItems()
	}

	// Parse SKIP
	if p.curTokenIs(TOKEN_SKIP) {
		p.nextToken()
		clause.Skip = p.parseExpression(LOWEST)
	}

	// Parse LIMIT
	if p.curTokenIs(TOKEN_LIMIT) {
		p.nextToken()
		clause.Limit = p.parseExpression(LOWEST)
	}

	return clause
}

// parseWithClause parses WITH [DISTINCT] items [WHERE ...] [ORDER BY ...] [SKIP n] [LIMIT n]
func (p *Parser) parseWithClause() *WithClause {
	clause := &WithClause{}
	p.nextToken() // consume WITH

	// Check for DISTINCT
	if p.curTokenIs(TOKEN_DISTINCT) {
		clause.Distinct = true
		p.nextToken()
	}

	// Parse return items
	clause.Items = p.parseReturnItems()

	// Parse WHERE
	if p.curTokenIs(TOKEN_WHERE) {
		p.nextToken()
		clause.Where = p.parseExpression(LOWEST)
	}

	// Parse ORDER BY
	if p.curTokenIs(TOKEN_ORDER) {
		p.nextToken()
		if !p.curTokenIs(TOKEN_BY) {
			p.addError("expected BY after ORDER")
			return clause
		}
		p.nextToken()
		clause.OrderBy = p.parseOrderItems()
	}

	// Parse SKIP
	if p.curTokenIs(TOKEN_SKIP) {
		p.nextToken()
		clause.Skip = p.parseExpression(LOWEST)
	}

	// Parse LIMIT
	if p.curTokenIs(TOKEN_LIMIT) {
		p.nextToken()
		clause.Limit = p.parseExpression(LOWEST)
	}

	return clause
}

// parseSetClause parses SET item [, item ...]
func (p *Parser) parseSetClause() *SetClause {
	clause := &SetClause{}
	p.nextToken() // consume SET

	for {
		item := p.parseSetItem()
		if item != nil {
			clause.Items = append(clause.Items, item)
		}
		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
	}

	return clause
}

// parseSetItem parses a single SET item
func (p *Parser) parseSetItem() *SetItem {
	item := &SetItem{}

	// Parse left side (variable or property access)
	// Use COMPARISON precedence so = is not consumed as an infix comparison operator
	left := p.parseExpression(COMPARISON)

	// Check for property access
	if prop, ok := left.(*PropertyAccess); ok {
		item.Property = prop
	} else if ident, ok := left.(*Identifier); ok {
		item.Variable = ident.Name
	}

	// Check for = or +=
	if p.curTokenIs(TOKEN_EQ) {
		p.nextToken()
	} else if p.curToken.Literal == "+=" {
		item.Append = true
		p.nextToken()
	} else {
		p.addError("expected = or += in SET")
		return nil
	}

	// Parse right side
	item.Expression = p.parseExpression(LOWEST)
	return item
}

// parseDeleteClause parses [DETACH] DELETE items
func (p *Parser) parseDeleteClause(detach bool) *DeleteClause {
	clause := &DeleteClause{Detach: detach}
	p.nextToken() // consume DELETE

	for {
		expr := p.parseExpression(LOWEST)
		clause.Expressions = append(clause.Expressions, expr)
		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
	}

	return clause
}

// parseRemoveClause parses REMOVE items: REMOVE n.prop, n:Label, ...
func (p *Parser) parseRemoveClause() *RemoveClause {
	clause := &RemoveClause{}
	p.nextToken() // consume REMOVE

	for {
		if !p.curTokenIs(TOKEN_IDENT) {
			break
		}
		varName := p.curToken.Literal
		p.nextToken()

		item := &RemoveItem{Variable: varName}
		if p.curTokenIs(TOKEN_COLON) {
			p.nextToken() // consume :
			if p.curToken.Literal != "" && !p.curTokenIs(TOKEN_EOF) {
				item.Label = p.curToken.Literal
				p.nextToken()
			}
		} else if p.curTokenIs(TOKEN_DOT) {
			p.nextToken() // consume .
			if p.curToken.Literal != "" && !p.curTokenIs(TOKEN_EOF) {
				item.Property = p.curToken.Literal
				p.nextToken()
			}
		} else {
			break
		}

		clause.Items = append(clause.Items, item)

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
	}

	return clause
}

// parseUnwindClause parses UNWIND expr AS variable
func (p *Parser) parseUnwindClause() *UnwindClause {
	clause := &UnwindClause{}
	p.nextToken() // consume UNWIND

	clause.Expression = p.parseExpression(LOWEST)

	if !p.curTokenIs(TOKEN_AS) {
		p.addError("expected AS in UNWIND")
		return nil
	}
	p.nextToken()

	if p.curToken.Literal == "" || p.curTokenIs(TOKEN_EOF) {
		p.addError("expected identifier after AS")
		return nil
	}
	clause.Variable = p.curToken.Literal
	p.nextToken()

	return clause
}

// parseLoadCSVClause parses LOAD CSV [WITH HEADERS] FROM 'file' AS row [FIELDTERMINATOR 'sep']
func (p *Parser) parseLoadCSVClause() *LoadCSVClause {
	clause := &LoadCSVClause{FieldTerminator: ","}
	p.nextToken() // consume LOAD

	if !p.curTokenIs(TOKEN_CSV) {
		p.addError("expected CSV after LOAD")
		return nil
	}
	p.nextToken() // consume CSV

	// Optional WITH HEADERS
	if p.curTokenIs(TOKEN_WITH) {
		p.nextToken() // consume WITH
		if !p.curTokenIs(TOKEN_HEADERS) {
			p.addError("expected HEADERS after WITH in LOAD CSV")
			return nil
		}
		clause.WithHeaders = true
		p.nextToken() // consume HEADERS
	}

	if !p.curTokenIs(TOKEN_FROM) {
		p.addError("expected FROM in LOAD CSV")
		return nil
	}
	p.nextToken() // consume FROM

	// Parse file path (string literal)
	if !p.curTokenIs(TOKEN_STRING) {
		p.addError("expected file path string after FROM")
		return nil
	}
	clause.FilePath = p.curToken.Literal
	p.nextToken()

	// Expect AS variable
	if !p.curTokenIs(TOKEN_AS) {
		p.addError("expected AS in LOAD CSV")
		return nil
	}
	p.nextToken() // consume AS

	if p.curToken.Literal == "" || p.curTokenIs(TOKEN_EOF) {
		p.addError("expected variable name after AS")
		return nil
	}
	clause.Variable = p.curToken.Literal
	p.nextToken()

	// Optional FIELDTERMINATOR
	if p.curTokenIs(TOKEN_FIELDTERMINATOR) {
		p.nextToken() // consume FIELDTERMINATOR
		if !p.curTokenIs(TOKEN_STRING) {
			p.addError("expected string after FIELDTERMINATOR")
			return nil
		}
		clause.FieldTerminator = p.curToken.Literal
		p.nextToken()
	}

	return clause
}

// parseReturnItems parses comma-separated return items
func (p *Parser) parseReturnItems() []*ReturnItem {
	items := []*ReturnItem{}

	for {
		item := &ReturnItem{}

		// Check for *
		if p.curTokenIs(TOKEN_STAR) {
			item.Expression = &Star{}
			p.nextToken()
		} else {
			item.Expression = p.parseExpression(LOWEST)
		}

		// Check for AS alias
		if p.curTokenIs(TOKEN_AS) {
			p.nextToken()
			if p.curToken.Literal != "" && !p.curTokenIs(TOKEN_EOF) {
				item.Alias = p.curToken.Literal
				p.nextToken()
			}
		}

		items = append(items, item)

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
	}

	return items
}

// parseOrderItems parses ORDER BY items
func (p *Parser) parseOrderItems() []*OrderItem {
	items := []*OrderItem{}

	for {
		item := &OrderItem{}
		item.Expression = p.parseExpression(LOWEST)

		// Check for ASC/DESC
		if p.curTokenIs(TOKEN_ASC) {
			p.nextToken()
		} else if p.curTokenIs(TOKEN_DESC) {
			item.Descending = true
			p.nextToken()
		}

		items = append(items, item)

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
	}

	return items
}

// ============================================================================
// Pattern Parsing
// ============================================================================

// parsePattern parses a graph pattern (possibly multiple parts)
func (p *Parser) parsePattern() *Pattern {
	pattern := &Pattern{
		Parts: []*PatternPart{},
	}

	for {
		part := p.parsePatternPart()
		if part == nil {
			return nil
		}
		pattern.Parts = append(pattern.Parts, part)

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
	}

	return pattern
}

// parsePatternPart parses a single pattern part
func (p *Parser) parsePatternPart() *PatternPart {
	part := &PatternPart{
		Elements: []PatternElement{},
	}

	// Check for path variable assignment: path = shortestPath(...)
	if p.curTokenIs(TOKEN_IDENT) && p.peekTokenIs(TOKEN_EQ) {
		part.Variable = p.curToken.Literal
		p.nextToken() // consume identifier
		p.nextToken() // consume =
	}

	// Check for shortestPath, allShortestPaths, or earliestPath
	if p.curTokenIs(TOKEN_SHORTESTPATH) || p.curTokenIs(TOKEN_ALLSHORTESTPATHS) || p.curTokenIs(TOKEN_EARLIESTPATH) {
		sp := p.parseShortestPath()
		if sp != nil {
			// Propagate the outer path variable (e.g. "p" in MATCH p = shortestPath(...))
			if sp.Variable == "" {
				sp.Variable = part.Variable
			}
			part.Elements = append(part.Elements, sp)
		}
		return part
	}

	// Parse alternating nodes and relationships
	for {
		// Parse node
		node := p.parseNodePattern()
		if node == nil {
			if len(part.Elements) == 0 {
				return nil
			}
			break
		}
		part.Elements = append(part.Elements, node)

		// Check for relationship
		if p.curTokenIs(TOKEN_DASH) || p.curTokenIs(TOKEN_ARROW_LEFT) {
			rel := p.parseRelationshipPattern()
			if rel == nil {
				break
			}
			part.Elements = append(part.Elements, rel)
		} else {
			break
		}
	}

	return part
}

// parseNodePattern parses (n:Label {props})
func (p *Parser) parseNodePattern() *NodePattern {
	if !p.curTokenIs(TOKEN_LPAREN) {
		return nil
	}
	p.nextToken() // consume (

	node := &NodePattern{}

	// Parse optional variable
	if p.curTokenIs(TOKEN_IDENT) {
		node.Variable = p.curToken.Literal
		p.nextToken()
	}

	// Parse labels
	for p.curTokenIs(TOKEN_COLON) {
		p.nextToken()
		if p.curToken.Literal == "" || p.curTokenIs(TOKEN_EOF) {
			p.addError("expected label after :")
			return nil
		}
		node.Labels = append(node.Labels, p.curToken.Literal)
		p.nextToken()
	}

	// Parse optional properties
	if p.curTokenIs(TOKEN_LBRACE) {
		node.Properties = p.parseMapLiteral()
	}

	if !p.curTokenIs(TOKEN_RPAREN) {
		p.addError("expected ) to close node pattern")
		return nil
	}
	p.nextToken() // consume )

	return node
}

// parseRelationshipPattern parses -[r:TYPE {props}]->
func (p *Parser) parseRelationshipPattern() *RelationshipPattern {
	rel := &RelationshipPattern{}

	// Determine start direction
	if p.curTokenIs(TOKEN_ARROW_LEFT) {
		rel.Direction = DirectionLeft
		p.nextToken()
	} else if p.curTokenIs(TOKEN_DASH) {
		p.nextToken()
	} else {
		return nil
	}

	// Parse relationship details [...]
	if p.curTokenIs(TOKEN_LBRACKET) {
		p.nextToken()

		// Parse optional variable
		if p.curTokenIs(TOKEN_IDENT) {
			rel.Variable = p.curToken.Literal
			p.nextToken()
		}

		// Parse types
		if p.curTokenIs(TOKEN_COLON) {
			p.nextToken()
			for {
				if p.curToken.Literal == "" || p.curTokenIs(TOKEN_EOF) {
					p.addError("expected relationship type after :")
					return nil
				}
				rel.Types = append(rel.Types, p.curToken.Literal)
				p.nextToken()
				if p.curTokenIs(TOKEN_PIPE) {
					p.nextToken()
				} else {
					break
				}
			}
		}

		// Parse variable-length pattern *min..max
		if p.curTokenIs(TOKEN_STAR) {
			rel.VarLength = true
			p.nextToken()

			// Parse optional min
			if p.curTokenIs(TOKEN_INT) {
				min, _ := strconv.Atoi(p.curToken.Literal)
				rel.MinHops = &min
				p.nextToken()
			}

			// Parse optional ..max
			if p.curTokenIs(TOKEN_DOTDOT) {
				p.nextToken()
				if p.curTokenIs(TOKEN_INT) {
					max, _ := strconv.Atoi(p.curToken.Literal)
					rel.MaxHops = &max
					p.nextToken()
				}
			} else if rel.MinHops != nil {
				// Just *n means exactly n hops
				rel.MaxHops = rel.MinHops
			}
		}

		// Parse optional properties
		if p.curTokenIs(TOKEN_LBRACE) {
			rel.Properties = p.parseMapLiteral()
		}

		if !p.curTokenIs(TOKEN_RBRACKET) {
			p.addError("expected ] to close relationship pattern")
			return nil
		}
		p.nextToken()
	}

	// Parse end direction
	if p.curTokenIs(TOKEN_ARROW_RIGHT) {
		if rel.Direction == DirectionLeft {
			rel.Direction = DirectionBoth
		} else {
			rel.Direction = DirectionRight
		}
		p.nextToken()
	} else if p.curTokenIs(TOKEN_DASH) {
		p.nextToken()
	}

	return rel
}

// parseShortestPath parses shortestPath(...) or allShortestPaths(...)
func (p *Parser) parseShortestPath() *ShortestPathPattern {
	sp := &ShortestPathPattern{
		Function: strings.ToLower(p.curToken.Literal),
	}
	p.nextToken() // consume function name

	if !p.curTokenIs(TOKEN_LPAREN) {
		p.addError("expected ( after %s", sp.Function)
		return nil
	}
	p.nextToken()

	// Parse inner pattern part
	sp.Pattern = p.parsePatternPart()

	if !p.curTokenIs(TOKEN_RPAREN) {
		p.addError("expected ) to close %s", sp.Function)
		return nil
	}
	p.nextToken()

	return sp
}

// ============================================================================
// Expression Parsing (Pratt Parser)
// ============================================================================

// Precedence levels
const (
	LOWEST      = iota
	OR          // OR
	XOR         // XOR
	AND         // AND
	NOT         // NOT
	COMPARISON  // =, <>, <, >, <=, >=, IN, CONTAINS, etc.
	ADDITIVE    // +, -
	MULTIPLICATIVE // *, /, %
	POWER       // ^
	UNARY       // -x, NOT x
	POSTFIX     // ., [], ()
)

var precedences = map[TokenType]int{
	TOKEN_OR:       OR,
	TOKEN_XOR:      XOR,
	TOKEN_AND:      AND,
	TOKEN_EQ:       COMPARISON,
	TOKEN_NEQ:      COMPARISON,
	TOKEN_LT:       COMPARISON,
	TOKEN_GT:       COMPARISON,
	TOKEN_LTE:      COMPARISON,
	TOKEN_GTE:      COMPARISON,
	TOKEN_IN:       COMPARISON,
	TOKEN_SIMILAR:  COMPARISON,
	TOKEN_CONTAINS: COMPARISON,
	TOKEN_STARTS:   COMPARISON,
	TOKEN_ENDS:     COMPARISON,
	TOKEN_IS:       COMPARISON,
	TOKEN_PLUS:     ADDITIVE,
	TOKEN_MINUS:    ADDITIVE,
	TOKEN_DASH:     ADDITIVE,
	TOKEN_STAR:     MULTIPLICATIVE,
	TOKEN_SLASH:    MULTIPLICATIVE,
	TOKEN_PERCENT:  MULTIPLICATIVE,
	TOKEN_CARET:    POWER,
	TOKEN_DOT:      POSTFIX,
	TOKEN_LBRACKET: POSTFIX,
	TOKEN_LPAREN:   POSTFIX,
}

func (p *Parser) curPrecedence() int {
	if prec, ok := precedences[p.curToken.Type]; ok {
		return prec
	}
	return LOWEST
}

func (p *Parser) peekPrecedence() int {
	if prec, ok := precedences[p.peekToken.Type]; ok {
		return prec
	}
	return LOWEST
}

// parseExpression parses an expression using Pratt parsing
func (p *Parser) parseExpression(precedence int) Expression {
	// Parse prefix
	var left Expression

	switch p.curToken.Type {
	case TOKEN_IDENT:
		left = p.parseIdentifier()
	case TOKEN_INT:
		left = p.parseIntegerLiteral()
	case TOKEN_FLOAT:
		left = p.parseFloatLiteral()
	case TOKEN_STRING:
		left = p.parseStringLiteral()
	case TOKEN_TRUE, TOKEN_FALSE:
		left = p.parseBooleanLiteral()
	case TOKEN_NULL:
		left = p.parseNullLiteral()
	case TOKEN_PARAM:
		left = &Parameter{Name: p.curToken.Literal}
		p.nextToken()
	case TOKEN_LBRACKET:
		left = p.parseListLiteral()
	case TOKEN_LBRACE:
		left = p.parseMapLiteral()
	case TOKEN_LPAREN:
		left = p.parseGroupedExpression()
	case TOKEN_NOT:
		left = p.parseNotExpression()
	case TOKEN_MINUS, TOKEN_DASH:
		left = p.parseNegationExpression()
	case TOKEN_CASE:
		left = p.parseCaseExpression()
	case TOKEN_COUNT, TOKEN_SUM, TOKEN_AVG, TOKEN_MIN, TOKEN_MAX, TOKEN_COLLECT:
		left = p.parseAggregateFunction()
	case TOKEN_EXISTS:
		left = p.parseExistsFunction()
	case TOKEN_ANY, TOKEN_ALL, TOKEN_NONE, TOKEN_SINGLE:
		left = p.parseListPredicate()
	default:
		// Treat keyword tokens as identifiers when used as variable/alias names
		// (e.g., ORDER BY from, RETURN x AS to)
		if p.curToken.Literal != "" && !p.curTokenIs(TOKEN_EOF) {
			left = p.parseIdentifier()
		} else {
			p.addError("unexpected token in expression: %s", p.curToken.Literal)
			return nil
		}
	}

	// Parse infix
	for !p.curTokenIs(TOKEN_EOF) && precedence < p.curPrecedence() {
		switch p.curToken.Type {
		case TOKEN_DOT:
			left = p.parsePropertyAccess(left)
		case TOKEN_LBRACKET:
			left = p.parseIndexAccess(left)
		case TOKEN_PLUS, TOKEN_MINUS, TOKEN_DASH, TOKEN_STAR, TOKEN_SLASH, TOKEN_PERCENT, TOKEN_CARET:
			left = p.parseBinaryExpression(left)
		case TOKEN_AND, TOKEN_OR, TOKEN_XOR:
			left = p.parseBinaryExpression(left)
		case TOKEN_EQ, TOKEN_NEQ, TOKEN_LT, TOKEN_GT, TOKEN_LTE, TOKEN_GTE:
			left = p.parseComparisonExpression(left)
		case TOKEN_IN:
			left = p.parseInExpression(left)
		case TOKEN_IS:
			left = p.parseIsNullExpression(left)
		case TOKEN_CONTAINS:
			left = p.parseStringPredicate(left, "CONTAINS")
		case TOKEN_STARTS:
			left = p.parseStartsWithExpression(left)
		case TOKEN_ENDS:
			left = p.parseEndsWithExpression(left)
		case TOKEN_SIMILAR:
			left = p.parseSimilarToInWhereExpression(left)
		default:
			return left
		}
	}

	return left
}

func (p *Parser) parseIdentifier() Expression {
	ident := &Identifier{Name: p.curToken.Literal}
	p.nextToken()

	// Check for function call
	if p.curTokenIs(TOKEN_LPAREN) {
		return p.parseFunctionCallWithName(ident.Name)
	}

	return ident
}

func (p *Parser) parseIntegerLiteral() Expression {
	val, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
	if err != nil {
		p.addError("could not parse %s as integer", p.curToken.Literal)
		return nil
	}
	lit := &IntegerLiteral{Value: val}
	p.nextToken()
	return lit
}

func (p *Parser) parseFloatLiteral() Expression {
	val, err := strconv.ParseFloat(p.curToken.Literal, 64)
	if err != nil {
		p.addError("could not parse %s as float", p.curToken.Literal)
		return nil
	}
	lit := &FloatLiteral{Value: val}
	p.nextToken()
	return lit
}

func (p *Parser) parseStringLiteral() Expression {
	lit := &StringLiteral{Value: p.curToken.Literal}
	p.nextToken()
	return lit
}

func (p *Parser) parseBooleanLiteral() Expression {
	lit := &BooleanLiteral{Value: p.curTokenIs(TOKEN_TRUE)}
	p.nextToken()
	return lit
}

func (p *Parser) parseNullLiteral() Expression {
	p.nextToken()
	return &NullLiteral{}
}

func (p *Parser) parseListLiteral() Expression {
	p.nextToken() // consume [

	// Check for list comprehension: [x IN list WHERE cond | expr]
	// Detect by: identifier followed by IN
	if (p.curTokenIs(TOKEN_IDENT) || (p.curToken.Literal != "" && !p.curTokenIs(TOKEN_EOF) && !p.curTokenIs(TOKEN_RBRACKET))) && p.peekTokenIs(TOKEN_IN) {
		return p.parseListComprehension()
	}

	// Check for pattern comprehension: [(node)-[:REL]->(m) | expr]
	if p.curTokenIs(TOKEN_LPAREN) {
		return p.parsePatternComprehension()
	}

	list := &ListLiteral{}
	for !p.curTokenIs(TOKEN_RBRACKET) && !p.curTokenIs(TOKEN_EOF) {
		elem := p.parseExpression(LOWEST)
		list.Elements = append(list.Elements, elem)
		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken()
		}
	}

	if !p.curTokenIs(TOKEN_RBRACKET) {
		p.addError("expected ]")
		return nil
	}
	p.nextToken()
	return list
}

// parseListComprehension parses [x IN list WHERE cond | expr]
// Called after consuming [, with cursor on the variable name.
func (p *Parser) parseListComprehension() Expression {
	lc := &ListComprehension{}
	lc.Variable = p.curToken.Literal
	p.nextToken() // consume variable

	if !p.curTokenIs(TOKEN_IN) {
		p.addError("expected IN in list comprehension")
		return nil
	}
	p.nextToken() // consume IN

	lc.List = p.parseExpression(LOWEST)

	// Optional WHERE
	if p.curTokenIs(TOKEN_WHERE) {
		p.nextToken() // consume WHERE
		lc.Where = p.parseExpression(LOWEST)
	}

	// Expect |
	if !p.curTokenIs(TOKEN_PIPE) {
		p.addError("expected | in list comprehension")
		return nil
	}
	p.nextToken() // consume |

	lc.Projection = p.parseExpression(LOWEST)

	if !p.curTokenIs(TOKEN_RBRACKET) {
		p.addError("expected ] to close list comprehension")
		return nil
	}
	p.nextToken() // consume ]

	return lc
}

// parsePatternComprehension parses [(n)-[:REL]->(m) | expr] or [(n)-[:REL]->(m) WHERE cond | expr]
// Called after consuming [, with cursor on (.
func (p *Parser) parsePatternComprehension() Expression {
	part := p.parsePatternPart()
	if part == nil {
		p.addError("expected pattern in pattern comprehension")
		return nil
	}

	pc := &PatternComprehension{Pattern: part}

	if p.curTokenIs(TOKEN_WHERE) {
		p.nextToken() // consume WHERE
		pc.Where = p.parseExpression(LOWEST)
	}

	if !p.curTokenIs(TOKEN_PIPE) {
		p.addError("expected | in pattern comprehension")
		return nil
	}
	p.nextToken() // consume |

	pc.Projection = p.parseExpression(LOWEST)

	if !p.curTokenIs(TOKEN_RBRACKET) {
		p.addError("expected ] to close pattern comprehension")
		return nil
	}
	p.nextToken() // consume ]

	return pc
}

func (p *Parser) parseMapLiteral() Expression {
	m := &MapLiteral{}
	p.nextToken() // consume {

	for !p.curTokenIs(TOKEN_RBRACE) && !p.curTokenIs(TOKEN_EOF) {
		// Parse key — identifiers, strings, or reserved words used as property names (e.g. {drop: x, from: y})
		key := p.curToken.Literal
		if key == "" || p.curTokenIs(TOKEN_RBRACE) || p.curTokenIs(TOKEN_COMMA) || p.curTokenIs(TOKEN_EOF) {
			p.addError("expected key in map literal")
			return nil
		}
		p.nextToken()

		// Expect colon
		if !p.curTokenIs(TOKEN_COLON) {
			p.addError("expected : after key in map")
			return nil
		}
		p.nextToken()

		// Parse value
		value := p.parseExpression(LOWEST)
		m.Pairs = append(m.Pairs, &MapPair{Key: key, Value: value})

		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken()
		}
	}

	if !p.curTokenIs(TOKEN_RBRACE) {
		p.addError("expected }")
		return nil
	}
	p.nextToken()
	return m
}

func (p *Parser) parseGroupedExpression() Expression {
	p.nextToken() // consume (
	expr := p.parseExpression(LOWEST)

	if !p.curTokenIs(TOKEN_RPAREN) {
		p.addError("expected )")
		return nil
	}
	p.nextToken()
	return expr
}

func (p *Parser) parseNotExpression() Expression {
	p.nextToken() // consume NOT
	operand := p.parseExpression(NOT)
	return &UnaryExpression{Operator: "NOT", Operand: operand}
}

func (p *Parser) parseNegationExpression() Expression {
	p.nextToken() // consume -
	operand := p.parseExpression(UNARY)
	return &UnaryExpression{Operator: "-", Operand: operand}
}

func (p *Parser) parsePropertyAccess(left Expression) Expression {
	p.nextToken() // consume .
	if p.curToken.Literal == "" || p.curTokenIs(TOKEN_EOF) {
		p.addError("expected property name after .")
		return nil
	}
	prop := p.curToken.Literal
	p.nextToken()
	return &PropertyAccess{Object: left, Property: prop}
}

func (p *Parser) parseIndexAccess(left Expression) Expression {
	p.nextToken() // consume [
	index := p.parseExpression(LOWEST)

	// Check for slice
	if p.curTokenIs(TOKEN_DOTDOT) {
		p.nextToken()
		end := p.parseExpression(LOWEST)
		if !p.curTokenIs(TOKEN_RBRACKET) {
			p.addError("expected ]")
			return nil
		}
		p.nextToken()
		return &SliceAccess{Object: left, Start: index, End: end}
	}

	if !p.curTokenIs(TOKEN_RBRACKET) {
		p.addError("expected ]")
		return nil
	}
	p.nextToken()
	return &IndexAccess{Object: left, Index: index}
}

func (p *Parser) parseBinaryExpression(left Expression) Expression {
	operator := p.curToken.Literal
	precedence := p.curPrecedence()
	p.nextToken()
	right := p.parseExpression(precedence)
	return &BinaryExpression{Left: left, Operator: operator, Right: right}
}

func (p *Parser) parseComparisonExpression(left Expression) Expression {
	operator := p.curToken.Literal
	precedence := p.curPrecedence()
	p.nextToken()
	right := p.parseExpression(precedence)
	return &ComparisonExpression{Left: left, Operator: operator, Right: right}
}

func (p *Parser) parseInExpression(left Expression) Expression {
	not := false
	// Check for NOT IN
	if p.peekTokenIs(TOKEN_NOT) {
		not = true
		p.nextToken()
	}
	p.nextToken() // consume IN
	right := p.parseExpression(COMPARISON)
	return &InExpression{Expression: left, List: right, Not: not}
}

func (p *Parser) parseIsNullExpression(left Expression) Expression {
	p.nextToken() // consume IS

	not := false
	if p.curTokenIs(TOKEN_NOT) {
		not = true
		p.nextToken()
	}

	if !p.curTokenIs(TOKEN_NULL) {
		p.addError("expected NULL after IS")
		return nil
	}
	p.nextToken()

	return &IsNullExpression{Expression: left, Not: not}
}

func (p *Parser) parseStringPredicate(left Expression, predicate string) Expression {
	p.nextToken() // consume CONTAINS/etc.
	right := p.parseExpression(COMPARISON)
	return &ComparisonExpression{Left: left, Operator: predicate, Right: right}
}

func (p *Parser) parseStartsWithExpression(left Expression) Expression {
	p.nextToken() // consume STARTS
	if !p.curTokenIs(TOKEN_WITH) {
		p.addError("expected WITH after STARTS")
		return nil
	}
	p.nextToken()
	right := p.parseExpression(COMPARISON)
	return &ComparisonExpression{Left: left, Operator: "STARTS WITH", Right: right}
}

func (p *Parser) parseEndsWithExpression(left Expression) Expression {
	p.nextToken() // consume ENDS
	if !p.curTokenIs(TOKEN_WITH) {
		p.addError("expected WITH after ENDS")
		return nil
	}
	p.nextToken()
	right := p.parseExpression(COMPARISON)
	return &ComparisonExpression{Left: left, Operator: "ENDS WITH", Right: right}
}

// parseSimilarToInWhereExpression parses `variable SIMILAR TO "query" [THRESHOLD x]`
// inside a WHERE clause, producing a SimilarToExpression.
func (p *Parser) parseSimilarToInWhereExpression(left Expression) Expression {
	ident, ok := left.(*Identifier)
	if !ok {
		p.addError("expected identifier before SIMILAR TO")
		return nil
	}
	p.nextToken() // consume SIMILAR
	if !p.curTokenIs(TOKEN_TO) {
		p.addError("expected TO after SIMILAR")
		return nil
	}
	p.nextToken() // consume TO

	queryExpr := p.parseExpression(COMPARISON)
	str, ok := queryExpr.(*StringLiteral)
	if !ok {
		p.addError("expected string literal after SIMILAR TO")
		return nil
	}

	expr := &SimilarToExpression{
		Variable:  ident.Name,
		Query:     str.Value,
		Threshold: 0, // 0 means no minimum threshold
	}

	if p.curTokenIs(TOKEN_THRESHOLD) {
		p.nextToken()
		threshExpr := p.parseExpression(LOWEST)
		switch v := threshExpr.(type) {
		case *FloatLiteral:
			expr.Threshold = float32(v.Value)
		case *IntegerLiteral:
			expr.Threshold = float32(v.Value)
		}
	}

	return expr
}

func (p *Parser) parseFunctionCall() Expression {
	name := p.curToken.Literal
	p.nextToken()
	return p.parseFunctionCallWithName(name)
}

func (p *Parser) parseFunctionCallWithName(name string) Expression {
	fc := &FunctionCall{Name: name}
	p.nextToken() // consume (

	// Check for DISTINCT
	if p.curTokenIs(TOKEN_DISTINCT) {
		fc.Distinct = true
		p.nextToken()
	}

	// Parse arguments
	for !p.curTokenIs(TOKEN_RPAREN) && !p.curTokenIs(TOKEN_EOF) {
		arg := p.parseExpression(LOWEST)
		fc.Arguments = append(fc.Arguments, arg)
		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken()
		}
	}

	if !p.curTokenIs(TOKEN_RPAREN) {
		p.addError("expected ) to close function call")
		return nil
	}
	p.nextToken()

	return fc
}

func (p *Parser) parseAggregateFunction() Expression {
	return p.parseFunctionCall()
}

// parseExistsFunction parses EXISTS(expr) as a function call
func (p *Parser) parseExistsFunction() Expression {
	name := p.curToken.Literal // "EXISTS"
	p.nextToken()              // consume EXISTS
	if !p.curTokenIs(TOKEN_LPAREN) {
		// Not a function call — treat as identifier
		return &Identifier{Name: name}
	}
	return p.parseFunctionCallWithName(name)
}

// parseListPredicate parses ANY/ALL/NONE/SINGLE(variable IN list WHERE condition)
func (p *Parser) parseListPredicate() Expression {
	funcName := p.curToken.Literal // ANY, ALL, NONE, SINGLE
	p.nextToken()                  // consume function name

	if !p.curTokenIs(TOKEN_LPAREN) {
		return &Identifier{Name: funcName}
	}
	p.nextToken() // consume (

	expr := &ListPredicateExpression{Function: funcName}

	// Parse variable name
	if p.curToken.Literal == "" || p.curTokenIs(TOKEN_EOF) {
		p.addError("expected variable in %s()", funcName)
		return nil
	}
	expr.Variable = p.curToken.Literal
	p.nextToken()

	// Expect IN
	if !p.curTokenIs(TOKEN_IN) {
		p.addError("expected IN after variable in %s()", funcName)
		return nil
	}
	p.nextToken() // consume IN

	// Parse list expression
	expr.List = p.parseExpression(LOWEST)

	// Expect WHERE or |
	if p.curTokenIs(TOKEN_WHERE) {
		p.nextToken() // consume WHERE
	} else if p.curTokenIs(TOKEN_PIPE) {
		p.nextToken() // consume |
	} else {
		p.addError("expected WHERE or | in %s()", funcName)
		return nil
	}

	// Parse condition
	expr.Condition = p.parseExpression(LOWEST)

	if !p.curTokenIs(TOKEN_RPAREN) {
		p.addError("expected ) to close %s()", funcName)
		return nil
	}
	p.nextToken() // consume )

	return expr
}

func (p *Parser) parseCaseExpression() Expression {
	ce := &CaseExpression{}
	p.nextToken() // consume CASE

	// Check for simple CASE (CASE expr WHEN ...)
	if !p.curTokenIs(TOKEN_WHEN) {
		ce.Test = p.parseExpression(LOWEST)
	}

	// Parse WHEN clauses
	for p.curTokenIs(TOKEN_WHEN) {
		p.nextToken()
		when := p.parseExpression(LOWEST)

		if !p.curTokenIs(TOKEN_THEN) {
			p.addError("expected THEN after WHEN condition")
			return nil
		}
		p.nextToken()
		then := p.parseExpression(LOWEST)

		ce.Whens = append(ce.Whens, &CaseWhen{When: when, Then: then})
	}

	// Parse optional ELSE
	if p.curTokenIs(TOKEN_ELSE) {
		p.nextToken()
		ce.ElseResult = p.parseExpression(LOWEST)
	}

	// Expect END
	if !p.curTokenIs(TOKEN_END) {
		p.addError("expected END to close CASE")
		return nil
	}
	p.nextToken()

	return ce
}

// parseForeachClause parses FOREACH (variable IN list | SET ...)
func (p *Parser) parseForeachClause() *ForeachClause {
	clause := &ForeachClause{}
	p.nextToken() // consume FOREACH

	if !p.curTokenIs(TOKEN_LPAREN) {
		p.addError("expected ( after FOREACH")
		return nil
	}
	p.nextToken() // consume (

	// Parse variable
	if p.curToken.Literal == "" || p.curTokenIs(TOKEN_EOF) {
		p.addError("expected variable in FOREACH")
		return nil
	}
	clause.Variable = p.curToken.Literal
	p.nextToken()

	// Expect IN
	if !p.curTokenIs(TOKEN_IN) {
		p.addError("expected IN after variable in FOREACH")
		return nil
	}
	p.nextToken() // consume IN

	// Parse list expression
	clause.List = p.parseExpression(LOWEST)

	// Expect | (pipe)
	if !p.curTokenIs(TOKEN_PIPE) {
		p.addError("expected | after list in FOREACH")
		return nil
	}
	p.nextToken() // consume |

	// Parse SET operations inside FOREACH
	for !p.curTokenIs(TOKEN_RPAREN) && !p.curTokenIs(TOKEN_EOF) {
		if p.curTokenIs(TOKEN_SET) {
			p.nextToken() // consume SET
			for {
				item := p.parseSetItem()
				if item != nil {
					clause.Updates = append(clause.Updates, item)
				}
				if !p.curTokenIs(TOKEN_COMMA) {
					break
				}
				p.nextToken()
			}
		} else {
			p.addError("expected SET inside FOREACH, got %s", p.curToken.Literal)
			return nil
		}
	}

	if !p.curTokenIs(TOKEN_RPAREN) {
		p.addError("expected ) to close FOREACH")
		return nil
	}
	p.nextToken() // consume )

	return clause
}

// parseCallClause parses CALL procedure([config]) YIELD var1, var2, ...
func (p *Parser) parseCallClause() *CallClause {
	clause := &CallClause{}
	p.nextToken() // consume CALL

	// Parse procedure name (may be a keyword like "pageRank" — treat any token as name)
	clause.Procedure = p.curToken.Literal
	p.nextToken()

	// Parse optional arguments: (config)
	if p.curTokenIs(TOKEN_LPAREN) {
		p.nextToken() // consume (
		if !p.curTokenIs(TOKEN_RPAREN) {
			clause.Config = p.parseExpression(LOWEST)
		}
		if !p.curTokenIs(TOKEN_RPAREN) {
			p.addError("expected ) after CALL arguments")
			return nil
		}
		p.nextToken() // consume )
	}

	// Parse YIELD
	if !p.curTokenIs(TOKEN_YIELD) {
		p.addError("expected YIELD after CALL procedure")
		return nil
	}
	p.nextToken() // consume YIELD

	// Parse comma-separated yield variables
	for {
		clause.YieldItems = append(clause.YieldItems, p.curToken.Literal)
		p.nextToken()
		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	// Check for THROUGH TIME modifier
	if p.curTokenIs(TOKEN_THROUGH) {
		p.nextToken() // consume THROUGH
		if !p.curTokenIs(TOKEN_TIME) {
			p.addError("expected TIME after THROUGH")
			return nil
		}
		p.nextToken() // consume TIME
		clause.ThroughTime = true
	}

	return clause
}

// ============================================================================
// Custom Extension Clauses
// ============================================================================

// parseTimeClause parses AT TIME EARLIEST or AT TIME <timestamp>
func (p *Parser) parseTimeClause() *TimeClause {
	clause := &TimeClause{}
	p.nextToken() // consume AT

	if !p.curTokenIs(TOKEN_TIME) {
		p.addError("expected TIME after AT")
		return nil
	}
	p.nextToken()

	if p.curTokenIs(TOKEN_EARLIEST) {
		clause.Mode = "EARLIEST"
		p.nextToken()
	} else {
		clause.Mode = "TIMESTAMP"
		clause.Timestamp = p.parseExpression(LOWEST)
	}

	return clause
}

// parseEmbedClause parses EMBED expression
func (p *Parser) parseEmbedClause() *EmbedClause {
	clause := &EmbedClause{}
	p.nextToken() // consume EMBED

	// First token should be variable
	if p.curToken.Literal == "" || p.curTokenIs(TOKEN_EOF) {
		p.addError("expected variable in EMBED clause")
		return nil
	}
	clause.Variable = p.curToken.Literal
	p.nextToken()

	// Check for property access (EMBED p.description)
	if p.curTokenIs(TOKEN_DOT) {
		p.nextToken()
		if p.curToken.Literal == "" || p.curTokenIs(TOKEN_EOF) {
			p.addError("expected property name after .")
			return nil
		}
		clause.Mode = "PROPERTY"
		clause.Property = p.curToken.Literal
		p.nextToken()
		return clause
	}

	// Check for literal text (EMBED p "text")
	if p.curTokenIs(TOKEN_STRING) {
		clause.Mode = "TEXT"
		clause.Text = p.curToken.Literal
		p.nextToken()
		return clause
	}

	// Default to AUTO
	clause.Mode = "AUTO"
	return clause
}

// parseSimilarToClause parses SIMILAR TO "query" [LIMIT n] [THRESHOLD t]
func (p *Parser) parseSimilarToClause() *SimilarToClause {
	clause := &SimilarToClause{}
	p.nextToken() // consume SIMILAR

	if !p.curTokenIs(TOKEN_TO) {
		p.addError("expected TO after SIMILAR")
		return nil
	}
	p.nextToken()

	// Parse query expression (usually a string)
	clause.Query = p.parseExpression(LOWEST)

	// Parse optional [VERSIONS] DRIFT THROUGH TIME or [VERSIONS] THROUGH TIME (must come before LIMIT/THRESHOLD)
	if p.curTokenIs(TOKEN_DRIFT) {
		p.nextToken()
		if !p.curTokenIs(TOKEN_THROUGH) {
			p.addError("expected THROUGH after DRIFT")
			return nil
		}
		p.nextToken()
		if !p.curTokenIs(TOKEN_TIME) {
			p.addError("expected TIME after THROUGH")
			return nil
		}
		p.nextToken()
		clause.ThroughTime = true
		clause.DriftMode = true
	} else if p.curTokenIs(TOKEN_VERSIONS) {
		// Handle optional VERSIONS keyword before THROUGH TIME
		p.nextToken()
		if !p.curTokenIs(TOKEN_THROUGH) {
			p.addError("expected THROUGH after VERSIONS")
			return nil
		}
		p.nextToken()
		if !p.curTokenIs(TOKEN_TIME) {
			p.addError("expected TIME after THROUGH")
			return nil
		}
		p.nextToken()
		clause.ThroughTime = true
	} else if p.curTokenIs(TOKEN_THROUGH) {
		p.nextToken()
		if !p.curTokenIs(TOKEN_TIME) {
			p.addError("expected TIME after THROUGH")
			return nil
		}
		p.nextToken()
		clause.ThroughTime = true
	}

	// Parse optional LIMIT
	if p.curTokenIs(TOKEN_LIMIT) {
		p.nextToken()
		clause.Limit = p.parseExpression(LOWEST)
	}

	// Parse optional THRESHOLD
	if p.curTokenIs(TOKEN_THRESHOLD) {
		p.nextToken()
		clause.Threshold = p.parseExpression(LOWEST)
	}

	return clause
}
