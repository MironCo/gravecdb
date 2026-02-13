package cypher

import (
	"fmt"
	"strconv"
)

// ConvertToLegacy converts the new AST Query to the old graph.Query format
// This allows gradual migration while using the existing executor
func ConvertToLegacy(query *Query) (*LegacyQuery, error) {
	lq := &LegacyQuery{}

	for _, clause := range query.Clauses {
		switch c := clause.(type) {
		case *MatchClause:
			lq.QueryType = "MATCH"
			pattern, err := convertPattern(c.Pattern)
			if err != nil {
				return nil, err
			}
			lq.MatchPattern = pattern

			// Check for shortestPath
			if c.Pattern != nil && len(c.Pattern.Parts) > 0 {
				part := c.Pattern.Parts[0]
				for _, elem := range part.Elements {
					if sp, ok := elem.(*ShortestPathPattern); ok {
						lq.IsPathQuery = true
						lq.MatchPattern.PathFunction = convertShortestPath(sp)
					}
				}
			}

		case *CreateClause:
			createClause, err := convertCreateClause(c)
			if err != nil {
				return nil, err
			}
			if lq.QueryType == "" {
				lq.QueryType = "CREATE"
			}
			lq.CreateClause = createClause

		case *WhereClause:
			whereClause, err := convertWhereClause(c)
			if err != nil {
				return nil, err
			}
			lq.WhereClause = whereClause

		case *ReturnClause:
			returnClause, err := convertReturnClause(c)
			if err != nil {
				return nil, err
			}
			lq.ReturnClause = returnClause

		case *SetClause:
			setClause, err := convertSetClause(c)
			if err != nil {
				return nil, err
			}
			lq.SetClause = setClause

		case *DeleteClause:
			deleteClause, err := convertDeleteClause(c)
			if err != nil {
				return nil, err
			}
			lq.DeleteClause = deleteClause

		case *TimeClause:
			timeClause, err := convertTimeClause(c)
			if err != nil {
				return nil, err
			}
			lq.TimeClause = timeClause

		case *EmbedClause:
			embedClause := convertEmbedClause(c)
			lq.EmbedClause = embedClause

		case *SimilarToClause:
			similarClause, err := convertSimilarToClause(c)
			if err != nil {
				return nil, err
			}
			lq.SimilarToClause = similarClause
		}
	}

	return lq, nil
}

// LegacyQuery mirrors the old graph.Query structure
type LegacyQuery struct {
	QueryType       string
	MatchPattern    *LegacyMatchPattern
	CreateClause    *LegacyCreateClause
	SetClause       *LegacySetClause
	DeleteClause    *LegacyDeleteClause
	WhereClause     *LegacyWhereClause
	ReturnClause    *LegacyReturnClause
	TimeClause      *LegacyTimeClause
	EmbedClause     *LegacyEmbedClause
	SimilarToClause *LegacySimilarToClause
	IsPathQuery     bool
}

type LegacyMatchPattern struct {
	Nodes         []LegacyNodePattern
	Relationships []LegacyRelPattern
	PathFunction  *LegacyPathFunction
}

type LegacyPathFunction struct {
	Function     string
	Variable     string
	StartPattern LegacyNodePattern
	EndPattern   LegacyNodePattern
	RelTypes     []string
	MaxDepth     int
}

type LegacyNodePattern struct {
	Variable   string
	Labels     []string
	Properties map[string]interface{}
}

type LegacyRelPattern struct {
	Variable  string
	Types     []string
	FromIndex int
	ToIndex   int
	Direction string
}

type LegacyCreateClause struct {
	Nodes         []LegacyCreateNode
	Relationships []LegacyCreateRelationship
}

type LegacyCreateNode struct {
	Variable   string
	Labels     []string
	Properties map[string]interface{}
}

type LegacyCreateRelationship struct {
	Variable   string
	Type       string
	FromVar    string
	ToVar      string
	Properties map[string]interface{}
}

type LegacySetClause struct {
	Updates []LegacyPropertyUpdate
}

type LegacyPropertyUpdate struct {
	Variable string
	Property string
	Value    interface{}
}

type LegacyDeleteClause struct {
	Variables []string
	Detach    bool
}

type LegacyWhereClause struct {
	Conditions []LegacyCondition
}

type LegacyCondition struct {
	Variable string
	Property string
	Operator string
	Value    interface{}
}

type LegacyReturnClause struct {
	Items []LegacyReturnItem
}

type LegacyReturnItem struct {
	Variable string
	Property string
}

type LegacyTimeClause struct {
	Mode      string
	Timestamp int64
}

type LegacyEmbedClause struct {
	Variable string
	Mode     string
	Text     string
	Property string
}

type LegacySimilarToClause struct {
	Variable  string
	QueryText string
	Limit     int
	Threshold float32
}

// Conversion functions

func convertPattern(p *Pattern) (*LegacyMatchPattern, error) {
	if p == nil || len(p.Parts) == 0 {
		return nil, nil
	}

	lp := &LegacyMatchPattern{
		Nodes:         []LegacyNodePattern{},
		Relationships: []LegacyRelPattern{},
	}

	// Process each pattern part
	for _, part := range p.Parts {
		nodeIndex := len(lp.Nodes)

		for i, elem := range part.Elements {
			switch e := elem.(type) {
			case *NodePattern:
				props, err := convertMapToProps(e.Properties)
				if err != nil {
					return nil, err
				}
				lp.Nodes = append(lp.Nodes, LegacyNodePattern{
					Variable:   e.Variable,
					Labels:     e.Labels,
					Properties: props,
				})

			case *RelationshipPattern:
				direction := "-"
				switch e.Direction {
				case DirectionRight:
					direction = "->"
				case DirectionLeft:
					direction = "<-"
				}

				lp.Relationships = append(lp.Relationships, LegacyRelPattern{
					Variable:  e.Variable,
					Types:     e.Types,
					FromIndex: nodeIndex + (i-1)/2,
					ToIndex:   nodeIndex + (i+1)/2,
					Direction: direction,
				})

			case *ShortestPathPattern:
				// Handled separately
				continue
			}
		}
	}

	return lp, nil
}

func convertShortestPath(sp *ShortestPathPattern) *LegacyPathFunction {
	pf := &LegacyPathFunction{
		Function: sp.Function,
		Variable: sp.Variable,
	}

	if sp.Pattern != nil && len(sp.Pattern.Elements) >= 1 {
		// Get start node
		if node, ok := sp.Pattern.Elements[0].(*NodePattern); ok {
			props, _ := convertMapToProps(node.Properties)
			pf.StartPattern = LegacyNodePattern{
				Variable:   node.Variable,
				Labels:     node.Labels,
				Properties: props,
			}
		}

		// Get end node (should be last element)
		lastIdx := len(sp.Pattern.Elements) - 1
		if node, ok := sp.Pattern.Elements[lastIdx].(*NodePattern); ok {
			props, _ := convertMapToProps(node.Properties)
			pf.EndPattern = LegacyNodePattern{
				Variable:   node.Variable,
				Labels:     node.Labels,
				Properties: props,
			}
		}

		// Get relationship types and max depth
		for _, elem := range sp.Pattern.Elements {
			if rel, ok := elem.(*RelationshipPattern); ok {
				pf.RelTypes = rel.Types
				if rel.MaxHops != nil {
					pf.MaxDepth = *rel.MaxHops
				}
			}
		}
	}

	return pf
}

func convertCreateClause(c *CreateClause) (*LegacyCreateClause, error) {
	lc := &LegacyCreateClause{
		Nodes:         []LegacyCreateNode{},
		Relationships: []LegacyCreateRelationship{},
	}

	if c.Pattern == nil {
		return lc, nil
	}

	for _, part := range c.Pattern.Parts {
		var lastNodeVar string

		for i, elem := range part.Elements {
			switch e := elem.(type) {
			case *NodePattern:
				props, err := convertMapToProps(e.Properties)
				if err != nil {
					return nil, err
				}
				lc.Nodes = append(lc.Nodes, LegacyCreateNode{
					Variable:   e.Variable,
					Labels:     e.Labels,
					Properties: props,
				})
				lastNodeVar = e.Variable

			case *RelationshipPattern:
				// Get the next node variable
				var nextNodeVar string
				if i+1 < len(part.Elements) {
					if node, ok := part.Elements[i+1].(*NodePattern); ok {
						nextNodeVar = node.Variable
					}
				}

				props, err := convertMapToProps(e.Properties)
				if err != nil {
					return nil, err
				}

				relType := ""
				if len(e.Types) > 0 {
					relType = e.Types[0]
				}

				lc.Relationships = append(lc.Relationships, LegacyCreateRelationship{
					Variable:   e.Variable,
					Type:       relType,
					FromVar:    lastNodeVar,
					ToVar:      nextNodeVar,
					Properties: props,
				})
			}
		}
	}

	return lc, nil
}

func convertWhereClause(w *WhereClause) (*LegacyWhereClause, error) {
	lw := &LegacyWhereClause{
		Conditions: []LegacyCondition{},
	}

	// Convert expression tree to flat conditions
	conditions, err := extractConditions(w.Condition)
	if err != nil {
		return nil, err
	}
	lw.Conditions = conditions

	return lw, nil
}

func extractConditions(expr Expression) ([]LegacyCondition, error) {
	var conditions []LegacyCondition

	switch e := expr.(type) {
	case *BinaryExpression:
		if e.Operator == "AND" || e.Operator == "and" {
			leftConds, err := extractConditions(e.Left)
			if err != nil {
				return nil, err
			}
			rightConds, err := extractConditions(e.Right)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, leftConds...)
			conditions = append(conditions, rightConds...)
		} else {
			// Direct comparison
			cond, err := extractSingleCondition(expr)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, cond)
		}

	case *ComparisonExpression:
		cond, err := extractSingleCondition(expr)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)

	default:
		return nil, fmt.Errorf("unsupported WHERE expression type")
	}

	return conditions, nil
}

func extractSingleCondition(expr Expression) (LegacyCondition, error) {
	var cond LegacyCondition

	switch e := expr.(type) {
	case *ComparisonExpression:
		// Left should be property access
		if prop, ok := e.Left.(*PropertyAccess); ok {
			if ident, ok := prop.Object.(*Identifier); ok {
				cond.Variable = ident.Name
			}
			cond.Property = prop.Property
		}
		cond.Operator = e.Operator
		cond.Value = extractValue(e.Right)

	case *BinaryExpression:
		// Left should be property access
		if prop, ok := e.Left.(*PropertyAccess); ok {
			if ident, ok := prop.Object.(*Identifier); ok {
				cond.Variable = ident.Name
			}
			cond.Property = prop.Property
		}
		cond.Operator = e.Operator
		cond.Value = extractValue(e.Right)
	}

	return cond, nil
}

func convertReturnClause(r *ReturnClause) (*LegacyReturnClause, error) {
	lr := &LegacyReturnClause{
		Items: []LegacyReturnItem{},
	}

	for _, item := range r.Items {
		li := LegacyReturnItem{}

		switch e := item.Expression.(type) {
		case *Identifier:
			li.Variable = e.Name
		case *PropertyAccess:
			if ident, ok := e.Object.(*Identifier); ok {
				li.Variable = ident.Name
			}
			li.Property = e.Property
		case *Star:
			li.Variable = "*"
		}

		if item.Alias != "" {
			li.Variable = item.Alias
		}

		lr.Items = append(lr.Items, li)
	}

	return lr, nil
}

func convertSetClause(s *SetClause) (*LegacySetClause, error) {
	ls := &LegacySetClause{
		Updates: []LegacyPropertyUpdate{},
	}

	for _, item := range s.Items {
		update := LegacyPropertyUpdate{}

		if item.Property != nil {
			if ident, ok := item.Property.Object.(*Identifier); ok {
				update.Variable = ident.Name
			}
			update.Property = item.Property.Property
		} else {
			update.Variable = item.Variable
		}

		update.Value = extractValue(item.Expression)
		ls.Updates = append(ls.Updates, update)
	}

	return ls, nil
}

func convertDeleteClause(d *DeleteClause) (*LegacyDeleteClause, error) {
	ld := &LegacyDeleteClause{
		Variables: []string{},
		Detach:    d.Detach,
	}

	for _, expr := range d.Expressions {
		if ident, ok := expr.(*Identifier); ok {
			ld.Variables = append(ld.Variables, ident.Name)
		}
	}

	return ld, nil
}

func convertTimeClause(t *TimeClause) (*LegacyTimeClause, error) {
	lt := &LegacyTimeClause{
		Mode: t.Mode,
	}

	if t.Timestamp != nil {
		if intLit, ok := t.Timestamp.(*IntegerLiteral); ok {
			lt.Timestamp = intLit.Value
		}
	}

	return lt, nil
}

func convertEmbedClause(e *EmbedClause) *LegacyEmbedClause {
	return &LegacyEmbedClause{
		Variable: e.Variable,
		Mode:     e.Mode,
		Text:     e.Text,
		Property: e.Property,
	}
}

func convertSimilarToClause(s *SimilarToClause) (*LegacySimilarToClause, error) {
	ls := &LegacySimilarToClause{}

	if str, ok := s.Query.(*StringLiteral); ok {
		ls.QueryText = str.Value
	}

	if s.Limit != nil {
		if intLit, ok := s.Limit.(*IntegerLiteral); ok {
			ls.Limit = int(intLit.Value)
		}
	}

	if s.Threshold != nil {
		if floatLit, ok := s.Threshold.(*FloatLiteral); ok {
			ls.Threshold = float32(floatLit.Value)
		}
	}

	return ls, nil
}

// Helper functions

func convertMapToProps(expr Expression) (map[string]interface{}, error) {
	props := make(map[string]interface{})

	if expr == nil {
		return props, nil
	}

	mapLit, ok := expr.(*MapLiteral)
	if !ok {
		return props, nil
	}

	for _, pair := range mapLit.Pairs {
		props[pair.Key] = extractValue(pair.Value)
	}

	return props, nil
}

func extractValue(expr Expression) interface{} {
	switch e := expr.(type) {
	case *StringLiteral:
		return e.Value
	case *IntegerLiteral:
		return int(e.Value)
	case *FloatLiteral:
		return e.Value
	case *BooleanLiteral:
		return e.Value
	case *NullLiteral:
		return nil
	case *ListLiteral:
		var list []interface{}
		for _, elem := range e.Elements {
			list = append(list, extractValue(elem))
		}
		return list
	case *MapLiteral:
		m := make(map[string]interface{})
		for _, pair := range e.Pairs {
			m[pair.Key] = extractValue(pair.Value)
		}
		return m
	case *Identifier:
		return e.Name
	default:
		return nil
	}
}

// Parse is a convenience function that parses and converts to legacy format
func Parse(input string) (*LegacyQuery, error) {
	parser := NewParser(input)
	query, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	return ConvertToLegacy(query)
}

// ParseInt64 helper for parsing int64 from string
func ParseInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
