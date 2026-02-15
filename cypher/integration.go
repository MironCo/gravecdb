package cypher

import (
	"fmt"
	"strings"
)

// ParseToGraph converts the new parser output to graph.Query compatible structures
// This is the main entry point for integrating with the existing query executor
func ParseToGraph(input string) (*GraphQuery, error) {
	parser := NewParser(input)
	ast, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	return ConvertToGraphQuery(ast)
}

// GraphQuery is compatible with graph.Query
type GraphQuery struct {
	QueryType       string
	MatchPattern    *GraphMatchPattern
	CreateClause    *GraphCreateClause
	MergeClause     *GraphMergeClause
	SetClause       *GraphSetClause
	DeleteClause    *GraphDeleteClause
	RemoveClause    *GraphRemoveClause
	UnwindClause    *GraphUnwindClause
	WhereClause     *GraphWhereClause
	ReturnClause    *GraphReturnClause
	TimeClause      *GraphTimeClause
	EmbedClause     *GraphEmbedClause
	SimilarToClause *GraphSimilarToClause
	IsPathQuery     bool
}

type GraphMatchPattern struct {
	Nodes         []GraphNodePattern
	Relationships []GraphRelPattern
	PathFunction  *GraphPathFunction
}

type GraphPathFunction struct {
	Function     string
	Variable     string
	StartPattern GraphNodePattern
	EndPattern   GraphNodePattern
	RelTypes     []string
	MaxDepth     int
}

type GraphNodePattern struct {
	Variable   string
	Labels     []string
	Properties map[string]interface{}
}

type GraphRelPattern struct {
	Variable  string
	Types     []string
	FromIndex int
	ToIndex   int
	Direction string
}

type GraphCreateClause struct {
	Nodes         []GraphCreateNode
	Relationships []GraphCreateRelationship
}

type GraphCreateNode struct {
	Variable   string
	Labels     []string
	Properties map[string]interface{}
}

type GraphCreateRelationship struct {
	Variable   string
	Type       string
	FromVar    string
	ToVar      string
	Properties map[string]interface{}
}

type GraphSetClause struct {
	Updates []GraphPropertyUpdate
}

type GraphPropertyUpdate struct {
	Variable string
	Property string
	Value    interface{}
}

type GraphDeleteClause struct {
	Variables []string
	Detach    bool
}

type GraphRemoveClause struct {
	Items []GraphRemoveItem
}

type GraphRemoveItem struct {
	Variable string
	Property string // For property removal (n.prop)
	Label    string // For label removal (n:Label)
}

type GraphUnwindClause struct {
	Expression interface{} // The list expression to unwind
	Variable   string      // AS variable
}

type GraphWhereClause struct {
	Conditions []GraphCondition
}

type GraphCondition struct {
	Variable string
	Property string
	Operator string
	Value    interface{}
}

type GraphReturnClause struct {
	Items    []GraphReturnItem
	Distinct bool
	OrderBy  []GraphOrderItem
	Skip     int
	Limit    int
}

type GraphReturnItem struct {
	Variable    string
	Property    string
	Aggregation string // COUNT, SUM, AVG, MIN, MAX, COLLECT
	Alias       string
}

type GraphOrderItem struct {
	Variable   string
	Property   string
	Descending bool
}

type GraphTimeClause struct {
	Mode      string
	Timestamp int64
}

type GraphEmbedClause struct {
	Variable string
	Mode     string
	Text     string
	Property string
}

type GraphSimilarToClause struct {
	Variable    string
	QueryText   string
	Limit       int
	Threshold   float32
	ThroughTime bool
	DriftMode   bool
}

type GraphMergeClause struct {
	Pattern      *GraphMatchPattern
	OnCreateSets []GraphPropertyUpdate
	OnMatchSets  []GraphPropertyUpdate
}

// ConvertToGraphQuery converts the AST to graph-compatible query
func ConvertToGraphQuery(ast *Query) (*GraphQuery, error) {
	gq := &GraphQuery{}

	for _, clause := range ast.Clauses {
		switch c := clause.(type) {
		case *MatchClause:
			gq.QueryType = "MATCH"
			pattern, err := convertPatternToGraph(c.Pattern)
			if err != nil {
				return nil, err
			}
			gq.MatchPattern = pattern

			// Check for path functions
			if c.Pattern != nil {
				for _, part := range c.Pattern.Parts {
					for _, elem := range part.Elements {
						if sp, ok := elem.(*ShortestPathPattern); ok {
							gq.IsPathQuery = true
							gq.MatchPattern.PathFunction = convertPathFunctionToGraph(sp)
						}
					}
				}
			}

		case *CreateClause:
			cc, err := convertCreateToGraph(c)
			if err != nil {
				return nil, err
			}
			if gq.QueryType == "" {
				gq.QueryType = "CREATE"
			}
			gq.CreateClause = cc

		case *WhereClause:
			wc, err := convertWhereToGraph(c)
			if err != nil {
				return nil, err
			}
			gq.WhereClause = wc

		case *ReturnClause:
			rc := convertReturnToGraph(c)
			gq.ReturnClause = rc

		case *SetClause:
			sc := convertSetToGraph(c)
			gq.SetClause = sc

		case *DeleteClause:
			dc := convertDeleteToGraph(c)
			gq.DeleteClause = dc

		case *RemoveClause:
			rc := convertRemoveToGraph(c)
			gq.RemoveClause = rc

		case *UnwindClause:
			uc := convertUnwindToGraph(c)
			if gq.QueryType == "" {
				gq.QueryType = "UNWIND"
			}
			gq.UnwindClause = uc

		case *TimeClause:
			tc := convertTimeToGraph(c)
			gq.TimeClause = tc

		case *EmbedClause:
			ec := convertEmbedToGraph(c)
			gq.EmbedClause = ec

		case *SimilarToClause:
			stc := convertSimilarToGraph(c)
			gq.SimilarToClause = stc

		case *MergeClause:
			mc, err := convertMergeToGraph(c)
			if err != nil {
				return nil, err
			}
			if gq.QueryType == "" {
				gq.QueryType = "MERGE"
			}
			gq.MergeClause = mc
		}
	}

	return gq, nil
}

func convertMergeToGraph(m *MergeClause) (*GraphMergeClause, error) {
	gm := &GraphMergeClause{
		OnCreateSets: []GraphPropertyUpdate{},
		OnMatchSets:  []GraphPropertyUpdate{},
	}

	// Convert pattern
	if m.Pattern != nil {
		pattern, err := convertPatternToGraph(m.Pattern)
		if err != nil {
			return nil, err
		}
		gm.Pattern = pattern
	}

	// Convert ON CREATE SET expressions
	for _, expr := range m.OnCreate {
		if setItem, ok := expr.(*SetItem); ok {
			update := GraphPropertyUpdate{}
			if setItem.Property != nil {
				if ident, ok := setItem.Property.Object.(*Identifier); ok {
					update.Variable = ident.Name
				}
				update.Property = setItem.Property.Property
			}
			update.Value = extractExprValue(setItem.Expression)
			gm.OnCreateSets = append(gm.OnCreateSets, update)
		}
	}

	// Convert ON MATCH SET expressions
	for _, expr := range m.OnMatch {
		if setItem, ok := expr.(*SetItem); ok {
			update := GraphPropertyUpdate{}
			if setItem.Property != nil {
				if ident, ok := setItem.Property.Object.(*Identifier); ok {
					update.Variable = ident.Name
				}
				update.Property = setItem.Property.Property
			}
			update.Value = extractExprValue(setItem.Expression)
			gm.OnMatchSets = append(gm.OnMatchSets, update)
		}
	}

	return gm, nil
}

func convertPatternToGraph(p *Pattern) (*GraphMatchPattern, error) {
	if p == nil {
		return nil, nil
	}

	gp := &GraphMatchPattern{
		Nodes:         []GraphNodePattern{},
		Relationships: []GraphRelPattern{},
	}

	for _, part := range p.Parts {
		baseNodeIndex := len(gp.Nodes)

		for i, elem := range part.Elements {
			switch e := elem.(type) {
			case *NodePattern:
				props, err := extractMapProps(e.Properties)
				if err != nil {
					return nil, err
				}
				gp.Nodes = append(gp.Nodes, GraphNodePattern{
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

				// Calculate indices within this part
				nodeIdx := (i + 1) / 2 // Node index relative to this pattern part
				gp.Relationships = append(gp.Relationships, GraphRelPattern{
					Variable:  e.Variable,
					Types:     e.Types,
					FromIndex: baseNodeIndex + nodeIdx - 1,
					ToIndex:   baseNodeIndex + nodeIdx,
					Direction: direction,
				})
			}
		}
	}

	return gp, nil
}

func convertPathFunctionToGraph(sp *ShortestPathPattern) *GraphPathFunction {
	pf := &GraphPathFunction{
		Function: sp.Function,
		Variable: sp.Variable,
	}

	if sp.Pattern != nil && len(sp.Pattern.Elements) >= 1 {
		// Get start node
		if node, ok := sp.Pattern.Elements[0].(*NodePattern); ok {
			props, _ := extractMapProps(node.Properties)
			pf.StartPattern = GraphNodePattern{
				Variable:   node.Variable,
				Labels:     node.Labels,
				Properties: props,
			}
		}

		// Get end node
		lastIdx := len(sp.Pattern.Elements) - 1
		if node, ok := sp.Pattern.Elements[lastIdx].(*NodePattern); ok {
			props, _ := extractMapProps(node.Properties)
			pf.EndPattern = GraphNodePattern{
				Variable:   node.Variable,
				Labels:     node.Labels,
				Properties: props,
			}
		}

		// Get relationship info
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

func convertCreateToGraph(c *CreateClause) (*GraphCreateClause, error) {
	gc := &GraphCreateClause{
		Nodes:         []GraphCreateNode{},
		Relationships: []GraphCreateRelationship{},
	}

	if c.Pattern == nil {
		return gc, nil
	}

	for _, part := range c.Pattern.Parts {
		var lastNodeVar string

		for i, elem := range part.Elements {
			switch e := elem.(type) {
			case *NodePattern:
				props, err := extractMapProps(e.Properties)
				if err != nil {
					return nil, err
				}
				// Only create a new node if it has labels or properties
				// Otherwise it's just a reference to an existing node (from MATCH)
				if len(e.Labels) > 0 || len(props) > 0 {
					gc.Nodes = append(gc.Nodes, GraphCreateNode{
						Variable:   e.Variable,
						Labels:     e.Labels,
						Properties: props,
					})
				}
				lastNodeVar = e.Variable

			case *RelationshipPattern:
				var nextNodeVar string
				if i+1 < len(part.Elements) {
					if node, ok := part.Elements[i+1].(*NodePattern); ok {
						nextNodeVar = node.Variable
					}
				}

				props, err := extractMapProps(e.Properties)
				if err != nil {
					return nil, err
				}

				relType := ""
				if len(e.Types) > 0 {
					relType = e.Types[0]
				}

				gc.Relationships = append(gc.Relationships, GraphCreateRelationship{
					Variable:   e.Variable,
					Type:       relType,
					FromVar:    lastNodeVar,
					ToVar:      nextNodeVar,
					Properties: props,
				})
			}
		}
	}

	return gc, nil
}

func convertWhereToGraph(w *WhereClause) (*GraphWhereClause, error) {
	gw := &GraphWhereClause{
		Conditions: []GraphCondition{},
	}

	conditions, err := extractGraphConditions(w.Condition)
	if err != nil {
		return nil, err
	}
	gw.Conditions = conditions

	return gw, nil
}

func extractGraphConditions(expr Expression) ([]GraphCondition, error) {
	var conditions []GraphCondition

	switch e := expr.(type) {
	case *BinaryExpression:
		if e.Operator == "AND" || e.Operator == "and" {
			left, err := extractGraphConditions(e.Left)
			if err != nil {
				return nil, err
			}
			right, err := extractGraphConditions(e.Right)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, left...)
			conditions = append(conditions, right...)
		}

	case *ComparisonExpression:
		cond := GraphCondition{Operator: e.Operator}
		if prop, ok := e.Left.(*PropertyAccess); ok {
			if ident, ok := prop.Object.(*Identifier); ok {
				cond.Variable = ident.Name
			}
			cond.Property = prop.Property
		}
		cond.Value = extractExprValue(e.Right)
		conditions = append(conditions, cond)
	}

	return conditions, nil
}

func convertReturnToGraph(r *ReturnClause) *GraphReturnClause {
	gr := &GraphReturnClause{
		Items:    []GraphReturnItem{},
		Distinct: r.Distinct,
	}

	// Convert SKIP
	if r.Skip != nil {
		if intLit, ok := r.Skip.(*IntegerLiteral); ok {
			gr.Skip = int(intLit.Value)
		}
	}

	// Convert LIMIT
	if r.Limit != nil {
		if intLit, ok := r.Limit.(*IntegerLiteral); ok {
			gr.Limit = int(intLit.Value)
		}
	}

	// Convert ORDER BY
	for _, orderItem := range r.OrderBy {
		goi := GraphOrderItem{
			Descending: orderItem.Descending,
		}
		switch e := orderItem.Expression.(type) {
		case *Identifier:
			goi.Variable = e.Name
		case *PropertyAccess:
			if ident, ok := e.Object.(*Identifier); ok {
				goi.Variable = ident.Name
			}
			goi.Property = e.Property
		}
		gr.OrderBy = append(gr.OrderBy, goi)
	}

	// Convert return items
	for _, item := range r.Items {
		gi := GraphReturnItem{}

		// Check if it's a function call (aggregation)
		if fc, ok := item.Expression.(*FunctionCall); ok {
			gi.Aggregation = strings.ToUpper(fc.Name)
			// Get the argument (e.g., COUNT(p) -> p)
			if len(fc.Arguments) > 0 {
				switch arg := fc.Arguments[0].(type) {
				case *Identifier:
					gi.Variable = arg.Name
				case *PropertyAccess:
					if ident, ok := arg.Object.(*Identifier); ok {
						gi.Variable = ident.Name
					}
					gi.Property = arg.Property
				case *Star:
					gi.Variable = "*"
				}
			} else if gi.Aggregation == "COUNT" {
				// COUNT() with no args is same as COUNT(*)
				gi.Variable = "*"
			}
		} else {
			switch e := item.Expression.(type) {
			case *Identifier:
				gi.Variable = e.Name
			case *PropertyAccess:
				if ident, ok := e.Object.(*Identifier); ok {
					gi.Variable = ident.Name
				}
				gi.Property = e.Property
			case *Star:
				gi.Variable = "*"
			}
		}

		if item.Alias != "" {
			gi.Alias = item.Alias
		}

		gr.Items = append(gr.Items, gi)
	}

	return gr
}

func convertSetToGraph(s *SetClause) *GraphSetClause {
	gs := &GraphSetClause{
		Updates: []GraphPropertyUpdate{},
	}

	for _, item := range s.Items {
		update := GraphPropertyUpdate{}

		if item.Property != nil {
			if ident, ok := item.Property.Object.(*Identifier); ok {
				update.Variable = ident.Name
			}
			update.Property = item.Property.Property
		} else {
			update.Variable = item.Variable
		}

		update.Value = extractExprValue(item.Expression)
		gs.Updates = append(gs.Updates, update)
	}

	return gs
}

func convertDeleteToGraph(d *DeleteClause) *GraphDeleteClause {
	gd := &GraphDeleteClause{
		Variables: []string{},
		Detach:    d.Detach,
	}

	for _, expr := range d.Expressions {
		if ident, ok := expr.(*Identifier); ok {
			gd.Variables = append(gd.Variables, ident.Name)
		}
	}

	return gd
}

func convertRemoveToGraph(r *RemoveClause) *GraphRemoveClause {
	gr := &GraphRemoveClause{
		Items: []GraphRemoveItem{},
	}

	for _, item := range r.Items {
		ri := GraphRemoveItem{}

		switch e := item.(type) {
		case *PropertyAccess:
			// REMOVE n.property
			if ident, ok := e.Object.(*Identifier); ok {
				ri.Variable = ident.Name
			}
			ri.Property = e.Property
		case *Identifier:
			// Could be label removal syntax - but typically it's n:Label
			ri.Variable = e.Name
		}

		gr.Items = append(gr.Items, ri)
	}

	return gr
}

func convertUnwindToGraph(u *UnwindClause) *GraphUnwindClause {
	return &GraphUnwindClause{
		Expression: extractExprValue(u.Expression),
		Variable:   u.Variable,
	}
}

func convertTimeToGraph(t *TimeClause) *GraphTimeClause {
	gt := &GraphTimeClause{
		Mode: t.Mode,
	}

	if t.Timestamp != nil {
		if intLit, ok := t.Timestamp.(*IntegerLiteral); ok {
			gt.Timestamp = intLit.Value
		}
	}

	return gt
}

func convertEmbedToGraph(e *EmbedClause) *GraphEmbedClause {
	return &GraphEmbedClause{
		Variable: e.Variable,
		Mode:     e.Mode,
		Text:     e.Text,
		Property: e.Property,
	}
}

func convertSimilarToGraph(s *SimilarToClause) *GraphSimilarToClause {
	gs := &GraphSimilarToClause{}

	if str, ok := s.Query.(*StringLiteral); ok {
		gs.QueryText = str.Value
	}

	if s.Limit != nil {
		if intLit, ok := s.Limit.(*IntegerLiteral); ok {
			gs.Limit = int(intLit.Value)
		}
	}

	if s.Threshold != nil {
		if floatLit, ok := s.Threshold.(*FloatLiteral); ok {
			gs.Threshold = float32(floatLit.Value)
		}
	}

	gs.ThroughTime = s.ThroughTime
	gs.DriftMode = s.DriftMode

	return gs
}

func extractMapProps(expr Expression) (map[string]interface{}, error) {
	props := make(map[string]interface{})

	if expr == nil {
		return props, nil
	}

	mapLit, ok := expr.(*MapLiteral)
	if !ok {
		return props, nil
	}

	for _, pair := range mapLit.Pairs {
		props[pair.Key] = extractExprValue(pair.Value)
	}

	return props, nil
}

func extractExprValue(expr Expression) interface{} {
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
			list = append(list, extractExprValue(elem))
		}
		return list
	case *MapLiteral:
		m := make(map[string]interface{})
		for _, pair := range e.Pairs {
			m[pair.Key] = extractExprValue(pair.Value)
		}
		return m
	case *Identifier:
		return e.Name
	default:
		return nil
	}
}

// Validate validates a query string without executing it
func Validate(input string) error {
	parser := NewParser(input)
	_, err := parser.Parse()
	if err != nil {
		return fmt.Errorf("invalid query: %w", err)
	}
	return nil
}
