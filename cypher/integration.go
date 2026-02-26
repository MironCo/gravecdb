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
	SetClause       *GraphSetClause
	DeleteClause    *GraphDeleteClause
	WhereClause     *GraphWhereClause
	ReturnClause    *GraphReturnClause
	TimeClause      *GraphTimeClause
	EmbedClause     *GraphEmbedClause
	SimilarToClause *GraphSimilarToClause
	UnwindClause    *GraphUnwindClause
	MergeClause     *GraphCreateClause // reuses create structure; execution differs
	RemoveClause    *GraphRemoveClause
	Pipeline        *GraphPipeline     // set when WITH clause is present
	IsPathQuery     bool
	Optional        bool               // true when OPTIONAL MATCH (no-match → one empty row)
	UnionQueries    []*GraphQuery      // sub-queries for UNION
	UnionAll        bool               // true for UNION ALL (keep duplicates)
}

// GraphPipelineStage is one MATCH+WHERE step in a WITH-chained query.
// WithVars lists the variable names projected by the WITH clause after this stage
// (empty means this is the final stage before RETURN).
type GraphPipelineStage struct {
	MatchPattern *GraphMatchPattern
	WhereClause  *GraphWhereClause
	WithVars     []string // variables kept after WITH projection
}

// GraphPipeline represents a multi-stage query connected by WITH clauses.
type GraphPipeline struct {
	Stages      []GraphPipelineStage
	FinalReturn *GraphReturnClause
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
	VarLength bool
	MinHops   int // 0 = unspecified (defaults to 1)
	MaxHops   int // 0 = unspecified (defaults to 10)
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

type GraphWhereClause struct {
	Conditions         []GraphCondition
	SemanticConditions []GraphSemanticCondition
	BoolExpr           Expression // full parsed tree; used when Conditions can't represent it (OR, NOT, etc.)
}

type GraphCondition struct {
	Variable     string
	Property     string
	FunctionName string      // e.g. "toupper" — applied to the property value before comparing
	Operator     string
	Value        interface{}
}

type GraphSemanticCondition struct {
	Variable  string
	QueryText string
	Threshold float32
}

type GraphReturnClause struct {
	Items    []GraphReturnItem
	Distinct bool
	OrderBy  []GraphOrderItem
	Skip     int
	Limit    int
}

type GraphReturnItem struct {
	Variable     string
	Property     string
	Aggregation  string     // COUNT, SUM, AVG, MIN, MAX, COLLECT
	Distinct     bool       // true for COUNT(DISTINCT x)
	FunctionName string     // non-aggregation function: "duration", "toupper", etc.
	Alias        string
	Expr         Expression // non-nil for CASE WHEN and other complex expressions
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

type GraphUnwindClause struct {
	List     []interface{}
	ListExpr Expression // non-nil when List comes from a dynamic expression (e.g. range())
	Variable string
}

type GraphSimilarToClause struct {
	Variable    string
	QueryText   string
	Limit       int
	Threshold   float32
	ThroughTime bool
	DriftMode   bool
}

type GraphRemoveItem struct {
	Variable string
	Property string // set for REMOVE n.property
	Label    string // set for REMOVE n:Label
}

type GraphRemoveClause struct {
	Items []GraphRemoveItem
}

// ConvertToGraphQuery converts the AST to graph-compatible query
func ConvertToGraphQuery(ast *Query) (*GraphQuery, error) {
	// Handle UNION queries
	if ast.IsUnion {
		gq := &GraphQuery{
			QueryType: "UNION",
			UnionAll:  ast.UnionAll,
		}
		for _, sub := range ast.SubQueries {
			subQuery, err := ConvertToGraphQuery(sub)
			if err != nil {
				return nil, err
			}
			gq.UnionQueries = append(gq.UnionQueries, subQuery)
		}
		return gq, nil
	}

	// Route to pipeline executor when WITH is present OR multiple MATCH clauses exist
	matchCount := 0
	for _, clause := range ast.Clauses {
		switch clause.(type) {
		case *WithClause:
			return buildPipelineQuery(ast)
		case *MatchClause:
			matchCount++
		}
	}
	if matchCount > 1 {
		return buildPipelineQuery(ast)
	}

	gq := &GraphQuery{}

	for _, clause := range ast.Clauses {
		switch c := clause.(type) {
		case *MatchClause:
			gq.QueryType = "MATCH"
			gq.Optional = c.Optional
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

		case *TimeClause:
			tc := convertTimeToGraph(c)
			gq.TimeClause = tc

		case *EmbedClause:
			ec := convertEmbedToGraph(c)
			gq.EmbedClause = ec

		case *SimilarToClause:
			stc := convertSimilarToGraph(c)
			gq.SimilarToClause = stc

		case *UnwindClause:
			gq.QueryType = "UNWIND"
			gq.UnwindClause = convertUnwindToGraph(c)

		case *MergeClause:
			if gq.QueryType == "" {
				gq.QueryType = "MERGE"
			}
			mc, err := convertCreateToGraph(&CreateClause{Pattern: c.Pattern})
			if err != nil {
				return nil, err
			}
			gq.MergeClause = mc

		case *RemoveClause:
			rc := &GraphRemoveClause{}
			for _, item := range c.Items {
				rc.Items = append(rc.Items, GraphRemoveItem{
					Variable: item.Variable,
					Property: item.Property,
					Label:    item.Label,
				})
			}
			gq.RemoveClause = rc
		}
	}

	return gq, nil
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

				minHops, maxHops := 0, 0
				if e.MinHops != nil {
					minHops = *e.MinHops
				}
				if e.MaxHops != nil {
					maxHops = *e.MaxHops
				}

				// Calculate indices within this part
				nodeIdx := (i + 1) / 2 // Node index relative to this pattern part
				gp.Relationships = append(gp.Relationships, GraphRelPattern{
					Variable:  e.Variable,
					Types:     e.Types,
					FromIndex: baseNodeIndex + nodeIdx - 1,
					ToIndex:   baseNodeIndex + nodeIdx,
					Direction: direction,
					VarLength: e.VarLength,
					MinHops:   minHops,
					MaxHops:   maxHops,
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
	gw := &GraphWhereClause{}

	conditions, semanticConditions, err := extractGraphConditions(w.Condition)
	if err != nil {
		return nil, err
	}
	gw.Conditions = conditions
	gw.SemanticConditions = semanticConditions
	gw.BoolExpr = w.Condition // full tree for OR / NOT / function evaluation

	return gw, nil
}

func extractGraphConditions(expr Expression) ([]GraphCondition, []GraphSemanticCondition, error) {
	var conditions []GraphCondition
	var semanticConditions []GraphSemanticCondition

	switch e := expr.(type) {
	case *BinaryExpression:
		if e.Operator == "AND" || e.Operator == "and" {
			lc, ls, err := extractGraphConditions(e.Left)
			if err != nil {
				return nil, nil, err
			}
			rc, rs, err := extractGraphConditions(e.Right)
			if err != nil {
				return nil, nil, err
			}
			conditions = append(conditions, lc...)
			conditions = append(conditions, rc...)
			semanticConditions = append(semanticConditions, ls...)
			semanticConditions = append(semanticConditions, rs...)
		}

	case *ComparisonExpression:
		cond := GraphCondition{Operator: e.Operator}
		switch left := e.Left.(type) {
		case *PropertyAccess:
			if ident, ok := left.Object.(*Identifier); ok {
				cond.Variable = ident.Name
			}
			cond.Property = left.Property
		case *FunctionCall:
			// e.g. toUpper(p.name) = 'ALICE'
			cond.FunctionName = strings.ToLower(left.Name)
			if len(left.Arguments) > 0 {
				switch arg := left.Arguments[0].(type) {
				case *Identifier:
					cond.Variable = arg.Name
				case *PropertyAccess:
					if ident, ok := arg.Object.(*Identifier); ok {
						cond.Variable = ident.Name
					}
					cond.Property = arg.Property
				}
			}
		}
		cond.Value = extractExprValue(e.Right)
		conditions = append(conditions, cond)

	case *SimilarToExpression:
		semanticConditions = append(semanticConditions, GraphSemanticCondition{
			Variable:  e.Variable,
			QueryText: e.Query,
			Threshold: e.Threshold,
		})
	}

	return conditions, semanticConditions, nil
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

		// Check if it's a function call (aggregation or scalar)
		if fc, ok := item.Expression.(*FunctionCall); ok {
			upperName := strings.ToUpper(fc.Name)
			isAggregation := map[string]bool{
				"COUNT": true, "SUM": true, "AVG": true,
				"MIN": true, "MAX": true, "COLLECT": true,
			}[upperName]

			if isAggregation {
				gi.Aggregation = upperName
				gi.Distinct = fc.Distinct
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
				} else if upperName == "COUNT" {
					gi.Variable = "*"
				}
			} else {
				// Non-aggregation scalar function: DURATION, toUpper, abs, coalesce, etc.
				// Store the full FunctionCall as Expr so evalExpr handles multi-arg functions.
				// Also populate FunctionName/Variable/Property for getColumnName formatting.
				gi.FunctionName = strings.ToLower(fc.Name)
				gi.Expr = fc
				if len(fc.Arguments) > 0 {
					switch arg := fc.Arguments[0].(type) {
					case *Identifier:
						gi.Variable = arg.Name
					case *PropertyAccess:
						if ident, ok := arg.Object.(*Identifier); ok {
							gi.Variable = ident.Name
						}
						gi.Property = arg.Property
					}
				}
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
			case *CaseExpression, *IsNullExpression, *InExpression,
				*BinaryExpression, *UnaryExpression, *FunctionCall:
				gi.Expr = e
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

func convertUnwindToGraph(u *UnwindClause) *GraphUnwindClause {
	if lit, ok := u.Expression.(*ListLiteral); ok {
		var list []interface{}
		for _, elem := range lit.Elements {
			list = append(list, extractExprValue(elem))
		}
		return &GraphUnwindClause{List: list, Variable: u.Variable}
	}
	// Dynamic expression (e.g. range(1, 5)) — store for evaluation at execution time
	return &GraphUnwindClause{ListExpr: u.Expression, Variable: u.Variable}
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
	case *UnaryExpression:
		if e.Operator == "-" {
			inner := extractExprValue(e.Operand)
			switch v := inner.(type) {
			case int:
				return -v
			case float64:
				return -v
			}
		}
		return nil
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

// buildPipelineQuery converts a WITH-chained query into a GraphQuery with Pipeline set.
// It walks clauses in order, grouping each MATCH+WHERE into a stage terminated by WITH.
// The final RETURN is stored in Pipeline.FinalReturn and also in GraphQuery.ReturnClause
// (so buildResult can be called directly with query.ReturnClause as usual).
func buildPipelineQuery(ast *Query) (*GraphQuery, error) {
	gq := &GraphQuery{QueryType: "PIPELINE"}
	pipeline := &GraphPipeline{}

	var currentStage GraphPipelineStage
	stageHasMatch := false

	for _, clause := range ast.Clauses {
		switch c := clause.(type) {
		case *MatchClause:
			if stageHasMatch {
				// Already have a match — this shouldn't happen before a WITH,
				// but flush the current stage defensively.
				pipeline.Stages = append(pipeline.Stages, currentStage)
				currentStage = GraphPipelineStage{}
			}
			pattern, err := convertPatternToGraph(c.Pattern)
			if err != nil {
				return nil, err
			}
			currentStage.MatchPattern = pattern
			stageHasMatch = true

		case *WhereClause:
			wc, err := convertWhereToGraph(c)
			if err != nil {
				return nil, err
			}
			currentStage.WhereClause = wc

		case *WithClause:
			// Extract projected variable names from WITH items
			for _, item := range c.Items {
				if ident, ok := item.Expression.(*Identifier); ok {
					currentStage.WithVars = append(currentStage.WithVars, ident.Name)
				}
			}
			pipeline.Stages = append(pipeline.Stages, currentStage)
			currentStage = GraphPipelineStage{}
			stageHasMatch = false

		case *ReturnClause:
			pipeline.FinalReturn = convertReturnToGraph(c)
			gq.ReturnClause = pipeline.FinalReturn
		}
	}

	// Flush any trailing stage (MATCH after the last WITH, before RETURN)
	if stageHasMatch {
		pipeline.Stages = append(pipeline.Stages, currentStage)
	}

	gq.Pipeline = pipeline
	return gq, nil
}
