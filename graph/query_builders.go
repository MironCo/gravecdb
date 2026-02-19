package graph

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// buildResult constructs the query result based on RETURN clause.
// Handles aggregation, DISTINCT, ORDER BY, SKIP, and LIMIT.
func buildResult(matches []Match, returnClause *ReturnClause) *QueryResult {
	result := &QueryResult{
		Columns: []string{},
		Rows:    []map[string]interface{}{},
	}

	if returnClause == nil {
		return result
	}

	hasAggregation := false
	for _, item := range returnClause.Items {
		if item.Aggregation != "" {
			hasAggregation = true
			break
		}
	}

	for _, item := range returnClause.Items {
		result.Columns = append(result.Columns, getColumnName(item))
	}

	if hasAggregation {
		result.Rows = buildAggregatedRows(matches, returnClause)
	} else {
		for _, match := range matches {
			result.Rows = append(result.Rows, buildRowFromMatch(match, returnClause.Items))
		}
	}

	if returnClause.Distinct {
		result.Rows = applyDistinct(result.Rows)
	}
	if len(returnClause.OrderBy) > 0 {
		applyOrderBy(result.Rows, returnClause.OrderBy)
	}
	if returnClause.Skip > 0 {
		if returnClause.Skip >= len(result.Rows) {
			result.Rows = []map[string]interface{}{}
		} else {
			result.Rows = result.Rows[returnClause.Skip:]
		}
	}
	if returnClause.Limit > 0 && returnClause.Limit < len(result.Rows) {
		result.Rows = result.Rows[:returnClause.Limit]
	}

	return result
}

// getColumnName returns the display name for a return item.
func getColumnName(item ReturnItem) string {
	if item.Alias != "" {
		return item.Alias
	}
	if item.Aggregation != "" {
		if item.Property != "" {
			return fmt.Sprintf("%s(%s.%s)", item.Aggregation, item.Variable, item.Property)
		}
		return fmt.Sprintf("%s(%s)", item.Aggregation, item.Variable)
	}
	if item.FunctionName != "" {
		if item.Property != "" {
			return fmt.Sprintf("%s(%s.%s)", item.FunctionName, item.Variable, item.Property)
		}
		return fmt.Sprintf("%s(%s)", item.FunctionName, item.Variable)
	}
	if item.Property != "" {
		return item.Variable + "." + item.Property
	}
	return item.Variable
}

// buildRowFromMatch builds a single row from a match.
func buildRowFromMatch(match Match, items []ReturnItem) map[string]interface{} {
	row := map[string]interface{}{}
	for _, item := range items {
		colName := getColumnName(item)

		// Scalar function evaluation (toUpper, abs, duration, etc.)
		if item.FunctionName != "" {
			entity, ok := match[item.Variable]
			if !ok {
				row[colName] = nil
				continue
			}
			// Extract property value first if a property is specified (e.g. toUpper(p.name))
			var rawValue interface{}
			if item.Property != "" {
				if node, ok := entity.(*Node); ok {
					rawValue = node.Properties[item.Property]
				} else if rel, ok := entity.(*Relationship); ok {
					rawValue = rel.Properties[item.Property]
				}
			} else {
				rawValue = entity
			}
			row[colName] = applyScalarFunction(item.FunctionName, rawValue)
			continue
		}

		entity, ok := match[item.Variable]
		if !ok {
			row[colName] = nil
			continue
		}
		if item.Property != "" {
			if node, ok := entity.(*Node); ok {
				row[colName] = node.Properties[item.Property]
			} else if rel, ok := entity.(*Relationship); ok {
				row[colName] = rel.Properties[item.Property]
			}
		} else {
			row[colName] = entity
		}
	}
	return row
}

// applyScalarFunction evaluates a scalar function against a value.
// value may be a plain scalar (string, int, float64) or a *Node/*Relationship for DURATION.
func applyScalarFunction(fn string, value interface{}) interface{} {
	switch fn {

	// ── temporal ──────────────────────────────────────────────────────────────
	case "duration":
		var validFrom time.Time
		var validTo *time.Time
		switch e := value.(type) {
		case *Node:
			validFrom = e.ValidFrom
			validTo = e.ValidTo
		case *Relationship:
			validFrom = e.ValidFrom
			validTo = e.ValidTo
		default:
			return nil
		}
		end := time.Now()
		if validTo != nil {
			end = *validTo
		}
		return end.Sub(validFrom).Hours() / 24 // days

	// ── string functions ──────────────────────────────────────────────────────
	case "toupper":
		s, ok := scalarToString(value)
		if !ok {
			return nil
		}
		return strings.ToUpper(s)

	case "tolower":
		s, ok := scalarToString(value)
		if !ok {
			return nil
		}
		return strings.ToLower(s)

	case "trim":
		s, ok := scalarToString(value)
		if !ok {
			return nil
		}
		return strings.TrimSpace(s)

	case "ltrim":
		s, ok := scalarToString(value)
		if !ok {
			return nil
		}
		return strings.TrimLeft(s, " \t")

	case "rtrim":
		s, ok := scalarToString(value)
		if !ok {
			return nil
		}
		return strings.TrimRight(s, " \t")

	case "reverse":
		s, ok := scalarToString(value)
		if !ok {
			return nil
		}
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes)

	case "tostring":
		if value == nil {
			return nil
		}
		return fmt.Sprint(value)

	case "size":
		switch v := value.(type) {
		case string:
			return len(v)
		case []interface{}:
			return len(v)
		default:
			if s, ok := scalarToString(v); ok {
				return len(s)
			}
			return nil
		}

	case "tointeger":
		return scalarToInteger(value)

	case "tofloat":
		return scalarToFloat(value)

	case "toboolean":
		switch v := value.(type) {
		case bool:
			return v
		case string:
			l := strings.ToLower(v)
			if l == "true" {
				return true
			}
			if l == "false" {
				return false
			}
			return nil
		default:
			return nil
		}

	// ── math functions ────────────────────────────────────────────────────────
	case "abs":
		f, ok := scalarToNumeric(value)
		if !ok {
			return nil
		}
		return math.Abs(f)

	case "ceil":
		f, ok := scalarToNumeric(value)
		if !ok {
			return nil
		}
		return math.Ceil(f)

	case "floor":
		f, ok := scalarToNumeric(value)
		if !ok {
			return nil
		}
		return math.Floor(f)

	case "round":
		f, ok := scalarToNumeric(value)
		if !ok {
			return nil
		}
		return math.Round(f)

	case "sqrt":
		f, ok := scalarToNumeric(value)
		if !ok {
			return nil
		}
		return math.Sqrt(f)

	case "sign":
		f, ok := scalarToNumeric(value)
		if !ok {
			return nil
		}
		if f < 0 {
			return -1.0
		}
		if f > 0 {
			return 1.0
		}
		return 0.0

	case "log":
		f, ok := scalarToNumeric(value)
		if !ok {
			return nil
		}
		return math.Log(f)

	case "log10":
		f, ok := scalarToNumeric(value)
		if !ok {
			return nil
		}
		return math.Log10(f)

	case "exp":
		f, ok := scalarToNumeric(value)
		if !ok {
			return nil
		}
		return math.Exp(f)
	}
	return nil
}

// scalarToString coerces a value to string. Returns (s, true) for string types,
// falls back to fmt.Sprint for non-nil non-node values.
func scalarToString(v interface{}) (string, bool) {
	if v == nil {
		return "", false
	}
	switch s := v.(type) {
	case string:
		return s, true
	case *Node, *Relationship:
		return "", false // don't stringify graph objects
	default:
		return fmt.Sprint(s), true
	}
}

// scalarToNumeric coerces a value to float64. Returns (f, true) on success.
func scalarToNumeric(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// scalarToInteger coerces a value to int.
func scalarToInteger(v interface{}) interface{} {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case float32:
		return int(n)
	case string:
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return int(i)
		}
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return int(f)
		}
		return nil
	default:
		return nil
	}
}

// scalarToFloat coerces a value to float64.
func scalarToFloat(v interface{}) interface{} {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f
		}
		return nil
	default:
		return nil
	}
}

// buildAggregatedRows handles aggregation functions (COUNT, SUM, AVG, MIN, MAX, COLLECT).
func buildAggregatedRows(matches []Match, returnClause *ReturnClause) []map[string]interface{} {
	var groupByItems, aggItems []ReturnItem
	for _, item := range returnClause.Items {
		if item.Aggregation != "" {
			aggItems = append(aggItems, item)
		} else {
			groupByItems = append(groupByItems, item)
		}
	}

	// No grouping columns → single result row
	if len(groupByItems) == 0 {
		row := map[string]interface{}{}
		for _, item := range aggItems {
			row[getColumnName(item)] = computeAggregation(matches, item)
		}
		return []map[string]interface{}{row}
	}

	// Group matches by grouping columns
	groups := make(map[string][]Match)
	var groupKeys []string
	for _, match := range matches {
		key := buildGroupKey(match, groupByItems)
		if _, exists := groups[key]; !exists {
			groupKeys = append(groupKeys, key)
		}
		groups[key] = append(groups[key], match)
	}

	var rows []map[string]interface{}
	for _, key := range groupKeys {
		groupMatches := groups[key]
		if len(groupMatches) == 0 {
			continue
		}
		row := map[string]interface{}{}
		first := groupMatches[0]
		for _, item := range groupByItems {
			colName := getColumnName(item)
			entity, ok := first[item.Variable]
			if !ok {
				row[colName] = nil
				continue
			}
			if item.Property != "" {
				if node, ok := entity.(*Node); ok {
					row[colName] = node.Properties[item.Property]
				} else if rel, ok := entity.(*Relationship); ok {
					row[colName] = rel.Properties[item.Property]
				}
			} else {
				row[colName] = entity
			}
		}
		for _, item := range aggItems {
			row[getColumnName(item)] = computeAggregation(groupMatches, item)
		}
		rows = append(rows, row)
	}
	return rows
}

// buildGroupKey creates a string key for grouping matches.
func buildGroupKey(match Match, groupByItems []ReturnItem) string {
	var parts []string
	for _, item := range groupByItems {
		entity, ok := match[item.Variable]
		if !ok {
			parts = append(parts, "<nil>")
			continue
		}
		if item.Property != "" {
			var val interface{}
			if node, ok := entity.(*Node); ok {
				val = node.Properties[item.Property]
			} else if rel, ok := entity.(*Relationship); ok {
				val = rel.Properties[item.Property]
			}
			parts = append(parts, fmt.Sprintf("%v", val))
		} else {
			if node, ok := entity.(*Node); ok {
				parts = append(parts, node.ID)
			} else if rel, ok := entity.(*Relationship); ok {
				parts = append(parts, rel.ID)
			}
		}
	}
	return strings.Join(parts, "|")
}

// computeAggregation computes one aggregation function over a set of matches.
func computeAggregation(matches []Match, item ReturnItem) interface{} {
	switch strings.ToUpper(item.Aggregation) {
	case "COUNT":
		if item.Variable == "*" {
			return len(matches)
		}
		count := 0
		for _, m := range matches {
			if _, ok := m[item.Variable]; ok {
				count++
			}
		}
		return count

	case "SUM":
		var sum float64
		for _, m := range matches {
			if v := getNumericValue(m, item); v != nil {
				sum += *v
			}
		}
		return sum

	case "AVG":
		var sum float64
		count := 0
		for _, m := range matches {
			if v := getNumericValue(m, item); v != nil {
				sum += *v
				count++
			}
		}
		if count == 0 {
			return nil
		}
		return sum / float64(count)

	case "MIN":
		var min *float64
		for _, m := range matches {
			if v := getNumericValue(m, item); v != nil && (min == nil || *v < *min) {
				min = v
			}
		}
		if min == nil {
			return nil
		}
		return *min

	case "MAX":
		var max *float64
		for _, m := range matches {
			if v := getNumericValue(m, item); v != nil && (max == nil || *v > *max) {
				max = v
			}
		}
		if max == nil {
			return nil
		}
		return *max

	case "COLLECT":
		var collected []interface{}
		for _, m := range matches {
			entity, ok := m[item.Variable]
			if !ok {
				continue
			}
			if item.Property != "" {
				if node, ok := entity.(*Node); ok {
					collected = append(collected, node.Properties[item.Property])
				} else if rel, ok := entity.(*Relationship); ok {
					collected = append(collected, rel.Properties[item.Property])
				}
			} else {
				collected = append(collected, entity)
			}
		}
		return collected
	}
	return nil
}

// getNumericValue extracts a float64 from a match for numeric aggregations.
func getNumericValue(match Match, item ReturnItem) *float64 {
	entity, ok := match[item.Variable]
	if !ok {
		return nil
	}
	var val interface{}
	if item.Property != "" {
		if node, ok := entity.(*Node); ok {
			val = node.Properties[item.Property]
		} else if rel, ok := entity.(*Relationship); ok {
			val = rel.Properties[item.Property]
		}
	}
	if val == nil {
		return nil
	}
	var f float64
	switch v := val.(type) {
	case int:
		f = float64(v)
	case int64:
		f = float64(v)
	case float64:
		f = v
	case float32:
		f = float64(v)
	default:
		return nil
	}
	return &f
}

// applyDistinct removes duplicate rows.
func applyDistinct(rows []map[string]interface{}) []map[string]interface{} {
	seen := make(map[string]bool)
	var result []map[string]interface{}
	for _, row := range rows {
		key := rowToKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

// rowToKey creates a canonical string key from a row for deduplication.
func rowToKey(row map[string]interface{}) string {
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		v := row[k]
		if node, ok := v.(*Node); ok {
			parts = append(parts, fmt.Sprintf("%s=node:%s", k, node.ID))
		} else if rel, ok := v.(*Relationship); ok {
			parts = append(parts, fmt.Sprintf("%s=rel:%s", k, rel.ID))
		} else {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
	}
	return strings.Join(parts, "|")
}

// applyOrderBy sorts rows in-place by the specified order items.
func applyOrderBy(rows []map[string]interface{}, orderBy []OrderItem) {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, order := range orderBy {
			colName := order.Variable
			if order.Property != "" {
				colName = order.Variable + "." + order.Property
			}
			vi := getOrderValue(rows[i], colName, order)
			vj := getOrderValue(rows[j], colName, order)
			cmp := compareValues(vi, vj)
			if cmp != 0 {
				if order.Descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}

// getOrderValue extracts the value to sort by for a given row and order item.
func getOrderValue(row map[string]interface{}, colName string, order OrderItem) interface{} {
	if v, ok := row[colName]; ok {
		return v
	}
	if order.Property != "" {
		if entity, ok := row[order.Variable]; ok {
			if node, ok := entity.(*Node); ok {
				return node.Properties[order.Property]
			} else if rel, ok := entity.(*Relationship); ok {
				return rel.Properties[order.Property]
			}
		}
	}
	return nil
}

// copyMatch creates a shallow copy of a Match map.
func copyMatch(m Match) Match {
	cp := make(Match, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
