package graph

import (
	"fmt"
	"sort"
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

		// Scalar function evaluation (DURATION, etc.)
		if item.FunctionName != "" {
			entity, ok := match[item.Variable]
			if !ok {
				row[colName] = nil
				continue
			}
			row[colName] = applyScalarFunction(item.FunctionName, entity)
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

// applyScalarFunction evaluates a scalar function against an entity or value.
func applyScalarFunction(fn string, entity interface{}) interface{} {
	switch fn {
	case "duration":
		var validFrom time.Time
		var validTo *time.Time
		switch e := entity.(type) {
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
	}
	return nil
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
