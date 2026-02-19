package graph

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
)

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
		colName := getColumnName(item)
		result.Columns = append(result.Columns, colName)
	}

	if hasAggregation {
		result.Rows = buildAggregatedRows(matches, returnClause)
	} else {
		for _, match := range matches {
			row := buildRowFromMatch(match, returnClause.Items)
			result.Rows = append(result.Rows, row)
		}
	}

	if returnClause.Distinct {
		result.Rows = applyDistinct(result.Rows)
	}

	if len(returnClause.OrderBy) > 0 {
		applyOrderBy(result.Rows, returnClause.OrderBy)
	}

	if returnClause.Skip > 0 && returnClause.Skip < len(result.Rows) {
		result.Rows = result.Rows[returnClause.Skip:]
	} else if returnClause.Skip >= len(result.Rows) {
		result.Rows = []map[string]interface{}{}
	}

	if returnClause.Limit > 0 && returnClause.Limit < len(result.Rows) {
		result.Rows = result.Rows[:returnClause.Limit]
	}

	return result
}

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

func buildRowFromMatch(match Match, items []ReturnItem) map[string]interface{} {
	row := map[string]interface{}{}

	for _, item := range items {
		colName := getColumnName(item)

		if item.FunctionName != "" {
			var rawVal interface{}
			entity, hasEntity := match[item.Variable]
			if hasEntity {
				if item.Property != "" {
					if node, ok := entity.(*Node); ok {
						rawVal = node.Properties[item.Property]
					} else if rel, ok := entity.(*Relationship); ok {
						rawVal = rel.Properties[item.Property]
					}
				} else {
					rawVal = entity
				}
			} else {
				rawVal = item.Variable
			}
			row[colName] = applyScalarFunction(item.FunctionName, rawVal)
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

func applyScalarFunction(name string, val interface{}) interface{} {
	asStr := func() string { return fmt.Sprint(val) }
	asFloat := func() (float64, bool) {
		switch v := val.(type) {
		case float64:
			return v, true
		case float32:
			return float64(v), true
		case int:
			return float64(v), true
		case int64:
			return float64(v), true
		case string:
			f, err := strconv.ParseFloat(v, 64)
			return f, err == nil
		}
		return 0, false
	}

	switch name {
	case "toupper":
		return strings.ToUpper(asStr())
	case "tolower":
		return strings.ToLower(asStr())
	case "trim":
		return strings.TrimSpace(asStr())
	case "ltrim":
		return strings.TrimLeft(asStr(), " \t")
	case "rtrim":
		return strings.TrimRight(asStr(), " \t")
	case "reverse":
		r := []rune(asStr())
		for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}
		return string(r)
	case "size":
		switch v := val.(type) {
		case string:
			return int64(len([]rune(v)))
		case []interface{}:
			return int64(len(v))
		}
		return int64(len([]rune(asStr())))
	case "left", "right", "replace":
		return asStr()
	case "tostring":
		return asStr()
	case "tointeger", "toint":
		if f, ok := asFloat(); ok {
			return int64(f)
		}
		if i, err := strconv.ParseInt(asStr(), 10, 64); err == nil {
			return i
		}
		return nil
	case "tofloat":
		if f, ok := asFloat(); ok {
			return f
		}
		return nil
	case "toboolean":
		s := strings.ToLower(strings.TrimSpace(asStr()))
		if s == "true" {
			return true
		}
		if s == "false" {
			return false
		}
		return nil
	case "abs":
		if f, ok := asFloat(); ok {
			return math.Abs(f)
		}
	case "ceil":
		if f, ok := asFloat(); ok {
			return math.Ceil(f)
		}
	case "floor":
		if f, ok := asFloat(); ok {
			return math.Floor(f)
		}
	case "round":
		if f, ok := asFloat(); ok {
			return math.Round(f)
		}
	case "sqrt":
		if f, ok := asFloat(); ok {
			return math.Sqrt(f)
		}
	case "sign":
		if f, ok := asFloat(); ok {
			if f > 0 {
				return float64(1)
			} else if f < 0 {
				return float64(-1)
			}
			return float64(0)
		}
	case "rand":
		return rand.Float64()
	case "log":
		if f, ok := asFloat(); ok {
			return math.Log(f)
		}
	case "log10":
		if f, ok := asFloat(); ok {
			return math.Log10(f)
		}
	case "exp":
		if f, ok := asFloat(); ok {
			return math.Exp(f)
		}
	case "e":
		return math.E
	case "pi":
		return math.Pi
	}

	return val
}

func buildAggregatedRows(matches []Match, returnClause *ReturnClause) []map[string]interface{} {
	var groupByItems []ReturnItem
	var aggItems []ReturnItem

	for _, item := range returnClause.Items {
		if item.Aggregation != "" {
			aggItems = append(aggItems, item)
		} else {
			groupByItems = append(groupByItems, item)
		}
	}

	if len(groupByItems) == 0 {
		row := map[string]interface{}{}
		for _, item := range aggItems {
			colName := getColumnName(item)
			row[colName] = computeAggregation(matches, item)
		}
		return []map[string]interface{}{row}
	}

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
		firstMatch := groupMatches[0]
		for _, item := range groupByItems {
			colName := getColumnName(item)
			entity, ok := firstMatch[item.Variable]
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
			colName := getColumnName(item)
			row[colName] = computeAggregation(groupMatches, item)
		}

		rows = append(rows, row)
	}

	return rows
}

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

func computeAggregation(matches []Match, item ReturnItem) interface{} {
	switch strings.ToUpper(item.Aggregation) {
	case "COUNT":
		if item.Variable == "*" {
			return len(matches)
		}
		count := 0
		for _, match := range matches {
			if _, ok := match[item.Variable]; ok {
				count++
			}
		}
		return count

	case "SUM":
		var sum float64
		for _, match := range matches {
			if val := getNumericValue(match, item); val != nil {
				sum += *val
			}
		}
		return sum

	case "AVG":
		var sum float64
		count := 0
		for _, match := range matches {
			if val := getNumericValue(match, item); val != nil {
				sum += *val
				count++
			}
		}
		if count == 0 {
			return nil
		}
		return sum / float64(count)

	case "MIN":
		var min *float64
		for _, match := range matches {
			if val := getNumericValue(match, item); val != nil {
				if min == nil || *val < *min {
					min = val
				}
			}
		}
		if min == nil {
			return nil
		}
		return *min

	case "MAX":
		var max *float64
		for _, match := range matches {
			if val := getNumericValue(match, item); val != nil {
				if max == nil || *val > *max {
					max = val
				}
			}
		}
		if max == nil {
			return nil
		}
		return *max

	case "COLLECT":
		var collected []interface{}
		for _, match := range matches {
			entity, ok := match[item.Variable]
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
