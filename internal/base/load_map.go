/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package base

import (
	"fmt"
	"strconv"
	"strings"
)

// LoadMap represents a mapping of status variable names to threshold values
// Used for max-load and critical-load settings
type LoadMap map[string]int64

// NewLoadMap creates a new empty LoadMap
func NewLoadMap() LoadMap {
	return make(LoadMap)
}

// ParseLoadMap parses a comma-separated list of name=value pairs
// Example: "active_connections=100,idle_in_transaction=10"
func ParseLoadMap(s string) (LoadMap, error) {
	result := NewLoadMap()
	if s == "" {
		return result, nil
	}

	pairs := strings.Split(s, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid load map entry: %s", pair)
		}

		name := strings.TrimSpace(parts[0])
		valueStr := strings.TrimSpace(parts[1])

		value, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid value for %s: %s", name, valueStr)
		}

		result[name] = value
	}

	return result, nil
}

// String returns a string representation of the LoadMap
func (m LoadMap) String() string {
	if len(m) == 0 {
		return ""
	}

	parts := make([]string, 0, len(m))
	for name, value := range m {
		parts = append(parts, fmt.Sprintf("%s=%d", name, value))
	}
	return strings.Join(parts, ",")
}

// Duplicate creates a copy of the LoadMap
func (m LoadMap) Duplicate() LoadMap {
	result := NewLoadMap()
	for k, v := range m {
		result[k] = v
	}
	return result
}
