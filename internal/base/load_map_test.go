/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package base

import (
	"testing"
)

func TestParseLoadMap(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    map[string]int64
		wantErr bool
	}{
		{
			name:  "empty string",
			input: "",
			want:  map[string]int64{},
		},
		{
			name:  "single entry",
			input: "active_connections=100",
			want:  map[string]int64{"active_connections": 100},
		},
		{
			name:  "multiple entries",
			input: "active_connections=100,idle_in_transaction=10",
			want:  map[string]int64{"active_connections": 100, "idle_in_transaction": 10},
		},
		{
			name:  "with spaces",
			input: " active_connections = 100 , waiting = 50 ",
			want:  map[string]int64{"active_connections": 100, "waiting": 50},
		},
		{
			name:    "invalid format - no equals",
			input:   "active_connections",
			wantErr: true,
		},
		{
			name:    "invalid value - not a number",
			input:   "active_connections=abc",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseLoadMap(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseLoadMap(%q) expected error, got nil", tt.input)
				}
				return
			}

			if err != nil {
				t.Errorf("ParseLoadMap(%q) unexpected error: %v", tt.input, err)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("ParseLoadMap(%q) = %v, want %v", tt.input, got, tt.want)
				return
			}

			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("ParseLoadMap(%q)[%q] = %d, want %d", tt.input, k, got[k], v)
				}
			}
		})
	}
}

func TestLoadMapString(t *testing.T) {
	m := LoadMap{
		"active_connections": 100,
	}

	str := m.String()
	if str != "active_connections=100" {
		t.Errorf("String() = %q, want 'active_connections=100'", str)
	}

	// Empty map
	empty := NewLoadMap()
	if empty.String() != "" {
		t.Errorf("Empty map String() = %q, want ''", empty.String())
	}
}

func TestLoadMapDuplicate(t *testing.T) {
	original := LoadMap{
		"active_connections": 100,
		"waiting":            50,
	}

	dup := original.Duplicate()

	// Should have same values
	if dup["active_connections"] != 100 {
		t.Errorf("Duplicate active_connections = %d, want 100", dup["active_connections"])
	}

	// Should be independent
	dup["active_connections"] = 200
	if original["active_connections"] != 100 {
		t.Error("Modifying duplicate affected original")
	}
}
