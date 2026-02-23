/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package sql

import (
	"testing"
)

func TestParseAlterStatement(t *testing.T) {
	tests := []struct {
		name        string
		statement   string
		wantErr     bool
		wantRename  bool
		wantAdd     bool
		wantDrop    bool
		wantClause  string
	}{
		{
			name:       "simple add column",
			statement:  "ADD COLUMN status VARCHAR(50)",
			wantErr:    false,
			wantAdd:    true,
			wantClause: "ADD COLUMN status VARCHAR(50)",
		},
		{
			name:       "add column with default",
			statement:  "ADD COLUMN active BOOLEAN DEFAULT true",
			wantErr:    false,
			wantAdd:    true,
			wantClause: "ADD COLUMN active BOOLEAN DEFAULT true",
		},
		{
			name:       "drop column",
			statement:  "DROP COLUMN old_field",
			wantErr:    false,
			wantDrop:   true,
			wantClause: "DROP COLUMN old_field",
		},
		{
			name:       "full alter table statement",
			statement:  "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			wantErr:    false,
			wantAdd:    true,
			wantClause: "ADD COLUMN email VARCHAR(255)",
		},
		{
			name:       "alter with schema",
			statement:  "ALTER TABLE public.users ADD COLUMN age INTEGER",
			wantErr:    false,
			wantAdd:    true,
			wantClause: "ADD COLUMN age INTEGER",
		},
		{
			name:       "rename table - should fail",
			statement:  "ALTER TABLE users RENAME TO users_old",
			wantErr:    true,
			wantRename: true,
		},
		{
			name:       "multiple operations",
			statement:  "ADD COLUMN col1 TEXT, ADD COLUMN col2 INTEGER",
			wantErr:    false,
			wantAdd:    true,
			wantClause: "ADD COLUMN col1 TEXT, ADD COLUMN col2 INTEGER",
		},
		{
			name:       "alter column type",
			statement:  "ALTER COLUMN name TYPE TEXT",
			wantErr:    false,
			wantClause: "ALTER COLUMN name TYPE TEXT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewAlterTableParser()
			err := p.ParseAlterStatement(tt.statement)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if p.IsRename() != tt.wantRename {
				t.Errorf("IsRename() = %v, want %v", p.IsRename(), tt.wantRename)
			}

			if p.HasAddColumn() != tt.wantAdd {
				t.Errorf("HasAddColumn() = %v, want %v", p.HasAddColumn(), tt.wantAdd)
			}

			if p.HasDropColumn() != tt.wantDrop {
				t.Errorf("HasDropColumn() = %v, want %v", p.HasDropColumn(), tt.wantDrop)
			}

			if tt.wantClause != "" && p.GetAlterClause() != tt.wantClause {
				t.Errorf("GetAlterClause() = %q, want %q", p.GetAlterClause(), tt.wantClause)
			}
		})
	}
}

func TestBuildAlterGhostStatement(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		schema    string
		ghost     string
		want      string
	}{
		{
			name:      "simple add column",
			statement: "ADD COLUMN status VARCHAR(50)",
			schema:    "public",
			ghost:     "_users_gho",
			want:      `ALTER TABLE "public"."_users_gho" ADD COLUMN status VARCHAR(50)`,
		},
		{
			name:      "full alter table statement",
			statement: "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			schema:    "myschema",
			ghost:     "_users_gho",
			want:      `ALTER TABLE "myschema"."_users_gho" ADD COLUMN email VARCHAR(255)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewAlterTableParser()
			err := p.ParseAlterStatement(tt.statement)
			if err != nil {
				t.Fatalf("ParseAlterStatement failed: %v", err)
			}

			got := p.BuildAlterGhostStatement(tt.schema, tt.ghost)
			if got != tt.want {
				t.Errorf("BuildAlterGhostStatement() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"users", `"users"`},
		{"public", `"public"`},
		{"my_table", `"my_table"`},
		{`"already_quoted"`, `"already_quoted"`},
		{`has"quote`, `"has""quote"`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := QuoteIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestUnquoteIdentifier(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`"users"`, "users"},
		{`"public"`, "public"},
		{"unquoted", "unquoted"},
		{`"has""quote"`, `has"quote`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := UnquoteIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("UnquoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
