/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromFile(t *testing.T) {
	// Create a temporary config file
	content := `
database:
  host: testhost
  port: 5433
  user: testuser
  password: testpass
  database: testdb
  sslmode: require

table:
  schema: myschema
  name: mytable
  alter: "ADD COLUMN foo INT"

processing:
  chunk_size: 2000
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := LoadFromFile(configPath)
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	// Verify database settings
	if cfg.Database.Host != "testhost" {
		t.Errorf("Database.Host = %q, want 'testhost'", cfg.Database.Host)
	}
	if cfg.Database.Port != 5433 {
		t.Errorf("Database.Port = %d, want 5433", cfg.Database.Port)
	}
	if cfg.Database.User != "testuser" {
		t.Errorf("Database.User = %q, want 'testuser'", cfg.Database.User)
	}
	if cfg.Database.SSLMode != "require" {
		t.Errorf("Database.SSLMode = %q, want 'require'", cfg.Database.SSLMode)
	}

	// Verify table settings
	if cfg.Table.Schema != "myschema" {
		t.Errorf("Table.Schema = %q, want 'myschema'", cfg.Table.Schema)
	}
	if cfg.Table.Name != "mytable" {
		t.Errorf("Table.Name = %q, want 'mytable'", cfg.Table.Name)
	}

	// Verify processing settings
	if cfg.Processing.ChunkSize != 2000 {
		t.Errorf("Processing.ChunkSize = %d, want 2000", cfg.Processing.ChunkSize)
	}
}

func TestLoadFromFileWithDefaults(t *testing.T) {
	// Minimal config - should apply defaults
	content := `
database:
  user: testuser
  database: testdb

table:
  name: mytable
  alter: "ADD COLUMN foo INT"
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := LoadFromFile(configPath)
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	// Verify defaults were applied
	if cfg.Database.Host != "localhost" {
		t.Errorf("Database.Host = %q, want 'localhost' (default)", cfg.Database.Host)
	}
	if cfg.Database.Port != 5432 {
		t.Errorf("Database.Port = %d, want 5432 (default)", cfg.Database.Port)
	}
	if cfg.Database.SSLMode != "prefer" {
		t.Errorf("Database.SSLMode = %q, want 'prefer' (default)", cfg.Database.SSLMode)
	}
	if cfg.Table.Schema != "public" {
		t.Errorf("Table.Schema = %q, want 'public' (default)", cfg.Table.Schema)
	}
	if cfg.Processing.ChunkSize != 1000 {
		t.Errorf("Processing.ChunkSize = %d, want 1000 (default)", cfg.Processing.ChunkSize)
	}
	if cfg.Throttle.MaxLagMs != 1500 {
		t.Errorf("Throttle.MaxLagMs = %d, want 1500 (default)", cfg.Throttle.MaxLagMs)
	}
}

func TestEnvironmentVariableExpansion(t *testing.T) {
	// Set environment variables
	os.Setenv("TEST_PG_HOST", "envhost")
	os.Setenv("TEST_PG_USER", "envuser")
	os.Setenv("TEST_PG_PASSWORD", "envpass")
	defer func() {
		os.Unsetenv("TEST_PG_HOST")
		os.Unsetenv("TEST_PG_USER")
		os.Unsetenv("TEST_PG_PASSWORD")
	}()

	content := `
database:
  host: "${TEST_PG_HOST}"
  user: $TEST_PG_USER
  password: "${TEST_PG_PASSWORD}"
  database: testdb

table:
  name: mytable
  alter: "ADD COLUMN foo INT"
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := LoadFromFile(configPath)
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	if cfg.Database.Host != "envhost" {
		t.Errorf("Database.Host = %q, want 'envhost'", cfg.Database.Host)
	}
	if cfg.Database.User != "envuser" {
		t.Errorf("Database.User = %q, want 'envuser'", cfg.Database.User)
	}
	if cfg.Database.Password != "envpass" {
		t.Errorf("Database.Password = %q, want 'envpass'", cfg.Database.Password)
	}
}

func TestValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name: "missing database.user",
			content: `
database:
  database: testdb
table:
  name: mytable
  alter: "ADD COLUMN foo INT"
`,
			wantErr: "database.user is required",
		},
		{
			name: "missing database.database",
			content: `
database:
  user: testuser
table:
  name: mytable
  alter: "ADD COLUMN foo INT"
`,
			wantErr: "database.database is required",
		},
		{
			name: "missing table.name",
			content: `
database:
  user: testuser
  database: testdb
table:
  alter: "ADD COLUMN foo INT"
`,
			wantErr: "table.name is required",
		},
		{
			name: "missing table.alter",
			content: `
database:
  user: testuser
  database: testdb
table:
  name: mytable
`,
			wantErr: "table.alter is required",
		},
		{
			name: "invalid chunk_size (too small)",
			content: `
database:
  user: testuser
  database: testdb
table:
  name: mytable
  alter: "ADD COLUMN foo INT"
processing:
  chunk_size: 50
`,
			wantErr: "chunk_size must be between 100 and 100000",
		},
		{
			name: "replica without host",
			content: `
database:
  user: testuser
  database: testdb
table:
  name: mytable
  alter: "ADD COLUMN foo INT"
replica:
  port: 5432
`,
			wantErr: "replica.host is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "config.yaml")
			if err := os.WriteFile(configPath, []byte(tt.content), 0644); err != nil {
				t.Fatalf("Failed to write test config: %v", err)
			}

			_, err := LoadFromFile(configPath)
			if err == nil {
				t.Errorf("Expected error containing %q, got nil", tt.wantErr)
				return
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Errorf("Error = %q, want error containing %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestReplicaDefaults(t *testing.T) {
	content := `
database:
  host: primary.example.com
  user: testuser
  password: testpass
  database: testdb
  sslmode: require

replica:
  host: replica.example.com

table:
  name: mytable
  alter: "ADD COLUMN foo INT"
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := LoadFromFile(configPath)
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	// Replica should inherit from database section
	if cfg.Replica.User != "testuser" {
		t.Errorf("Replica.User = %q, want 'testuser' (inherited)", cfg.Replica.User)
	}
	if cfg.Replica.Password != "testpass" {
		t.Errorf("Replica.Password = %q, want 'testpass' (inherited)", cfg.Replica.Password)
	}
	if cfg.Replica.SSLMode != "require" {
		t.Errorf("Replica.SSLMode = %q, want 'require' (inherited)", cfg.Replica.SSLMode)
	}
	if cfg.Replica.Port != 5432 {
		t.Errorf("Replica.Port = %d, want 5432 (default)", cfg.Replica.Port)
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
