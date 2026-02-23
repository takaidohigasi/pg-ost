/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package pg

import (
	"testing"
)

func TestPostgreSQLVersion(t *testing.T) {
	tests := []struct {
		name                      string
		version                   PostgreSQLVersion
		wantLogicalReplication    bool
		wantStreamingTransactions bool
		wantString                string
	}{
		{
			name:                      "PostgreSQL 16",
			version:                   PostgreSQLVersion{Major: 16, Minor: 2, Patch: 0},
			wantLogicalReplication:    true,
			wantStreamingTransactions: true,
			wantString:                "16.2.0",
		},
		{
			name:                      "PostgreSQL 15",
			version:                   PostgreSQLVersion{Major: 15, Minor: 4, Patch: 0},
			wantLogicalReplication:    true,
			wantStreamingTransactions: true,
			wantString:                "15.4.0",
		},
		{
			name:                      "PostgreSQL 14",
			version:                   PostgreSQLVersion{Major: 14, Minor: 0, Patch: 0},
			wantLogicalReplication:    true,
			wantStreamingTransactions: true,
			wantString:                "14.0.0",
		},
		{
			name:                      "PostgreSQL 13 (not supported)",
			version:                   PostgreSQLVersion{Major: 13, Minor: 10, Patch: 0},
			wantLogicalReplication:    true,
			wantStreamingTransactions: false,
			wantString:                "13.10.0",
		},
		{
			name:                      "PostgreSQL 10",
			version:                   PostgreSQLVersion{Major: 10, Minor: 0, Patch: 0},
			wantLogicalReplication:    true,
			wantStreamingTransactions: false,
			wantString:                "10.0.0",
		},
		{
			name:                      "PostgreSQL 9.6 (no logical replication)",
			version:                   PostgreSQLVersion{Major: 9, Minor: 6, Patch: 0},
			wantLogicalReplication:    false,
			wantStreamingTransactions: false,
			wantString:                "9.6.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.version.SupportsLogicalReplication(); got != tt.wantLogicalReplication {
				t.Errorf("SupportsLogicalReplication() = %v, want %v", got, tt.wantLogicalReplication)
			}
			if got := tt.version.SupportsStreamingLargeTransactions(); got != tt.wantStreamingTransactions {
				t.Errorf("SupportsStreamingLargeTransactions() = %v, want %v", got, tt.wantStreamingTransactions)
			}
			if got := tt.version.String(); got != tt.wantString {
				t.Errorf("String() = %q, want %q", got, tt.wantString)
			}
		})
	}
}

func TestConnectionConfig(t *testing.T) {
	config := NewConnectionConfig()

	if config.Host != "localhost" {
		t.Errorf("Host = %q, want 'localhost'", config.Host)
	}
	if config.Port != 5432 {
		t.Errorf("Port = %d, want 5432", config.Port)
	}
	if config.SSLMode != "prefer" {
		t.Errorf("SSLMode = %q, want 'prefer'", config.SSLMode)
	}
}

func TestConnectionConfigDSN(t *testing.T) {
	config := &ConnectionConfig{
		Host:     "db.example.com",
		Port:     5433,
		User:     "testuser",
		Password: "testpass",
		Database: "testdb",
		SSLMode:  "require",
	}

	dsn := config.GetDSN()
	expected := "host=db.example.com port=5433 user=testuser password=testpass dbname=testdb sslmode=require"
	if dsn != expected {
		t.Errorf("GetDSN() = %q, want %q", dsn, expected)
	}

	replDSN := config.GetReplicationDSN()
	expectedRepl := "host=db.example.com port=5433 user=testuser password=testpass dbname=testdb sslmode=require replication=database"
	if replDSN != expectedRepl {
		t.Errorf("GetReplicationDSN() = %q, want %q", replDSN, expectedRepl)
	}
}

func TestConnectionConfigDuplicate(t *testing.T) {
	original := &ConnectionConfig{
		Host:     "db.example.com",
		Port:     5433,
		User:     "testuser",
		Password: "testpass",
		Database: "testdb",
		SSLMode:  "require",
	}

	dup := original.Duplicate()

	// Should have same values
	if dup.Host != original.Host {
		t.Errorf("Duplicate Host = %q, want %q", dup.Host, original.Host)
	}
	if dup.Port != original.Port {
		t.Errorf("Duplicate Port = %d, want %d", dup.Port, original.Port)
	}

	// Should be independent
	dup.Host = "other.example.com"
	if original.Host != "db.example.com" {
		t.Error("Modifying duplicate affected original")
	}
}

func TestConnectionConfigString(t *testing.T) {
	config := &ConnectionConfig{
		Host:     "db.example.com",
		Port:     5433,
		User:     "testuser",
		Password: "secret",
		Database: "testdb",
		SSLMode:  "require",
	}

	str := config.String()
	expected := "testuser@db.example.com:5433/testdb"
	if str != expected {
		t.Errorf("String() = %q, want %q", str, expected)
	}

	// Should not contain password
	if contains(str, "secret") {
		t.Error("String() should not contain password")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
