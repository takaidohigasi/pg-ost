/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package pg

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// ConnectionConfig holds PostgreSQL connection configuration
type ConnectionConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

// NewConnectionConfig creates a new ConnectionConfig with defaults
func NewConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
		Host:    "localhost",
		Port:    5432,
		SSLMode: "prefer",
	}
}

// GetDSN returns the connection string in DSN format
func (c *ConnectionConfig) GetDSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Database, c.SSLMode,
	)
}

// GetReplicationDSN returns the connection string for replication connections
func (c *ConnectionConfig) GetReplicationDSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s replication=database",
		c.Host, c.Port, c.User, c.Password, c.Database, c.SSLMode,
	)
}

// Connect creates a standard database connection
func (c *ConnectionConfig) Connect(ctx context.Context) (*sql.DB, error) {
	config, err := pgx.ParseConfig(c.GetDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	db := stdlib.OpenDB(*config)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// ConnectReplication creates a replication connection using pgconn
func (c *ConnectionConfig) ConnectReplication(ctx context.Context) (*pgconn.PgConn, error) {
	conn, err := pgconn.Connect(ctx, c.GetReplicationDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to create replication connection: %w", err)
	}
	return conn, nil
}

// Duplicate creates a copy of the ConnectionConfig
func (c *ConnectionConfig) Duplicate() *ConnectionConfig {
	return &ConnectionConfig{
		Host:     c.Host,
		Port:     c.Port,
		User:     c.User,
		Password: c.Password,
		Database: c.Database,
		SSLMode:  c.SSLMode,
	}
}

// String returns a safe string representation (without password)
func (c *ConnectionConfig) String() string {
	return fmt.Sprintf("%s@%s:%d/%s", c.User, c.Host, c.Port, c.Database)
}

// PostgreSQLVersion represents a PostgreSQL version
type PostgreSQLVersion struct {
	Major int
	Minor int
	Patch int
}

// GetVersion queries the PostgreSQL server version
func GetVersion(ctx context.Context, db *sql.DB) (*PostgreSQLVersion, error) {
	var versionStr string
	err := db.QueryRowContext(ctx, "SHOW server_version").Scan(&versionStr)
	if err != nil {
		return nil, fmt.Errorf("failed to get server version: %w", err)
	}

	var major, minor, patch int
	fmt.Sscanf(versionStr, "%d.%d.%d", &major, &minor, &patch)

	return &PostgreSQLVersion{
		Major: major,
		Minor: minor,
		Patch: patch,
	}, nil
}

// SupportsLogicalReplication returns true if the version supports logical replication
func (v *PostgreSQLVersion) SupportsLogicalReplication() bool {
	return v.Major >= 10
}

// SupportsStreamingLargeTransactions returns true if streaming large transactions is supported
func (v *PostgreSQLVersion) SupportsStreamingLargeTransactions() bool {
	return v.Major >= 14
}

// String returns the version as a string
func (v *PostgreSQLVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}
