/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package logic

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"

	"github.com/takaidohigasi/pg-ost/internal/base"
	"github.com/takaidohigasi/pg-ost/internal/pg"
	sqlpkg "github.com/takaidohigasi/pg-ost/internal/sql"
)

// Inspector inspects the original table structure and validates conditions
type Inspector struct {
	connectionConfig *pg.ConnectionConfig
	db               *sql.DB
	migrationContext *base.MigrationContext
	version          *pg.PostgreSQLVersion
}

// NewInspector creates a new Inspector
func NewInspector(ctx *base.MigrationContext) *Inspector {
	return &Inspector{
		migrationContext: ctx,
		connectionConfig: &pg.ConnectionConfig{
			Host:     ctx.Host,
			Port:     ctx.Port,
			User:     ctx.User,
			Password: ctx.Password,
			Database: ctx.DatabaseName,
			SSLMode:  ctx.SSLMode,
		},
	}
}

// InitDBConnections initializes database connections
func (i *Inspector) InitDBConnections() error {
	ctx := context.Background()
	db, err := i.connectionConfig.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	i.db = db

	// Get PostgreSQL version
	version, err := pg.GetVersion(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to get version: %w", err)
	}
	i.version = version

	if !version.SupportsLogicalReplication() {
		return fmt.Errorf("PostgreSQL %s does not support logical replication (requires 10+)", version)
	}

	i.migrationContext.Log.Info("Connected to PostgreSQL %s", version)
	return nil
}

// Close closes the database connection
func (i *Inspector) Close() {
	if i.db != nil {
		i.db.Close()
	}
}

// ValidateOriginalTable validates the original table exists and is suitable
func (i *Inspector) ValidateOriginalTable() error {
	ctx := context.Background()

	// Check table exists
	exists, err := i.tableExists(ctx, i.migrationContext.SchemaName, i.migrationContext.OriginalTableName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("table %s.%s does not exist",
			i.migrationContext.SchemaName, i.migrationContext.OriginalTableName)
	}

	// Get unique key
	uniqueKey, err := i.getBestUniqueKey(ctx)
	if err != nil {
		return err
	}
	if uniqueKey == nil {
		return fmt.Errorf("table %s.%s has no PRIMARY KEY or UNIQUE index",
			i.migrationContext.SchemaName, i.migrationContext.OriginalTableName)
	}

	i.migrationContext.UniqueKeyColumns = uniqueKey.Columns
	i.migrationContext.Log.Info("Using unique key: %s", uniqueKey.String())

	// Get table columns
	columns, err := i.getTableColumns(ctx)
	if err != nil {
		return err
	}
	i.migrationContext.OriginalTableColumns = columns.Names()

	return nil
}

// tableExists checks if a table exists
func (i *Inspector) tableExists(ctx context.Context, schemaName, tableName string) (bool, error) {
	var count int
	err := i.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_name = $2
	`, schemaName, tableName).Scan(&count)

	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	return count > 0, nil
}

// getBestUniqueKey finds the best unique key for the table (prefer PK)
func (i *Inspector) getBestUniqueKey(ctx context.Context) (*sqlpkg.UniqueKey, error) {
	// First, try to get the primary key
	pk, err := i.getPrimaryKey(ctx)
	if err != nil {
		return nil, err
	}
	if pk != nil {
		return pk, nil
	}

	// Fall back to unique indexes
	uniqueKeys, err := i.getUniqueIndexes(ctx)
	if err != nil {
		return nil, err
	}

	if len(uniqueKeys) == 0 {
		return nil, nil
	}

	// Prefer non-nullable unique keys
	for _, uk := range uniqueKeys {
		if !uk.IsNullable {
			return uk, nil
		}
	}

	// Return first unique key
	return uniqueKeys[0], nil
}

// getPrimaryKey gets the primary key for the table
func (i *Inspector) getPrimaryKey(ctx context.Context) (*sqlpkg.UniqueKey, error) {
	rows, err := i.db.QueryContext(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		  AND c.relname = $2
		  AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)
	`, i.migrationContext.SchemaName, i.migrationContext.OriginalTableName)

	if err != nil {
		return nil, fmt.Errorf("failed to get primary key: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		columns = append(columns, colName)
	}

	if len(columns) == 0 {
		return nil, nil
	}

	return sqlpkg.NewUniqueKey("PRIMARY", columns, true), nil
}

// getUniqueIndexes gets unique indexes for the table
func (i *Inspector) getUniqueIndexes(ctx context.Context) ([]*sqlpkg.UniqueKey, error) {
	rows, err := i.db.QueryContext(ctx, `
		SELECT i.relname as index_name,
		       array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns,
		       bool_or(a.attnotnull = false) as has_nullable
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class c ON c.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(ix.indkey)
		WHERE n.nspname = $1
		  AND c.relname = $2
		  AND ix.indisunique
		  AND NOT ix.indisprimary
		GROUP BY i.relname
		ORDER BY i.relname
	`, i.migrationContext.SchemaName, i.migrationContext.OriginalTableName)

	if err != nil {
		return nil, fmt.Errorf("failed to get unique indexes: %w", err)
	}
	defer rows.Close()

	var keys []*sqlpkg.UniqueKey
	for rows.Next() {
		var indexName string
		var columns []string
		var hasNullable bool

		if err := rows.Scan(&indexName, &columns, &hasNullable); err != nil {
			return nil, fmt.Errorf("failed to scan index: %w", err)
		}

		uk := sqlpkg.NewUniqueKey(indexName, columns, false)
		uk.IsNullable = hasNullable
		keys = append(keys, uk)
	}

	return keys, nil
}

// getTableColumns gets the columns of the table
func (i *Inspector) getTableColumns(ctx context.Context) (*sqlpkg.ColumnList, error) {
	rows, err := i.db.QueryContext(ctx, `
		SELECT column_name, data_type, is_nullable, column_default, ordinal_position
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, i.migrationContext.SchemaName, i.migrationContext.OriginalTableName)

	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	defer rows.Close()

	columns := sqlpkg.NewColumnList()
	for rows.Next() {
		var col sqlpkg.Column
		var isNullable string
		if err := rows.Scan(&col.Name, &col.Type, &isNullable, &col.DefaultValue, &col.OrdinalPos); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		col.IsNullable = (isNullable == "YES")
		columns.Add(col)
	}

	return columns, nil
}

// EnsureReplicaIdentityFull sets REPLICA IDENTITY FULL on the source table
func (i *Inspector) EnsureReplicaIdentityFull() error {
	ctx := context.Background()

	// Get current replica identity
	var replicaIdentity string
	err := i.db.QueryRowContext(ctx, `
		SELECT CASE relreplident
			WHEN 'd' THEN 'DEFAULT'
			WHEN 'n' THEN 'NOTHING'
			WHEN 'f' THEN 'FULL'
			WHEN 'i' THEN 'INDEX'
		END
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
	`, i.migrationContext.SchemaName, i.migrationContext.OriginalTableName).Scan(&replicaIdentity)

	if err != nil {
		return fmt.Errorf("failed to get replica identity: %w", err)
	}

	i.migrationContext.OriginalReplicaIdentity = replicaIdentity

	if replicaIdentity == "FULL" {
		i.migrationContext.Log.Debug("Table already has REPLICA IDENTITY FULL")
		return nil
	}

	// Set REPLICA IDENTITY FULL
	query := fmt.Sprintf(
		"ALTER TABLE %s.%s REPLICA IDENTITY FULL",
		sqlpkg.QuoteIdentifier(i.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(i.migrationContext.OriginalTableName),
	)

	_, err = i.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to set replica identity: %w", err)
	}

	i.migrationContext.Log.Info("Set REPLICA IDENTITY FULL on %s.%s",
		i.migrationContext.SchemaName, i.migrationContext.OriginalTableName)
	return nil
}

// RestoreReplicaIdentity restores the original replica identity
func (i *Inspector) RestoreReplicaIdentity() error {
	if i.migrationContext.OriginalReplicaIdentity == "" ||
		i.migrationContext.OriginalReplicaIdentity == "FULL" {
		return nil
	}

	ctx := context.Background()

	query := fmt.Sprintf(
		"ALTER TABLE %s.%s REPLICA IDENTITY %s",
		sqlpkg.QuoteIdentifier(i.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(i.migrationContext.OriginalTableName),
		i.migrationContext.OriginalReplicaIdentity,
	)

	_, err := i.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to restore replica identity: %w", err)
	}

	i.migrationContext.Log.Info("Restored REPLICA IDENTITY %s on %s.%s",
		i.migrationContext.OriginalReplicaIdentity,
		i.migrationContext.SchemaName,
		i.migrationContext.OriginalTableName)
	return nil
}

// EstimateTableRows estimates the row count using statistics
func (i *Inspector) EstimateTableRows() error {
	ctx := context.Background()

	var estimate int64
	err := i.db.QueryRowContext(ctx, `
		SELECT reltuples::bigint
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
	`, i.migrationContext.SchemaName, i.migrationContext.OriginalTableName).Scan(&estimate)

	if err != nil {
		return fmt.Errorf("failed to estimate rows: %w", err)
	}

	if estimate < 0 {
		estimate = 0
	}

	atomic.StoreInt64(&i.migrationContext.RowsEstimate, estimate)
	i.migrationContext.UsedRowsEstimateMethod = base.TableStatsRowsEstimate
	i.migrationContext.Log.Info("Estimated %d rows in %s.%s",
		estimate, i.migrationContext.SchemaName, i.migrationContext.OriginalTableName)

	return nil
}

// CountTableRows counts the exact number of rows
func (i *Inspector) CountTableRows() error {
	ctx := context.Background()

	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s",
		sqlpkg.QuoteIdentifier(i.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(i.migrationContext.OriginalTableName),
	)

	var count int64
	err := i.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to count rows: %w", err)
	}

	atomic.StoreInt64(&i.migrationContext.RowsEstimate, count)
	i.migrationContext.UsedRowsEstimateMethod = base.CountRowsEstimate
	i.migrationContext.Log.Info("Counted %d rows in %s.%s",
		count, i.migrationContext.SchemaName, i.migrationContext.OriginalTableName)

	return nil
}

// GetWALLevel checks if wal_level is set to 'logical'
func (i *Inspector) GetWALLevel(ctx context.Context) (string, error) {
	var walLevel string
	err := i.db.QueryRowContext(ctx, "SHOW wal_level").Scan(&walLevel)
	if err != nil {
		return "", fmt.Errorf("failed to get wal_level: %w", err)
	}
	return walLevel, nil
}

// ValidateWALLevel checks that wal_level is 'logical'
func (i *Inspector) ValidateWALLevel() error {
	ctx := context.Background()
	walLevel, err := i.GetWALLevel(ctx)
	if err != nil {
		return err
	}

	if walLevel != "logical" {
		return fmt.Errorf("wal_level must be 'logical', got '%s'. Set wal_level = 'logical' in postgresql.conf and restart", walLevel)
	}

	i.migrationContext.Log.Info("wal_level = %s", walLevel)
	return nil
}

// ValidatePostgreSQLVersion checks that PostgreSQL version is 14 or higher
func (i *Inspector) ValidatePostgreSQLVersion() error {
	if i.version == nil {
		return fmt.Errorf("PostgreSQL version not available")
	}

	if !i.version.SupportsStreamingLargeTransactions() {
		return fmt.Errorf("PostgreSQL %s is not supported. pg-ost requires PostgreSQL 14 or higher for streaming in-progress transactions", i.version)
	}

	return nil
}

// ValidateReplicationPermission checks that the user has replication permission
func (i *Inspector) ValidateReplicationPermission() error {
	ctx := context.Background()

	var hasReplication bool
	err := i.db.QueryRowContext(ctx, `
		SELECT rolreplication OR rolsuper
		FROM pg_roles
		WHERE rolname = current_user
	`).Scan(&hasReplication)

	if err != nil {
		return fmt.Errorf("failed to check replication permission: %w", err)
	}

	if !hasReplication {
		return fmt.Errorf("user '%s' does not have replication permission. Grant REPLICATION role or use a superuser", i.migrationContext.User)
	}

	i.migrationContext.Log.Info("User has replication permission")
	return nil
}

// ValidateRequirements validates all PostgreSQL requirements for pg-ost
func (i *Inspector) ValidateRequirements() error {
	i.migrationContext.Log.Info("Validating PostgreSQL requirements on primary...")

	// Check PostgreSQL version
	if err := i.ValidatePostgreSQLVersion(); err != nil {
		return err
	}

	// Check wal_level
	if err := i.ValidateWALLevel(); err != nil {
		return err
	}

	// Check replication permission
	if err := i.ValidateReplicationPermission(); err != nil {
		return err
	}

	i.migrationContext.Log.Info("All primary requirements validated successfully")
	return nil
}

// GetSystemIdentifier returns the PostgreSQL cluster system identifier
func (i *Inspector) GetSystemIdentifier() (string, error) {
	ctx := context.Background()
	var systemID string
	err := i.db.QueryRowContext(ctx, "SELECT system_identifier FROM pg_control_system()").Scan(&systemID)
	if err != nil {
		return "", fmt.Errorf("failed to get system identifier: %w", err)
	}
	return systemID, nil
}

// ValidateReplicaRequirements validates the replica connection if configured
func (i *Inspector) ValidateReplicaRequirements(replicaConfig *pg.ConnectionConfig) error {
	if replicaConfig == nil {
		return nil
	}

	i.migrationContext.Log.Info("Validating replica requirements on %s:%d...",
		replicaConfig.Host, replicaConfig.Port)

	ctx := context.Background()

	// Get primary system identifier first
	primarySystemID, err := i.GetSystemIdentifier()
	if err != nil {
		return fmt.Errorf("failed to get primary system identifier: %w", err)
	}
	i.migrationContext.Log.Debug("Primary system identifier: %s", primarySystemID)

	// Connect to replica
	replicaDB, err := replicaConfig.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to replica: %w", err)
	}
	defer replicaDB.Close()

	// Get replica system identifier
	var replicaSystemID string
	err = replicaDB.QueryRowContext(ctx, "SELECT system_identifier FROM pg_control_system()").Scan(&replicaSystemID)
	if err != nil {
		return fmt.Errorf("failed to get replica system identifier: %w", err)
	}
	i.migrationContext.Log.Debug("Replica system identifier: %s", replicaSystemID)

	// Validate same cluster
	if primarySystemID != replicaSystemID {
		if i.migrationContext.SkipReplicaClusterValidation {
			i.migrationContext.Log.Warning("Primary and replica have different system_identifier (primary: %s, replica: %s). Skipping validation as requested.",
				primarySystemID, replicaSystemID)
		} else {
			return fmt.Errorf("primary and replica are not in the same cluster. Primary system_identifier: %s, Replica system_identifier: %s. Use --skip-replica-cluster-validation to bypass this check (use with caution)",
				primarySystemID, replicaSystemID)
		}
	} else {
		i.migrationContext.Log.Info("Confirmed primary and replica are in the same cluster (system_identifier: %s)", primarySystemID)
	}

	// Check if it's actually a replica (in recovery)
	var isInRecovery bool
	err = replicaDB.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery)
	if err != nil {
		return fmt.Errorf("failed to check replica status: %w", err)
	}

	if !isInRecovery {
		i.migrationContext.Log.Warning("The specified replica host is not in recovery mode (might be primary)")
	} else {
		i.migrationContext.Log.Info("Confirmed replica is in recovery mode")
	}

	// Check PostgreSQL version on replica
	replicaVersion, err := pg.GetVersion(ctx, replicaDB)
	if err != nil {
		return fmt.Errorf("failed to get replica version: %w", err)
	}

	if !replicaVersion.SupportsStreamingLargeTransactions() {
		return fmt.Errorf("replica PostgreSQL %s is not supported. pg-ost requires PostgreSQL 14+", replicaVersion)
	}

	i.migrationContext.Log.Info("Replica PostgreSQL version: %s", replicaVersion)

	// Check hot_standby_feedback (recommended for avoiding replication conflicts)
	var hotStandbyFeedback string
	err = replicaDB.QueryRowContext(ctx, "SHOW hot_standby_feedback").Scan(&hotStandbyFeedback)
	if err != nil {
		i.migrationContext.Log.Warning("Could not check hot_standby_feedback: %v", err)
	} else if hotStandbyFeedback != "on" {
		i.migrationContext.Log.Warning("hot_standby_feedback is '%s'. Consider setting it to 'on' to avoid replication conflicts", hotStandbyFeedback)
	} else {
		i.migrationContext.Log.Info("hot_standby_feedback = on")
	}

	// Check replication permission on replica
	var hasReplication bool
	err = replicaDB.QueryRowContext(ctx, `
		SELECT rolreplication OR rolsuper
		FROM pg_roles
		WHERE rolname = current_user
	`).Scan(&hasReplication)

	if err != nil {
		return fmt.Errorf("failed to check replication permission on replica: %w", err)
	}

	if !hasReplication {
		return fmt.Errorf("user '%s' does not have replication permission on replica", replicaConfig.User)
	}

	i.migrationContext.Log.Info("Replica requirements validated successfully")
	return nil
}
