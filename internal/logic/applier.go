/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package logic

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/takaidohigasi/pg-ost/internal/base"
	"github.com/takaidohigasi/pg-ost/internal/dml"
	"github.com/takaidohigasi/pg-ost/internal/pg"
	sqlpkg "github.com/takaidohigasi/pg-ost/internal/sql"
)

// Applier applies changes to the ghost table
type Applier struct {
	connectionConfig   *pg.ConnectionConfig
	db                 *sql.DB
	singletonDB        *sql.DB // For operations requiring single connection
	migrationContext   *base.MigrationContext
	parser             *sqlpkg.AlterTableParser
	finalChunkReturned bool // Track if we've already returned the final chunk
}

// NewApplier creates a new Applier
func NewApplier(ctx *base.MigrationContext) *Applier {
	return &Applier{
		migrationContext: ctx,
		connectionConfig: &pg.ConnectionConfig{
			Host:     ctx.Host,
			Port:     ctx.Port,
			User:     ctx.User,
			Password: ctx.Password,
			Database: ctx.DatabaseName,
			SSLMode:  ctx.SSLMode,
		},
		parser: sqlpkg.NewAlterTableParser(),
	}
}

// InitDBConnections initializes database connections
func (a *Applier) InitDBConnections() error {
	ctx := context.Background()

	db, err := a.connectionConfig.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	a.db = db

	// Create singleton connection for cutover
	singletonDB, err := a.connectionConfig.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to create singleton connection: %w", err)
	}
	singletonDB.SetMaxOpenConns(1)
	a.singletonDB = singletonDB

	return nil
}

// Close closes database connections
func (a *Applier) Close() {
	if a.db != nil {
		a.db.Close()
	}
	if a.singletonDB != nil {
		a.singletonDB.Close()
	}
}

// CreateGhostTable creates the ghost table as a copy of the original
func (a *Applier) CreateGhostTable() error {
	ctx := context.Background()
	ghostTableName := a.migrationContext.GetGhostTableName()

	// Drop existing ghost table if requested
	if a.migrationContext.InitiallyDropGhostTable {
		dropQuery := fmt.Sprintf(
			"DROP TABLE IF EXISTS %s.%s CASCADE",
			sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
			sqlpkg.QuoteIdentifier(ghostTableName),
		)
		if _, err := a.db.ExecContext(ctx, dropQuery); err != nil {
			return fmt.Errorf("failed to drop ghost table: %w", err)
		}
	}

	// Create ghost table as copy of original (structure only)
	createQuery := fmt.Sprintf(
		"CREATE TABLE %s.%s (LIKE %s.%s INCLUDING ALL)",
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(ghostTableName),
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(a.migrationContext.OriginalTableName),
	)

	if _, err := a.db.ExecContext(ctx, createQuery); err != nil {
		return fmt.Errorf("failed to create ghost table: %w", err)
	}

	a.migrationContext.Log.Info("Created ghost table %s.%s",
		a.migrationContext.SchemaName, ghostTableName)
	return nil
}

// AlterGhost applies the ALTER statement to the ghost table
func (a *Applier) AlterGhost() error {
	ctx := context.Background()

	if err := a.parser.ParseAlterStatement(a.migrationContext.AlterStatement); err != nil {
		return err
	}

	alterQuery := a.parser.BuildAlterGhostStatement(
		a.migrationContext.SchemaName,
		a.migrationContext.GetGhostTableName(),
	)

	if _, err := a.db.ExecContext(ctx, alterQuery); err != nil {
		return fmt.Errorf("failed to alter ghost table: %w", err)
	}

	a.migrationContext.Log.Info("Applied ALTER to ghost table: %s", alterQuery)
	return nil
}

// CreateChangelogTable creates the changelog table for heartbeat and state tracking
func (a *Applier) CreateChangelogTable() error {
	ctx := context.Background()
	changelogTableName := a.migrationContext.GetChangelogTableName()

	query := fmt.Sprintf(`
		CREATE TABLE %s.%s (
			id BIGSERIAL PRIMARY KEY,
			last_update TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			hint VARCHAR(64) NOT NULL,
			value VARCHAR(4096) NOT NULL,
			UNIQUE(hint)
		)
	`,
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(changelogTableName),
	)

	if _, err := a.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create changelog table: %w", err)
	}

	a.migrationContext.Log.Info("Created changelog table %s.%s",
		a.migrationContext.SchemaName, changelogTableName)
	return nil
}

// WriteChangelog writes a key-value entry to the changelog table
func (a *Applier) WriteChangelog(hint, value string) error {
	ctx := context.Background()
	changelogTableName := a.migrationContext.GetChangelogTableName()

	query := fmt.Sprintf(`
		INSERT INTO %s.%s (hint, value, last_update)
		VALUES ($1, $2, CURRENT_TIMESTAMP)
		ON CONFLICT (hint) DO UPDATE SET value = $2, last_update = CURRENT_TIMESTAMP
	`,
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(changelogTableName),
	)

	if _, err := a.db.ExecContext(ctx, query, hint, value); err != nil {
		return fmt.Errorf("failed to write changelog: %w", err)
	}

	return nil
}

// WriteHeartbeat writes a heartbeat to the changelog
func (a *Applier) WriteHeartbeat() error {
	return a.WriteChangelog("heartbeat", time.Now().Format(time.RFC3339Nano))
}

// DropChangelogTable drops the changelog table
func (a *Applier) DropChangelogTable() error {
	ctx := context.Background()
	changelogTableName := a.migrationContext.GetChangelogTableName()

	query := fmt.Sprintf(
		"DROP TABLE IF EXISTS %s.%s",
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(changelogTableName),
	)

	if _, err := a.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to drop changelog table: %w", err)
	}

	return nil
}

// DropOldTable drops the old table (after cutover)
func (a *Applier) DropOldTable() error {
	ctx := context.Background()
	oldTableName := a.migrationContext.GetOldTableName()

	query := fmt.Sprintf(
		"DROP TABLE IF EXISTS %s.%s",
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(oldTableName),
	)

	if _, err := a.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to drop old table: %w", err)
	}

	a.migrationContext.Log.Info("Dropped old table %s.%s",
		a.migrationContext.SchemaName, oldTableName)
	return nil
}

// ReadMigrationRangeValues reads the min/max values for the unique key
func (a *Applier) ReadMigrationRangeValues() error {
	ctx := context.Background()
	uniqueKey := a.migrationContext.UniqueKeyColumns

	if len(uniqueKey) == 0 {
		return fmt.Errorf("no unique key defined")
	}

	// Build column list
	columns := make([]string, len(uniqueKey))
	for i, col := range uniqueKey {
		columns[i] = sqlpkg.QuoteIdentifier(col)
	}
	columnList := strings.Join(columns, ", ")

	// Get min values
	minQuery := fmt.Sprintf(
		"SELECT %s FROM %s.%s ORDER BY %s LIMIT 1",
		columnList,
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(a.migrationContext.OriginalTableName),
		columnList,
	)

	minRow := a.db.QueryRowContext(ctx, minQuery)
	minValues := make([]interface{}, len(uniqueKey))
	minPtrs := make([]interface{}, len(uniqueKey))
	for i := range minValues {
		minPtrs[i] = &minValues[i]
	}
	if err := minRow.Scan(minPtrs...); err != nil {
		if err == sql.ErrNoRows {
			// Empty table
			a.migrationContext.Log.Info("Table is empty, nothing to copy")
			return nil
		}
		return fmt.Errorf("failed to read min values: %w", err)
	}
	a.migrationContext.MigrationRangeMinValues = minValues
	a.migrationContext.IterationRangeMinValues = minValues

	// Get max values
	maxQuery := fmt.Sprintf(
		"SELECT %s FROM %s.%s ORDER BY %s DESC LIMIT 1",
		columnList,
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(a.migrationContext.OriginalTableName),
		columnList,
	)

	maxRow := a.db.QueryRowContext(ctx, maxQuery)
	maxValues := make([]interface{}, len(uniqueKey))
	maxPtrs := make([]interface{}, len(uniqueKey))
	for i := range maxValues {
		maxPtrs[i] = &maxValues[i]
	}
	if err := maxRow.Scan(maxPtrs...); err != nil {
		return fmt.Errorf("failed to read max values: %w", err)
	}
	a.migrationContext.MigrationRangeMaxValues = maxValues

	a.migrationContext.Log.Info("Migration range: %v to %v", minValues, maxValues)
	return nil
}

// CalculateNextIterationRangeEndValues calculates the end values for the next chunk
func (a *Applier) CalculateNextIterationRangeEndValues() (bool, error) {
	ctx := context.Background()
	uniqueKey := a.migrationContext.UniqueKeyColumns
	chunkSize := atomic.LoadInt64(&a.migrationContext.ChunkSize)

	if len(uniqueKey) == 0 {
		return false, fmt.Errorf("no unique key defined")
	}

	// Build column list
	columns := make([]string, len(uniqueKey))
	for i, col := range uniqueKey {
		columns[i] = sqlpkg.QuoteIdentifier(col)
	}
	columnList := strings.Join(columns, ", ")

	// Build WHERE clause for current position
	whereClause := a.buildRangeWhereClause(uniqueKey, ">=", a.migrationContext.IterationRangeMinValues)

	// Find the next chunk boundary
	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s WHERE %s ORDER BY %s OFFSET %d LIMIT 1",
		columnList,
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(a.migrationContext.OriginalTableName),
		whereClause,
		columnList,
		chunkSize,
	)

	row := a.db.QueryRowContext(ctx, query)
	nextValues := make([]interface{}, len(uniqueKey))
	nextPtrs := make([]interface{}, len(uniqueKey))
	for i := range nextValues {
		nextPtrs[i] = &nextValues[i]
	}

	if err := row.Scan(nextPtrs...); err != nil {
		if err == sql.ErrNoRows {
			// Use max values as the end
			a.migrationContext.IterationRangeMaxValues = a.migrationContext.MigrationRangeMaxValues
			return a.hasMoreRows(), nil
		}
		return false, fmt.Errorf("failed to calculate next range: %w", err)
	}

	a.migrationContext.IterationRangeMaxValues = nextValues
	return true, nil
}

// hasMoreRows checks if there are more rows to process
func (a *Applier) hasMoreRows() bool {
	if a.migrationContext.IterationRangeMinValues == nil {
		return false
	}
	if a.migrationContext.MigrationRangeMaxValues == nil {
		return false
	}

	// Compare current position with max
	// If current min is less than the max, we still have rows to process
	for i := range a.migrationContext.IterationRangeMinValues {
		min := a.migrationContext.IterationRangeMinValues[i]
		max := a.migrationContext.MigrationRangeMaxValues[i]
		if fmt.Sprintf("%v", min) != fmt.Sprintf("%v", max) {
			return true
		}
	}
	// Current position equals max - this is the final chunk
	// Only return true once for the final chunk
	if a.finalChunkReturned {
		return false
	}
	a.finalChunkReturned = true
	return true
}

// buildRangeWhereClause builds a WHERE clause for range comparison
func (a *Applier) buildRangeWhereClause(columns []string, op string, values []interface{}) string {
	if len(columns) == 1 {
		return fmt.Sprintf("%s %s %s",
			sqlpkg.QuoteIdentifier(columns[0]),
			op,
			a.formatValue(values[0]))
	}

	// For composite keys, use row comparison
	quotedCols := make([]string, len(columns))
	formattedVals := make([]string, len(values))
	for i, col := range columns {
		quotedCols[i] = sqlpkg.QuoteIdentifier(col)
		formattedVals[i] = a.formatValue(values[i])
	}

	return fmt.Sprintf("(%s) %s (%s)",
		strings.Join(quotedCols, ", "),
		op,
		strings.Join(formattedVals, ", "))
}

// formatValue formats a value for SQL
func (a *Applier) formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''"))
	case []byte:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(string(val), "'", "''"))
	default:
		return fmt.Sprintf("%v", val)
	}
}

// ApplyIterationInsertQuery copies rows from the original table to the ghost table
func (a *Applier) ApplyIterationInsertQuery() (chunkSize int64, rowsAffected int64, duration time.Duration, err error) {
	startTime := time.Now()
	ctx := context.Background()

	chunkSize = atomic.LoadInt64(&a.migrationContext.ChunkSize)
	uniqueKey := a.migrationContext.UniqueKeyColumns
	columns := a.migrationContext.OriginalTableColumns

	// Build column lists
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = sqlpkg.QuoteIdentifier(col)
	}
	columnList := strings.Join(quotedColumns, ", ")

	// Build unique key columns for conflict
	quotedUK := make([]string, len(uniqueKey))
	for i, col := range uniqueKey {
		quotedUK[i] = sqlpkg.QuoteIdentifier(col)
	}
	ukList := strings.Join(quotedUK, ", ")

	// Build update set clause (all columns except unique key)
	var updateCols []string
	for _, col := range columns {
		isUK := false
		for _, uk := range uniqueKey {
			if col == uk {
				isUK = true
				break
			}
		}
		if !isUK {
			quotedCol := sqlpkg.QuoteIdentifier(col)
			updateCols = append(updateCols, fmt.Sprintf("%s = EXCLUDED.%s", quotedCol, quotedCol))
		}
	}
	updateSet := strings.Join(updateCols, ", ")

	// Build range WHERE clause
	rangeWhere := a.buildRangeComparison(uniqueKey,
		a.migrationContext.IterationRangeMinValues,
		a.migrationContext.IterationRangeMaxValues)

	// Build the INSERT ... SELECT ... ON CONFLICT query
	var query string
	if len(updateCols) > 0 {
		query = fmt.Sprintf(`
			INSERT INTO %s.%s (%s)
			SELECT %s FROM %s.%s
			WHERE %s
			ORDER BY %s
			LIMIT %d
			ON CONFLICT (%s) DO UPDATE SET %s
		`,
			sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
			sqlpkg.QuoteIdentifier(a.migrationContext.GetGhostTableName()),
			columnList,
			columnList,
			sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
			sqlpkg.QuoteIdentifier(a.migrationContext.OriginalTableName),
			rangeWhere,
			ukList,
			chunkSize,
			ukList,
			updateSet,
		)
	} else {
		// Table only has unique key columns
		query = fmt.Sprintf(`
			INSERT INTO %s.%s (%s)
			SELECT %s FROM %s.%s
			WHERE %s
			ORDER BY %s
			LIMIT %d
			ON CONFLICT (%s) DO NOTHING
		`,
			sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
			sqlpkg.QuoteIdentifier(a.migrationContext.GetGhostTableName()),
			columnList,
			columnList,
			sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
			sqlpkg.QuoteIdentifier(a.migrationContext.OriginalTableName),
			rangeWhere,
			ukList,
			chunkSize,
			ukList,
		)
	}

	result, err := a.db.ExecContext(ctx, query)
	if err != nil {
		return chunkSize, 0, time.Since(startTime), fmt.Errorf("failed to copy rows: %w", err)
	}

	rowsAffected, _ = result.RowsAffected()

	// Update iteration min values for next chunk
	a.migrationContext.IterationRangeMinValues = a.migrationContext.IterationRangeMaxValues

	return chunkSize, rowsAffected, time.Since(startTime), nil
}

// buildRangeComparison builds a WHERE clause for a range of values
func (a *Applier) buildRangeComparison(columns []string, minValues, maxValues []interface{}) string {
	if len(columns) == 1 {
		return fmt.Sprintf("%s >= %s AND %s <= %s",
			sqlpkg.QuoteIdentifier(columns[0]),
			a.formatValue(minValues[0]),
			sqlpkg.QuoteIdentifier(columns[0]),
			a.formatValue(maxValues[0]))
	}

	// For composite keys
	quotedCols := make([]string, len(columns))
	minVals := make([]string, len(minValues))
	maxVals := make([]string, len(maxValues))
	for i, col := range columns {
		quotedCols[i] = sqlpkg.QuoteIdentifier(col)
		minVals[i] = a.formatValue(minValues[i])
		maxVals[i] = a.formatValue(maxValues[i])
	}

	return fmt.Sprintf("(%s) >= (%s) AND (%s) <= (%s)",
		strings.Join(quotedCols, ", "),
		strings.Join(minVals, ", "),
		strings.Join(quotedCols, ", "),
		strings.Join(maxVals, ", "))
}

// ApplyDMLEventQueries applies DML events to the ghost table
func (a *Applier) ApplyDMLEventQueries(events []*dml.Event) error {
	if len(events) == 0 {
		return nil
	}

	ctx := context.Background()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, event := range events {
		var query string
		var args []interface{}

		switch event.DML {
		case dml.InsertDML:
			query, args = a.buildInsertQuery(event)
		case dml.UpdateDML:
			query, args = a.buildUpdateQuery(event)
		case dml.DeleteDML:
			query, args = a.buildDeleteQuery(event)
		}

		if query != "" {
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf("failed to apply %s: %w", event.DML, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// buildInsertQuery builds an INSERT query for a DML event
func (a *Applier) buildInsertQuery(event *dml.Event) (string, []interface{}) {
	if event.NewColumnValues == nil {
		return "", nil
	}

	columns := event.NewColumnValues.Columns
	values := event.NewColumnValues.Values

	quotedCols := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = sqlpkg.QuoteIdentifier(col)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	// Build ON CONFLICT clause using unique key
	ukCols := make([]string, len(a.migrationContext.UniqueKeyColumns))
	for i, col := range a.migrationContext.UniqueKeyColumns {
		ukCols[i] = sqlpkg.QuoteIdentifier(col)
	}

	// Update all columns except unique key
	var updateCols []string
	for _, col := range columns {
		isUK := false
		for _, uk := range a.migrationContext.UniqueKeyColumns {
			if col == uk {
				isUK = true
				break
			}
		}
		if !isUK {
			quotedCol := sqlpkg.QuoteIdentifier(col)
			updateCols = append(updateCols, fmt.Sprintf("%s = EXCLUDED.%s", quotedCol, quotedCol))
		}
	}

	var query string
	if len(updateCols) > 0 {
		query = fmt.Sprintf(
			"INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
			sqlpkg.QuoteIdentifier(a.migrationContext.GetGhostTableName()),
			strings.Join(quotedCols, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(ukCols, ", "),
			strings.Join(updateCols, ", "),
		)
	} else {
		query = fmt.Sprintf(
			"INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
			sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
			sqlpkg.QuoteIdentifier(a.migrationContext.GetGhostTableName()),
			strings.Join(quotedCols, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(ukCols, ", "),
		)
	}

	return query, values
}

// buildUpdateQuery builds an UPDATE query for a DML event
func (a *Applier) buildUpdateQuery(event *dml.Event) (string, []interface{}) {
	if event.NewColumnValues == nil || event.OldColumnValues == nil {
		return "", nil
	}

	// Check if unique key changed
	ukChanged := false
	for _, uk := range a.migrationContext.UniqueKeyColumns {
		oldVal, oldOk := event.OldColumnValues.GetValueByName(uk)
		newVal, newOk := event.NewColumnValues.GetValueByName(uk)
		if oldOk && newOk && fmt.Sprintf("%v", oldVal) != fmt.Sprintf("%v", newVal) {
			ukChanged = true
			break
		}
	}

	if ukChanged {
		// Unique key changed - DELETE old + INSERT new
		// For simplicity, we just do an upsert with the new values
		return a.buildInsertQuery(event)
	}

	// Regular UPDATE
	columns := event.NewColumnValues.Columns
	values := event.NewColumnValues.Values

	var setClauses []string
	var args []interface{}
	argIdx := 1

	for i, col := range columns {
		isUK := false
		for _, uk := range a.migrationContext.UniqueKeyColumns {
			if col == uk {
				isUK = true
				break
			}
		}
		if !isUK {
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", sqlpkg.QuoteIdentifier(col), argIdx))
			args = append(args, values[i])
			argIdx++
		}
	}

	if len(setClauses) == 0 {
		return "", nil
	}

	// Build WHERE clause using old unique key values
	var whereClauses []string
	for _, uk := range a.migrationContext.UniqueKeyColumns {
		val, ok := event.OldColumnValues.GetValueByName(uk)
		if ok {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", sqlpkg.QuoteIdentifier(uk), argIdx))
			args = append(args, val)
			argIdx++
		}
	}

	query := fmt.Sprintf(
		"UPDATE %s.%s SET %s WHERE %s",
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(a.migrationContext.GetGhostTableName()),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	return query, args
}

// buildDeleteQuery builds a DELETE query for a DML event
func (a *Applier) buildDeleteQuery(event *dml.Event) (string, []interface{}) {
	if event.OldColumnValues == nil {
		return "", nil
	}

	var whereClauses []string
	var args []interface{}
	argIdx := 1

	for _, uk := range a.migrationContext.UniqueKeyColumns {
		val, ok := event.OldColumnValues.GetValueByName(uk)
		if ok {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", sqlpkg.QuoteIdentifier(uk), argIdx))
			args = append(args, val)
			argIdx++
		}
	}

	if len(whereClauses) == 0 {
		return "", nil
	}

	query := fmt.Sprintf(
		"DELETE FROM %s.%s WHERE %s",
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(a.migrationContext.GetGhostTableName()),
		strings.Join(whereClauses, " AND "),
	)

	return query, args
}

// AtomicCutOver performs the atomic cutover (table rename)
func (a *Applier) AtomicCutOver() error {
	ctx := context.Background()

	oldTableName := a.migrationContext.GetOldTableName()
	ghostTableName := a.migrationContext.GetGhostTableName()

	// Drop old table if it exists and requested
	if a.migrationContext.InitiallyDropOldTable {
		dropQuery := fmt.Sprintf(
			"DROP TABLE IF EXISTS %s.%s",
			sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
			sqlpkg.QuoteIdentifier(oldTableName),
		)
		if _, err := a.singletonDB.ExecContext(ctx, dropQuery); err != nil {
			return fmt.Errorf("failed to drop old table: %w", err)
		}
	}

	// Begin transaction with ACCESS EXCLUSIVE lock
	tx, err := a.singletonDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin cutover transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Set lock timeout
	lockTimeout := fmt.Sprintf("SET LOCAL lock_timeout = '%ds'", a.migrationContext.CutOverLockTimeoutSeconds)
	if _, err := tx.ExecContext(ctx, lockTimeout); err != nil {
		return fmt.Errorf("failed to set lock timeout: %w", err)
	}

	// Lock the original table
	lockQuery := fmt.Sprintf(
		"LOCK TABLE %s.%s IN ACCESS EXCLUSIVE MODE",
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(a.migrationContext.OriginalTableName),
	)

	a.migrationContext.LockTablesStartTime = time.Now()
	if _, err := tx.ExecContext(ctx, lockQuery); err != nil {
		return fmt.Errorf("failed to lock original table: %w", err)
	}

	a.migrationContext.Log.Info("Acquired lock on original table")

	// Rename original -> old
	renameOriginal := fmt.Sprintf(
		"ALTER TABLE %s.%s RENAME TO %s",
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(a.migrationContext.OriginalTableName),
		sqlpkg.QuoteIdentifier(oldTableName),
	)
	if _, err := tx.ExecContext(ctx, renameOriginal); err != nil {
		return fmt.Errorf("failed to rename original table: %w", err)
	}

	// Rename ghost -> original
	renameGhost := fmt.Sprintf(
		"ALTER TABLE %s.%s RENAME TO %s",
		sqlpkg.QuoteIdentifier(a.migrationContext.SchemaName),
		sqlpkg.QuoteIdentifier(ghostTableName),
		sqlpkg.QuoteIdentifier(a.migrationContext.OriginalTableName),
	)
	if _, err := tx.ExecContext(ctx, renameGhost); err != nil {
		return fmt.Errorf("failed to rename ghost table: %w", err)
	}

	// Commit
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit cutover: %w", err)
	}

	a.migrationContext.RenameTablesEndTime = time.Now()
	a.migrationContext.Log.Info("Cutover completed: %s -> %s, %s -> %s",
		a.migrationContext.OriginalTableName, oldTableName,
		ghostTableName, a.migrationContext.OriginalTableName)

	return nil
}
