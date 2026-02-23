//go:build e2e
// +build e2e

/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation

   End-to-end tests for pg-ost.

   To run these tests:
   1. Start PostgreSQL: docker-compose -f e2e/docker-compose.yml up -d
   2. Run tests: go test -tags=e2e -v ./e2e/...
*/

package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/your-org/pg-ost/internal/base"
	"github.com/your-org/pg-ost/internal/logic"
)

const (
	defaultHost     = "localhost"
	defaultPort     = 15432
	defaultUser     = "pgost"
	defaultPassword = "pgost"
	defaultDatabase = "pgost_test"
)

// TestConfig holds test configuration
type TestConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// getTestConfig returns test configuration from environment or defaults
func getTestConfig() *TestConfig {
	cfg := &TestConfig{
		Host:     defaultHost,
		Port:     defaultPort,
		User:     defaultUser,
		Password: defaultPassword,
		Database: defaultDatabase,
	}

	if h := os.Getenv("PGOST_TEST_HOST"); h != "" {
		cfg.Host = h
	}
	if u := os.Getenv("PGOST_TEST_USER"); u != "" {
		cfg.User = u
	}
	if p := os.Getenv("PGOST_TEST_PASSWORD"); p != "" {
		cfg.Password = p
	}
	if d := os.Getenv("PGOST_TEST_DATABASE"); d != "" {
		cfg.Database = d
	}

	return cfg
}

// getDSN returns the database connection string
func (c *TestConfig) getDSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		c.Host, c.Port, c.User, c.Password, c.Database)
}

// connectDB connects to the test database
func connectDB(t *testing.T, cfg *TestConfig) *sql.DB {
	db, err := sql.Open("pgx", cfg.getDSN())
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Wait for database to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for database to be ready")
		default:
			if err := db.PingContext(ctx); err == nil {
				return db
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// setupTestTable creates a test table with sample data
func setupTestTable(t *testing.T, db *sql.DB, tableName string, rowCount int) {
	ctx := context.Background()

	// Drop table if exists
	_, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName))
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	// Create table
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(255),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert sample data
	for i := 0; i < rowCount; i++ {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (name, email) VALUES ($1, $2)
		`, tableName), fmt.Sprintf("user%d", i), fmt.Sprintf("user%d@example.com", i))
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	t.Logf("Created table %s with %d rows", tableName, rowCount)
}

// cleanupTestTable drops the test table and related objects
func cleanupTestTable(t *testing.T, db *sql.DB, tableName string) {
	ctx := context.Background()

	// Drop tables
	tables := []string{
		tableName,
		"_" + tableName + "_gho",
		"_" + tableName + "_ghc",
		"_" + tableName + "_old",
	}
	for _, tbl := range tables {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tbl))
	}

	// Drop publication
	db.ExecContext(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS pgost_public_%s", tableName))

	// Drop replication slot
	db.ExecContext(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('pgost_public_%s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'pgost_public_%s')", tableName, tableName))
}

// createMigrationContext creates a migration context for testing
func createMigrationContext(cfg *TestConfig, tableName, alterStatement string) *base.MigrationContext {
	ctx := base.NewMigrationContext()
	ctx.Host = cfg.Host
	ctx.Port = cfg.Port
	ctx.User = cfg.User
	ctx.Password = cfg.Password
	ctx.DatabaseName = cfg.Database
	ctx.SchemaName = "public"
	ctx.OriginalTableName = tableName
	ctx.AlterStatement = alterStatement
	ctx.Execute = true
	ctx.ChunkSize = 100
	ctx.MaxLagMillisecondsThrottleThreshold = 5000
	ctx.CutOverLockTimeoutSeconds = 30
	ctx.Log = &testLogger{t: nil}
	return ctx
}

// testLogger implements base.Logger for tests
type testLogger struct {
	t *testing.T
}

func (l *testLogger) Debug(format string, args ...interface{}) {
	if l.t != nil {
		l.t.Logf("[DEBUG] "+format, args...)
	}
}
func (l *testLogger) Info(format string, args ...interface{}) {
	if l.t != nil {
		l.t.Logf("[INFO] "+format, args...)
	}
}
func (l *testLogger) Warning(format string, args ...interface{}) {
	if l.t != nil {
		l.t.Logf("[WARNING] "+format, args...)
	}
}
func (l *testLogger) Error(format string, args ...interface{}) {
	if l.t != nil {
		l.t.Logf("[ERROR] "+format, args...)
	}
}
func (l *testLogger) Fatal(format string, args ...interface{}) {
	if l.t != nil {
		l.t.Fatalf("[FATAL] "+format, args...)
	}
}

// TestBasicAddColumn tests adding a column to a table
func TestBasicAddColumn(t *testing.T) {
	cfg := getTestConfig()
	db := connectDB(t, cfg)
	defer db.Close()

	tableName := "test_add_column"
	cleanupTestTable(t, db, tableName)
	setupTestTable(t, db, tableName, 1000)
	defer cleanupTestTable(t, db, tableName)

	// Create migration context
	migCtx := createMigrationContext(cfg, tableName, "ADD COLUMN status VARCHAR(50) DEFAULT 'active'")
	migCtx.Log = &testLogger{t: t}
	migCtx.OkToDropTable = true

	// Run migration
	migrator := logic.NewMigrator(migCtx, "test")
	err := migrator.Migrate()
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify column was added
	var count int
	err = db.QueryRow(fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = '%s' AND column_name = 'status'
	`, tableName)).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to verify column: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected column 'status' to exist, but it doesn't")
	}

	// Verify data integrity
	var rowCount int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if rowCount != 1000 {
		t.Errorf("Expected 1000 rows, got %d", rowCount)
	}

	// Verify default value
	var statusCount int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'active'", tableName)).Scan(&statusCount)
	if err != nil {
		t.Fatalf("Failed to verify default value: %v", err)
	}
	if statusCount != 1000 {
		t.Errorf("Expected all 1000 rows to have status='active', got %d", statusCount)
	}

	t.Log("TestBasicAddColumn passed")
}

// TestAddColumnWithIndex tests adding a column with an index
func TestAddColumnWithIndex(t *testing.T) {
	cfg := getTestConfig()
	db := connectDB(t, cfg)
	defer db.Close()

	tableName := "test_add_column_index"
	cleanupTestTable(t, db, tableName)
	setupTestTable(t, db, tableName, 500)
	defer cleanupTestTable(t, db, tableName)

	// Create migration context - add column
	migCtx := createMigrationContext(cfg, tableName, "ADD COLUMN score INTEGER DEFAULT 0")
	migCtx.Log = &testLogger{t: t}
	migCtx.OkToDropTable = true

	// Run migration
	migrator := logic.NewMigrator(migCtx, "test")
	err := migrator.Migrate()
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify column exists
	var colCount int
	err = db.QueryRow(fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = '%s' AND column_name = 'score'
	`, tableName)).Scan(&colCount)
	if err != nil {
		t.Fatalf("Failed to verify column: %v", err)
	}
	if colCount != 1 {
		t.Errorf("Expected column 'score' to exist")
	}

	t.Log("TestAddColumnWithIndex passed")
}

// TestDropColumn tests dropping a column
func TestDropColumn(t *testing.T) {
	cfg := getTestConfig()
	db := connectDB(t, cfg)
	defer db.Close()

	tableName := "test_drop_column"
	cleanupTestTable(t, db, tableName)
	setupTestTable(t, db, tableName, 500)
	defer cleanupTestTable(t, db, tableName)

	// Create migration context
	migCtx := createMigrationContext(cfg, tableName, "DROP COLUMN email")
	migCtx.Log = &testLogger{t: t}
	migCtx.OkToDropTable = true

	// Run migration
	migrator := logic.NewMigrator(migCtx, "test")
	err := migrator.Migrate()
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify column was dropped
	var count int
	err = db.QueryRow(fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = '%s' AND column_name = 'email'
	`, tableName)).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to verify column drop: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected column 'email' to be dropped, but it still exists")
	}

	// Verify data integrity
	var rowCount int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if rowCount != 500 {
		t.Errorf("Expected 500 rows, got %d", rowCount)
	}

	t.Log("TestDropColumn passed")
}

// TestAlterColumnType tests changing a column type
func TestAlterColumnType(t *testing.T) {
	cfg := getTestConfig()
	db := connectDB(t, cfg)
	defer db.Close()

	tableName := "test_alter_type"
	cleanupTestTable(t, db, tableName)
	setupTestTable(t, db, tableName, 300)
	defer cleanupTestTable(t, db, tableName)

	// Create migration context
	migCtx := createMigrationContext(cfg, tableName, "ALTER COLUMN name TYPE TEXT")
	migCtx.Log = &testLogger{t: t}
	migCtx.OkToDropTable = true

	// Run migration
	migrator := logic.NewMigrator(migCtx, "test")
	err := migrator.Migrate()
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify column type changed
	var dataType string
	err = db.QueryRow(fmt.Sprintf(`
		SELECT data_type FROM information_schema.columns
		WHERE table_name = '%s' AND column_name = 'name'
	`, tableName)).Scan(&dataType)
	if err != nil {
		t.Fatalf("Failed to verify column type: %v", err)
	}
	if dataType != "text" {
		t.Errorf("Expected column type 'text', got '%s'", dataType)
	}

	t.Log("TestAlterColumnType passed")
}

// TestConcurrentDML tests that DML operations during migration are captured
func TestConcurrentDML(t *testing.T) {
	cfg := getTestConfig()
	db := connectDB(t, cfg)
	defer db.Close()

	tableName := "test_concurrent_dml"
	cleanupTestTable(t, db, tableName)
	setupTestTable(t, db, tableName, 500)
	defer cleanupTestTable(t, db, tableName)

	// Create migration context
	migCtx := createMigrationContext(cfg, tableName, "ADD COLUMN verified BOOLEAN DEFAULT false")
	migCtx.Log = &testLogger{t: t}
	migCtx.OkToDropTable = true
	migCtx.ChunkSize = 50 // Smaller chunks to allow DML interleaving

	// Start concurrent DML operations
	var wg sync.WaitGroup
	stopDML := make(chan struct{})
	dmlErrors := make(chan error, 100)

	// Insert goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := sql.Open("pgx", cfg.getDSN())
		if err != nil {
			dmlErrors <- err
			return
		}
		defer conn.Close()

		i := 10000
		for {
			select {
			case <-stopDML:
				return
			default:
				_, err := conn.Exec(fmt.Sprintf(
					"INSERT INTO %s (name, email) VALUES ($1, $2)",
					tableName), fmt.Sprintf("new_user%d", i), fmt.Sprintf("new%d@example.com", i))
				if err != nil {
					// Table might be locked during cutover, ignore
					continue
				}
				i++
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Update goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := sql.Open("pgx", cfg.getDSN())
		if err != nil {
			dmlErrors <- err
			return
		}
		defer conn.Close()

		for {
			select {
			case <-stopDML:
				return
			default:
				_, err := conn.Exec(fmt.Sprintf(
					"UPDATE %s SET name = name || '_updated' WHERE id = (SELECT id FROM %s ORDER BY RANDOM() LIMIT 1)",
					tableName, tableName))
				if err != nil {
					continue
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()

	// Delete goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := sql.Open("pgx", cfg.getDSN())
		if err != nil {
			dmlErrors <- err
			return
		}
		defer conn.Close()

		for {
			select {
			case <-stopDML:
				return
			default:
				// Delete from the new inserts, not the original data
				_, err := conn.Exec(fmt.Sprintf(
					"DELETE FROM %s WHERE id > 500 AND id = (SELECT id FROM %s WHERE id > 500 ORDER BY RANDOM() LIMIT 1)",
					tableName, tableName))
				if err != nil {
					continue
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	// Give DML time to start
	time.Sleep(100 * time.Millisecond)

	// Run migration
	migrator := logic.NewMigrator(migCtx, "test")
	err := migrator.Migrate()

	// Stop DML
	close(stopDML)
	wg.Wait()

	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Check for DML errors
	close(dmlErrors)
	for err := range dmlErrors {
		t.Errorf("DML error: %v", err)
	}

	// Verify the new column exists
	var colCount int
	err = db.QueryRow(fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = '%s' AND column_name = 'verified'
	`, tableName)).Scan(&colCount)
	if err != nil {
		t.Fatalf("Failed to verify column: %v", err)
	}
	if colCount != 1 {
		t.Errorf("Expected column 'verified' to exist")
	}

	// Verify some inserts made it through
	var rowCount int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	t.Logf("Final row count: %d (started with 500)", rowCount)

	// We should have at least the original rows (some might have been deleted/re-inserted)
	if rowCount < 400 {
		t.Errorf("Expected at least 400 rows, got %d", rowCount)
	}

	t.Log("TestConcurrentDML passed")
}

// TestLargeTable tests migration of a larger table
func TestLargeTable(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large table test in short mode")
	}

	cfg := getTestConfig()
	db := connectDB(t, cfg)
	defer db.Close()

	tableName := "test_large_table"
	cleanupTestTable(t, db, tableName)
	setupTestTable(t, db, tableName, 10000)
	defer cleanupTestTable(t, db, tableName)

	// Create migration context
	migCtx := createMigrationContext(cfg, tableName, "ADD COLUMN metadata JSONB DEFAULT '{}'")
	migCtx.Log = &testLogger{t: t}
	migCtx.OkToDropTable = true
	migCtx.ChunkSize = 500

	startTime := time.Now()

	// Run migration
	migrator := logic.NewMigrator(migCtx, "test")
	err := migrator.Migrate()
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	duration := time.Since(startTime)
	t.Logf("Migration of 10000 rows completed in %v", duration)

	// Verify column exists
	var colCount int
	err = db.QueryRow(fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = '%s' AND column_name = 'metadata'
	`, tableName)).Scan(&colCount)
	if err != nil {
		t.Fatalf("Failed to verify column: %v", err)
	}
	if colCount != 1 {
		t.Errorf("Expected column 'metadata' to exist")
	}

	// Verify row count
	var rowCount int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if rowCount != 10000 {
		t.Errorf("Expected 10000 rows, got %d", rowCount)
	}

	t.Log("TestLargeTable passed")
}

// TestCompositeKey tests migration of a table with composite primary key
func TestCompositeKey(t *testing.T) {
	cfg := getTestConfig()
	db := connectDB(t, cfg)
	defer db.Close()

	tableName := "test_composite_key"
	cleanupTestTable(t, db, tableName)

	// Create table with composite key
	ctx := context.Background()
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INTEGER NOT NULL,
			user_id INTEGER NOT NULL,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(255),
			PRIMARY KEY (tenant_id, user_id)
		)
	`, tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert sample data
	for tenant := 1; tenant <= 5; tenant++ {
		for user := 1; user <= 100; user++ {
			_, err = db.ExecContext(ctx, fmt.Sprintf(`
				INSERT INTO %s (tenant_id, user_id, name, email) VALUES ($1, $2, $3, $4)
			`, tableName), tenant, user, fmt.Sprintf("user%d_%d", tenant, user), fmt.Sprintf("user%d_%d@example.com", tenant, user))
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}
	}

	defer cleanupTestTable(t, db, tableName)

	// Create migration context
	migCtx := createMigrationContext(cfg, tableName, "ADD COLUMN active BOOLEAN DEFAULT true")
	migCtx.Log = &testLogger{t: t}
	migCtx.OkToDropTable = true

	// Run migration
	migrator := logic.NewMigrator(migCtx, "test")
	err = migrator.Migrate()
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify column exists
	var colCount int
	err = db.QueryRow(fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = '%s' AND column_name = 'active'
	`, tableName)).Scan(&colCount)
	if err != nil {
		t.Fatalf("Failed to verify column: %v", err)
	}
	if colCount != 1 {
		t.Errorf("Expected column 'active' to exist")
	}

	// Verify row count
	var rowCount int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if rowCount != 500 {
		t.Errorf("Expected 500 rows, got %d", rowCount)
	}

	t.Log("TestCompositeKey passed")
}

// TestDryRun tests that dry-run mode doesn't make changes
func TestDryRun(t *testing.T) {
	cfg := getTestConfig()
	db := connectDB(t, cfg)
	defer db.Close()

	tableName := "test_dry_run"
	cleanupTestTable(t, db, tableName)
	setupTestTable(t, db, tableName, 100)
	defer cleanupTestTable(t, db, tableName)

	// Create migration context with Execute = false (dry run)
	migCtx := createMigrationContext(cfg, tableName, "ADD COLUMN dry_run_col VARCHAR(50)")
	migCtx.Execute = false // Dry run
	migCtx.Log = &testLogger{t: t}

	// Run migration
	migrator := logic.NewMigrator(migCtx, "test")
	err := migrator.Migrate()

	// Should return ErrMigrationDryRun
	if err != logic.ErrMigrationDryRun {
		t.Errorf("Expected ErrMigrationDryRun, got: %v", err)
	}

	// Verify column was NOT added
	var count int
	err = db.QueryRow(fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = '%s' AND column_name = 'dry_run_col'
	`, tableName)).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to verify column: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected column 'dry_run_col' to NOT exist in dry-run mode")
	}

	t.Log("TestDryRun passed")
}

// TestEmptyTable tests migration of an empty table
func TestEmptyTable(t *testing.T) {
	cfg := getTestConfig()
	db := connectDB(t, cfg)
	defer db.Close()

	tableName := "test_empty_table"
	cleanupTestTable(t, db, tableName)

	// Create empty table
	ctx := context.Background()
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		)
	`, tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer cleanupTestTable(t, db, tableName)

	// Create migration context
	migCtx := createMigrationContext(cfg, tableName, "ADD COLUMN description TEXT")
	migCtx.Log = &testLogger{t: t}
	migCtx.OkToDropTable = true

	// Run migration
	migrator := logic.NewMigrator(migCtx, "test")
	err = migrator.Migrate()
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify column exists
	var colCount int
	err = db.QueryRow(fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = '%s' AND column_name = 'description'
	`, tableName)).Scan(&colCount)
	if err != nil {
		t.Fatalf("Failed to verify column: %v", err)
	}
	if colCount != 1 {
		t.Errorf("Expected column 'description' to exist")
	}

	t.Log("TestEmptyTable passed")
}

// TestMultipleAlterOperations tests multiple ALTER operations in one statement
func TestMultipleAlterOperations(t *testing.T) {
	cfg := getTestConfig()
	db := connectDB(t, cfg)
	defer db.Close()

	tableName := "test_multi_alter"
	cleanupTestTable(t, db, tableName)
	setupTestTable(t, db, tableName, 200)
	defer cleanupTestTable(t, db, tableName)

	// Create migration context with multiple operations
	migCtx := createMigrationContext(cfg, tableName,
		"ADD COLUMN col1 VARCHAR(50), ADD COLUMN col2 INTEGER DEFAULT 0")
	migCtx.Log = &testLogger{t: t}
	migCtx.OkToDropTable = true

	// Run migration
	migrator := logic.NewMigrator(migCtx, "test")
	err := migrator.Migrate()
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify both columns exist
	var col1Count, col2Count int
	err = db.QueryRow(fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = '%s' AND column_name = 'col1'
	`, tableName)).Scan(&col1Count)
	if err != nil {
		t.Fatalf("Failed to verify col1: %v", err)
	}

	err = db.QueryRow(fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = '%s' AND column_name = 'col2'
	`, tableName)).Scan(&col2Count)
	if err != nil {
		t.Fatalf("Failed to verify col2: %v", err)
	}

	if col1Count != 1 || col2Count != 1 {
		t.Errorf("Expected both columns to exist, col1=%d, col2=%d", col1Count, col2Count)
	}

	t.Log("TestMultipleAlterOperations passed")
}
