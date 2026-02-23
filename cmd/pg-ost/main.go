/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation

   Trigger-less online schema change for PostgreSQL using logical replication.
*/

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/your-org/pg-ost/internal/base"
	"github.com/your-org/pg-ost/internal/logic"
)

// AppVersion is set at build time
var AppVersion = "0.1.0-dev"

func main() {
	ctx := base.NewMigrationContext()

	// Database connection flags
	flag.StringVar(&ctx.Host, "host", "localhost", "PostgreSQL host")
	flag.IntVar(&ctx.Port, "port", 5432, "PostgreSQL port")
	flag.StringVar(&ctx.User, "user", "", "PostgreSQL user (required)")
	flag.StringVar(&ctx.Password, "password", "", "PostgreSQL password")
	flag.StringVar(&ctx.DatabaseName, "database", "", "Database name (required)")
	flag.StringVar(&ctx.SSLMode, "sslmode", "prefer", "SSL mode (disable, allow, prefer, require, verify-ca, verify-full)")

	// Table and ALTER flags
	flag.StringVar(&ctx.SchemaName, "schema", "public", "Schema name")
	flag.StringVar(&ctx.OriginalTableName, "table", "", "Table name (required)")
	flag.StringVar(&ctx.AlterStatement, "alter", "", "ALTER statement (required)")

	// Execution flags
	flag.BoolVar(&ctx.Execute, "execute", false, "Actually execute the migration (dry-run otherwise)")
	flag.BoolVar(&ctx.Noop, "noop", false, "Do not apply any changes, for testing")

	// Chunk and throttle settings
	chunkSize := flag.Int64("chunk-size", 1000, "Number of rows per chunk (100-100000)")
	flag.Int64Var(&ctx.MaxLagMillisecondsThrottleThreshold, "max-lag-millis", 1500, "Maximum replication lag before throttling")
	flag.Float64Var(&ctx.NiceRatio, "nice-ratio", 0, "Ratio of sleep time between chunks (0=aggressive, 1=slower)")
	flag.Int64Var(&ctx.DMLBatchSize, "dml-batch-size", 100, "Number of DML events to batch together")

	// Throttle control flags
	flag.StringVar(&ctx.ThrottleFlagFile, "throttle-flag-file", "", "File that, when exists, will pause migration")
	flag.StringVar(&ctx.ThrottleAdditionalFlagFile, "throttle-additional-flag-file", "", "Additional throttle flag file")
	flag.StringVar(&ctx.PostponeCutOverFlagFile, "postpone-cut-over-flag-file", "", "File that, when exists, will postpone cutover")
	flag.StringVar(&ctx.PanicFlagFile, "panic-flag-file", "", "File that, when exists, will abort migration")
	flag.StringVar(&ctx.ThrottleQuery, "throttle-query", "", "Query that, if returns > 0, will pause migration")
	flag.StringVar(&ctx.ThrottleHTTP, "throttle-http", "", "HTTP endpoint to check for throttling (non-200 = throttle)")
	maxLoad := flag.String("max-load", "", "Comma-separated list of status=threshold pairs (e.g., active_connections=100)")
	criticalLoad := flag.String("critical-load", "", "Comma-separated list of status=threshold pairs that cause hibernation")

	// Table management
	flag.BoolVar(&ctx.OkToDropTable, "ok-to-drop-table", false, "Allow dropping the old table after cutover")
	flag.BoolVar(&ctx.InitiallyDropOldTable, "initially-drop-old-table", false, "Drop any existing _old table before migration")
	flag.BoolVar(&ctx.InitiallyDropGhostTable, "initially-drop-ghost-table", false, "Drop any existing ghost table before migration")
	flag.BoolVar(&ctx.TimestampOldTable, "timestamp-old-table", false, "Add timestamp to old table name")

	// Row count estimation
	flag.BoolVar(&ctx.CountTableRows, "exact-rowcount", false, "Use exact row count (slower)")
	flag.BoolVar(&ctx.ConcurrentCountTableRows, "concurrent-rowcount", false, "Count rows concurrently with migration")

	// Cutover settings
	flag.Int64Var(&ctx.CutOverLockTimeoutSeconds, "cut-over-lock-timeout-seconds", 10, "Lock timeout for cutover")

	// Server settings
	flag.StringVar(&ctx.ServeSocketFile, "serve-socket-file", "", "Unix socket file for online control")
	flag.Int64Var(&ctx.ServeTCPPort, "serve-tcp-port", 0, "TCP port for online control (0 = disabled)")
	flag.BoolVar(&ctx.DropServeSocket, "drop-serve-socket", false, "Drop existing socket file if it exists")

	// Hooks
	flag.StringVar(&ctx.HooksPath, "hooks-path", "", "Directory containing hook scripts")
	flag.StringVar(&ctx.HooksHintMessage, "hooks-hint", "", "Message to pass to hooks")
	flag.StringVar(&ctx.HooksHintOwner, "hooks-hint-owner", "", "Owner to pass to hooks")
	flag.StringVar(&ctx.HooksHintToken, "hooks-hint-token", "", "Token to pass to hooks")

	// Heartbeat
	flag.Int64Var(&ctx.HeartbeatIntervalMilliseconds, "heartbeat-interval-millis", 500, "Heartbeat interval in milliseconds")

	// Verbose/version
	version := flag.Bool("version", false, "Print version and exit")
	verbose := flag.Bool("verbose", false, "Enable verbose output")

	flag.Parse()

	if *version {
		fmt.Printf("pg-ost %s\n", AppVersion)
		os.Exit(0)
	}

	// Validate required flags
	if ctx.DatabaseName == "" {
		log.Fatal("--database is required")
	}
	if ctx.OriginalTableName == "" {
		log.Fatal("--table is required")
	}
	if ctx.AlterStatement == "" {
		log.Fatal("--alter is required")
	}
	if ctx.User == "" {
		log.Fatal("--user is required")
	}

	// Apply chunk size with validation
	if *chunkSize < 100 {
		*chunkSize = 100
	} else if *chunkSize > 100000 {
		*chunkSize = 100000
	}
	ctx.ChunkSize = *chunkSize

	// Parse load maps
	if *maxLoad != "" {
		var err error
		ctx.MaxLoad, err = base.ParseLoadMap(*maxLoad)
		if err != nil {
			log.Fatalf("Invalid --max-load: %v", err)
		}
	}
	if *criticalLoad != "" {
		var err error
		ctx.CriticalLoad, err = base.ParseLoadMap(*criticalLoad)
		if err != nil {
			log.Fatalf("Invalid --critical-load: %v", err)
		}
	}

	// Set up logger
	if *verbose {
		ctx.Log = &verboseLogger{}
	} else {
		ctx.Log = &standardLogger{}
	}

	// Dry-run warning
	if !ctx.Execute {
		ctx.Log.Info("Running in dry-run mode. Use --execute to actually perform migration.")
	}

	// Create migrator
	migrator := logic.NewMigrator(ctx, AppVersion)

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		ctx.Log.Warning("Received signal %v, initiating graceful shutdown...", sig)
		ctx.PanicAbort <- fmt.Errorf("received signal: %v", sig)
	}()

	// Run migration
	if err := migrator.Migrate(); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	ctx.Log.Info("Migration completed successfully")
}

// standardLogger is a simple logger for normal operation
type standardLogger struct{}

func (l *standardLogger) Debug(format string, args ...interface{}) {}
func (l *standardLogger) Info(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}
func (l *standardLogger) Warning(format string, args ...interface{}) {
	log.Printf("[WARNING] "+format, args...)
}
func (l *standardLogger) Error(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}
func (l *standardLogger) Fatal(format string, args ...interface{}) {
	log.Fatalf("[FATAL] "+format, args...)
}

// verboseLogger is a logger that includes debug output
type verboseLogger struct{}

func (l *verboseLogger) Debug(format string, args ...interface{}) {
	log.Printf("[DEBUG] "+format, args...)
}
func (l *verboseLogger) Info(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}
func (l *verboseLogger) Warning(format string, args ...interface{}) {
	log.Printf("[WARNING] "+format, args...)
}
func (l *verboseLogger) Error(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}
func (l *verboseLogger) Fatal(format string, args ...interface{}) {
	log.Fatalf("[FATAL] "+format, args...)
}
