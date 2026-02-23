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
	"github.com/your-org/pg-ost/internal/config"
	"github.com/your-org/pg-ost/internal/logic"
)

// AppVersion is set at build time
var AppVersion = "0.1.0-dev"

func main() {
	ctx := base.NewMigrationContext()

	// Config file flag (takes precedence, CLI flags override)
	configFile := flag.String("config", "", "Path to YAML configuration file")

	// Database connection flags (primary - for writes)
	flag.StringVar(&ctx.Host, "host", "", "PostgreSQL primary host")
	flag.IntVar(&ctx.Port, "port", 0, "PostgreSQL primary port")
	flag.StringVar(&ctx.User, "user", "", "PostgreSQL user")
	flag.StringVar(&ctx.Password, "password", "", "PostgreSQL password")
	flag.StringVar(&ctx.DatabaseName, "database", "", "Database name")
	flag.StringVar(&ctx.SSLMode, "sslmode", "", "SSL mode (disable, allow, prefer, require, verify-ca, verify-full)")

	// Replica connection flags (for reading replication stream)
	flag.StringVar(&ctx.ReplicaHost, "replica-host", "", "PostgreSQL replica host for streaming (if different from primary)")
	flag.IntVar(&ctx.ReplicaPort, "replica-port", 0, "PostgreSQL replica port")
	flag.StringVar(&ctx.ReplicaUser, "replica-user", "", "PostgreSQL replica user (defaults to --user)")
	flag.StringVar(&ctx.ReplicaPassword, "replica-password", "", "PostgreSQL replica password (defaults to --password)")
	flag.StringVar(&ctx.ReplicaSSLMode, "replica-sslmode", "", "Replica SSL mode (defaults to --sslmode)")
	flag.BoolVar(&ctx.SkipReplicaClusterValidation, "skip-replica-cluster-validation", false, "Skip validation that replica is in the same cluster as primary (use with caution)")

	// Table and ALTER flags
	flag.StringVar(&ctx.SchemaName, "schema", "", "Schema name")
	flag.StringVar(&ctx.OriginalTableName, "table", "", "Table name")
	flag.StringVar(&ctx.AlterStatement, "alter", "", "ALTER statement")

	// Execution flags
	flag.BoolVar(&ctx.Execute, "execute", false, "Actually execute the migration (dry-run otherwise)")
	flag.BoolVar(&ctx.Noop, "noop", false, "Do not apply any changes, for testing")

	// Chunk and throttle settings
	chunkSize := flag.Int64("chunk-size", 0, "Number of rows per chunk (100-100000)")
	maxLagMillis := flag.Int64("max-lag-millis", 0, "Maximum replication lag before throttling")
	flag.Float64Var(&ctx.NiceRatio, "nice-ratio", 0, "Ratio of sleep time between chunks (0=aggressive, 1=slower)")
	dmlBatchSize := flag.Int64("dml-batch-size", 0, "Number of DML events to batch together")

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
	cutOverLockTimeout := flag.Int64("cut-over-lock-timeout-seconds", 0, "Lock timeout for cutover")

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
	heartbeatInterval := flag.Int64("heartbeat-interval-millis", 0, "Heartbeat interval in milliseconds")

	// Verbose/version
	version := flag.Bool("version", false, "Print version and exit")
	verbose := flag.Bool("verbose", false, "Enable verbose output")

	flag.Parse()

	if *version {
		fmt.Printf("pg-ost %s\n", AppVersion)
		os.Exit(0)
	}

	// Load config file if specified
	if *configFile != "" {
		cfg, err := config.LoadFromFile(*configFile)
		if err != nil {
			log.Fatalf("Failed to load config file: %v", err)
		}
		applyConfigToContext(cfg, ctx)
		log.Printf("[INFO] Loaded configuration from %s", *configFile)
	}

	// CLI flags override config file values (only if explicitly set)
	applyCliOverrides(ctx, chunkSize, maxLagMillis, dmlBatchSize, cutOverLockTimeout, heartbeatInterval, maxLoad, criticalLoad)

	// Apply defaults for values not set by config or CLI
	applyDefaults(ctx)

	// Validate required fields
	if ctx.DatabaseName == "" {
		log.Fatal("--database is required (or set in config file)")
	}
	if ctx.OriginalTableName == "" {
		log.Fatal("--table is required (or set in config file)")
	}
	if ctx.AlterStatement == "" {
		log.Fatal("--alter is required (or set in config file)")
	}
	if ctx.User == "" {
		log.Fatal("--user is required (or set in config file)")
	}

	// Validate chunk size
	if ctx.ChunkSize < 100 {
		ctx.ChunkSize = 100
	} else if ctx.ChunkSize > 100000 {
		ctx.ChunkSize = 100000
	}

	// Default replica settings from primary if replica host is specified but other fields are not
	if ctx.ReplicaHost != "" {
		if ctx.ReplicaUser == "" {
			ctx.ReplicaUser = ctx.User
		}
		if ctx.ReplicaPassword == "" {
			ctx.ReplicaPassword = ctx.Password
		}
		if ctx.ReplicaSSLMode == "" {
			ctx.ReplicaSSLMode = ctx.SSLMode
		}
	}

	// Parse load maps from CLI (overrides config)
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

// applyConfigToContext applies configuration from YAML file to MigrationContext
func applyConfigToContext(cfg *config.Config, ctx *base.MigrationContext) {
	// Database settings
	ctx.Host = cfg.Database.Host
	ctx.Port = cfg.Database.Port
	ctx.User = cfg.Database.User
	ctx.Password = cfg.Database.Password
	ctx.DatabaseName = cfg.Database.Database
	ctx.SSLMode = cfg.Database.SSLMode

	// Replica settings
	if cfg.Replica != nil {
		ctx.ReplicaHost = cfg.Replica.Host
		ctx.ReplicaPort = cfg.Replica.Port
		ctx.ReplicaUser = cfg.Replica.User
		ctx.ReplicaPassword = cfg.Replica.Password
		ctx.ReplicaSSLMode = cfg.Replica.SSLMode
		ctx.SkipReplicaClusterValidation = cfg.Replica.SkipClusterValidation
	}

	// Table settings
	ctx.SchemaName = cfg.Table.Schema
	ctx.OriginalTableName = cfg.Table.Name
	ctx.AlterStatement = cfg.Table.Alter

	// Processing settings
	ctx.ChunkSize = cfg.Processing.ChunkSize
	ctx.DMLBatchSize = cfg.Processing.DMLBatchSize
	ctx.NiceRatio = cfg.Processing.NiceRatio
	ctx.HeartbeatIntervalMilliseconds = cfg.Processing.HeartbeatIntervalMs
	ctx.CountTableRows = cfg.Processing.ExactRowCount
	ctx.ConcurrentCountTableRows = cfg.Processing.ConcurrentRowCount

	// Throttle settings
	ctx.MaxLagMillisecondsThrottleThreshold = cfg.Throttle.MaxLagMs
	ctx.ThrottleFlagFile = cfg.Throttle.FlagFile
	ctx.ThrottleAdditionalFlagFile = cfg.Throttle.AdditionalFlagFile
	ctx.ThrottleQuery = cfg.Throttle.Query
	ctx.ThrottleHTTP = cfg.Throttle.HTTP
	ctx.ThrottleHTTPIntervalMillis = cfg.Throttle.HTTPIntervalMs
	ctx.IgnoreHTTPErrors = cfg.Throttle.IgnoreHTTPErrors
	ctx.CriticalLoadIntervalMilliseconds = cfg.Throttle.CriticalLoadIntervalMs
	ctx.CriticalLoadHibernateSeconds = cfg.Throttle.CriticalLoadHibernateS

	// Convert max_load and critical_load maps
	if len(cfg.Throttle.MaxLoad) > 0 {
		ctx.MaxLoad = base.LoadMap(cfg.Throttle.MaxLoad)
	}
	if len(cfg.Throttle.CriticalLoad) > 0 {
		ctx.CriticalLoad = base.LoadMap(cfg.Throttle.CriticalLoad)
	}

	// Cutover settings
	ctx.CutOverLockTimeoutSeconds = cfg.Cutover.LockTimeoutSeconds
	ctx.PostponeCutOverFlagFile = cfg.Cutover.PostponeFlagFile
	ctx.PanicFlagFile = cfg.Cutover.PanicFlagFile
	ctx.OkToDropTable = cfg.Cutover.OkToDropTable
	ctx.InitiallyDropOldTable = cfg.Cutover.InitiallyDropOld
	ctx.InitiallyDropGhostTable = cfg.Cutover.InitiallyDropGhost
	ctx.TimestampOldTable = cfg.Cutover.TimestampOldTable

	// Server settings
	if cfg.Server != nil {
		ctx.ServeSocketFile = cfg.Server.SocketFile
		ctx.ServeTCPPort = cfg.Server.TCPPort
		ctx.DropServeSocket = cfg.Server.DropSocket
	}

	// Hooks settings
	if cfg.Hooks != nil {
		ctx.HooksPath = cfg.Hooks.Path
		ctx.HooksHintMessage = cfg.Hooks.HintMessage
		ctx.HooksHintOwner = cfg.Hooks.HintOwner
		ctx.HooksHintToken = cfg.Hooks.HintToken
		ctx.HooksStatusIntervalSec = cfg.Hooks.StatusIntervalSec
	}
}

// applyCliOverrides applies CLI flag overrides (only non-zero/non-empty values)
func applyCliOverrides(ctx *base.MigrationContext, chunkSize, maxLagMillis, dmlBatchSize, cutOverLockTimeout, heartbeatInterval *int64, maxLoad, criticalLoad *string) {
	// Only override if CLI flag was explicitly set (non-zero/non-empty)
	if *chunkSize > 0 {
		ctx.ChunkSize = *chunkSize
	}
	if *maxLagMillis > 0 {
		ctx.MaxLagMillisecondsThrottleThreshold = *maxLagMillis
	}
	if *dmlBatchSize > 0 {
		ctx.DMLBatchSize = *dmlBatchSize
	}
	if *cutOverLockTimeout > 0 {
		ctx.CutOverLockTimeoutSeconds = *cutOverLockTimeout
	}
	if *heartbeatInterval > 0 {
		ctx.HeartbeatIntervalMilliseconds = *heartbeatInterval
	}
}

// applyDefaults sets default values for fields not set by config or CLI
func applyDefaults(ctx *base.MigrationContext) {
	if ctx.Host == "" {
		ctx.Host = "localhost"
	}
	if ctx.Port == 0 {
		ctx.Port = 5432
	}
	if ctx.SSLMode == "" {
		ctx.SSLMode = "prefer"
	}
	if ctx.SchemaName == "" {
		ctx.SchemaName = "public"
	}
	if ctx.ChunkSize == 0 {
		ctx.ChunkSize = 1000
	}
	if ctx.DMLBatchSize == 0 {
		ctx.DMLBatchSize = 100
	}
	if ctx.MaxLagMillisecondsThrottleThreshold == 0 {
		ctx.MaxLagMillisecondsThrottleThreshold = 1500
	}
	if ctx.HeartbeatIntervalMilliseconds == 0 {
		ctx.HeartbeatIntervalMilliseconds = 500
	}
	if ctx.CutOverLockTimeoutSeconds == 0 {
		ctx.CutOverLockTimeoutSeconds = 10
	}
	if ctx.ReplicaPort == 0 {
		ctx.ReplicaPort = 5432
	}
}
