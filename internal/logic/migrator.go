/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package logic

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/your-org/pg-ost/internal/base"
	"github.com/your-org/pg-ost/internal/dml"
	"github.com/your-org/pg-ost/internal/pg"
	"github.com/your-org/pg-ost/internal/sql"
)

var (
	ErrMigratorUnsupportedRenameAlter = errors.New("ALTER statement seems to RENAME the table. This is not supported")
	ErrMigrationDryRun                = errors.New("dry-run mode: migration not executed")
)

// ChangelogState represents the state of migration stored in changelog
type ChangelogState string

const (
	AllEventsUpToLockProcessed ChangelogState = "AllEventsUpToLockProcessed"
	GhostTableMigrated         ChangelogState = "GhostTableMigrated"
	Migrated                   ChangelogState = "Migrated"
	ReadMigrationRangeValues   ChangelogState = "ReadMigrationRangeValues"
)

// tableWriteFunc is a function that writes to the ghost table
type tableWriteFunc func() error

// applyEventStruct wraps either a write function or a DML event
type applyEventStruct struct {
	writeFunc *tableWriteFunc
	dmlEvent  *dml.Event
}

func newApplyEventStructByFunc(writeFunc *tableWriteFunc) *applyEventStruct {
	return &applyEventStruct{writeFunc: writeFunc}
}

func newApplyEventStructByDML(dmlEvent *dml.Event) *applyEventStruct {
	return &applyEventStruct{dmlEvent: dmlEvent}
}

// PrintStatusRule controls status printing behavior
type PrintStatusRule int

const (
	NoPrintStatusRule PrintStatusRule = iota
	HeuristicPrintStatusRule
	ForcePrintStatusRule
)

// Migrator is the main schema migration orchestrator
type Migrator struct {
	appVersion       string
	parser           *sql.AlterTableParser
	inspector        *Inspector
	applier          *Applier
	streamer         *Streamer
	server           *Server
	throttler        *Throttler
	hooksExecutor    *HooksExecutor
	migrationContext *base.MigrationContext

	// Coordination channels
	firstThrottlingCollected   chan bool
	ghostTableMigrated         chan bool
	rowCopyComplete            chan error
	allEventsUpToLockProcessed chan string

	// State flags
	rowCopyCompleteFlag int64
	finishedMigrating   int64

	// Queues
	copyRowsQueue    chan tableWriteFunc
	applyEventsQueue chan *applyEventStruct

	// Changelog state tracking
	handledChangelogStates map[string]bool
}

// NewMigrator creates a new Migrator
func NewMigrator(ctx *base.MigrationContext, appVersion string) *Migrator {
	return &Migrator{
		appVersion:       appVersion,
		migrationContext: ctx,
		parser:           sql.NewAlterTableParser(),
		hooksExecutor:    NewHooksExecutor(ctx),

		firstThrottlingCollected:   make(chan bool, 3),
		ghostTableMigrated:         make(chan bool),
		rowCopyComplete:            make(chan error),
		allEventsUpToLockProcessed: make(chan string),

		copyRowsQueue:          make(chan tableWriteFunc),
		applyEventsQueue:       make(chan *applyEventStruct, base.MaxEventsBatchSize),
		handledChangelogStates: make(map[string]bool),
	}
}

// Migrate performs the online schema migration
func (m *Migrator) Migrate() error {
	m.migrationContext.StartTime = time.Now()
	m.migrationContext.Log.Info("Starting migration on %s.%s",
		m.migrationContext.SchemaName,
		m.migrationContext.OriginalTableName)

	// Step 1: Validate ALTER statement
	if err := m.validateAlterStatement(); err != nil {
		return err
	}

	// Dry-run check
	if !m.migrationContext.Execute {
		m.migrationContext.Log.Info("Dry-run: ALTER statement validated successfully")
		m.migrationContext.Log.Info("Dry-run: Would ALTER %s.%s with: %s",
			m.migrationContext.SchemaName,
			m.migrationContext.OriginalTableName,
			m.migrationContext.AlterStatement)
		return ErrMigrationDryRun
	}

	// Step 2: Initialize inspector
	if err := m.initiateInspector(); err != nil {
		return fmt.Errorf("failed to initiate inspector: %w", err)
	}

	// Step 3: Validate PostgreSQL requirements (version, wal_level, replication permission)
	if err := m.inspector.ValidateRequirements(); err != nil {
		return fmt.Errorf("requirements validation failed: %w", err)
	}

	// Step 3b: Validate replica requirements if configured
	if m.migrationContext.HasReplicaConnection() {
		replicaConfig := &pg.ConnectionConfig{
			Host:     m.migrationContext.ReplicaHost,
			Port:     m.migrationContext.ReplicaPort,
			User:     m.migrationContext.ReplicaUser,
			Password: m.migrationContext.ReplicaPassword,
			Database: m.migrationContext.DatabaseName,
			SSLMode:  m.migrationContext.ReplicaSSLMode,
		}
		if err := m.inspector.ValidateReplicaRequirements(replicaConfig); err != nil {
			return fmt.Errorf("replica validation failed: %w", err)
		}
	}

	// Step 4: Validate table and get unique key
	if err := m.inspector.ValidateOriginalTable(); err != nil {
		return fmt.Errorf("table validation failed: %w", err)
	}

	// Step 5: Set REPLICA IDENTITY FULL
	if err := m.inspector.EnsureReplicaIdentityFull(); err != nil {
		return fmt.Errorf("failed to set replica identity: %w", err)
	}

	// Step 6: Initialize applier
	if err := m.initiateApplier(); err != nil {
		return fmt.Errorf("failed to initiate applier: %w", err)
	}

	// Step 7: Create publication and replication slot
	if err := m.initiateStreaming(); err != nil {
		return fmt.Errorf("failed to initiate streaming: %w", err)
	}

	// Step 8: Create ghost table
	if err := m.applier.CreateGhostTable(); err != nil {
		return fmt.Errorf("failed to create ghost table: %w", err)
	}

	// Step 9: Apply ALTER to ghost table
	if err := m.applier.AlterGhost(); err != nil {
		return fmt.Errorf("failed to alter ghost table: %w", err)
	}

	// Step 10: Create changelog table
	if err := m.applier.CreateChangelogTable(); err != nil {
		return fmt.Errorf("failed to create changelog table: %w", err)
	}

	// Step 11: Start streaming DML events (in background)
	go m.streamEvents()

	// Step 12: Initialize throttler
	if err := m.initiateThrottler(); err != nil {
		return fmt.Errorf("failed to initiate throttler: %w", err)
	}

	// Step 13: Initialize server for online control (optional)
	if err := m.initiateServer(); err != nil {
		return fmt.Errorf("failed to initiate server: %w", err)
	}

	// Step 14: Count rows (optionally)
	if err := m.countTableRows(); err != nil {
		return fmt.Errorf("failed to count table rows: %w", err)
	}

	// Step 15: Read migration range
	if err := m.readMigrationRangeValues(); err != nil {
		return fmt.Errorf("failed to read migration range: %w", err)
	}

	// Step 16: Start applying events (in background)
	go m.executeWriteFuncs()

	// Step 17: Start row copy
	go m.iterateChunks()

	// Step 18: Status printing
	go m.printStatus()

	// Step 19: Wait for row copy to complete
	if err := m.waitForRowCopy(); err != nil {
		return fmt.Errorf("row copy failed: %w", err)
	}

	// Step 20: Cutover
	if err := m.cutOver(); err != nil {
		return fmt.Errorf("cutover failed: %w", err)
	}

	// Step 21: Cleanup
	if err := m.cleanup(); err != nil {
		m.migrationContext.Log.Warning("Cleanup had errors: %v", err)
	}

	m.migrationContext.Log.Info("Migration completed successfully")
	return nil
}

// validateAlterStatement validates the ALTER statement
func (m *Migrator) validateAlterStatement() error {
	if err := m.parser.ParseAlterStatement(m.migrationContext.AlterStatement); err != nil {
		return err
	}
	if m.parser.IsRename() {
		return ErrMigratorUnsupportedRenameAlter
	}
	return nil
}

// initiateInspector initializes the inspector
func (m *Migrator) initiateInspector() error {
	m.inspector = NewInspector(m.migrationContext)
	return m.inspector.InitDBConnections()
}

// initiateApplier initializes the applier
func (m *Migrator) initiateApplier() error {
	m.applier = NewApplier(m.migrationContext)
	return m.applier.InitDBConnections()
}

// initiateStreaming initializes the streaming components
func (m *Migrator) initiateStreaming() error {
	m.streamer = NewStreamer(m.migrationContext)
	return m.streamer.InitDBConnections()
}

// initiateThrottler initializes the throttler
func (m *Migrator) initiateThrottler() error {
	m.throttler = NewThrottler(m.migrationContext, m.applier, m.inspector)
	go m.throttler.Run()
	return nil
}

// initiateServer initializes the online control server
func (m *Migrator) initiateServer() error {
	if m.migrationContext.ServeSocketFile == "" && m.migrationContext.ServeTCPPort == 0 {
		return nil
	}
	m.server = NewServer(m.migrationContext, m.hooksExecutor, m.getStatus)
	return m.server.Start()
}

// countTableRows counts rows in the original table
func (m *Migrator) countTableRows() error {
	if !m.migrationContext.CountTableRows {
		// Use estimated count
		return m.inspector.EstimateTableRows()
	}
	return m.inspector.CountTableRows()
}

// readMigrationRangeValues reads the min/max values for the unique key
func (m *Migrator) readMigrationRangeValues() error {
	return m.applier.ReadMigrationRangeValues()
}

// streamEvents streams DML events from logical replication
func (m *Migrator) streamEvents() {
	ctx := context.Background()
	eventsChan := make(chan *dml.Event, base.MaxEventsBatchSize)

	// Start streaming
	go func() {
		if err := m.streamer.StreamEvents(ctx, eventsChan); err != nil {
			if !m.migrationContext.CanStopStreaming() {
				m.migrationContext.PanicAbort <- fmt.Errorf("streaming error: %w", err)
			}
		}
	}()

	// Process events
	for event := range eventsChan {
		if event.SchemaName == m.migrationContext.SchemaName &&
			event.TableName == m.migrationContext.OriginalTableName {
			// This is a DML event on the original table
			m.applyEventsQueue <- newApplyEventStructByDML(event)
		} else if event.SchemaName == m.migrationContext.SchemaName &&
			event.TableName == m.migrationContext.GetChangelogTableName() {
			// This is an event on the changelog table - handle state changes
			m.handleChangelogEvent(event)
		}
	}
}

// handleChangelogEvent handles events on the changelog table
func (m *Migrator) handleChangelogEvent(event *dml.Event) {
	if event.DML != dml.InsertDML || event.NewColumnValues == nil {
		return
	}

	hint, ok := event.NewColumnValues.GetValueByName("hint")
	if !ok {
		return
	}
	value, ok := event.NewColumnValues.GetValueByName("value")
	if !ok {
		return
	}

	hintStr := fmt.Sprintf("%v", hint)
	valueStr := fmt.Sprintf("%v", value)

	// Track heartbeat
	if hintStr == "heartbeat" {
		m.migrationContext.SetLastHeartbeatOnChangelogTime(time.Now())
		return
	}

	// Handle state changes
	if hintStr == "state" {
		state := ChangelogState(valueStr)
		switch state {
		case AllEventsUpToLockProcessed:
			select {
			case m.allEventsUpToLockProcessed <- valueStr:
			default:
			}
		}
	}
}

// executeWriteFuncs executes write functions with DML events having higher priority
func (m *Migrator) executeWriteFuncs() {
	batch := dml.NewEventBatch(int(m.migrationContext.DMLBatchSize))

	for {
		select {
		case <-m.migrationContext.PanicAbort:
			return
		default:
		}

		// Priority 1: Process DML events
		select {
		case applyEvent := <-m.applyEventsQueue:
			if applyEvent.dmlEvent != nil {
				batch.Add(applyEvent.dmlEvent)
				// Try to batch more events
			DrainEvents:
				for batch.Len() < int(m.migrationContext.DMLBatchSize) {
					select {
					case moreEvent := <-m.applyEventsQueue:
						if moreEvent.dmlEvent != nil {
							batch.Add(moreEvent.dmlEvent)
						}
					default:
						break DrainEvents
					}
				}
				// Apply the batch
				if err := m.applier.ApplyDMLEventQueries(batch.Events); err != nil {
					m.migrationContext.PanicAbort <- err
					return
				}
				m.migrationContext.AddTotalDMLEventsApplied(int64(batch.Len()))
				batch.Clear()
			} else if applyEvent.writeFunc != nil {
				if err := (*applyEvent.writeFunc)(); err != nil {
					m.migrationContext.PanicAbort <- err
					return
				}
			}
		default:
			// Priority 2: Process row copy
			select {
			case copyRowsFunc := <-m.copyRowsQueue:
				if err := copyRowsFunc(); err != nil {
					m.migrationContext.PanicAbort <- err
					return
				}
			case <-time.After(100 * time.Millisecond):
				// No work to do
			}
		}

		// Check if we're done
		if atomic.LoadInt64(&m.finishedMigrating) > 0 {
			return
		}
	}
}

// iterateChunks iterates through chunks of rows to copy
func (m *Migrator) iterateChunks() {
	m.migrationContext.MarkRowCopyStartTime()

	for {
		select {
		case <-m.migrationContext.PanicAbort:
			return
		default:
		}

		// Check throttle
		if throttled, reason, _ := m.migrationContext.IsThrottled(); throttled {
			m.migrationContext.Log.Debug("Throttled: %s", reason)
			time.Sleep(time.Second)
			continue
		}

		// Get next chunk
		hasMore, err := m.applier.CalculateNextIterationRangeEndValues()
		if err != nil {
			m.migrationContext.PanicAbort <- err
			return
		}

		if !hasMore {
			// No more chunks
			m.migrationContext.MarkRowCopyEndTime()
			atomic.StoreInt64(&m.rowCopyCompleteFlag, 1)
			m.rowCopyComplete <- nil
			return
		}

		// Queue the copy
		copyFunc := func() error {
			_, rowsCopied, _, err := m.applier.ApplyIterationInsertQuery()
			if err != nil {
				return err
			}
			m.migrationContext.AddTotalRowsCopied(rowsCopied)
			m.migrationContext.IncrementIteration()

			// Nice ratio sleep
			if m.migrationContext.NiceRatio > 0 {
				time.Sleep(time.Duration(float64(time.Second) * m.migrationContext.NiceRatio))
			}
			return nil
		}

		m.copyRowsQueue <- copyFunc
	}
}

// printStatus prints migration status periodically
func (m *Migrator) printStatus() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.migrationContext.Log.Info(m.getStatus())
		case <-m.migrationContext.PanicAbort:
			return
		}

		if atomic.LoadInt64(&m.finishedMigrating) > 0 {
			return
		}
	}
}

// getStatus returns the current migration status
func (m *Migrator) getStatus() string {
	totalRows := m.migrationContext.GetTotalRowsCopied()
	dmlEvents := m.migrationContext.GetTotalDMLEventsApplied()
	progress := m.migrationContext.GetProgressPct()
	eta := m.migrationContext.GetETA()

	throttled, reason, _ := m.migrationContext.IsThrottled()
	throttleStatus := ""
	if throttled {
		throttleStatus = fmt.Sprintf(" [throttled: %s]", reason)
	}

	return fmt.Sprintf("Copy: %d/%d rows (%.1f%%), DML: %d events, ETA: %v%s",
		totalRows,
		atomic.LoadInt64(&m.migrationContext.RowsEstimate),
		progress,
		dmlEvents,
		eta,
		throttleStatus)
}

// waitForRowCopy waits for row copy to complete
func (m *Migrator) waitForRowCopy() error {
	select {
	case err := <-m.rowCopyComplete:
		return err
	case err := <-m.migrationContext.PanicAbort:
		return err
	}
}

// cutOver performs the table cutover
func (m *Migrator) cutOver() error {
	m.migrationContext.Log.Info("Starting cutover...")

	// Check for postpone flag
	for {
		if m.migrationContext.PostponeCutOverFlagFile != "" {
			// Check if file exists
			// TODO: Implement file check
		}
		break
	}

	// Mark entering critical section
	m.migrationContext.SetInCutOverCriticalSection(true)
	defer m.migrationContext.SetInCutOverCriticalSection(false)

	// Wait for all events to be processed
	if err := m.waitForEventsUpToLock(); err != nil {
		return err
	}

	// Perform atomic cutover
	if err := m.applier.AtomicCutOver(); err != nil {
		return err
	}

	m.migrationContext.MarkCutOverComplete()
	return nil
}

// waitForEventsUpToLock waits for all events up to lock to be processed
func (m *Migrator) waitForEventsUpToLock() error {
	marker := fmt.Sprintf("cutover-%d", time.Now().UnixNano())

	// Write marker to changelog
	if err := m.applier.WriteChangelog("state", marker); err != nil {
		return err
	}

	// Wait for marker to appear via replication
	timeout := time.NewTimer(time.Duration(m.migrationContext.CutOverLockTimeoutSeconds) * time.Second)
	defer timeout.Stop()

	for {
		select {
		case value := <-m.allEventsUpToLockProcessed:
			if value == marker {
				return nil
			}
		case <-timeout.C:
			return fmt.Errorf("timeout waiting for events up to lock")
		case err := <-m.migrationContext.PanicAbort:
			return err
		}
	}
}

// cleanup performs post-migration cleanup
func (m *Migrator) cleanup() error {
	atomic.StoreInt64(&m.finishedMigrating, 1)
	m.migrationContext.MarkCleanupImminent()

	var errs []error

	// Stop server
	if m.server != nil {
		m.server.Stop()
	}

	// Drop old table if requested
	if m.migrationContext.OkToDropTable {
		if err := m.applier.DropOldTable(); err != nil {
			errs = append(errs, err)
		}
	}

	// Drop changelog table
	if err := m.applier.DropChangelogTable(); err != nil {
		errs = append(errs, err)
	}

	// Drop replication slot
	if err := m.streamer.DropReplicationSlot(); err != nil {
		errs = append(errs, err)
	}

	// Drop publication
	if err := m.streamer.DropPublication(); err != nil {
		errs = append(errs, err)
	}

	// Restore replica identity
	if err := m.inspector.RestoreReplicaIdentity(); err != nil {
		errs = append(errs, err)
	}

	// Close connections
	if m.inspector != nil {
		m.inspector.Close()
	}
	if m.applier != nil {
		m.applier.Close()
	}
	if m.streamer != nil {
		m.streamer.Close()
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}
