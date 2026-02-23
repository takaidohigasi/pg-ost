/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package logic

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"

	"github.com/your-org/pg-ost/internal/base"
	"github.com/your-org/pg-ost/internal/dml"
	"github.com/your-org/pg-ost/internal/pg"
	"github.com/your-org/pg-ost/internal/replication"
)

// Streamer wraps the replication components for streaming DML events
type Streamer struct {
	connectionConfig *pg.ConnectionConfig
	migrationContext *base.MigrationContext

	reader      *replication.Reader
	slotManager *replication.SlotManager
	pubManager  *replication.PublicationManager
}

// NewStreamer creates a new Streamer
func NewStreamer(ctx *base.MigrationContext) *Streamer {
	connConfig := &pg.ConnectionConfig{
		Host:     ctx.Host,
		Port:     ctx.Port,
		User:     ctx.User,
		Password: ctx.Password,
		Database: ctx.DatabaseName,
		SSLMode:  ctx.SSLMode,
	}

	return &Streamer{
		connectionConfig: connConfig,
		migrationContext: ctx,
	}
}

// InitDBConnections initializes database connections for replication
func (s *Streamer) InitDBConnections() error {
	ctx := context.Background()

	// Create a regular database connection for slot and publication management
	db, err := s.connectionConfig.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Generate names for publication and slot
	if s.migrationContext.PublicationName == "" {
		s.migrationContext.PublicationName = fmt.Sprintf("pgost_%s_%s",
			s.migrationContext.SchemaName,
			s.migrationContext.OriginalTableName)
	}
	if s.migrationContext.ReplicationSlotName == "" {
		s.migrationContext.ReplicationSlotName = fmt.Sprintf("pgost_%s_%s",
			s.migrationContext.SchemaName,
			s.migrationContext.OriginalTableName)
	}

	// Initialize slot manager
	s.slotManager = replication.NewSlotManager(db, s.migrationContext.ReplicationSlotName)

	// Initialize publication manager (for original table and changelog)
	s.pubManager = replication.NewPublicationManager(
		db,
		s.migrationContext.PublicationName,
		s.migrationContext.SchemaName,
		s.migrationContext.OriginalTableName,
	)

	// Drop existing publication if exists
	exists, err := s.pubManager.Exists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check publication: %w", err)
	}
	if exists {
		if err := s.pubManager.Drop(ctx); err != nil {
			return fmt.Errorf("failed to drop existing publication: %w", err)
		}
	}

	// Create publication for original table
	if err := s.pubManager.Create(ctx); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}
	s.migrationContext.Log.Info("Created publication %s", s.migrationContext.PublicationName)

	// Add changelog table to publication
	if err := s.pubManager.AddTable(ctx, s.migrationContext.SchemaName, s.migrationContext.GetChangelogTableName()); err != nil {
		// Changelog table may not exist yet, that's okay
		s.migrationContext.Log.Debug("Could not add changelog to publication (may not exist yet): %v", err)
	}

	// Drop existing slot if exists
	slotExists, err := s.slotManager.Exists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check slot: %w", err)
	}
	if slotExists {
		// Check if slot is active
		active, err := s.slotManager.IsActive(ctx)
		if err != nil {
			return fmt.Errorf("failed to check slot status: %w", err)
		}
		if active {
			return fmt.Errorf("replication slot %s is already in use", s.migrationContext.ReplicationSlotName)
		}
		if err := s.slotManager.Drop(ctx); err != nil {
			return fmt.Errorf("failed to drop existing slot: %w", err)
		}
	}

	// Create replication slot
	lsn, err := s.slotManager.Create(ctx)
	if err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}
	s.migrationContext.StartLSN = lsn
	s.migrationContext.Log.Info("Created replication slot %s at LSN %s",
		s.migrationContext.ReplicationSlotName, lsn)

	// Initialize reader
	s.reader = replication.NewReader(s.migrationContext, s.connectionConfig)

	return nil
}

// Close closes all connections
func (s *Streamer) Close() {
	if s.reader != nil {
		s.reader.Close()
	}
}

// StreamEvents starts streaming events to the provided channel
func (s *Streamer) StreamEvents(ctx context.Context, events chan<- *dml.Event) error {
	// Connect for replication
	if err := s.reader.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect reader: %w", err)
	}

	// Start replication
	if err := s.reader.StartReplication(ctx); err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	// Stream events
	return s.reader.StreamEvents(ctx, events)
}

// GetCurrentLSN returns the current LSN position
func (s *Streamer) GetCurrentLSN() pglogrepl.LSN {
	if s.reader != nil {
		return s.reader.GetCurrentLSN()
	}
	return s.migrationContext.GetCurrentLSN()
}

// DropReplicationSlot drops the replication slot
func (s *Streamer) DropReplicationSlot() error {
	if s.slotManager == nil {
		return nil
	}

	ctx := context.Background()
	exists, err := s.slotManager.Exists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check slot: %w", err)
	}
	if !exists {
		return nil
	}

	if err := s.slotManager.Drop(ctx); err != nil {
		return fmt.Errorf("failed to drop slot: %w", err)
	}

	s.migrationContext.Log.Info("Dropped replication slot %s", s.migrationContext.ReplicationSlotName)
	return nil
}

// DropPublication drops the publication
func (s *Streamer) DropPublication() error {
	if s.pubManager == nil {
		return nil
	}

	ctx := context.Background()
	if err := s.pubManager.Drop(ctx); err != nil {
		return fmt.Errorf("failed to drop publication: %w", err)
	}

	s.migrationContext.Log.Info("Dropped publication %s", s.migrationContext.PublicationName)
	return nil
}

// AddChangelogToPublication adds the changelog table to the publication
func (s *Streamer) AddChangelogToPublication() error {
	if s.pubManager == nil {
		return nil
	}

	ctx := context.Background()
	return s.pubManager.AddTable(ctx,
		s.migrationContext.SchemaName,
		s.migrationContext.GetChangelogTableName())
}

// GetReplicationLag returns the replication lag in bytes
func (s *Streamer) GetReplicationLag() (int64, error) {
	if s.slotManager == nil {
		return 0, nil
	}

	ctx := context.Background()
	return s.slotManager.GetLag(ctx)
}
