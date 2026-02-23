/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package replication

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/your-org/pg-ost/internal/base"
	"github.com/your-org/pg-ost/internal/dml"
	"github.com/your-org/pg-ost/internal/pg"
)

// Reader reads from PostgreSQL logical replication stream
type Reader struct {
	connectionConfig *pg.ConnectionConfig
	conn             *pgconn.PgConn
	migrationContext *base.MigrationContext

	slotName        string
	publicationName string
	startLSN        pglogrepl.LSN
	currentLSN      pglogrepl.LSN
	lastAckLSN      pglogrepl.LSN

	decoder *PgOutputDecoder

	// Standby status update interval
	standbyMessageTimeout time.Duration
	nextStandbyUpdate     time.Time
}

// NewReader creates a new replication reader
func NewReader(ctx *base.MigrationContext, connConfig *pg.ConnectionConfig) *Reader {
	return &Reader{
		connectionConfig:      connConfig,
		migrationContext:      ctx,
		slotName:              ctx.ReplicationSlotName,
		publicationName:       ctx.PublicationName,
		standbyMessageTimeout: 10 * time.Second,
		decoder:               NewPgOutputDecoder(),
	}
}

// Connect establishes the replication connection
func (r *Reader) Connect(ctx context.Context) error {
	conn, err := r.connectionConfig.ConnectReplication(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect for replication: %w", err)
	}
	r.conn = conn
	return nil
}

// Close closes the replication connection
func (r *Reader) Close() error {
	if r.conn != nil {
		return r.conn.Close(context.Background())
	}
	return nil
}

// GetSlotName returns the replication slot name
func (r *Reader) GetSlotName() string {
	return r.slotName
}

// GetPublicationName returns the publication name
func (r *Reader) GetPublicationName() string {
	return r.publicationName
}

// GetCurrentLSN returns the current LSN position
func (r *Reader) GetCurrentLSN() pglogrepl.LSN {
	return r.currentLSN
}

// SetStartLSN sets the starting LSN for streaming
func (r *Reader) SetStartLSN(lsn pglogrepl.LSN) {
	r.startLSN = lsn
}

// StartReplication starts the logical replication stream
func (r *Reader) StartReplication(ctx context.Context) error {
	if r.startLSN == 0 {
		// Get current LSN from slot
		lsn, err := r.getSlotConfirmedLSN(ctx)
		if err != nil {
			return fmt.Errorf("failed to get slot LSN: %w", err)
		}
		r.startLSN = lsn
	}

	// Build plugin options for pgoutput
	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", r.publicationName),
		"messages 'true'",
		"streaming 'true'",
	}

	err := pglogrepl.StartReplication(
		ctx,
		r.conn,
		r.slotName,
		r.startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArguments,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	r.currentLSN = r.startLSN
	r.nextStandbyUpdate = time.Now().Add(r.standbyMessageTimeout)

	r.migrationContext.Log.Info("Started replication from LSN %s", r.startLSN)
	return nil
}

// getSlotConfirmedLSN gets the confirmed flush LSN from the replication slot
func (r *Reader) getSlotConfirmedLSN(ctx context.Context) (pglogrepl.LSN, error) {
	result := r.conn.Exec(ctx, fmt.Sprintf(
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'",
		r.slotName,
	))

	// Read result rows
	var lsnStr string
	results, err := result.ReadAll()
	if err != nil {
		return 0, err
	}

	for _, res := range results {
		for _, row := range res.Rows {
			if len(row) > 0 {
				lsnStr = string(row[0])
			}
		}
	}

	if lsnStr == "" {
		return 0, fmt.Errorf("replication slot %s not found", r.slotName)
	}

	return pglogrepl.ParseLSN(lsnStr)
}

// StreamEvents streams replication events to the provided channel
func (r *Reader) StreamEvents(ctx context.Context, events chan<- *dml.Event) error {
	var currentXID uint32

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check if we should stop
		if r.migrationContext.CanStopStreaming() {
			return nil
		}

		// Send standby status update if needed
		if time.Now().After(r.nextStandbyUpdate) {
			if err := r.sendStandbyStatusUpdate(ctx); err != nil {
				return fmt.Errorf("failed to send standby status: %w", err)
			}
			r.nextStandbyUpdate = time.Now().Add(r.standbyMessageTimeout)
		}

		// Receive message with timeout
		receiveCtx, cancel := context.WithTimeout(ctx, r.standbyMessageTimeout)
		rawMsg, err := r.conn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("failed to receive message: %w", err)
		}

		// Handle different message types
		switch msg := rawMsg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse keepalive: %w", err)
				}
				if pkm.ReplyRequested {
					if err := r.sendStandbyStatusUpdate(ctx); err != nil {
						return fmt.Errorf("failed to send standby status: %w", err)
					}
					r.nextStandbyUpdate = time.Now().Add(r.standbyMessageTimeout)
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse xlog data: %w", err)
				}

				r.currentLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				r.migrationContext.SetCurrentLSN(r.currentLSN)

				// Parse the logical replication message
				event, err := r.decoder.Decode(xld.WALData, xld.WALStart, currentXID)
				if err != nil {
					return fmt.Errorf("failed to decode message: %w", err)
				}

				if event != nil {
					select {
					case events <- event:
					case <-ctx.Done():
						return ctx.Err()
					}
				}

				// Track transaction ID from begin messages
				if r.decoder.LastBeginXID != 0 {
					currentXID = r.decoder.LastBeginXID
					r.decoder.LastBeginXID = 0
				}
			}
		}
	}
}

// sendStandbyStatusUpdate sends a standby status update to the server
func (r *Reader) sendStandbyStatusUpdate(ctx context.Context) error {
	// Acknowledge up to current LSN
	err := pglogrepl.SendStandbyStatusUpdate(
		ctx,
		r.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: r.currentLSN,
			WALFlushPosition: r.currentLSN,
			WALApplyPosition: r.currentLSN,
			ClientTime:       time.Now(),
			ReplyRequested:   false,
		},
	)
	if err != nil {
		return err
	}
	r.lastAckLSN = r.currentLSN
	return nil
}

// AcknowledgeLSN explicitly acknowledges up to the given LSN
func (r *Reader) AcknowledgeLSN(ctx context.Context, lsn pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(
		ctx,
		r.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lsn,
			WALFlushPosition: lsn,
			WALApplyPosition: lsn,
			ClientTime:       time.Now(),
			ReplyRequested:   false,
		},
	)
}
