/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package replication

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pglogrepl"
)

// SlotManager manages PostgreSQL logical replication slots
type SlotManager struct {
	db       *sql.DB
	slotName string
}

// NewSlotManager creates a new slot manager
func NewSlotManager(db *sql.DB, slotName string) *SlotManager {
	return &SlotManager{
		db:       db,
		slotName: slotName,
	}
}

// Create creates a new logical replication slot
// Returns the consistent point LSN
func (s *SlotManager) Create(ctx context.Context) (pglogrepl.LSN, error) {
	var slotName, consistentPoint string
	var snapshotName, plugin sql.NullString

	err := s.db.QueryRowContext(ctx,
		"SELECT slot_name, consistent_point, snapshot_name, plugin FROM pg_create_logical_replication_slot($1, 'pgoutput')",
		s.slotName,
	).Scan(&slotName, &consistentPoint, &snapshotName, &plugin)

	if err != nil {
		return 0, fmt.Errorf("failed to create replication slot %s: %w", s.slotName, err)
	}

	lsn, err := pglogrepl.ParseLSN(consistentPoint)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN %s: %w", consistentPoint, err)
	}

	return lsn, nil
}

// Drop drops the replication slot
func (s *SlotManager) Drop(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx,
		"SELECT pg_drop_replication_slot($1)",
		s.slotName,
	)
	if err != nil {
		return fmt.Errorf("failed to drop replication slot %s: %w", s.slotName, err)
	}
	return nil
}

// Exists checks if the replication slot exists
func (s *SlotManager) Exists(ctx context.Context) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1",
		s.slotName,
	).Scan(&count)

	if err != nil {
		return false, fmt.Errorf("failed to check replication slot: %w", err)
	}

	return count > 0, nil
}

// GetConfirmedLSN returns the confirmed flush LSN for the slot
func (s *SlotManager) GetConfirmedLSN(ctx context.Context) (pglogrepl.LSN, error) {
	var lsnStr sql.NullString
	err := s.db.QueryRowContext(ctx,
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
		s.slotName,
	).Scan(&lsnStr)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("replication slot %s not found", s.slotName)
		}
		return 0, fmt.Errorf("failed to get confirmed LSN: %w", err)
	}

	if !lsnStr.Valid {
		return 0, nil
	}

	return pglogrepl.ParseLSN(lsnStr.String)
}

// GetRestartLSN returns the restart LSN for the slot
func (s *SlotManager) GetRestartLSN(ctx context.Context) (pglogrepl.LSN, error) {
	var lsnStr sql.NullString
	err := s.db.QueryRowContext(ctx,
		"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = $1",
		s.slotName,
	).Scan(&lsnStr)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("replication slot %s not found", s.slotName)
		}
		return 0, fmt.Errorf("failed to get restart LSN: %w", err)
	}

	if !lsnStr.Valid {
		return 0, nil
	}

	return pglogrepl.ParseLSN(lsnStr.String)
}

// IsActive checks if the replication slot is currently active
func (s *SlotManager) IsActive(ctx context.Context) (bool, error) {
	var active sql.NullBool
	err := s.db.QueryRowContext(ctx,
		"SELECT active FROM pg_replication_slots WHERE slot_name = $1",
		s.slotName,
	).Scan(&active)

	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("failed to check slot active status: %w", err)
	}

	return active.Valid && active.Bool, nil
}

// GetLag returns the replication lag in bytes
func (s *SlotManager) GetLag(ctx context.Context) (int64, error) {
	var lag sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
		 FROM pg_replication_slots WHERE slot_name = $1`,
		s.slotName,
	).Scan(&lag)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("replication slot %s not found", s.slotName)
		}
		return 0, fmt.Errorf("failed to get replication lag: %w", err)
	}

	if !lag.Valid {
		return 0, nil
	}

	return lag.Int64, nil
}

// Advance advances the replication slot to a given LSN
func (s *SlotManager) Advance(ctx context.Context, lsn pglogrepl.LSN) error {
	_, err := s.db.ExecContext(ctx,
		"SELECT pg_replication_slot_advance($1, $2)",
		s.slotName, lsn.String(),
	)
	if err != nil {
		return fmt.Errorf("failed to advance replication slot: %w", err)
	}
	return nil
}
