/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package replication

import (
	"fmt"

	"github.com/jackc/pglogrepl"

	"github.com/your-org/pg-ost/internal/dml"
	"github.com/your-org/pg-ost/internal/sql"
)

// PgOutputDecoder decodes pgoutput format messages
type PgOutputDecoder struct {
	// relations maps relation ID to relation info
	relations map[uint32]*pglogrepl.RelationMessageV2
	// typeMap maps type OID to type info
	typeMap map[uint32]*pglogrepl.TypeMessageV2
	// LastBeginXID holds the XID from the last BEGIN message
	LastBeginXID uint32
}

// NewPgOutputDecoder creates a new decoder
func NewPgOutputDecoder() *PgOutputDecoder {
	return &PgOutputDecoder{
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
		typeMap:   make(map[uint32]*pglogrepl.TypeMessageV2),
	}
}

// Decode decodes a pgoutput message and returns a DML event if applicable
func (d *PgOutputDecoder) Decode(data []byte, lsn pglogrepl.LSN, xid uint32) (*dml.Event, error) {
	msg, err := pglogrepl.ParseV2(data, false) // false = not in streaming mode
	if err != nil {
		return nil, fmt.Errorf("parse message: %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.RelationMessageV2:
		d.relations[m.RelationID] = m
		return nil, nil

	case *pglogrepl.TypeMessageV2:
		d.typeMap[m.DataType] = m
		return nil, nil

	case *pglogrepl.InsertMessageV2:
		return d.decodeInsert(m, lsn, xid)

	case *pglogrepl.UpdateMessageV2:
		return d.decodeUpdate(m, lsn, xid)

	case *pglogrepl.DeleteMessageV2:
		return d.decodeDelete(m, lsn, xid)

	case *pglogrepl.BeginMessage:
		d.LastBeginXID = m.Xid
		return nil, nil

	case *pglogrepl.CommitMessage:
		// Commit message - could track commit time
		return nil, nil

	case *pglogrepl.OriginMessage:
		// Origin message - ignore for now
		return nil, nil

	case *pglogrepl.TruncateMessageV2:
		// Truncate is not supported in online schema change
		return nil, fmt.Errorf("TRUNCATE is not supported during migration")

	case *pglogrepl.StreamStartMessageV2:
		// Streaming transaction start (PostgreSQL 14+)
		return nil, nil

	case *pglogrepl.StreamStopMessageV2:
		// Streaming transaction stop
		return nil, nil

	case *pglogrepl.StreamCommitMessageV2:
		// Streaming transaction commit
		return nil, nil

	case *pglogrepl.StreamAbortMessageV2:
		// Streaming transaction abort
		return nil, nil

	case *pglogrepl.LogicalDecodingMessageV2:
		// Logical decoding message - ignore
		return nil, nil

	default:
		// Unknown message type - log and skip
		return nil, nil
	}
}

// decodeInsert decodes an INSERT message
func (d *PgOutputDecoder) decodeInsert(msg *pglogrepl.InsertMessageV2, lsn pglogrepl.LSN, xid uint32) (*dml.Event, error) {
	rel, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	newValues, err := d.decodeTuple(rel, msg.Tuple)
	if err != nil {
		return nil, fmt.Errorf("decode new tuple: %w", err)
	}

	return dml.NewInsertEvent(rel.Namespace, rel.RelationName, newValues, lsn, xid), nil
}

// decodeUpdate decodes an UPDATE message
func (d *PgOutputDecoder) decodeUpdate(msg *pglogrepl.UpdateMessageV2, lsn pglogrepl.LSN, xid uint32) (*dml.Event, error) {
	rel, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	var oldValues *sql.ColumnValues
	if msg.OldTuple != nil {
		var err error
		oldValues, err = d.decodeTuple(rel, msg.OldTuple)
		if err != nil {
			return nil, fmt.Errorf("decode old tuple: %w", err)
		}
	}

	newValues, err := d.decodeTuple(rel, msg.NewTuple)
	if err != nil {
		return nil, fmt.Errorf("decode new tuple: %w", err)
	}

	return dml.NewUpdateEvent(rel.Namespace, rel.RelationName, oldValues, newValues, lsn, xid), nil
}

// decodeDelete decodes a DELETE message
func (d *PgOutputDecoder) decodeDelete(msg *pglogrepl.DeleteMessageV2, lsn pglogrepl.LSN, xid uint32) (*dml.Event, error) {
	rel, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	oldValues, err := d.decodeTuple(rel, msg.OldTuple)
	if err != nil {
		return nil, fmt.Errorf("decode old tuple: %w", err)
	}

	return dml.NewDeleteEvent(rel.Namespace, rel.RelationName, oldValues, lsn, xid), nil
}

// decodeTuple decodes a tuple data into column values
func (d *PgOutputDecoder) decodeTuple(rel *pglogrepl.RelationMessageV2, tuple *pglogrepl.TupleData) (*sql.ColumnValues, error) {
	if tuple == nil {
		return nil, nil
	}

	columns := make([]string, len(rel.Columns))
	for i, col := range rel.Columns {
		columns[i] = col.Name
	}

	values := sql.NewColumnValues(columns)

	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}

		switch col.DataType {
		case 'n': // NULL
			values.SetValue(i, nil)
		case 'u': // Unchanged TOAST value
			// For unchanged TOAST values, we need the old value
			// This shouldn't happen with REPLICA IDENTITY FULL
			values.SetValue(i, nil)
		case 't': // Text value
			values.SetValue(i, string(col.Data))
		case 'b': // Binary value
			// Binary values are sent as-is
			values.SetValue(i, col.Data)
		}
	}

	return values, nil
}

// GetRelation returns the relation info for a given ID
func (d *PgOutputDecoder) GetRelation(id uint32) (*pglogrepl.RelationMessageV2, bool) {
	rel, ok := d.relations[id]
	return rel, ok
}

// Clear clears the decoder state
func (d *PgOutputDecoder) Clear() {
	d.relations = make(map[uint32]*pglogrepl.RelationMessageV2)
	d.typeMap = make(map[uint32]*pglogrepl.TypeMessageV2)
	d.LastBeginXID = 0
}
