/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package dml

import (
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/takaidohigasi/pg-ost/internal/sql"
)

// EventDML represents the type of DML operation
type EventDML int

const (
	// InsertDML represents an INSERT operation
	InsertDML EventDML = iota
	// UpdateDML represents an UPDATE operation
	UpdateDML
	// DeleteDML represents a DELETE operation
	DeleteDML
)

// String returns the string representation of the DML type
func (e EventDML) String() string {
	switch e {
	case InsertDML:
		return "INSERT"
	case UpdateDML:
		return "UPDATE"
	case DeleteDML:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// Event represents a DML event captured from logical replication
type Event struct {
	// SchemaName is the schema containing the table
	SchemaName string
	// TableName is the name of the table
	TableName string
	// DML is the type of operation (INSERT, UPDATE, DELETE)
	DML EventDML
	// OldColumnValues contains the old values (for UPDATE and DELETE)
	// Requires REPLICA IDENTITY FULL to have values for non-key columns
	OldColumnValues *sql.ColumnValues
	// NewColumnValues contains the new values (for INSERT and UPDATE)
	NewColumnValues *sql.ColumnValues
	// LSN is the log sequence number of this event
	LSN pglogrepl.LSN
	// XID is the transaction ID
	XID uint32
}

// NewInsertEvent creates a new INSERT event
func NewInsertEvent(schemaName, tableName string, newValues *sql.ColumnValues, lsn pglogrepl.LSN, xid uint32) *Event {
	return &Event{
		SchemaName:      schemaName,
		TableName:       tableName,
		DML:             InsertDML,
		NewColumnValues: newValues,
		LSN:             lsn,
		XID:             xid,
	}
}

// NewUpdateEvent creates a new UPDATE event
func NewUpdateEvent(schemaName, tableName string, oldValues, newValues *sql.ColumnValues, lsn pglogrepl.LSN, xid uint32) *Event {
	return &Event{
		SchemaName:      schemaName,
		TableName:       tableName,
		DML:             UpdateDML,
		OldColumnValues: oldValues,
		NewColumnValues: newValues,
		LSN:             lsn,
		XID:             xid,
	}
}

// NewDeleteEvent creates a new DELETE event
func NewDeleteEvent(schemaName, tableName string, oldValues *sql.ColumnValues, lsn pglogrepl.LSN, xid uint32) *Event {
	return &Event{
		SchemaName:      schemaName,
		TableName:       tableName,
		DML:             DeleteDML,
		OldColumnValues: oldValues,
		LSN:             lsn,
		XID:             xid,
	}
}

// String returns a string representation of the event
func (e *Event) String() string {
	return fmt.Sprintf("%s %s.%s @ %s (xid=%d)", e.DML.String(), e.SchemaName, e.TableName, e.LSN, e.XID)
}

// Entry represents a replication entry that may contain multiple events
// or control messages (BEGIN, COMMIT, etc.)
type Entry struct {
	// Events contains the DML events in this entry
	Events []*Event
	// LSN is the log sequence number
	LSN pglogrepl.LSN
	// XID is the transaction ID
	XID uint32
	// IsCommit indicates this is a COMMIT entry
	IsCommit bool
	// IsBegin indicates this is a BEGIN entry
	IsBegin bool
	// CommitTime is the commit timestamp (for COMMIT entries)
	CommitTime int64
}

// NewEntry creates a new entry
func NewEntry() *Entry {
	return &Entry{
		Events: make([]*Event, 0),
	}
}

// AddEvent adds an event to the entry
func (e *Entry) AddEvent(event *Event) {
	e.Events = append(e.Events, event)
}

// HasEvents returns true if the entry has events
func (e *Entry) HasEvents() bool {
	return len(e.Events) > 0
}

// Len returns the number of events
func (e *Entry) Len() int {
	return len(e.Events)
}

// EventBatch represents a batch of events to be applied
type EventBatch struct {
	Events []*Event
}

// NewEventBatch creates a new event batch
func NewEventBatch(capacity int) *EventBatch {
	return &EventBatch{
		Events: make([]*Event, 0, capacity),
	}
}

// Add adds an event to the batch
func (b *EventBatch) Add(event *Event) {
	b.Events = append(b.Events, event)
}

// Len returns the number of events in the batch
func (b *EventBatch) Len() int {
	return len(b.Events)
}

// Clear clears the batch
func (b *EventBatch) Clear() {
	b.Events = b.Events[:0]
}
