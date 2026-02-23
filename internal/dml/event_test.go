/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package dml

import (
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/takaidohigasi/pg-ost/internal/sql"
)

func TestEventDMLString(t *testing.T) {
	tests := []struct {
		dml  EventDML
		want string
	}{
		{InsertDML, "INSERT"},
		{UpdateDML, "UPDATE"},
		{DeleteDML, "DELETE"},
		{EventDML(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.dml.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNewInsertEvent(t *testing.T) {
	cols := []string{"id", "name"}
	values := sql.NewColumnValues(cols)
	values.SetValue(0, 1)
	values.SetValue(1, "test")

	event := NewInsertEvent("public", "users", values, pglogrepl.LSN(100), 12345)

	if event.SchemaName != "public" {
		t.Errorf("SchemaName = %q, want 'public'", event.SchemaName)
	}
	if event.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", event.TableName)
	}
	if event.DML != InsertDML {
		t.Errorf("DML = %v, want InsertDML", event.DML)
	}
	if event.OldColumnValues != nil {
		t.Error("OldColumnValues should be nil for INSERT")
	}
	if event.NewColumnValues == nil {
		t.Error("NewColumnValues should not be nil")
	}
	if event.LSN != pglogrepl.LSN(100) {
		t.Errorf("LSN = %v, want 100", event.LSN)
	}
	if event.XID != 12345 {
		t.Errorf("XID = %d, want 12345", event.XID)
	}
}

func TestNewUpdateEvent(t *testing.T) {
	cols := []string{"id", "name"}
	oldValues := sql.NewColumnValues(cols)
	oldValues.SetValue(0, 1)
	oldValues.SetValue(1, "old")

	newValues := sql.NewColumnValues(cols)
	newValues.SetValue(0, 1)
	newValues.SetValue(1, "new")

	event := NewUpdateEvent("public", "users", oldValues, newValues, pglogrepl.LSN(200), 12346)

	if event.DML != UpdateDML {
		t.Errorf("DML = %v, want UpdateDML", event.DML)
	}
	if event.OldColumnValues == nil {
		t.Error("OldColumnValues should not be nil for UPDATE")
	}
	if event.NewColumnValues == nil {
		t.Error("NewColumnValues should not be nil for UPDATE")
	}
}

func TestNewDeleteEvent(t *testing.T) {
	cols := []string{"id", "name"}
	oldValues := sql.NewColumnValues(cols)
	oldValues.SetValue(0, 1)
	oldValues.SetValue(1, "deleted")

	event := NewDeleteEvent("public", "users", oldValues, pglogrepl.LSN(300), 12347)

	if event.DML != DeleteDML {
		t.Errorf("DML = %v, want DeleteDML", event.DML)
	}
	if event.OldColumnValues == nil {
		t.Error("OldColumnValues should not be nil for DELETE")
	}
	if event.NewColumnValues != nil {
		t.Error("NewColumnValues should be nil for DELETE")
	}
}

func TestEventString(t *testing.T) {
	cols := []string{"id"}
	values := sql.NewColumnValues(cols)
	values.SetValue(0, 1)

	event := NewInsertEvent("public", "users", values, pglogrepl.LSN(100), 12345)
	str := event.String()

	expected := "INSERT public.users @ 0/64 (xid=12345)"
	if str != expected {
		t.Errorf("String() = %q, want %q", str, expected)
	}
}

func TestEntry(t *testing.T) {
	entry := NewEntry()

	if entry.HasEvents() {
		t.Error("New entry should not have events")
	}
	if entry.Len() != 0 {
		t.Errorf("Len() = %d, want 0", entry.Len())
	}

	// Add events
	cols := []string{"id"}
	values := sql.NewColumnValues(cols)
	values.SetValue(0, 1)

	event1 := NewInsertEvent("public", "users", values, pglogrepl.LSN(100), 1)
	event2 := NewInsertEvent("public", "users", values, pglogrepl.LSN(101), 1)

	entry.AddEvent(event1)
	entry.AddEvent(event2)

	if !entry.HasEvents() {
		t.Error("Entry should have events after adding")
	}
	if entry.Len() != 2 {
		t.Errorf("Len() = %d, want 2", entry.Len())
	}
}

func TestEventBatch(t *testing.T) {
	batch := NewEventBatch(10)

	if batch.Len() != 0 {
		t.Errorf("New batch Len() = %d, want 0", batch.Len())
	}

	// Add events
	cols := []string{"id"}
	values := sql.NewColumnValues(cols)
	values.SetValue(0, 1)

	for i := 0; i < 5; i++ {
		event := NewInsertEvent("public", "users", values, pglogrepl.LSN(uint64(i)), uint32(i))
		batch.Add(event)
	}

	if batch.Len() != 5 {
		t.Errorf("Len() = %d, want 5", batch.Len())
	}

	// Clear batch
	batch.Clear()
	if batch.Len() != 0 {
		t.Errorf("After Clear(), Len() = %d, want 0", batch.Len())
	}
}
