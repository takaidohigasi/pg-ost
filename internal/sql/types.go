/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package sql

import (
	"database/sql"
	"fmt"
	"strings"
)

// Column represents a database column
type Column struct {
	Name         string
	Type         string
	IsNullable   bool
	DefaultValue sql.NullString
	OrdinalPos   int
}

// ColumnList is a list of columns
type ColumnList struct {
	Columns []Column
}

// NewColumnList creates a new empty column list
func NewColumnList() *ColumnList {
	return &ColumnList{
		Columns: make([]Column, 0),
	}
}

// Add adds a column to the list
func (cl *ColumnList) Add(col Column) {
	cl.Columns = append(cl.Columns, col)
}

// Names returns a slice of column names
func (cl *ColumnList) Names() []string {
	names := make([]string, len(cl.Columns))
	for i, col := range cl.Columns {
		names[i] = col.Name
	}
	return names
}

// QuotedNames returns column names as quoted identifiers
func (cl *ColumnList) QuotedNames() []string {
	names := make([]string, len(cl.Columns))
	for i, col := range cl.Columns {
		names[i] = QuoteIdentifier(col.Name)
	}
	return names
}

// Len returns the number of columns
func (cl *ColumnList) Len() int {
	return len(cl.Columns)
}

// UniqueKey represents a unique key (primary key or unique index)
type UniqueKey struct {
	Name       string
	Columns    []string
	IsPrimary  bool
	IsNullable bool // True if any column is nullable
}

// NewUniqueKey creates a new unique key
func NewUniqueKey(name string, columns []string, isPrimary bool) *UniqueKey {
	return &UniqueKey{
		Name:      name,
		Columns:   columns,
		IsPrimary: isPrimary,
	}
}

// QuotedColumns returns the column names as quoted identifiers
func (uk *UniqueKey) QuotedColumns() []string {
	quoted := make([]string, len(uk.Columns))
	for i, col := range uk.Columns {
		quoted[i] = QuoteIdentifier(col)
	}
	return quoted
}

// String returns a string representation of the unique key
func (uk *UniqueKey) String() string {
	keyType := "UNIQUE"
	if uk.IsPrimary {
		keyType = "PRIMARY KEY"
	}
	return fmt.Sprintf("%s %s(%s)", uk.Name, keyType, strings.Join(uk.Columns, ", "))
}

// Len returns the number of columns in the key
func (uk *UniqueKey) Len() int {
	return len(uk.Columns)
}

// ColumnValues represents values for a set of columns
type ColumnValues struct {
	Columns []string
	Values  []interface{}
}

// NewColumnValues creates a new ColumnValues
func NewColumnValues(columns []string) *ColumnValues {
	return &ColumnValues{
		Columns: columns,
		Values:  make([]interface{}, len(columns)),
	}
}

// SetValue sets the value for a column by index
func (cv *ColumnValues) SetValue(idx int, value interface{}) {
	if idx >= 0 && idx < len(cv.Values) {
		cv.Values[idx] = value
	}
}

// GetValue gets the value for a column by index
func (cv *ColumnValues) GetValue(idx int) interface{} {
	if idx >= 0 && idx < len(cv.Values) {
		return cv.Values[idx]
	}
	return nil
}

// GetValueByName gets the value for a column by name
func (cv *ColumnValues) GetValueByName(name string) (interface{}, bool) {
	for i, col := range cv.Columns {
		if col == name {
			return cv.Values[i], true
		}
	}
	return nil, false
}

// Len returns the number of columns
func (cv *ColumnValues) Len() int {
	return len(cv.Columns)
}

// ValuesAsArgs returns the values as a slice suitable for query args
func (cv *ColumnValues) ValuesAsArgs() []interface{} {
	return cv.Values
}

// ReplicaIdentity represents the replica identity setting for a table
type ReplicaIdentity string

const (
	ReplicaIdentityDefault ReplicaIdentity = "d" // DEFAULT
	ReplicaIdentityNothing ReplicaIdentity = "n" // NOTHING
	ReplicaIdentityFull    ReplicaIdentity = "f" // FULL
	ReplicaIdentityIndex   ReplicaIdentity = "i" // INDEX
)

// String returns the replica identity as a human-readable string
func (ri ReplicaIdentity) String() string {
	switch ri {
	case ReplicaIdentityDefault:
		return "DEFAULT"
	case ReplicaIdentityNothing:
		return "NOTHING"
	case ReplicaIdentityFull:
		return "FULL"
	case ReplicaIdentityIndex:
		return "INDEX"
	default:
		return string(ri)
	}
}
