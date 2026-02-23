/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package sql

import (
	"testing"
)

func TestColumnList(t *testing.T) {
	cl := NewColumnList()

	cl.Add(Column{Name: "id", Type: "integer", OrdinalPos: 1})
	cl.Add(Column{Name: "name", Type: "varchar", OrdinalPos: 2})
	cl.Add(Column{Name: "email", Type: "varchar", IsNullable: true, OrdinalPos: 3})

	if cl.Len() != 3 {
		t.Errorf("Len() = %d, want 3", cl.Len())
	}

	names := cl.Names()
	if len(names) != 3 {
		t.Errorf("Names() returned %d items, want 3", len(names))
	}
	if names[0] != "id" || names[1] != "name" || names[2] != "email" {
		t.Errorf("Names() = %v, want [id, name, email]", names)
	}

	quotedNames := cl.QuotedNames()
	if quotedNames[0] != `"id"` {
		t.Errorf("QuotedNames()[0] = %q, want %q", quotedNames[0], `"id"`)
	}
}

func TestUniqueKey(t *testing.T) {
	pk := NewUniqueKey("PRIMARY", []string{"id"}, true)

	if pk.Name != "PRIMARY" {
		t.Errorf("Name = %q, want PRIMARY", pk.Name)
	}

	if !pk.IsPrimary {
		t.Error("IsPrimary should be true")
	}

	if pk.Len() != 1 {
		t.Errorf("Len() = %d, want 1", pk.Len())
	}

	quoted := pk.QuotedColumns()
	if quoted[0] != `"id"` {
		t.Errorf("QuotedColumns()[0] = %q, want %q", quoted[0], `"id"`)
	}

	str := pk.String()
	if str != "PRIMARY PRIMARY KEY(id)" {
		t.Errorf("String() = %q", str)
	}
}

func TestColumnValues(t *testing.T) {
	columns := []string{"id", "name", "email"}
	cv := NewColumnValues(columns)

	cv.SetValue(0, 1)
	cv.SetValue(1, "John")
	cv.SetValue(2, "john@example.com")

	if cv.Len() != 3 {
		t.Errorf("Len() = %d, want 3", cv.Len())
	}

	if cv.GetValue(0) != 1 {
		t.Errorf("GetValue(0) = %v, want 1", cv.GetValue(0))
	}

	val, ok := cv.GetValueByName("name")
	if !ok {
		t.Error("GetValueByName('name') returned false")
	}
	if val != "John" {
		t.Errorf("GetValueByName('name') = %v, want John", val)
	}

	_, ok = cv.GetValueByName("nonexistent")
	if ok {
		t.Error("GetValueByName('nonexistent') should return false")
	}

	args := cv.ValuesAsArgs()
	if len(args) != 3 {
		t.Errorf("ValuesAsArgs() returned %d items, want 3", len(args))
	}
}

func TestReplicaIdentity(t *testing.T) {
	tests := []struct {
		ri   ReplicaIdentity
		want string
	}{
		{ReplicaIdentityDefault, "DEFAULT"},
		{ReplicaIdentityNothing, "NOTHING"},
		{ReplicaIdentityFull, "FULL"},
		{ReplicaIdentityIndex, "INDEX"},
		{ReplicaIdentity("x"), "x"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.ri.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
