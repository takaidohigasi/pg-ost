/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package sql

import (
	"fmt"
	"strings"
)

// QueryBuilder helps build SQL queries
type QueryBuilder struct {
	schemaName string
	tableName  string
}

// NewQueryBuilder creates a new query builder
func NewQueryBuilder(schemaName, tableName string) *QueryBuilder {
	return &QueryBuilder{
		schemaName: schemaName,
		tableName:  tableName,
	}
}

// FullTableName returns the fully qualified table name
func (qb *QueryBuilder) FullTableName() string {
	return fmt.Sprintf("%s.%s", QuoteIdentifier(qb.schemaName), QuoteIdentifier(qb.tableName))
}

// BuildSelectQuery builds a SELECT query
func (qb *QueryBuilder) BuildSelectQuery(columns []string, where string, orderBy string, limit int64) string {
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = QuoteIdentifier(col)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(quotedCols, ", "), qb.FullTableName())

	if where != "" {
		query += " WHERE " + where
	}
	if orderBy != "" {
		query += " ORDER BY " + orderBy
	}
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	return query
}

// BuildInsertQuery builds an INSERT query with placeholders
func (qb *QueryBuilder) BuildInsertQuery(columns []string) string {
	quotedCols := make([]string, len(columns))
	placeholders := make([]string, len(columns))

	for i, col := range columns {
		quotedCols[i] = QuoteIdentifier(col)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		qb.FullTableName(),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
	)
}

// BuildUpsertQuery builds an INSERT ... ON CONFLICT query
func (qb *QueryBuilder) BuildUpsertQuery(columns []string, uniqueKeyColumns []string) string {
	quotedCols := make([]string, len(columns))
	placeholders := make([]string, len(columns))

	for i, col := range columns {
		quotedCols[i] = QuoteIdentifier(col)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	quotedUK := make([]string, len(uniqueKeyColumns))
	for i, col := range uniqueKeyColumns {
		quotedUK[i] = QuoteIdentifier(col)
	}

	// Build update clause for non-key columns
	var updateCols []string
	for _, col := range columns {
		isUK := false
		for _, uk := range uniqueKeyColumns {
			if col == uk {
				isUK = true
				break
			}
		}
		if !isUK {
			quotedCol := QuoteIdentifier(col)
			updateCols = append(updateCols, fmt.Sprintf("%s = EXCLUDED.%s", quotedCol, quotedCol))
		}
	}

	if len(updateCols) == 0 {
		return fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
			qb.FullTableName(),
			strings.Join(quotedCols, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(quotedUK, ", "),
		)
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		qb.FullTableName(),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(quotedUK, ", "),
		strings.Join(updateCols, ", "),
	)
}

// BuildUpdateQuery builds an UPDATE query with placeholders
func (qb *QueryBuilder) BuildUpdateQuery(setColumns []string, whereColumns []string) string {
	setClauses := make([]string, len(setColumns))
	argIdx := 1

	for i, col := range setColumns {
		setClauses[i] = fmt.Sprintf("%s = $%d", QuoteIdentifier(col), argIdx)
		argIdx++
	}

	whereClauses := make([]string, len(whereColumns))
	for i, col := range whereColumns {
		whereClauses[i] = fmt.Sprintf("%s = $%d", QuoteIdentifier(col), argIdx)
		argIdx++
	}

	return fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		qb.FullTableName(),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)
}

// BuildDeleteQuery builds a DELETE query with placeholders
func (qb *QueryBuilder) BuildDeleteQuery(whereColumns []string) string {
	whereClauses := make([]string, len(whereColumns))
	for i, col := range whereColumns {
		whereClauses[i] = fmt.Sprintf("%s = $%d", QuoteIdentifier(col), i+1)
	}

	return fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		qb.FullTableName(),
		strings.Join(whereClauses, " AND "),
	)
}

// BuildRangeCondition builds a range condition for composite keys
func BuildRangeCondition(columns []string, op string, argStart int) string {
	if len(columns) == 1 {
		return fmt.Sprintf("%s %s $%d", QuoteIdentifier(columns[0]), op, argStart)
	}

	quotedCols := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = QuoteIdentifier(col)
		placeholders[i] = fmt.Sprintf("$%d", argStart+i)
	}

	return fmt.Sprintf("(%s) %s (%s)",
		strings.Join(quotedCols, ", "),
		op,
		strings.Join(placeholders, ", "))
}

// BuildBetweenCondition builds a BETWEEN condition for range queries
func BuildBetweenCondition(columns []string, minArgStart, maxArgStart int) string {
	if len(columns) == 1 {
		return fmt.Sprintf("%s >= $%d AND %s <= $%d",
			QuoteIdentifier(columns[0]), minArgStart,
			QuoteIdentifier(columns[0]), maxArgStart)
	}

	quotedCols := make([]string, len(columns))
	minPlaceholders := make([]string, len(columns))
	maxPlaceholders := make([]string, len(columns))

	for i, col := range columns {
		quotedCols[i] = QuoteIdentifier(col)
		minPlaceholders[i] = fmt.Sprintf("$%d", minArgStart+i)
		maxPlaceholders[i] = fmt.Sprintf("$%d", maxArgStart+i)
	}

	colList := strings.Join(quotedCols, ", ")
	return fmt.Sprintf("(%s) >= (%s) AND (%s) <= (%s)",
		colList, strings.Join(minPlaceholders, ", "),
		colList, strings.Join(maxPlaceholders, ", "))
}

// BuildOrderByClause builds an ORDER BY clause
func BuildOrderByClause(columns []string, desc bool) string {
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = QuoteIdentifier(col)
	}

	direction := ""
	if desc {
		direction = " DESC"
	}

	return strings.Join(quotedCols, ", ") + direction
}
