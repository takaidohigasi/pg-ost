/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package replication

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	sqlpkg "github.com/takaidohigasi/pg-ost/internal/sql"
)

// PublicationManager manages PostgreSQL publications for logical replication
type PublicationManager struct {
	db              *sql.DB
	publicationName string
	schemaName      string
	tableName       string
}

// NewPublicationManager creates a new publication manager
func NewPublicationManager(db *sql.DB, publicationName, schemaName, tableName string) *PublicationManager {
	return &PublicationManager{
		db:              db,
		publicationName: publicationName,
		schemaName:      schemaName,
		tableName:       tableName,
	}
}

// Create creates a new publication for the specified table
func (p *PublicationManager) Create(ctx context.Context) error {
	// Create publication for the specific table
	query := fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE %s.%s",
		sqlpkg.QuoteIdentifier(p.publicationName),
		sqlpkg.QuoteIdentifier(p.schemaName),
		sqlpkg.QuoteIdentifier(p.tableName),
	)

	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create publication %s: %w", p.publicationName, err)
	}

	return nil
}

// CreateForTables creates a publication for multiple tables
func (p *PublicationManager) CreateForTables(ctx context.Context, tables []string) error {
	tableRefs := make([]string, len(tables))
	for i, table := range tables {
		tableRefs[i] = fmt.Sprintf("%s.%s",
			sqlpkg.QuoteIdentifier(p.schemaName),
			sqlpkg.QuoteIdentifier(table))
	}

	query := fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE %s",
		sqlpkg.QuoteIdentifier(p.publicationName),
		strings.Join(tableRefs, ", "),
	)

	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	return nil
}

// Drop drops the publication
func (p *PublicationManager) Drop(ctx context.Context) error {
	query := fmt.Sprintf(
		"DROP PUBLICATION IF EXISTS %s",
		sqlpkg.QuoteIdentifier(p.publicationName),
	)

	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop publication %s: %w", p.publicationName, err)
	}

	return nil
}

// Exists checks if the publication exists
func (p *PublicationManager) Exists(ctx context.Context) (bool, error) {
	var count int
	err := p.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pg_publication WHERE pubname = $1",
		p.publicationName,
	).Scan(&count)

	if err != nil {
		return false, fmt.Errorf("failed to check publication: %w", err)
	}

	return count > 0, nil
}

// AddTable adds a table to the publication
func (p *PublicationManager) AddTable(ctx context.Context, schemaName, tableName string) error {
	query := fmt.Sprintf(
		"ALTER PUBLICATION %s ADD TABLE %s.%s",
		sqlpkg.QuoteIdentifier(p.publicationName),
		sqlpkg.QuoteIdentifier(schemaName),
		sqlpkg.QuoteIdentifier(tableName),
	)

	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to add table to publication: %w", err)
	}

	return nil
}

// RemoveTable removes a table from the publication
func (p *PublicationManager) RemoveTable(ctx context.Context, schemaName, tableName string) error {
	query := fmt.Sprintf(
		"ALTER PUBLICATION %s DROP TABLE %s.%s",
		sqlpkg.QuoteIdentifier(p.publicationName),
		sqlpkg.QuoteIdentifier(schemaName),
		sqlpkg.QuoteIdentifier(tableName),
	)

	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to remove table from publication: %w", err)
	}

	return nil
}

// GetTables returns the tables in the publication
func (p *PublicationManager) GetTables(ctx context.Context) ([]string, error) {
	rows, err := p.db.QueryContext(ctx,
		`SELECT schemaname, tablename
		 FROM pg_publication_tables
		 WHERE pubname = $1`,
		p.publicationName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get publication tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		tables = append(tables, fmt.Sprintf("%s.%s", schemaName, tableName))
	}

	return tables, nil
}

// SetPublishOperations sets which operations to publish
func (p *PublicationManager) SetPublishOperations(ctx context.Context, insert, update, delete, truncate bool) error {
	var ops []string
	if insert {
		ops = append(ops, "insert")
	}
	if update {
		ops = append(ops, "update")
	}
	if delete {
		ops = append(ops, "delete")
	}
	if truncate {
		ops = append(ops, "truncate")
	}

	query := fmt.Sprintf(
		"ALTER PUBLICATION %s SET (publish = '%s')",
		sqlpkg.QuoteIdentifier(p.publicationName),
		strings.Join(ops, ", "),
	)

	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to set publish operations: %w", err)
	}

	return nil
}
