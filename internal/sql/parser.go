/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package sql

import (
	"fmt"
	"regexp"
	"strings"
)

// AlterTableParser parses ALTER TABLE statements
type AlterTableParser struct {
	alterStatement string
	tableName      string
	schemaName     string
	alterClause    string
	isRename       bool
	hasAddColumn   bool
	hasDropColumn  bool
}

// NewAlterTableParser creates a new parser
func NewAlterTableParser() *AlterTableParser {
	return &AlterTableParser{}
}

// ParseAlterStatement parses an ALTER TABLE statement
func (p *AlterTableParser) ParseAlterStatement(statement string) error {
	p.alterStatement = strings.TrimSpace(statement)

	// Normalize whitespace
	normalizedStatement := regexp.MustCompile(`\s+`).ReplaceAllString(p.alterStatement, " ")

	// Check for RENAME
	renameRegexp := regexp.MustCompile(`(?i)^\s*ALTER\s+TABLE\s+.*\s+RENAME\s+TO\s+`)
	if renameRegexp.MatchString(normalizedStatement) {
		p.isRename = true
		return fmt.Errorf("ALTER TABLE ... RENAME TO is not supported. Please run RENAME separately")
	}

	// Parse table name from statement (optional, since we get it from context)
	tableRegexp := regexp.MustCompile(`(?i)^\s*ALTER\s+TABLE\s+(?:ONLY\s+)?(?:IF\s+EXISTS\s+)?(?:("?\w+"?\.)?("?\w+"?))\s+(.*)$`)
	matches := tableRegexp.FindStringSubmatch(normalizedStatement)
	if matches != nil {
		if matches[1] != "" {
			p.schemaName = strings.Trim(matches[1], `".`)
		}
		if matches[2] != "" {
			p.tableName = strings.Trim(matches[2], `"`)
		}
		p.alterClause = matches[3]
	} else {
		// Statement might not start with ALTER TABLE, treat entire thing as alter clause
		p.alterClause = p.alterStatement
	}

	// Check for ADD COLUMN
	addColumnRegexp := regexp.MustCompile(`(?i)\bADD\s+(?:COLUMN\s+)?`)
	p.hasAddColumn = addColumnRegexp.MatchString(p.alterClause)

	// Check for DROP COLUMN
	dropColumnRegexp := regexp.MustCompile(`(?i)\bDROP\s+(?:COLUMN\s+)?`)
	p.hasDropColumn = dropColumnRegexp.MatchString(p.alterClause)

	return nil
}

// GetAlterClause returns just the ALTER clause (without ALTER TABLE prefix)
func (p *AlterTableParser) GetAlterClause() string {
	return p.alterClause
}

// GetTableName returns the parsed table name (if any)
func (p *AlterTableParser) GetTableName() string {
	return p.tableName
}

// GetSchemaName returns the parsed schema name (if any)
func (p *AlterTableParser) GetSchemaName() string {
	return p.schemaName
}

// IsRename returns true if this is a RENAME statement
func (p *AlterTableParser) IsRename() bool {
	return p.isRename
}

// HasAddColumn returns true if statement adds a column
func (p *AlterTableParser) HasAddColumn() bool {
	return p.hasAddColumn
}

// HasDropColumn returns true if statement drops a column
func (p *AlterTableParser) HasDropColumn() bool {
	return p.hasDropColumn
}

// BuildAlterGhostStatement builds the ALTER statement for the ghost table
func (p *AlterTableParser) BuildAlterGhostStatement(schemaName, ghostTableName string) string {
	alterClause := p.alterClause
	if alterClause == "" {
		alterClause = p.alterStatement
	}

	// If statement already starts with ALTER TABLE, extract just the clause
	alterTableRegexp := regexp.MustCompile(`(?i)^\s*ALTER\s+TABLE\s+(?:ONLY\s+)?(?:IF\s+EXISTS\s+)?(?:"?\w+"?\.)?(?:"?\w+"?)\s+`)
	if alterTableRegexp.MatchString(alterClause) {
		alterClause = alterTableRegexp.ReplaceAllString(alterClause, "")
	}

	return fmt.Sprintf("ALTER TABLE %s.%s %s",
		QuoteIdentifier(schemaName),
		QuoteIdentifier(ghostTableName),
		alterClause)
}

// QuoteIdentifier quotes a PostgreSQL identifier
func QuoteIdentifier(name string) string {
	// If already quoted, return as is
	if strings.HasPrefix(name, `"`) && strings.HasSuffix(name, `"`) {
		return name
	}
	// Quote the identifier
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// UnquoteIdentifier removes quotes from a PostgreSQL identifier
func UnquoteIdentifier(name string) string {
	if strings.HasPrefix(name, `"`) && strings.HasSuffix(name, `"`) {
		return strings.ReplaceAll(name[1:len(name)-1], `""`, `"`)
	}
	return name
}
