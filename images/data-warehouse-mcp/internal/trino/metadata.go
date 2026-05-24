package trino

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type TableMatch struct {
	Schema        string `json:"schema"`
	Table         string `json:"table"`
	TableType     string `json:"table_type"`
	Description   string `json:"description"`
	QualifiedName string `json:"qualified_name"`
}

type Column struct {
	Name            string `json:"name"`
	Type            string `json:"type"`
	Nullable        bool   `json:"nullable"`
	Description     string `json:"description"`
	OrdinalPosition int    `json:"ordinal_position"`
}

type TableSchema struct {
	Catalog     string   `json:"catalog"`
	Schema      string   `json:"schema"`
	Table       string   `json:"table"`
	Description string   `json:"description"`
	Columns     []Column `json:"columns"`
}

type TableRef struct {
	Catalog string
	Schema  string
	Table   string
}

func (c *Client) SearchTables(ctx context.Context, query, schema string, limit int) ([]TableMatch, error) {
	if schema == "" {
		schema = c.cfg.Schema
	}
	if err := validateIdentifierOrEmpty(schema); err != nil {
		return nil, err
	}
	tokens := strings.Fields(strings.ToLower(query))
	sqlText := fmt.Sprintf(`SELECT t.table_schema, t.table_name, t.table_type, COALESCE(tc.comment, '')
FROM %s.information_schema.tables t
LEFT JOIN system.metadata.table_comments tc ON tc.catalog_name = %s AND tc.schema_name = t.table_schema AND tc.table_name = t.table_name
WHERE 1=1`, quoteIdentifier(c.cfg.Catalog), "'"+c.cfg.Catalog+"'")
	args := []any{}
	for _, token := range tokens {
		like := "%" + token + "%"
		sqlText += " AND (lower(t.table_name) LIKE ? OR lower(COALESCE(tc.comment, '')) LIKE ?)"
		args = append(args, like, like)
	}
	if schema != "" {
		sqlText += " AND t.table_schema = ?"
		args = append(args, schema)
	}
	sqlText += fmt.Sprintf(" ORDER BY t.table_schema, t.table_name LIMIT %d", limit)
	rows, err := c.query(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	matches := make([]TableMatch, 0, limit)
	for rows.Next() {
		var match TableMatch
		if err := rows.Scan(&match.Schema, &match.Table, &match.TableType, &match.Description); err != nil {
			return nil, fmt.Errorf("scan table search result: %w", err)
		}
		match.QualifiedName = strings.Join([]string{c.cfg.Catalog, match.Schema, match.Table}, ".")
		matches = append(matches, match)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table search results: %w", err)
	}
	return matches, nil
}

func (c *Client) GetTableSchema(ctx context.Context, input string) (*TableSchema, error) {
	ref, err := ParseTableRef(input, c.cfg.Catalog, c.cfg.Schema)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`SELECT column_name, data_type, is_nullable, COALESCE(comment, ''), ordinal_position
FROM %s.information_schema.columns
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position`, quoteIdentifier(ref.Catalog))
	rows, err := c.query(ctx, query, ref.Schema, ref.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &TableSchema{Catalog: ref.Catalog, Schema: ref.Schema, Table: ref.Table}
	for rows.Next() {
		var nullable string
		var column Column
		if err := rows.Scan(&column.Name, &column.Type, &nullable, &column.Description, &column.OrdinalPosition); err != nil {
			return nil, fmt.Errorf("scan schema column: %w", err)
		}
		column.Nullable = strings.EqualFold(nullable, "YES")
		result.Columns = append(result.Columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema columns: %w", err)
	}
	if len(result.Columns) == 0 {
		return nil, fmt.Errorf("table not found")
	}
	commentRows, err := c.query(ctx, `SELECT COALESCE(comment, '') FROM system.metadata.table_comments WHERE catalog_name = ? AND schema_name = ? AND table_name = ?`, ref.Catalog, ref.Schema, ref.Table)
	if err != nil {
		return nil, err
	}
	defer commentRows.Close()
	if commentRows.Next() {
		if err := commentRows.Scan(&result.Description); err != nil {
			return nil, fmt.Errorf("scan table description: %w", err)
		}
	}
	if err := commentRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table descriptions: %w", err)
	}
	return result, nil
}

func ParseTableRef(input, defaultCatalog, defaultSchema string) (TableRef, error) {
	parts := strings.Split(strings.TrimSpace(input), ".")
	switch len(parts) {
	case 1:
		parts = []string{defaultCatalog, defaultSchema, parts[0]}
	case 2:
		parts = []string{defaultCatalog, parts[0], parts[1]}
	case 3:
	default:
		return TableRef{}, fmt.Errorf("table must be table, schema.table, or catalog.schema.table")
	}
	for _, part := range parts {
		if err := validateIdentifier(part); err != nil {
			return TableRef{}, err
		}
	}
	return TableRef{Catalog: parts[0], Schema: parts[1], Table: parts[2]}, nil
}

func validateIdentifierOrEmpty(identifier string) error {
	if identifier == "" {
		return nil
	}
	return validateIdentifier(identifier)
}

func validateIdentifier(identifier string) error {
	if !identifierPattern.MatchString(identifier) {
		return fmt.Errorf("invalid identifier: %s", identifier)
	}
	return nil
}

func quoteIdentifier(identifier string) string {
	return `"` + identifier + `"`
}

func (c *Client) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := c.db.QueryContext(ctx, query, args...)
	status := "success"
	if err != nil {
		status = "error"
	}
	c.metrics.ObserveTrino(status, time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("trino query failed: %w", err)
	}
	return rows, nil
}
