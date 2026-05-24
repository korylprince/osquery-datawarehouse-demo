package trino

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type InlineResult struct {
	Columns   []string `json:"columns"`
	RowCount  int      `json:"row_count"`
	Truncated bool     `json:"truncated"`
	Rows      [][]any  `json:"rows"`
}

type FileResult struct {
	Path         string `json:"path"`
	BytesWritten int64  `json:"bytes_written"`
	RowCount     int    `json:"row_count"`
	OutputFormat string `json:"output_format"`
}

func (c *Client) ExecuteInline(ctx context.Context, sqlText string) (*InlineResult, error) {
	rows, err := c.query(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read columns: %w", err)
	}
	result := &InlineResult{Columns: columns, Rows: make([][]any, 0, c.cfg.MaxInlineRows)}
	for rows.Next() {
		if result.RowCount >= c.cfg.MaxInlineRows {
			result.Truncated = true
			break
		}
		values, err := scanRow(rows, len(columns))
		if err != nil {
			return nil, err
		}
		result.Rows = append(result.Rows, values)
		result.RowCount++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate query rows: %w", err)
	}
	return result, nil
}

func (c *Client) ExecuteToFile(ctx context.Context, sqlText, outputFormat, hostPath, publicPath string) (*FileResult, error) {
	tempFile, err := os.CreateTemp(filepath.Dir(hostPath), filepath.Base(hostPath)+".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
	}()

	rows, err := c.query(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read columns: %w", err)
	}
	writer := &countingWriter{w: tempFile, max: c.cfg.MaxQueryFileBytes}
	rowCount := 0

	switch outputFormat {
	case "csv":
		csvWriter := csv.NewWriter(writer)
		if err := csvWriter.Write(columns); err != nil {
			return nil, fmt.Errorf("write csv header: %w", err)
		}
		for rows.Next() {
			values, err := scanRow(rows, len(columns))
			if err != nil {
				return nil, err
			}
			record := make([]string, len(values))
			for i, value := range values {
				record[i] = stringify(value)
			}
			if err := csvWriter.Write(record); err != nil {
				return nil, fmt.Errorf("write csv row: %w", err)
			}
			rowCount++
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return nil, fmt.Errorf("flush csv: %w", err)
		}
	case "json":
		if _, err := writer.Write([]byte("[")); err != nil {
			return nil, err
		}
		first := true
		for rows.Next() {
			values, err := scanRow(rows, len(columns))
			if err != nil {
				return nil, err
			}
			obj := make(map[string]any, len(columns))
			for i, column := range columns {
				obj[column] = values[i]
			}
			payload, err := json.Marshal(obj)
			if err != nil {
				return nil, fmt.Errorf("marshal json row: %w", err)
			}
			if !first {
				if _, err := writer.Write([]byte(",")); err != nil {
					return nil, err
				}
			}
			if _, err := writer.Write(payload); err != nil {
				return nil, err
			}
			first = false
			rowCount++
		}
		if _, err := writer.Write([]byte("]")); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported output format")
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate query rows: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return nil, fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tempPath, hostPath); err != nil {
		return nil, fmt.Errorf("move query file into place: %w", err)
	}
	return &FileResult{Path: publicPath, BytesWritten: writer.n, RowCount: rowCount, OutputFormat: outputFormat}, nil
}

type countingWriter struct {
	w   io.Writer
	n   int64
	max int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	if w.n+int64(len(p)) > w.max {
		return 0, fmt.Errorf("result exceeds maximum allowed size")
	}
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func scanRow(rows *sql.Rows, count int) ([]any, error) {
	raw := make([]any, count)
	dest := make([]any, count)
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}
	values := make([]any, count)
	for i, value := range raw {
		values[i] = normalizeValue(value)
	}
	return values, nil
}

func normalizeValue(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return string(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	case fmt.Stringer:
		return v.String()
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = normalizeValue(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = normalizeValue(item)
		}
		return out
	default:
		return v
	}
}

func stringify(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		data, err := json.Marshal(v)
		if err == nil && !strings.HasPrefix(string(data), "\"") {
			return string(data)
		}
		return fmt.Sprint(v)
	}
}
