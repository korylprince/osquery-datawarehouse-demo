package trino

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/httpapi"
	trinoapi "github.com/trinodb/trino-go-client/trino"
)

type Config struct {
	Scheme            string
	Host              string
	Port              int
	Catalog           string
	Schema            string
	Source            string
	MaxInlineRows     int
	MaxQueryFileBytes int64
}

type Client struct {
	cfg     Config
	db      *sql.DB
	logger  *slog.Logger
	metrics *httpapi.Metrics
}

func NewClient(cfg Config, logger *slog.Logger, metrics *httpapi.Metrics) (*Client, error) {
	dsn, err := (&trinoapi.Config{
		ServerURI: fmt.Sprintf("%s://data-warehouse-mcp@%s:%d", cfg.Scheme, cfg.Host, cfg.Port),
		Catalog:   cfg.Catalog,
		Schema:    cfg.Schema,
		Source:    cfg.Source,
	}).FormatDSN()
	if err != nil {
		return nil, fmt.Errorf("format trino dsn: %w", err)
	}
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, fmt.Errorf("open trino connection: %w", err)
	}
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(8)
	return &Client{cfg: cfg, db: db, logger: logger, metrics: metrics}, nil
}

func (c *Client) Close() error {
	return c.db.Close()
}

func (c *Client) Ready(ctx context.Context) error {
	return c.db.PingContext(ctx)
}
