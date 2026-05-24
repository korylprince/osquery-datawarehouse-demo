package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/config"
	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/httpapi"
	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/sandbox"
	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/session"
	trinoapp "github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/trino"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type Dependencies struct {
	Config         config.Config
	Logger         *slog.Logger
	Metrics        *httpapi.Metrics
	Sessions       *session.Manager
	Sandbox        *sandbox.Runner
	Trino          *trinoapp.Client
	ServerName     string
	ServerVersion  string
	ReadinessCheck func(context.Context) error
}

type Server struct {
	cfg      config.Config
	logger   *slog.Logger
	metrics  *httpapi.Metrics
	sessions *session.Manager
	sandbox  *sandbox.Runner
	trino    *trinoapp.Client
	http     http.Handler
}

func New(deps Dependencies) *Server {
	impl := &Server{
		cfg:      deps.Config,
		logger:   deps.Logger,
		metrics:  deps.Metrics,
		sessions: deps.Sessions,
		sandbox:  deps.Sandbox,
		trino:    deps.Trino,
	}

	// Log all MCP protocol requests except tools/call, which is already logged
	// with richer detail (tool name, status, duration) by wrapTool.
	hooks := &server.Hooks{}
	hooks.AddBeforeAny(func(ctx context.Context, id any, method mcp.MCPMethod, message any) {
		if method != mcp.MethodToolsCall {
			impl.logger.Info("mcp request", "method", string(method))
		}
	})
	hooks.AddOnError(func(ctx context.Context, id any, method mcp.MCPMethod, message any, err error) {
		if method != mcp.MethodToolsCall {
			impl.logger.Warn("mcp error", "method", string(method), "error", err.Error())
		}
	})

	mcpServer := server.NewMCPServer(deps.ServerName, deps.ServerVersion,
		server.WithToolCapabilities(false),
		server.WithHooks(hooks),
		server.WithInstructions(buildServerInstructions(deps.Config.TrinoCatalog, deps.Config.TrinoSchema)),
	)
	impl.registerTools(mcpServer)
	impl.http = server.NewStreamableHTTPServer(mcpServer, server.WithEndpointPath("/mcp"))
	return impl
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.http.ServeHTTP(w, r)
}

func (s *Server) wrapTool(name string, handler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error)) server.ToolHandlerFunc {
	return func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		start := time.Now()
		status := "success"
		result, err := handler(ctx, request)
		if err != nil {
			status = "error"
			s.logger.Warn("tool failed", "tool", name, "error", err.Error())
			result = mcp.NewToolResultError(sanitizeForClient(err).Error())
		}
		s.metrics.ObserveTool(name, status)
		s.logger.Info("tool completed", "tool", name, "status", status, "duration_ms", time.Since(start).Milliseconds())
		return result, nil
	}
}

// sanitizeForClient replaces errors that wrap *os.PathError or *os.LinkError with a
// generic message so that internal filesystem paths (e.g. /sessions/…) are never
// exposed to MCP clients.  The full error is already logged by wrapTool before this
// function is called.  All other errors (session-not-found, Trino errors, validation
// errors, etc.) are returned unchanged.
func sanitizeForClient(err error) error {
	var pathErr *os.PathError
	var linkErr *os.LinkError
	if errors.As(err, &pathErr) || errors.As(err, &linkErr) {
		return errors.New("internal error")
	}
	return err
}

func structuredResult(value any) *mcp.CallToolResult {
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return mcp.NewToolResultStructuredOnly(value)
	}
	return mcp.NewToolResultStructured(value, string(payload))
}

func resolveTimeout(requestedSeconds int, fallback, max time.Duration) (time.Duration, error) {
	if requestedSeconds == 0 {
		return fallback, nil
	}
	if requestedSeconds < 0 {
		return 0, fmt.Errorf("timeout_seconds must be greater than or equal to zero")
	}
	timeout := time.Duration(requestedSeconds) * time.Second
	if timeout > max {
		return 0, fmt.Errorf("timeout_seconds exceeds maximum allowed value")
	}
	return timeout, nil
}
