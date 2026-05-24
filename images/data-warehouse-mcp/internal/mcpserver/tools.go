package mcpserver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/files"
	trinoapp "github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/trino"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type searchTablesInput struct {
	Query  string `json:"query"`
	Schema string `json:"schema,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type getTableSchemaInput struct {
	Table string `json:"table"`
}

type executeQueryInput struct {
	SessionID      string `json:"session_id,omitempty"`
	SQL            string `json:"sql"`
	OutputFormat   string `json:"output_format"`
	Destination    string `json:"destination"`
	Path           string `json:"path,omitempty"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
}

type workspaceBashInput struct {
	SessionID      string   `json:"session_id"`
	Argv           []string `json:"argv"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty"`
}

type fileEdit struct {
	OldStr string `json:"old_str"`
	NewStr string `json:"new_str"`
}

type workspaceEditInput struct {
	SessionID string     `json:"session_id"`
	Path      string     `json:"path"`
	Content   *string    `json:"content"`
	Edits     []fileEdit `json:"edits,omitempty"`
}

type getImageURLInput struct {
	SessionID string `json:"session_id"`
	Path      string `json:"path"`
}

func buildServerInstructions(catalog, schema string) string {
	return fmt.Sprintf(`This MCP server provides tools for querying a Trino data warehouse and analyzing results in an isolated per-session sandbox.

## Data catalog

Default catalog: %s
Default schema: %s

All SQL queries and unqualified table references use these defaults. Use catalog.schema.table notation to reference other locations.

## Typical analysis workflow

1. Call create_session → returns a session_id.
2. Call execute_query (session_id=<id>, destination=file, path=/data.csv) to save query results to a file.
  - repeat as needed to run more queries and save results to different files.
3. Call workspace_edit (session_id=<id>, path=/plot.py) to write an analysis script.
4. Call workspace_bash (session_id=<id>, argv=["python3", "/plot.py"]) to run it.
5. Call get_image_url (session_id=<id>, path=/chart.png) → returns a public URL.
6. Display the image to the user by output standard Markdown (not in a code tag): ![chart](https://...)

session_id is required for every sandbox tool call. Always pass the same session_id throughout a task.

For simple read-back that fits in 100 rows, skip steps 1–2 and use execute_query with destination=inline (no session_id needed).
But for any analysis (e.g. a Python script), you should create a session and use the destination=file workflow to save results and share files between tools.

## Sessions

- Call create_session once per analysis task. Reuse the returned session_id for every subsequent tool call.
- Sessions expire after 1 hour of inactivity; every tool call refreshes the timer.
- Each session has its own isolated sandbox; files are not shared between sessions.

Parent directories are created automatically by execute_query and workspace_edit.

## Sandbox environment (workspace_bash)

Commands run in a chrooted sandbox. cwd=/. You can create any files or structure you need.

Available CLI tools: bash, python3, grep, sed, awk, jq, find, sort, wc, head, tail, cut, tr, xargs, file, less, ps

Available Python packages: numpy, pandas, matplotlib, seaborn, pyarrow, scipy, scikit-learn, statsmodels

Useful environment variables: HOME=/, TMPDIR=/tmp, MPLCONFIGDIR=/tmp/matplotlib, PYTHONUNBUFFERED=1
`, catalog, schema)
}

func (s *Server) registerTools(mcpServer *server.MCPServer) {
	mcpServer.AddTool(mcp.NewTool("search_tables",
		mcp.WithDescription("Search tables by name or description. Pass an empty string to list all tables (up to the limit)."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithString("query", mcp.Description("Search term. Empty string returns all tables up to the limit.")),
		mcp.WithString("schema", mcp.Description("Schema to search (defaults to the configured default schema).")),
		mcp.WithNumber("limit", mcp.Description("Maximum results (default 20, max 50).")),
	), s.wrapTool("search_tables", s.handleSearchTables))

	mcpServer.AddTool(mcp.NewTool("get_table_schema",
		mcp.WithDescription("Get schema details for a table."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithString("table", mcp.Description("Table, schema.table, or catalog.schema.table."), mcp.Required()),
	), s.wrapTool("get_table_schema", s.handleGetTableSchema))

	mcpServer.AddTool(mcp.NewTool("create_session",
		mcp.WithDescription("Create an isolated analysis session. Must be called before using workspace_bash, workspace_edit, get_image_url, or execute_query with destination=file. Returns a session_id that must be passed to all subsequent workspace tool calls."),
	), s.wrapTool("create_session", s.handleCreateSession))

	mcpServer.AddTool(mcp.NewTool("execute_query",
		mcp.WithDescription("Execute a read-only SQL query against Trino. Use destination=inline for small results (up to 100 rows returned directly). Use destination=file to write larger results as CSV or JSON to the sandbox; requires session_id and path."),
		mcp.WithString("session_id", mcp.Description("Session ID (from create_session). Required when destination=file.")),
		mcp.WithString("sql", mcp.Description("SQL to execute."), mcp.Required()),
		mcp.WithString("output_format", mcp.Description("json or csv."), mcp.Enum("json", "csv"), mcp.Required()),
		mcp.WithString("destination", mcp.Description("inline (return rows directly, max 100) or file (write to sandbox path)."), mcp.Enum("inline", "file"), mcp.Required()),
		mcp.WithString("path", mcp.Description("Sandbox-absolute output path, e.g. /data.csv. Required when destination=file. Parent directories are created automatically.")),
		mcp.WithNumber("timeout_seconds", mcp.Description("Optional query timeout in seconds.")),
	), s.wrapTool("execute_query", s.handleExecuteQuery))

	mcpServer.AddTool(mcp.NewTool("workspace_bash",
		mcp.WithDescription("Execute a command inside the session sandbox (chrooted, cwd=/). All tools use the same sandbox path space - /data.csv written by workspace_edit is /data.csv here. Available CLI: bash, python3, grep, sed, awk, jq, find, sort, wc, head, tail, cut, tr, xargs, file. Available Python packages: numpy, pandas, matplotlib, seaborn, pyarrow, scipy, scikit-learn, statsmodels."),
		mcp.WithString("session_id", mcp.Description("Session ID (from create_session)."), mcp.Required()),
		mcp.WithArray("argv", mcp.Description(`Command and arguments as an array, e.g. ["python3", "/plot.py"].`), mcp.WithStringItems(), mcp.Required()),
		mcp.WithNumber("timeout_seconds", mcp.Description("Optional command timeout in seconds.")),
	), s.wrapTool("workspace_bash", s.handleWorkspaceBash))

	mcpServer.AddTool(mcp.NewTool("workspace_edit",
		mcp.WithDescription(`Create or overwrite a file (content mode) or apply targeted string replacements to an existing file (edits mode). Exactly one of content or edits must be provided. Paths are sandbox-absolute, e.g. /plot.py. Parent directories are created automatically.`),
		mcp.WithString("session_id", mcp.Description("Session ID (from create_session)."), mcp.Required()),
		mcp.WithString("path", mcp.Description("Sandbox-absolute file path, e.g. /plot.py."), mcp.Required()),
		mcp.WithString("content", mcp.Description(`Full file content (create/replace mode). Replaces the entire file; creates it if it does not exist.`)),
		mcp.WithArray("edits", mcp.Description(`Selective edit mode. Array of {"old_str":"...","new_str":"..."} objects applied in order. Each old_str must match exactly one occurrence in the current file content.`)),
	), s.wrapTool("workspace_edit", s.handleWorkspaceEdit))

	mcpServer.AddTool(mcp.NewTool("get_image_url",
		mcp.WithDescription("Publish an image from the sandbox and return a public URL. The image must already exist (e.g. written by workspace_bash). Supported formats: PNG, JPEG, GIF, WebP. The URL is suitable for embedding in Markdown messages: ![alt text](url)."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithString("session_id", mcp.Description("Session ID (from create_session)."), mcp.Required()),
		mcp.WithString("path", mcp.Description("Sandbox-absolute image path, e.g. /chart.png."), mcp.Required()),
	), s.wrapTool("get_image_url", s.handleGetImageURL))
}

func (s *Server) handleSearchTables(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var input searchTablesInput
	if err := request.BindArguments(&input); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	if input.Limit == 0 {
		input.Limit = 20
	}
	if input.Limit < 0 || input.Limit > 50 {
		return nil, fmt.Errorf("limit must be between 1 and 50")
	}
	matches, err := s.trino.SearchTables(ctx, input.Query, input.Schema, input.Limit)
	if err != nil {
		return nil, err
	}
	return structuredResult(struct {
		Catalog string                `json:"catalog"`
		Matches []trinoapp.TableMatch `json:"matches"`
	}{Catalog: s.cfg.TrinoCatalog, Matches: matches}), nil
}

func (s *Server) handleGetTableSchema(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var input getTableSchemaInput
	if err := request.BindArguments(&input); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	if input.Table == "" {
		return nil, fmt.Errorf("table is required")
	}
	schema, err := s.trino.GetTableSchema(ctx, input.Table)
	if err != nil {
		return nil, err
	}
	return structuredResult(schema), nil
}

func (s *Server) handleCreateSession(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sess, err := s.sessions.Create(ctx)
	if err != nil {
		return nil, err
	}
	meta := sess.Snapshot()
	return structuredResult(struct {
		SessionID   string `json:"session_id"`
		ExpiresAt   string `json:"expires_at"`
		IdleTimeout string `json:"idle_timeout"`
	}{SessionID: sess.ID, ExpiresAt: meta.ExpiresAt.Format(time.RFC3339), IdleTimeout: s.cfg.SessionTimeout.String()}), nil
}

func (s *Server) handleExecuteQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var input executeQueryInput
	if err := request.BindArguments(&input); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	if input.SQL == "" {
		return nil, fmt.Errorf("sql is required")
	}
	if input.OutputFormat != "json" && input.OutputFormat != "csv" {
		return nil, fmt.Errorf("output_format must be json or csv")
	}
	if input.Destination != "inline" && input.Destination != "file" {
		return nil, fmt.Errorf("destination must be inline or file")
	}
	queryTimeout, err := resolveTimeout(input.TimeoutSeconds, s.cfg.DefaultQueryTimeout, s.cfg.MaxQueryTimeout)
	if err != nil {
		return nil, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	if input.Destination == "inline" {
		result, err := s.trino.ExecuteInline(queryCtx, input.SQL)
		if err != nil {
			return nil, err
		}
		return structuredResult(result), nil
	}
	if input.SessionID == "" {
		return nil, fmt.Errorf("session_id is required when destination=file")
	}
	if input.Path == "" {
		return nil, fmt.Errorf("path is required when destination=file")
	}
	sess, err := s.sessions.GetAndTouch(ctx, input.SessionID)
	if err != nil {
		return nil, err
	}
	normalized, err := files.NormalizeWorkspacePath(input.Path)
	if err != nil {
		return nil, err
	}
	hostPath, err := files.ResolvePath(sess.RootFS, normalized, true)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(hostPath), 0o755); err != nil {
		return nil, fmt.Errorf("create query output directory: %w", err)
	}
	result, err := s.trino.ExecuteToFile(queryCtx, input.SQL, input.OutputFormat, hostPath, normalized)
	if err != nil {
		return nil, err
	}
	return structuredResult(result), nil
}

func (s *Server) handleWorkspaceBash(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var input workspaceBashInput
	if err := request.BindArguments(&input); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	if input.SessionID == "" {
		return nil, fmt.Errorf("session_id is required")
	}
	if len(input.Argv) == 0 {
		return nil, fmt.Errorf("argv must contain at least one element")
	}
	timeout, err := resolveTimeout(input.TimeoutSeconds, s.cfg.DefaultBashTimeout, s.cfg.MaxBashTimeout)
	if err != nil {
		return nil, err
	}
	sess, err := s.sessions.GetAndTouch(ctx, input.SessionID)
	if err != nil {
		return nil, err
	}
	result, err := s.sandbox.Run(ctx, s.sessions, sess, input.Argv, timeout)
	if err != nil {
		return nil, err
	}
	return structuredResult(result), nil
}

func (s *Server) handleWorkspaceEdit(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var input workspaceEditInput
	if err := request.BindArguments(&input); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	if input.SessionID == "" || input.Path == "" {
		return nil, fmt.Errorf("session_id and path are required")
	}
	hasContent := input.Content != nil
	hasEdits := len(input.Edits) > 0
	if hasContent && hasEdits {
		return nil, fmt.Errorf("provide either content or edits, not both")
	}
	if !hasContent && !hasEdits {
		return nil, fmt.Errorf("either content or edits is required")
	}
	sess, err := s.sessions.GetAndTouch(ctx, input.SessionID)
	if err != nil {
		return nil, err
	}
	var normalized string
	var bytesWritten int64
	if hasEdits {
		// Convert local fileEdit slice to files.Edit slice.
		edits := make([]files.Edit, len(input.Edits))
		for i, e := range input.Edits {
			edits[i] = files.Edit{OldStr: e.OldStr, NewStr: e.NewStr}
		}
		normalized, bytesWritten, err = files.ApplyEdits(sess.RootFS, input.Path, edits, s.cfg.MaxEditBytes)
	} else {
		normalized, bytesWritten, err = files.WriteTextFile(sess.RootFS, input.Path, *input.Content, s.cfg.MaxEditBytes)
	}
	if err != nil {
		return nil, err
	}
	return structuredResult(struct {
		Path         string `json:"path"`
		BytesWritten int64  `json:"bytes_written"`
	}{Path: normalized, BytesWritten: bytesWritten}), nil
}

func (s *Server) handleGetImageURL(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var input getImageURLInput
	if err := request.BindArguments(&input); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	if input.SessionID == "" || input.Path == "" {
		return nil, fmt.Errorf("session_id and path are required")
	}
	sess, err := s.sessions.GetAndTouch(ctx, input.SessionID)
	if err != nil {
		return nil, err
	}
	filename, _, _, err := files.PublishImageFile(sess.RootFS, input.Path, s.cfg.ImagesRoot, s.cfg.MaxImageBytes)
	if err != nil {
		return nil, err
	}
	url := s.cfg.ImageBaseURL + "/images/" + filename
	return structuredResult(struct {
		URL      string `json:"url"`
		Filename string `json:"filename"`
	}{URL: url, Filename: filename}), nil
}
