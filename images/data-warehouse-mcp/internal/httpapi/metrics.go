package httpapi

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry           *prometheus.Registry
	SessionsActive     prometheus.Gauge
	SessionsCreated    prometheus.Counter
	SessionsExpired    prometheus.Counter
	ToolRequests       *prometheus.CounterVec
	TrinoQueries       *prometheus.CounterVec
	TrinoQueryDuration prometheus.Histogram
	BashCommands       *prometheus.CounterVec
	BashDuration       prometheus.Histogram
}

func NewMetrics() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		SessionsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "dwmcp_sessions_active",
			Help: "Current number of active sessions.",
		}),
		SessionsCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dwmcp_sessions_created_total",
			Help: "Total number of created sessions.",
		}),
		SessionsExpired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dwmcp_sessions_expired_total",
			Help: "Total number of expired sessions.",
		}),
		ToolRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dwmcp_tool_requests_total",
			Help: "Total number of MCP tool requests by tool and status.",
		}, []string{"tool", "status"}),
		TrinoQueries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dwmcp_trino_queries_total",
			Help: "Total number of Trino queries by status.",
		}, []string{"status"}),
		TrinoQueryDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "dwmcp_trino_query_duration_seconds",
			Help:    "Duration of Trino queries in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		BashCommands: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dwmcp_bash_commands_total",
			Help: "Total number of workspace bash commands by status.",
		}, []string{"status"}),
		BashDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "dwmcp_bash_duration_seconds",
			Help:    "Duration of workspace bash commands in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	m.registry.MustRegister(
		m.SessionsActive,
		m.SessionsCreated,
		m.SessionsExpired,
		m.ToolRequests,
		m.TrinoQueries,
		m.TrinoQueryDuration,
		m.BashCommands,
		m.BashDuration,
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) ObserveTool(tool, status string) {
	m.ToolRequests.WithLabelValues(tool, status).Inc()
}

func (m *Metrics) ObserveTrino(status string, duration time.Duration) {
	m.TrinoQueries.WithLabelValues(status).Inc()
	m.TrinoQueryDuration.Observe(duration.Seconds())
}

func (m *Metrics) ObserveBash(status string, duration time.Duration) {
	m.BashCommands.WithLabelValues(status).Inc()
	m.BashDuration.Observe(duration.Seconds())
}
