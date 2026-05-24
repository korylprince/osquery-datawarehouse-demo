package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/costexplorer"
	"github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	osquery "github.com/osquery/osquery-go"
	"github.com/osquery/osquery-go/plugin/table"
)

var (
	flSocket  = flag.String("socket", "", "")
	flTimeout = flag.Int("timeout", 0, "")
	_         = flag.Int("interval", 0, "")
	_         = flag.Bool("verbose", false, "")
	flSpec    = flag.Bool("spec", false, "") // unused, absorbed by osquery
)

const cacheTTL = 12 * time.Hour

var billingCache struct {
	mu     sync.Mutex
	rows   []map[string]string
	expiry time.Time
}

// awsBillingTable returns the osquery table plugin for AWS billing data.
func awsBillingTable() *table.Plugin {
	return table.NewPlugin(
		"aws_billing",
		[]table.ColumnDefinition{
			table.TextColumn("month"),
			table.TextColumn("amount"),
			table.TextColumn("currency"),
			table.TextColumn("start_date"),
			table.TextColumn("end_date"),
			table.TextColumn("estimated"),
		},
		generateBillingRows,
	)
}

func generateBillingRows(ctx context.Context, queryContext table.QueryContext) ([]map[string]string, error) {
	billingCache.mu.Lock()
	defer billingCache.mu.Unlock()
	if time.Now().Before(billingCache.expiry) && billingCache.rows != nil {
		fmt.Println("using cached billing data")
		cached := billingCache.rows
		return cached, nil
	}
	if billingCache.rows == nil {
		billingCache.rows = []map[string]string{}
	}

	rows, err := fetchBilling(ctx, queryContext)
	if err != nil {
		return nil, err
	}

	billingCache.rows = rows
	billingCache.expiry = time.Now().Add(cacheTTL)

	return rows, nil
}

func fetchBilling(ctx context.Context, queryContext table.QueryContext) ([]map[string]string, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := costexplorer.NewFromConfig(cfg)

	// Determine the month from WHERE constraints, default to current month.
	month := time.Now().Format("2006-01")
	if constraints, ok := queryContext.Constraints["month"]; ok {
		for _, c := range constraints.Constraints {
			if c.Operator == table.OperatorEquals {
				month = c.Expression
			}
		}
	}

	// Parse the month and build the date range.
	parsed, err := time.Parse("2006-01", month)
	if err != nil {
		return nil, fmt.Errorf("invalid month format %q, expected YYYY-MM: %w", month, err)
	}

	start := time.Date(parsed.Year(), parsed.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0).Add(-time.Second) // last moment of the month

	input := &costexplorer.GetCostAndUsageInput{
		TimePeriod: &types.DateInterval{
			Start: aws.String(start.Format(time.DateOnly)),
			End:   aws.String(end.Format(time.DateOnly)),
		},
		Granularity: types.GranularityMonthly,
		Metrics:     []string{"BlendedCost"},
	}

	output, err := client.GetCostAndUsage(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get cost and usage: %w", err)
	}

	var rows []map[string]string
	for _, result := range output.ResultsByTime {
		metric := result.Total["BlendedCost"]
		rows = append(rows, map[string]string{
			"month":      month,
			"amount":     aws.ToString(metric.Amount),
			"currency":   aws.ToString(metric.Unit),
			"start_date": aws.ToString(result.TimePeriod.Start),
			"end_date":   aws.ToString(result.TimePeriod.End),
			"estimated":  strings.ToLower(fmt.Sprintf("%v", result.Estimated)),
		})
	}

	return rows, nil
}

func main() {
	flag.Parse()

	tbl := awsBillingTable()

	// Wait for osquery to create the extension socket.
	time.Sleep(1 * time.Second)

	timeout := time.Duration(*flTimeout) * time.Second
	if timeout == 0 {
		timeout = 3 * time.Second
	}

	server, err := osquery.NewExtensionManagerServer(
		"aws_billing_extension",
		*flSocket,
		osquery.ServerTimeout(timeout),
	)
	if err != nil {
		log.Fatalf("Error creating extension: %s\n", err)
	}

	server.RegisterPlugin(tbl)
	if err := server.Run(); err != nil {
		log.Fatal(err)
	}
}
