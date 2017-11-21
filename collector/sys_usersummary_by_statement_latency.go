// Scrape `sys.usersummary_by_statement_latency`.

package collector

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

const sysUserSummaryByLatencyQuery = `
		SELECT
		  user,
			total_latency
		  FROM sys.x$user_summary_by_statement_latency
		  ORDER BY total_latency DESC LIMIT 30
		`

// Metrics descriptors.
var (
	sysUserStatementMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sysSchema, "user_summary_by_statement_latency"),
		"Latency created per user",
		[]string{"user"}, nil,
	)
)

// ScrapeUserSummary collects from `sys.user_summary`.
func ScrapeUserSummaryByStatementLatency(db *sql.DB, ch chan<- prometheus.Metric) error {
	sysUserStatementLatencyRows, err := db.Query(sysUserSummaryByLatencyQuery)
	if err != nil {
		return err
	}

	defer sysUserStatementLatencyRows.Close()

	var (
		user  string
		value float64
	)

	for sysUserStatementLatencyRows.Next() {
		if err := sysUserStatementLatencyRows.Scan(
			&user, &value,
		); err != nil {
			return err
		}

		metricName := "sys_statement_latency_per_user"
		// MySQL returns counters named two different ways. "counter" and "status_counter"
		// value >= 0 is necessary due to upstream bugs: http://bugs.mysql.com/bug.php?id=75966
		description := prometheus.NewDesc(
			prometheus.BuildFQName(namespace, sysSchema, metricName),
			"Statement latency per user", []string{"user"}, nil,
		)
		ch <- prometheus.MustNewConstMetric(
			description,
			prometheus.GaugeValue,
			value,
			user,
		)
	}
	return nil
}
