// Scrape `sys.usersummary`.

package collector

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

const sysCurrentConnectionQuery = `
		SELECT
		  user,
			current_connections
		  FROM sys.user_summary
		  ORDER BY current_connections DESC
		`

// Metrics descriptors.
var (
	//sysCurrentConnectionUser = prometheus.NewDesc(
	//	prometheus.BuildFQName(namespace, informationSchema, "user"),
	//	"Total number of buffer pages read total.",
	//	[]string{"type"}, nil,
	//)
	sysCurrentConnection = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sys, "current_connections"),
		"Number of connection per user",
		[]string{"user"}, nil,
	)
)

// ScrapeUserSummary collects from `sys.user_summary`.
func ScrapeUserSummary(db *sql.DB, ch chan<- prometheus.Metric) error {
	sysConnectionRows, err := db.Query(sysCurrentConnectionQuery)
	if err != nil {
		return err
	}

	defer sysConnectionRows.Close()

	var (
		user    string
		value   float64
	)

	for sysConnectionRows.Next() {
		if err := sysConnectionRows.Scan(
			&user, &value,
		); err != nil {
			return err
		}

		metricName := "sys_concurrent_connection"
		// MySQL returns counters named two different ways. "counter" and "status_counter"
		// value >= 0 is necessary due to upstream bugs: http://bugs.mysql.com/bug.php?id=75966
		description := prometheus.NewDesc(
			prometheus.BuildFQName(namespace, sys, metricName),
			comment, nil, nil,
		)
		ch <- prometheus.MustNewConstMetric(
			description,
			prometheus.GaugeValue,
			value,
		)
	}
	return nil
}
