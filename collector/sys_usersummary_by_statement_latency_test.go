// The query contains a dollar and therefore
// the mock does not process it correctly
// I'm currently not able to figure out how to fix that.

package collector

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

const sysUserSummaryByLatencyQueryEscaped = `
		SELECT
		  user,
			total_latency
		  FROM sys.x\$user_summary_by_statement_latency
		  ORDER BY total_latency DESC LIMIT 30
		`

func TestScrapeUserSummaryLatency(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening a stub database connection: %s", err)
	}
	defer db.Close()

	columns := []string{"user", "total_latency"}
	rows := sqlmock.NewRows(columns).
		AddRow("audience", "129304829084").
		AddRow("root", "18098903830").
		AddRow("monitor", "12310391230").
		AddRow("brighttalk", "2124203940")
	mock.ExpectQuery(sysUserSummaryByLatencyQueryEscaped).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		if err = ScrapeUserSummaryByStatementLatency(db, ch); err != nil {
			t.Errorf("error calling function on test: %s", err)
		}
		close(ch)
	}()

	counterExpected := []MetricResult{
		{labels: labelMap{"user": "audience"}, value: 129304829084, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"user": "root"}, value: 18098903830, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"user": "monitor"}, value: 12310391230, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"user": "brighttalk"}, value: 2124203940, metricType: dto.MetricType_GAUGE},
	}
	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range counterExpected {
			got := readMetric(<-ch)
			convey.So(got, convey.ShouldResemble, expect)
		}
	})

	// Ensure all SQL queries were executed
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}
