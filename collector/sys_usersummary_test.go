package collector

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func TestScrapeUserSummary(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening a stub database connection: %s", err)
	}
	defer db.Close()

	columns := []string{"user", "current_connections"}
	rows := sqlmock.NewRows(columns).
		AddRow("audience", "10").
		AddRow("root", "10").
		AddRow("brighttalk", "20").
		AddRow("monitor", "1")
	mock.ExpectQuery(sysCurrentConnectionQuery).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		if err = ScrapeUserSummary(db, ch); err != nil {
			t.Errorf("error calling function on test: %s", err)
		}
		close(ch)
	}()

	counterExpected := []MetricResult{
		{labels: labelMap{"user": "audience"}, value: 10, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"user": "root"}, value: 10, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"user": "brighttalk"}, value: 20, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"user": "monitor"}, value: 1, metricType: dto.MetricType_GAUGE},
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
