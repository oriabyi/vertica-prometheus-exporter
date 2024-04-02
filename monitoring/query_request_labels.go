package monitoring

import (
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
)

// QueryRequestLabel lists query performance metrics on the request_label level.
type QueryRequestLabel struct {
	RequestLabel      string `db:"request_label"`
	RequestDurationMS int    `db:"request_duration_ms"`
	MemoryAcquiredMB  int    `db:"memory_acquired_mb"`
}

// NewQueryRequestLabels returns query performance for all users.
func NewQueryRequestLabels(db *sqlx.DB) []QueryRequestLabel {
	sql := `
	SELECT
		request_label, 
		SUM(COALESCE(request_duration_ms,0))::INT request_duration_ms, 
		SUM(COALESCE(memory_acquired_mb,0))::INT memory_acquired_mb 
	FROM v_monitor.query_requests 
	WHERE is_executing IS true
	  AND request_label != ''
	  AND request_label NOT LIKE '%vkstream%'
	GROUP BY request_label;
	`

	queryRequests := []QueryRequestLabel{}
	err := db.Select(&queryRequests, sql)
	if err != nil {
		log.Fatal(err)
	}

	return queryRequests
}

// ToMetric converts QueryRequestLabel to a Map.
func (qr QueryRequestLabel) ToMetric() map[string]float32 {
	metrics := map[string]float32{}

	request_label := fmt.Sprintf("request_label=%q", qr.RequestLabel)
	metrics[fmt.Sprintf("vertica_label_request_duration_ms{%s}", request_label)] = float32(qr.RequestDurationMS)
	metrics[fmt.Sprintf("vertica_label_memory_acquired_mb{%s}", request_label)] = float32(qr.MemoryAcquiredMB)

	return metrics
}
