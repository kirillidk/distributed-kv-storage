package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetricsQueryRangePreservesLatencyGaps(t *testing.T) {
	const query = `histogram_quantile(0.95, sum by (le) (rate(kvengine_request_duration_seconds_bucket{node_id="node-1",shard_id="shard-b"}[1m])))`
	prometheus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("query"); got != query {
			t.Errorf("query = %q, want %q", got, query)
		}
		_, _ = io.WriteString(w, `{
			"status": "success",
			"data": {"resultType": "matrix", "result": [{
				"metric": {},
				"values": [[100,"NaN"],[115,"0.005"],[130,"+Inf"]]
			}]}
		}`)
	}))
	defer prometheus.Close()

	s := New("127.0.0.1:0", nil, nil, time.Second, prometheus.URL)
	recorder := httptest.NewRecorder()
	endpoint := "/api/v1/metrics/query_range?" + url.Values{"query": {query}}.Encode()
	s.httpServer.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, endpoint, nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"series":[{"metric":{},"values":[
		{"t":100,"v":null},{"t":115,"v":0.005},{"t":130,"v":null}
	]}]}`, recorder.Body.String())
}
