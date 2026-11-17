package promclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Client struct {
	base string
	http *http.Client
}

func New(baseURL string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Client{
		base: trimRightSlash(baseURL),
		http: &http.Client{Timeout: timeout},
	}
}

func trimRightSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

func (c *Client) Configured() bool {
	return c != nil && c.base != ""
}

type Point struct {
	T float64 `json:"t"`
	V float64 `json:"v"`
}

type Series struct {
	Metric map[string]string `json:"metric"`
	Values []Point           `json:"values"`
}

type QueryRangeResult struct {
	Series []Series `json:"series"`
}

type promAPIResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string   `json:"metric"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"result"`
	} `json:"data"`
	Error     string `json:"error"`
	ErrorType string `json:"errorType"`
}

func (c *Client) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (*QueryRangeResult, error) {
	if !c.Configured() {
		return nil, fmt.Errorf("prometheus URL is not configured")
	}
	if step <= 0 {
		step = 15 * time.Second
	}

	u, err := url.Parse(c.base + "/api/v1/query_range")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("query", query)
	q.Set("start", fmt.Sprintf("%d", start.Unix()))
	q.Set("end", fmt.Sprintf("%d", end.Unix()))
	q.Set("step", fmt.Sprintf("%.0fs", step.Seconds()))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("prometheus request: %w", err)
	}
	defer resp.Body.Close()

	var body promAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode prometheus response: %w", err)
	}
	if resp.StatusCode != http.StatusOK || body.Status != "success" {
		msg := body.Error
		if msg == "" {
			msg = resp.Status
		}
		return nil, fmt.Errorf("prometheus: %s", msg)
	}

	out := &QueryRangeResult{Series: make([]Series, 0, len(body.Data.Result))}
	for _, r := range body.Data.Result {
		s := Series{Metric: r.Metric, Values: make([]Point, 0, len(r.Values))}
		for _, pair := range r.Values {
			if len(pair) != 2 {
				continue
			}
			var ts float64
			var valStr string
			if err := json.Unmarshal(pair[0], &ts); err != nil {
				continue
			}
			if err := json.Unmarshal(pair[1], &valStr); err != nil {
				continue
			}
			v, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				continue
			}
			s.Values = append(s.Values, Point{T: ts, V: v})
		}
		out.Series = append(out.Series, s)
	}
	return out, nil
}

func (c *Client) Ready(ctx context.Context) error {
	if !c.Configured() {
		return fmt.Errorf("prometheus URL is not configured")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/-/healthy", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("prometheus healthy: status %s", resp.Status)
	}
	return nil
}
