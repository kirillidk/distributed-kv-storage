package kvengine

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsServerShutdownProcess(t *testing.T) {
	if os.Getenv("KV_ENGINE_METRICS_SHUTDOWN_TEST") != "1" {
		t.Skip("subprocess helper")
	}
	s := CreateServer(CreateMemoryStorage())
	s.metrics.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "kvengine_test_slow_scrape",
		Help: "A slow collector to keep a scrape active during SIGTERM.",
	}, func() float64 {
		if err := os.WriteFile(os.Getenv("KV_ENGINE_METRICS_STARTED"), []byte("started"), 0600); err != nil {
			return -1
		}
		time.Sleep(time.Second)
		return 1
	}))
	s.RunServer(0, os.Getenv("KV_ENGINE_METRICS_ADDR"))
}

func TestMetricsServerDrainsActiveScrapeOnSIGTERM(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	started := filepath.Join(dir, "scrape-started")
	logPath := filepath.Join(dir, "child.log")
	logs, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMetricsServerShutdownProcess$", "-test.timeout=15s")
	child.Env = append(os.Environ(),
		"KV_ENGINE_METRICS_SHUTDOWN_TEST=1",
		"KV_ENGINE_METRICS_ADDR="+address,
		"KV_ENGINE_METRICS_STARTED="+started,
	)
	child.Stdout, child.Stderr = logs, logs
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	waitFor := func(description string, ready func() bool) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
			if ready() {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		output, _ := os.ReadFile(logPath)
		t.Fatalf("%s did not complete; child logs:\n%s", description, output)
	}
	waitFor("metrics startup", func() bool {
		conn, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	})
	type scrapeResult struct {
		response *http.Response
		err      error
	}
	scraped := make(chan scrapeResult, 1)
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		response, err := client.Get("http://" + address + "/metrics")
		scraped <- scrapeResult{response, err}
	}()
	waitFor("metrics collection", func() bool {
		_, err := os.Stat(started)
		return err == nil
	})
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	result := <-scraped
	if result.err != nil {
		t.Fatalf("SIGTERM aborted an active scrape: %v", result.err)
	}
	defer result.response.Body.Close()
	body, err := io.ReadAll(result.response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if result.response.StatusCode != http.StatusOK || !strings.Contains(string(body), "kvengine_test_slow_scrape 1") {
		t.Fatalf("incomplete metrics response: status=%d, body=%s", result.response.StatusCode, body)
	}
	err = child.Wait()
	waited = true
	if err != nil {
		output, _ := os.ReadFile(logPath)
		t.Fatalf("engine did not stop cleanly: %v; child logs:\n%s", err, output)
	}
}
