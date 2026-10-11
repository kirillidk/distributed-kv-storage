package repl

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"
	"testing"
	"time"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestWALProcessRecoveryBeforeClientRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and crashes real repl/kv-engine processes")
	}
	dir := t.TempDir()
	for _, name := range []string{"repl", "kv-engine"} {
		cmd := exec.Command("go", "build", "-o", filepath.Join(dir, name), "./cmd/"+name)
		cmd.Dir = filepath.Join("..", "..")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", name, err, out)
		}
	}
	freeAddress := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		l.Close()
		return addr
	}
	replAddr, engineAddr := freeAddress(), freeAddress()
	_, port, err := net.SplitHostPort(engineAddr)
	if err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(dir, "node.wal")
	manifestPath := filepath.Join(dir, "cluster.yaml")
	manifest := fmt.Sprintf("nodes:\n  - id: smoke\n    repl:\n      listen_addr: %s\n      connect_addr: %s\n      wal_path: %s\n    engine:\n      connect_addr: %s\n      command: [%s, --port, '%s']\n      restart_delay: 100ms\n      shutdown_timeout: 2s\n", replAddr, replAddr, walPath, engineAddr, filepath.Join(dir, "kv-engine"), port)
	if err := os.WriteFile(manifestPath, []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	var active *exec.Cmd
	var done chan error
	start := func(name string) string {
		logPath := filepath.Join(dir, name)
		log, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(filepath.Join(dir, "repl"), "--manifest", manifestPath, "--node-id", "smoke")
		cmd.Stdout, cmd.Stderr = log, log
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		log.Close()
		active = cmd
		wait := make(chan error, 1)
		done = wait
		go func() { wait <- cmd.Wait() }()
		return logPath
	}
	t.Cleanup(func() {
		if active != nil {
			syscall.Kill(-active.Process.Pid, syscall.SIGKILL)
			<-done
		}
	})
	poll := func(message string, check func() bool) {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if check() {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		for _, name := range []string{"first.log", "second.log"} {
			data, _ := os.ReadFile(filepath.Join(dir, name))
			t.Logf("%s:\n%s", name, data)
		}
		t.Fatal(message)
	}
	connect := func(addr string) *grpc.ClientConn {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	pidPattern := regexp.MustCompile(`msg="engine started" pid=([0-9]+)`)
	enginePID := func(logPath string, count int) int {
		var pid int
		poll("engine was not started", func() bool {
			data, _ := os.ReadFile(logPath)
			matches := pidPattern.FindAllSubmatch(data, -1)
			if len(matches) < count {
				return false
			}
			pid, _ = strconv.Atoi(string(matches[count-1][1]))
			return pid != 0
		})
		return pid
	}
	log1 := start("first.log")
	replConn, engineConn := connect(replAddr), connect(engineAddr)
	poll("repl never became ready", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		r, err := healthpb.NewHealthClient(replConn).Check(ctx, &healthpb.HealthCheckRequest{})
		return err == nil && r.Status == healthpb.HealthCheckResponse_SERVING
	})
	firstPID := enginePID(log1, 1)
	keys := [][]byte{[]byte("permanent"), {0, 255, 1}, []byte("with-ttl")}
	values := [][]byte{[]byte("latest"), {255, 0, 128}, []byte("ttl-value")}
	var expected uint64
	set := func(key, value []byte, ttl uint64) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := kvv1.NewKVServiceClient(engineConn).Set(ctx, &kvv1.SetRequest{Key: key, Value: value, Ttl: ttl}); err != nil {
			t.Fatal(err)
		}
		expected += uint64(20 + len(key) + len(value))
	}
	set(keys[0], []byte("old"), 0)
	for i := range keys {
		var ttl uint64
		if i == 2 {
			ttl = 60
		}
		set(keys[i], values[i], ttl)
	}
	var expiry uint64
	verify := func(stage string) {
		for i, key := range keys {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			got, err := kvv1.NewKVServiceClient(engineConn).Get(ctx, &kvv1.GetRequest{Key: key})
			cancel()
			if err != nil || !bytes.Equal(got.GetValue(), values[i]) {
				t.Fatalf("%s key %x: %v %v", stage, key, got, err)
			}
			if i == 2 {
				if expiry == 0 {
					expiry = got.ExpiresAt
				}
				if got.ExpiresAt != expiry {
					t.Fatalf("%s extended TTL", stage)
				}
			} else if got.ExpiresAt != 0 {
				t.Fatalf("%s changed permanent expiry", stage)
			}
		}
	}
	waitOffset := func(stage string) {
		poll(stage+": WAL was not replayed before client requests", func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			r, err := enginev1.NewEngineServiceClient(engineConn).GetOffset(ctx, &enginev1.GetOffsetRequest{})
			return err == nil && r.Offset == expected
		})
		t.Logf("%s: appliedOffset=%d before any Set/Get", stage, expected)
	}
	verify("initial writes")
	before, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(firstPID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	secondPID := enginePID(log1, 2)
	if secondPID == firstPID {
		t.Fatal("engine was not restarted")
	}
	waitOffset("engine SIGKILL")
	verify("engine SIGKILL")
	if err := syscall.Kill(-active.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	<-done
	active = nil
	log2 := start("second.log")
	engineConn = connect(engineAddr)
	enginePID(log2, 1)
	waitOffset("repl and engine SIGKILL")
	verify("repl and engine SIGKILL")
	after, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("recovery duplicated or changed WAL records")
	}
	set([]byte("after-restart"), []byte("new-write"), 0)
	waitOffset("new write")
	if err := active.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		active = nil
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("repl did not shut down")
	}
}
