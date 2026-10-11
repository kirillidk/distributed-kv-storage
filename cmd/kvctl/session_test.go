package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeKVServer struct {
	kvv1.UnimplementedKVServiceServer

	mu          sync.Mutex
	lastSet     *kvv1.SetRequest
	setErr      error
	getResponse *kvv1.GetResponse
	getErr      error
	blockGet    bool
}

func (s *fakeKVServer) Set(_ context.Context, request *kvv1.SetRequest) (*kvv1.SetResponse, error) {
	s.mu.Lock()
	s.lastSet = &kvv1.SetRequest{
		Key:   append([]byte(nil), request.GetKey()...),
		Value: append([]byte(nil), request.GetValue()...),
		Ttl:   request.GetTtl(),
	}
	err := s.setErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &kvv1.SetResponse{}, nil
}

func (s *fakeKVServer) Get(ctx context.Context, _ *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	s.mu.Lock()
	block := s.blockGet
	response := s.getResponse
	err := s.getErr
	s.mu.Unlock()

	if block {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err != nil {
		return nil, err
	}
	if response == nil {
		response = &kvv1.GetResponse{}
	}
	return response, nil
}

func newFakeKVClient(t *testing.T, server *fakeKVServer) kvv1.KVServiceClient {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	kvv1.RegisterKVServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = listener.Close()
	})

	return kvv1.NewKVServiceClient(conn)
}

func TestExecuteSetAndGet(t *testing.T) {
	server := &fakeKVServer{getResponse: &kvv1.GetResponse{
		Value:     []byte("distributed kv"),
		ExpiresAt: 1_790_000_000,
	}}
	client := newFakeKVClient(t, server)
	var stdout bytes.Buffer
	executor := newCommandExecutor(client, time.Second, &stdout)

	exit, err := executor.execute("set name distributed kv 3600")
	require.NoError(t, err)
	assert.False(t, exit)

	server.mu.Lock()
	request := server.lastSet
	server.mu.Unlock()
	require.NotNil(t, request)
	assert.Equal(t, []byte("name"), request.GetKey())
	assert.Equal(t, []byte("distributed kv"), request.GetValue())
	assert.Equal(t, uint64(3600), request.GetTtl())

	exit, err = executor.execute("get name")
	require.NoError(t, err)
	assert.False(t, exit)
	assert.Equal(t, "OK\nvalue: \"distributed kv\"\nexpires_at: 1790000000\n", stdout.String())
}

func TestExecuteSetEmptyQuotedValue(t *testing.T) {
	server := &fakeKVServer{}
	client := newFakeKVClient(t, server)
	executor := newCommandExecutor(client, time.Second, io.Discard)

	_, err := executor.execute(`set empty "" 0`)
	require.NoError(t, err)

	server.mu.Lock()
	request := server.lastSet
	server.mu.Unlock()
	require.NotNil(t, request)
	assert.Empty(t, request.GetValue())
	assert.Zero(t, request.GetTtl())
}

func TestExecuteRejectsInvalidCommands(t *testing.T) {
	executor := newCommandExecutor(newFakeKVClient(t, &fakeKVServer{}), time.Second, io.Discard)

	tests := []struct {
		command string
		want    string
	}{
		{command: "set key value", want: "usage: set <key> <value> <ttl>"},
		{command: "set key value nope", want: `invalid ttl "nope"`},
		{command: "set key value -1", want: `invalid ttl "-1"`},
		{command: "get", want: "usage: get <key>"},
		{command: "get key extra", want: "usage: get <key>"},
		{command: "help extra", want: "usage: help"},
		{command: "unknown", want: `unknown command "unknown"`},
	}

	for _, test := range tests {
		t.Run(test.command, func(t *testing.T) {
			_, err := executor.execute(test.command)
			require.EqualError(t, err, test.want)
		})
	}
}

func TestExecutePreservesRPCCodes(t *testing.T) {
	for _, code := range []codes.Code{codes.NotFound, codes.Internal} {
		t.Run(code.String(), func(t *testing.T) {
			server := &fakeKVServer{getErr: status.Error(code, "failed")}
			executor := newCommandExecutor(newFakeKVClient(t, server), time.Second, io.Discard)

			_, err := executor.execute("get missing")
			require.Error(t, err)
			assert.Equal(t, code, status.Code(err))
		})
	}
}

func TestExecuteAppliesTimeout(t *testing.T) {
	server := &fakeKVServer{blockGet: true}
	executor := newCommandExecutor(newFakeKVClient(t, server), 10*time.Millisecond, io.Discard)

	_, err := executor.execute("get slow")
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
}

func TestRunSessionContinuesAfterError(t *testing.T) {
	server := &fakeKVServer{getErr: status.Error(codes.NotFound, "missing")}
	client := newFakeKVClient(t, server)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	executor := newCommandExecutor(client, time.Second, &stdout)
	reader := &scriptedReader{results: []readResult{
		{line: "get missing"},
		{line: "help"},
		{line: "exit"},
	}}

	require.NoError(t, runSession(reader, executor, &stderr))
	assert.Equal(t, "error: NotFound: missing\n", stderr.String())
	assert.Equal(t, helpText, stdout.String())
}

func TestExecuteHelpAndExit(t *testing.T) {
	var stdout bytes.Buffer
	executor := newCommandExecutor(newFakeKVClient(t, &fakeKVServer{}), time.Second, &stdout)

	exit, err := executor.execute("help")
	require.NoError(t, err)
	assert.False(t, exit)
	assert.Equal(t, helpText, stdout.String())

	for _, command := range []string{"exit", "quit"} {
		exit, err = executor.execute(command)
		require.NoError(t, err)
		assert.True(t, exit)
	}
}

func TestRunSessionStopsOnEOF(t *testing.T) {
	executor := newCommandExecutor(newFakeKVClient(t, &fakeKVServer{}), time.Second, io.Discard)
	reader := &scriptedReader{results: []readResult{{err: io.EOF}}}
	require.NoError(t, runSession(reader, executor, io.Discard))
}

type readResult struct {
	line string
	err  error
}

type scriptedReader struct {
	results []readResult
	next    int
}

func (r *scriptedReader) Readline() (string, error) {
	if r.next >= len(r.results) {
		return "", io.EOF
	}
	result := r.results[r.next]
	r.next++
	return strings.TrimSuffix(result.line, "\n"), result.err
}
