package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"testing"
	"time"

	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestExecuteNewMethods(t *testing.T) {
	tests := []struct {
		command  string
		method   string
		request  proto.Message
		response proto.Message
		output   string
	}{
		{
			command:  "add name distributed kv 3600",
			method:   kvv1.KVService_Add_FullMethodName,
			request:  &kvv1.AddRequest{Key: []byte("name"), Value: []byte("distributed kv"), Ttl: 3600},
			response: &kvv1.AddResponse{Success: true},
			output:   "success: true\n",
		},
		{
			command:  `add empty "" 0`,
			method:   kvv1.KVService_Add_FullMethodName,
			request:  &kvv1.AddRequest{Key: []byte("empty")},
			response: &kvv1.AddResponse{Success: false},
			output:   "success: false\n",
		},
		{
			command:  `add binary "\x00\n" 18446744073709551615`,
			method:   kvv1.KVService_Add_FullMethodName,
			request:  &kvv1.AddRequest{Key: []byte("binary"), Value: []byte{0, '\n'}, Ttl: ^uint64(0)},
			response: &kvv1.AddResponse{Success: true},
			output:   "success: true\n",
		},
		{
			command:  `replace name "new\nvalue" 1790000000`,
			method:   kvv1.KVService_Replace_FullMethodName,
			request:  &kvv1.ReplaceRequest{Key: []byte("name"), Value: []byte("new\nvalue"), Ttl: 1_790_000_000},
			response: &kvv1.ReplaceResponse{Success: true},
			output:   "success: true\n",
		},
		{
			command:  `replace empty "" 0`,
			method:   kvv1.KVService_Replace_FullMethodName,
			request:  &kvv1.ReplaceRequest{Key: []byte("empty")},
			response: &kvv1.ReplaceResponse{Success: false},
			output:   "success: false\n",
		},
		{
			command:  "delete name",
			method:   kvv1.KVService_Delete_FullMethodName,
			request:  &kvv1.DeleteRequest{Key: []byte("name")},
			response: &kvv1.DeleteResponse{Success: true},
			output:   "success: true\n",
		},
		{
			command:  "delete missing",
			method:   kvv1.KVService_Delete_FullMethodName,
			request:  &kvv1.DeleteRequest{Key: []byte("missing")},
			response: &kvv1.DeleteResponse{Success: false},
			output:   "success: false\n",
		},
		{
			command:  "get-expire-time name",
			method:   kvv1.KVService_GetExpireTime_FullMethodName,
			request:  &kvv1.GetExpireTimeRequest{Key: []byte("name")},
			response: &kvv1.GetExpireTimeResponse{ExpiresAt: 1_790_000_000},
			output:   "expires_at: 1790000000\n",
		},
		{
			command:  "get-expire-time forever",
			method:   kvv1.KVService_GetExpireTime_FullMethodName,
			request:  &kvv1.GetExpireTimeRequest{Key: []byte("forever")},
			response: &kvv1.GetExpireTimeResponse{},
			output:   "expires_at: 0\n",
		},
		{
			command:  "incr counter +5",
			method:   kvv1.KVService_Incr_FullMethodName,
			request:  &kvv1.IncrRequest{Key: []byte("counter"), Delta: 5},
			response: &kvv1.IncrResponse{Value: 15},
			output:   "value: 15\n",
		},
		{
			command:  "incr counter -9223372036854775808",
			method:   kvv1.KVService_Incr_FullMethodName,
			request:  &kvv1.IncrRequest{Key: []byte("counter"), Delta: -1 << 63},
			response: &kvv1.IncrResponse{Value: -1 << 63},
			output:   "value: -9223372036854775808\n",
		},
		{
			command:  "incr counter 9223372036854775807",
			method:   kvv1.KVService_Incr_FullMethodName,
			request:  &kvv1.IncrRequest{Key: []byte("counter"), Delta: 1<<63 - 1},
			response: &kvv1.IncrResponse{Value: 1<<63 - 1},
			output:   "value: 9223372036854775807\n",
		},
		{
			command:  "incr counter 0",
			method:   kvv1.KVService_Incr_FullMethodName,
			request:  &kvv1.IncrRequest{Key: []byte("counter")},
			response: &kvv1.IncrResponse{Value: 10},
			output:   "value: 10\n",
		},
		{
			command:  "decr counter -5",
			method:   kvv1.KVService_Decr_FullMethodName,
			request:  &kvv1.DecrRequest{Key: []byte("counter"), Delta: -5},
			response: &kvv1.DecrResponse{Value: 15},
			output:   "value: 15\n",
		},
		{
			command:  "decr counter -9223372036854775808",
			method:   kvv1.KVService_Decr_FullMethodName,
			request:  &kvv1.DecrRequest{Key: []byte("counter"), Delta: -1 << 63},
			response: &kvv1.DecrResponse{},
			output:   "value: 0\n",
		},
		{
			command:  "decr counter 9223372036854775807",
			method:   kvv1.KVService_Decr_FullMethodName,
			request:  &kvv1.DecrRequest{Key: []byte("counter"), Delta: 1<<63 - 1},
			response: &kvv1.DecrResponse{Value: -1<<63 + 1},
			output:   "value: -9223372036854775807\n",
		},
		{
			command:  "decr counter 0",
			method:   kvv1.KVService_Decr_FullMethodName,
			request:  &kvv1.DecrRequest{Key: []byte("counter")},
			response: &kvv1.DecrResponse{Value: -10},
			output:   "value: -10\n",
		},
	}

	for _, test := range tests {
		t.Run(test.command, func(t *testing.T) {
			client := newFakeKVClient(t, &fakeKVServer{}, grpc.UnaryInterceptor(
				func(_ context.Context, request any, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
					assert.Equal(t, test.method, info.FullMethod)
					assert.True(t, proto.Equal(test.request, request.(proto.Message)),
						"request = %v, want %v", request, test.request)
					return test.response, nil
				},
			))
			var stdout bytes.Buffer
			executor := newCommandExecutor(client, time.Second, &stdout)

			exit, err := executor.execute(test.command)

			require.NoError(t, err)
			assert.False(t, exit)
			assert.Equal(t, test.output, stdout.String())
		})
	}
}

func TestExecuteNewMethodsRejectInvalidArguments(t *testing.T) {
	executor := newCommandExecutor(nil, time.Second, io.Discard)
	tests := []struct {
		command string
		want    string
	}{
		{"delete", "usage: delete <key>"},
		{"delete key extra", "usage: delete <key>"},
		{"get-expire-time", "usage: get-expire-time <key>"},
		{"get-expire-time key extra", "usage: get-expire-time <key>"},
	}
	for _, command := range []string{"add", "replace"} {
		tests = append(tests, struct{ command, want string }{
			command + " key value", "usage: " + command + " <key> <value> <ttl>",
		})
		for _, ttl := range []string{"nope", "-1", "18446744073709551616"} {
			tests = append(tests, struct{ command, want string }{
				command + " key value " + ttl, fmt.Sprintf("invalid ttl %q", ttl),
			})
		}
		tests = append(tests, struct{ command, want string }{
			command + ` key "unfinished 0`, "invalid quoted value: invalid syntax",
		})
	}
	for _, command := range []string{"incr", "decr"} {
		for _, args := range []string{"", " key", " key 1 extra"} {
			tests = append(tests, struct{ command, want string }{
				command + args, "usage: " + command + " <key> <delta>",
			})
		}
		for _, delta := range []string{"nope", "1.5", "9223372036854775808", "-9223372036854775809"} {
			tests = append(tests, struct{ command, want string }{
				command + " key " + delta, fmt.Sprintf("invalid delta %q", delta),
			})
		}
	}

	for _, test := range tests {
		t.Run(test.command, func(t *testing.T) {
			exit, err := executor.execute(test.command)

			require.EqualError(t, err, test.want)
			assert.False(t, exit)
		})
	}
}

func TestExecuteNewMethodRPCFailures(t *testing.T) {
	tests := []struct {
		command string
		code    codes.Code
	}{
		{"add key value 0", codes.Unavailable},
		{"replace key value 0", codes.InvalidArgument},
		{"delete key", codes.Internal},
		{"get-expire-time key", codes.NotFound},
		{"incr key 1", codes.FailedPrecondition},
		{"decr key 1", codes.InvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.command, func(t *testing.T) {
			for _, timeout := range []bool{false, true} {
				t.Run(fmt.Sprintf("timeout=%t", timeout), func(t *testing.T) {
					client := newFakeKVClient(t, &fakeKVServer{}, grpc.UnaryInterceptor(
						func(ctx context.Context, _ any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
							if timeout {
								<-ctx.Done()
								return nil, status.FromContextError(ctx.Err()).Err()
							}
							return nil, status.Error(test.code, "failed")
						},
					))
					var stdout bytes.Buffer
					rpcTimeout := time.Second
					wantCode := test.code
					if timeout {
						rpcTimeout = 50 * time.Millisecond
						wantCode = codes.DeadlineExceeded
					}
					executor := newCommandExecutor(client, rpcTimeout, &stdout)

					exit, err := executor.execute(test.command)

					require.Error(t, err)
					assert.False(t, exit)
					assert.Equal(t, wantCode, status.Code(err))
					assert.Empty(t, stdout.String())
				})
			}
		})
	}
}

func TestHelpListsCommandsAndAPI(t *testing.T) {
	var stdout bytes.Buffer
	executor := newCommandExecutor(nil, time.Second, &stdout)

	exit, err := executor.execute("help")

	require.NoError(t, err)
	assert.False(t, exit)
	assert.Contains(t, stdout.String(), "API (kv.v1.KVService)")
	for _, entry := range []struct{ syntax, rpc string }{
		{"set <key> <value> <ttl>", "Set"},
		{"get <key>", "Get"},
		{"add <key> <value> <ttl>", "Add"},
		{"replace <key> <value> <ttl>", "Replace"},
		{"delete <key>", "Delete"},
		{"get-expire-time <key>", "GetExpireTime"},
		{"incr <key> <delta>", "Incr"},
		{"decr <key> <delta>", "Decr"},
	} {
		assert.Regexp(t, "(?m)^"+regexp.QuoteMeta(entry.syntax)+`\s+`+entry.rpc+`\s+`, stdout.String())
	}
	assert.Contains(t, stdout.String(), "signed int64")
	assert.Contains(t, stdout.String(), "Unix timestamp")
}
