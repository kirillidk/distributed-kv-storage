package main

import (
	"context"
	"strings"
	"testing"

	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type fakeKVClient struct {
	setRequest *kvv1.SetRequest
	getRequest *kvv1.GetRequest
	getValue   []byte
}

func (f *fakeKVClient) Set(
	_ context.Context,
	in *kvv1.SetRequest,
	_ ...grpc.CallOption,
) (*kvv1.SetResponse, error) {
	f.setRequest = in
	return &kvv1.SetResponse{}, nil
}

func (f *fakeKVClient) Get(
	_ context.Context,
	in *kvv1.GetRequest,
	_ ...grpc.CallOption,
) (*kvv1.GetResponse, error) {
	f.getRequest = in
	return &kvv1.GetResponse{Value: f.getValue}, nil
}

func TestExecuteCommandGet(t *testing.T) {
	client := &fakeKVClient{getValue: []byte("Ivan")}
	var out strings.Builder

	exit, err := executeCommand(client, "get name", &out)

	require.NoError(t, err)
	require.False(t, exit)
	require.Equal(t, []byte("name"), client.getRequest.Key)
	require.Equal(t, "Ivan\n", out.String())
}

func TestExecuteCommandSet(t *testing.T) {
	client := &fakeKVClient{}
	var out strings.Builder

	exit, err := executeCommand(client, "set name Ivan", &out)

	require.NoError(t, err)
	require.False(t, exit)
	require.Equal(t, []byte("name"), client.setRequest.Key)
	require.Equal(t, []byte("Ivan"), client.setRequest.Value)
	require.Equal(t, uint64(0), client.setRequest.Ttl)
	require.Equal(t, "OK\n", out.String())
}

func TestExecuteCommandSetWithTTL(t *testing.T) {
	client := &fakeKVClient{}
	var out strings.Builder

	exit, err := executeCommand(client, "set temp value 10", &out)

	require.NoError(t, err)
	require.False(t, exit)
	require.Equal(t, []byte("temp"), client.setRequest.Key)
	require.Equal(t, []byte("value"), client.setRequest.Value)
	require.Equal(t, uint64(10), client.setRequest.Ttl)
}

func TestExecuteCommandInvalidTTL(t *testing.T) {
	client := &fakeKVClient{}
	var out strings.Builder

	_, err := executeCommand(client, "set temp value abc", &out)

	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid ttl")
}

func TestExecuteCommandWrongArguments(t *testing.T) {
	client := &fakeKVClient{}
	var out strings.Builder

	_, err := executeCommand(client, "get", &out)

	require.EqualError(t, err, "usage: get <key>")
}

func TestExecuteCommandUnknown(t *testing.T) {
	client := &fakeKVClient{}
	var out strings.Builder

	_, err := executeCommand(client, "hello", &out)

	require.EqualError(t, err, "unknown command: hello")
}

func TestExecuteCommandExit(t *testing.T) {
	client := &fakeKVClient{}
	var out strings.Builder

	exit, err := executeCommand(client, "exit", &out)

	require.NoError(t, err)
	require.True(t, exit)
}
