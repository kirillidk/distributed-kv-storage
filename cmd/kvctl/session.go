package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/chzyer/readline"
	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"google.golang.org/grpc/status"
)

const helpText = `set <key> <value> <ttl>  set a value
get <key>                get a value and its expiration time
help                     show this help
exit | quit              exit kvctl

TTL:
0                     store forever
1..2592000            relative TTL in seconds
greater than 2592000  Unix timestamp
`

type lineReader interface {
	Readline() (string, error)
}

type commandExecutor struct {
	client  kvv1.KVServiceClient
	timeout time.Duration
	stdout  io.Writer
}

func newCommandExecutor(client kvv1.KVServiceClient, timeout time.Duration, stdout io.Writer) *commandExecutor {
	return &commandExecutor{client: client, timeout: timeout, stdout: stdout}
}

func runSession(reader lineReader, executor *commandExecutor, stderr io.Writer) error {
	for {
		line, err := reader.Readline()
		if errors.Is(err, readline.ErrInterrupt) || errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read command: %w", err)
		}

		exit, err := executor.execute(line)
		if err != nil {
			writeCommandError(stderr, err)
			continue
		}
		if exit {
			return nil
		}
	}
}

func (e *commandExecutor) execute(line string) (bool, error) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return false, nil
	}

	switch fields[0] {
	case "set":
		if len(fields) < 4 {
			return false, errors.New("usage: set <key> <value> <ttl>")
		}
		ttlText := fields[len(fields)-1]
		ttl, err := strconv.ParseUint(ttlText, 10, 64)
		if err != nil {
			return false, fmt.Errorf("invalid ttl %q", ttlText)
		}

		value, err := parseValue(strings.Join(fields[2:len(fields)-1], " "))
		if err != nil {
			return false, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
		defer cancel()
		_, err = e.client.Set(ctx, &kvv1.SetRequest{
			Key:   []byte(fields[1]),
			Value: []byte(value),
			Ttl:   ttl,
		})
		if err != nil {
			return false, err
		}
		fmt.Fprintln(e.stdout, "OK")

	case "get":
		if len(fields) != 2 {
			return false, errors.New("usage: get <key>")
		}
		ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
		defer cancel()
		response, err := e.client.Get(ctx, &kvv1.GetRequest{Key: []byte(fields[1])})
		if err != nil {
			return false, err
		}
		fmt.Fprintf(e.stdout, "value: %q\nexpires_at: %d\n", string(response.GetValue()), response.GetExpiresAt())

	case "help":
		if len(fields) != 1 {
			return false, errors.New("usage: help")
		}
		fmt.Fprint(e.stdout, helpText)

	case "exit", "quit":
		if len(fields) != 1 {
			return false, fmt.Errorf("usage: %s", fields[0])
		}
		return true, nil

	default:
		return false, fmt.Errorf("unknown command %q", fields[0])
	}

	return false, nil
}

func parseValue(value string) (string, error) {
	if !strings.HasPrefix(value, `"`) {
		return value, nil
	}
	unquoted, err := strconv.Unquote(value)
	if err != nil {
		return "", fmt.Errorf("invalid quoted value: %w", err)
	}
	return unquoted, nil
}

func writeCommandError(w io.Writer, err error) {
	if rpcStatus, ok := status.FromError(err); ok {
		fmt.Fprintf(w, "error: %s: %s\n", rpcStatus.Code(), rpcStatus.Message())
		return
	}
	fmt.Fprintln(w, "error:", err)
}
