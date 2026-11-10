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

const helpText = `Command                       API (kv.v1.KVService) Result
set <key> <value> <ttl>       Set                   OK
get <key>                     Get                   value, expires_at
add <key> <value> <ttl>       Add                   success: true|false
replace <key> <value> <ttl>   Replace               success: true|false
delete <key>                  Delete                success: true|false
get-expire-time <key>         GetExpireTime         expires_at
incr <key> <delta>            Incr                  value
decr <key> <delta>            Decr                  value
help                          -                     show this help
exit | quit                   -                     exit kvctl

add creates an absent key; replace and delete require an existing key.
success is false if the condition is not met. Expired keys count as absent.
Values may contain spaces; double-quoted values support escapes and "".

TTL:
0                     store forever
1..2592000            relative TTL in seconds
greater than 2592000  Unix timestamp
expires_at is an absolute Unix timestamp in seconds, or 0 for no expiration.

Delta:
signed int64; incr adds delta, decr subtracts delta, 0 leaves the value unchanged
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
	case "set", "add", "replace":
		if len(fields) < 4 {
			return false, fmt.Errorf("usage: %s <key> <value> <ttl>", fields[0])
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
		switch fields[0] {
		case "set":
			_, err = e.client.Set(ctx, &kvv1.SetRequest{
				Key:   []byte(fields[1]),
				Value: []byte(value),
				Ttl:   ttl,
			})
			if err != nil {
				return false, err
			}
			fmt.Fprintln(e.stdout, "OK")

		case "add":
			response, err := e.client.Add(ctx, &kvv1.AddRequest{
				Key:   []byte(fields[1]),
				Value: []byte(value),
				Ttl:   ttl,
			})
			if err != nil {
				return false, err
			}
			fmt.Fprintf(e.stdout, "success: %t\n", response.GetSuccess())

		case "replace":
			response, err := e.client.Replace(ctx, &kvv1.ReplaceRequest{
				Key:   []byte(fields[1]),
				Value: []byte(value),
				Ttl:   ttl,
			})
			if err != nil {
				return false, err
			}
			fmt.Fprintf(e.stdout, "success: %t\n", response.GetSuccess())
		}

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

	case "delete":
		if len(fields) != 2 {
			return false, errors.New("usage: delete <key>")
		}
		ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
		defer cancel()
		response, err := e.client.Delete(ctx, &kvv1.DeleteRequest{Key: []byte(fields[1])})
		if err != nil {
			return false, err
		}
		fmt.Fprintf(e.stdout, "success: %t\n", response.GetSuccess())

	case "get-expire-time":
		if len(fields) != 2 {
			return false, errors.New("usage: get-expire-time <key>")
		}
		ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
		defer cancel()
		response, err := e.client.GetExpireTime(ctx, &kvv1.GetExpireTimeRequest{Key: []byte(fields[1])})
		if err != nil {
			return false, err
		}
		fmt.Fprintf(e.stdout, "expires_at: %d\n", response.GetExpiresAt())

	case "incr", "decr":
		if len(fields) != 3 {
			return false, fmt.Errorf("usage: %s <key> <delta>", fields[0])
		}
		delta, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return false, fmt.Errorf("invalid delta %q", fields[2])
		}
		ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
		defer cancel()
		switch fields[0] {
		case "incr":
			response, err := e.client.Incr(ctx, &kvv1.IncrRequest{Key: []byte(fields[1]), Delta: delta})
			if err != nil {
				return false, err
			}
			fmt.Fprintf(e.stdout, "value: %d\n", response.GetValue())

		case "decr":
			response, err := e.client.Decr(ctx, &kvv1.DecrRequest{Key: []byte(fields[1]), Delta: delta})
			if err != nil {
				return false, err
			}
			fmt.Fprintf(e.stdout, "value: %d\n", response.GetValue())
		}

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
