package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const requestTimeout = 3 * time.Second

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, in io.Reader, out, errOut io.Writer) error {
	flags := flag.NewFlagSet("kvctl", flag.ContinueOnError)
	flags.SetOutput(errOut)

	address := flags.String("address", "localhost:50052", "KVService gRPC address")

	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: kvctl [--address HOST:PORT]")
	}

	conn, err := grpc.NewClient(
		*address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("create gRPC client: %w", err)
	}
	defer conn.Close()

	client := kvv1.NewKVServiceClient(conn)

	return interactive(client, in, out)
}

func interactive(client kvv1.KVServiceClient, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)

	for {
		fmt.Fprint(out, "kv> ")

		if !scanner.Scan() {
			fmt.Fprintln(out)
			return scanner.Err()
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		exit, err := executeCommand(client, line, out)
		if err != nil {
			fmt.Fprintln(out, err)
			continue
		}

		if exit {
			return nil
		}
	}
}

func executeCommand(client kvv1.KVServiceClient, line string, out io.Writer) (bool, error) {
	args := strings.Fields(line)
	if len(args) == 0 {
		return false, nil
	}

	switch strings.ToLower(args[0]) {
	case "get":
		if len(args) != 2 {
			return false, errors.New("usage: get <key>")
		}

		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()

		response, err := client.Get(ctx, &kvv1.GetRequest{
			Key: []byte(args[1]),
		})
		if err != nil {
			return false, formatRPCError(err)
		}

		fmt.Fprintln(out, string(response.Value))
		return false, nil

	case "set":
		if len(args) != 3 && len(args) != 4 {
			return false, errors.New("usage: set <key> <value> [ttl]")
		}

		var ttl uint64
		if len(args) == 4 {
			parsedTTL, err := strconv.ParseUint(args[3], 10, 64)
			if err != nil {
				return false, fmt.Errorf("invalid ttl %q: %w", args[3], err)
			}
			ttl = parsedTTL
		}

		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()

		_, err := client.Set(ctx, &kvv1.SetRequest{
			Key:   []byte(args[1]),
			Value: []byte(args[2]),
			Ttl:   ttl,
		})
		if err != nil {
			return false, formatRPCError(err)
		}

		fmt.Fprintln(out, "OK")
		return false, nil

	case "help":
		if len(args) != 1 {
			return false, errors.New("usage: help")
		}

		printHelp(out)
		return false, nil

	case "exit", "quit":
		if len(args) != 1 {
			return false, fmt.Errorf("usage: %s", args[0])
		}
		return true, nil

	default:
		return false, fmt.Errorf("unknown command: %s", args[0])
	}
}

func printHelp(out io.Writer) {
	fmt.Fprintln(out, "Commands:")
	fmt.Fprintln(out, "  get <key>")
	fmt.Fprintln(out, "  set <key> <value> [ttl]")
	fmt.Fprintln(out, "  help")
	fmt.Fprintln(out, "  exit")
	fmt.Fprintln(out, "  quit")
}

func formatRPCError(err error) error {
	if st, ok := status.FromError(err); ok {
		return fmt.Errorf("rpc error: %s: %s", st.Code(), st.Message())
	}
	return fmt.Errorf("rpc error: %w", err)
}
