package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chzyer/readline"
	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const usage = "usage: kvctl --address HOST:PORT [--timeout DURATION]"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("kvctl", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	address := flags.String("address", "", "KVService gRPC address")
	timeout := flags.Duration("timeout", 3*time.Second, "RPC timeout")
	flags.Usage = func() { fmt.Fprintln(flags.Output(), usage) }
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *address == "" || flags.NArg() != 0 {
		return errors.New(usage)
	}
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}

	conn, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer conn.Close()

	prompt, err := readline.NewEx(&readline.Config{
		Prompt:          "kv> ",
		HistoryLimit:    100,
		InterruptPrompt: "^C",
		EOFPrompt:       "^D",
	})
	if err != nil {
		return fmt.Errorf("create prompt: %w", err)
	}
	defer prompt.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = prompt.Close()
		case <-done:
		}
	}()

	executor := newCommandExecutor(kvv1.NewKVServiceClient(conn), *timeout, os.Stdout)
	err = runSession(prompt, executor, os.Stderr)
	close(done)
	if ctx.Err() != nil {
		return nil
	}
	return err
}
