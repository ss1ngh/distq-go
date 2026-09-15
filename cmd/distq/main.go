package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ss1ngh/distq-go/internal/queue"
	"github.com/ss1ngh/distq-go/internal/server"
	"github.com/ss1ngh/distq-go/internal/storage"
)

func main() {
	//initialize the storage layer
	store, err := storage.NewSQLiteStore("distq.db")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	//initialize the queue bridge
	q, err := queue.New(queue.Options{Store: store})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create queue: %v\n", err)
		os.Exit(1)
	}

	//set up global context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//initialize and start TCP server
	srv := server.New(":4040", q)

	go func() {
		if err := srv.Start(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		}
	}()

	//block the main thread until an OS interrupt (Ctrl+C)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutdown signal received...")

	//cancel context (stops listener) and wait for active jobs to finish
	cancel()
	srv.Stop()

	fmt.Println("System shutdown complete.")
}
