package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ss1ngh/distq-go/internal/client"
)

func main() {
	fmt.Println("[Worker] Booting Distributed Node...")

	// 1. Dial the Queue Server using the RPC Stub
	c, err := client.New("localhost:4040")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Worker] Fatal: Failed to connect to queue server: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Println("[Worker] Successfully connected to Queue Server at localhost:4040")

	// 2. Setup context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		fmt.Println("\n[Worker] Shutdown signal received. Draining current job...")
		cancel()
	}()

	fmt.Println("[Worker] Listening for jobs...")

	// 3. The Polling Engine
	for {
		// Check if OS requested a shutdown
		if ctx.Err() != nil {
			break
		}

		// 4. Request a job over TCP
		j, err := c.Dequeue(ctx)
		if err != nil {
			fmt.Printf("[Worker] Network error: %v\n", err)
			time.Sleep(2 * time.Second) // Network backoff
			continue
		}

		// If the queue is empty, the server returns nil. Sleep to prevent CPU spinning.
		if j == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		// 5. Execute Business Logic
		fmt.Printf("[Worker] Processing Job ID [%s] - Type: %s\n", j.ID, j.Type)

		// Simulate heavy computational work or I/O
		time.Sleep(2 * time.Second)

		// 6. Network Acknowledgement
		if err := c.Ack(ctx, j.ID); err != nil {
			fmt.Printf("[Worker] Failed to ACK Job [%s]: %v\n", j.ID, err)
		} else {
			fmt.Printf("[Worker] Successfully completed Job [%s]\n", j.ID)
		}
	}

	fmt.Println("[Worker] Engine shutdown complete.")
}
