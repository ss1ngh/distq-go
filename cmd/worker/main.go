package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ss1ngh/distq-go/internal/pb"
)

func main() {
	workerID := uuid.New().String()
	fmt.Printf("[Worker %s] Booting Distributed Node...\n", workerID[:8])

	conn, err := grpc.NewClient("localhost:4040", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Worker] Fatal: Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewJobQueueClient(conn)

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

	for {
		if ctx.Err() != nil {
			break
		}

		req := &pb.DequeueRequest{WorkerId: workerID}
		resp, err := client.Dequeue(ctx, req)

		if err != nil {
			fmt.Printf("[Worker] Network error: %v\n", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if resp.Error != "" {
			fmt.Printf("[Worker] Server error: %s\n", resp.Error)
			time.Sleep(2 * time.Second)
			continue
		}

		job := resp.GetJob()
		if job == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		fmt.Printf("[Worker] Processing Job [%s] - Type: %s\n", job.Id, job.Type)
		time.Sleep(2 * time.Second) // Simulate work

		ackResp, err := client.Ack(ctx, &pb.AckRequest{JobId: job.Id})
		if err != nil || ackResp.Error != "" {
			fmt.Printf("[Worker] Failed to ACK Job [%s]\n", job.Id)
		} else {
			fmt.Printf("[Worker] Successfully completed Job [%s]\n", job.Id)
		}
	}
	fmt.Println("[Worker] Engine shutdown complete.")
}
