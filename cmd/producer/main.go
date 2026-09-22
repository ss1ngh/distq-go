package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ss1ngh/distq-go/internal/config"
	"github.com/ss1ngh/distq-go/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// demo jobs:
//
//	normal — succeeds on the first attempt
//	flaky  — worker simulates two transient failures, then succeeds (retry path)
//	doomed — always fails; after exhausting retries it lands in the DLQ
func main() {
	jobType := "normal"
	count := 3
	if len(os.Args) > 1 {
		jobType = os.Args[1]
	}
	if len(os.Args) > 2 {
		n, err := strconv.Atoi(os.Args[2])
		if err != nil || n < 1 {
			fmt.Fprintln(os.Stderr, "count must be a positive integer")
			os.Exit(1)
		}
		count = n
	}

	fmt.Printf("[Producer] Booting up...\n")

	addr := config.ServerAddr()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Producer] Fatal: Could not connect to server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewJobQueueClient(conn)
	fmt.Printf("[Producer] Connected to gRPC Server at %s\n", addr)

	ctx := context.Background()
	fmt.Printf("[Producer] Injecting %d '%s' jobs into the network...\n", count, jobType)

	for i := 1; i <= count; i++ {
		payload := []byte(fmt.Sprintf(`{"demo": "%s", "seq": %d}`, jobType, i))

		req := &pb.EnqueueRequest{
			Job: &pb.Job{
				Type:    jobType,
				Payload: payload,
			},
		}

		resp, err := client.Enqueue(ctx, req)
		if err != nil {
			fmt.Printf("[Producer] Network error enqueuing job %d: %v\n", i, err)
			continue
		}
		if resp.Error != "" {
			fmt.Printf("[Producer] Server rejected job %d: %s\n", i, resp.Error)
			continue
		}

		fmt.Printf("[Producer] Enqueued job %d -> ID: %s\n", i, resp.JobId)
		time.Sleep(300 * time.Millisecond)
	}
	fmt.Println("[Producer] Finished injecting jobs. Shutting down.")
}
