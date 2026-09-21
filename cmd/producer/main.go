package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ss1ngh/distq-go/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fmt.Println("[Producer] Booting up...")

	//Dial grpc server, insecure for localhost
	conn, err := grpc.NewClient("localhost:4040", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Producer] Fatal: Could not connect to server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	//instantiate auto generated client stub
	client := pb.NewJobQueueClient(conn)
	fmt.Println("[Producer] Connected to gRPC Server at localhost:4040")

	ctx := context.Background()
	jobCount := 10 //inject 10 jobs

	fmt.Printf("[Producer] Injecting %d jobs into the network...\n", jobCount)

	for i := 1; i <= jobCount; i++ {
		//Generate payload
		payload := []byte(fmt.Sprintf(`{"video_id": "vid_%d", "resolution": "1080p"}`, i))

		req := &pb.EnqueueRequest{
			Job: &pb.Job{
				Type:    "process_video",
				Payload: payload,
			},
		}

		resp, err := client.Enqueue(ctx, req)
		if err != nil {
			fmt.Printf("[Producer] Network error enqueuing job %d: %v\n", i, err)
			continue
		}

		if resp.Error != "" {
			fmt.Printf("[Producer] Server rejected Job %d : %v\n", i, resp.Error)
			continue
		}

		fmt.Printf("[Producer] Successfully enqueued job %d -> ID: %s\n", i, resp.JobId)
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("[Producer] Finished injecting jobs. Shutting down.")
}
