package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ss1ngh/distq-go/internal/client"
)

func main() {
	fmt.Println("[Producer] Booting up...")

	// 1. Dial the Queue Server via the Client RPC Stub
	c, err := client.New("localhost:4040")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Producer] Fatal: Could not connect to server: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Println("[Producer] Connected to Queue Server at localhost:4040")

	ctx := context.Background()
	jobCount := 5 // Let's inject 5 jobs to watch the workers fight over them

	fmt.Printf("[Producer] Injecting %d jobs into the network...\n", jobCount)

	for i := 1; i <= jobCount; i++ {
		// 2. Generate the payload
		payload := []byte(fmt.Sprintf(`{"video_id": "vid_%d", "resolution": "1080p"}`, i))

		// 3. Fire the job over the TCP socket
		jobID, err := c.Enqueue(ctx, "process_video", payload)
		if err != nil {
			fmt.Printf("[Producer] Failed to enqueue job %d: %v\n", i, err)
			continue
		}

		fmt.Printf("[Producer] Successfully enqueued job %d -> ID: %s\n", i, jobID)

		// Add a tiny delay just so it's easier to read the terminal output
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("[Producer] Finished injecting jobs. Shutting down.")
}
