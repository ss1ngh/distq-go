// Command fenceprobe is a chaos-testing tool: it attempts a Complete RPC
// impersonating a given worker identity. Use it to demonstrate fencing —
// a zombie worker's late result must be rejected by the server.
//
// Usage: go run ./tools/fenceprobe <job-id> <impersonated-worker-id>
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ss1ngh/distq-go/internal/pb"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: fenceprobe <job-id> <impersonated-worker-id>")
		os.Exit(1)
	}
	jobID, workerID := os.Args[1], os.Args[2]

	conn, err := grpc.NewClient("localhost:4040", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewJobQueueClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Printf("[fenceprobe] zombie worker %q attempting to COMPLETE job %s...\n", workerID, jobID)
	resp, err := client.Complete(ctx, &pb.CompleteRequest{JobId: jobID, WorkerId: workerID})
	if err != nil {
		fmt.Printf("[fenceprobe] RPC error (fence held): %v\n", err)
		return
	}
	if resp.Error != "" {
		fmt.Printf("[fenceprobe] REJECTED by fence ✓: %s\n", resp.Error)
		return
	}
	fmt.Printf("[fenceprobe] ⚠ COMPLETION ACCEPTED — fencing is broken, the zombie won!\n")
}
