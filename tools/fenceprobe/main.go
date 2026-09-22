// Command fenceprobe is a chaos-testing tool: it opens a worker stream while
// impersonating a given worker identity and reports a job as completed. Use it to
// demonstrate fencing — a zombie worker's late result must be refused by the
// server once the job has been reassigned.
//
// Usage: go run ./tools/fenceprobe <job-id> <impersonated-worker-id>
//
// Opening the stream registers the probe as a worker looking for work, so it may
// also be handed a job; it ignores those, and abandons anything it is given when
// it exits. That job comes back on its own once its lease expires.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ss1ngh/distq-go/internal/config"
	"github.com/ss1ngh/distq-go/internal/pb"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: fenceprobe <job-id> <impersonated-worker-id>")
		os.Exit(1)
	}
	jobID, workerID := os.Args[1], os.Args[2]

	conn, err := grpc.NewClient(config.ServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := pb.NewJobQueueClient(conn).JobStream(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open stream: %v\n", err)
		os.Exit(1)
	}

	// Hello alone is enough: as far as the server is concerned this is the worker
	// it leased the job to, which is exactly the claim under test.
	greeting := &pb.WorkerMessage{Message: &pb.WorkerMessage_Hello{Hello: &pb.Hello{WorkerId: workerID}}}
	if err := stream.Send(greeting); err != nil {
		fmt.Fprintf(os.Stderr, "hello: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("[fenceprobe] zombie worker %q attempting to COMPLETE job %s...\n", workerID, jobID)
	completed := &pb.WorkerMessage{Message: &pb.WorkerMessage_Completed{Completed: &pb.Completed{JobId: jobID}}}
	if err := stream.Send(completed); err != nil {
		fmt.Fprintf(os.Stderr, "complete: %v\n", err)
		os.Exit(1)
	}

	// Work the server may push at this probe is noise; wait for the verdict.
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			fmt.Println("[fenceprobe] stream closed before the server answered")
			return
		}
		if err != nil {
			fmt.Printf("[fenceprobe] RPC error (fence held): %v\n", err)
			return
		}

		switch m := msg.GetMessage().(type) {
		case *pb.ServerMessage_Assigned:
			fmt.Printf("[fenceprobe] ignoring a job pushed to this worker (%s)\n", m.Assigned.GetJob().GetId()[:8])
		case *pb.ServerMessage_Lost:
			fmt.Printf("[fenceprobe] job %s was taken away\n", m.Lost.GetJobId())
		case *pb.ServerMessage_Result:
			printVerdict(m.Result, jobID)
			return
		}
	}
}

func printVerdict(r *pb.Result, jobID string) {
	if r.GetOutcome() == pb.Outcome_OUTCOME_REFUSED {
		fmt.Printf("[fenceprobe] REJECTED by fence ✓: %s\n", r.GetError())
		return
	}
	fmt.Printf("[fenceprobe] ⚠ COMPLETION ACCEPTED for job %s — fencing is broken, the zombie won!\n", jobID)
}
