package main

import (
	"context"
	"errors"
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

// flakyHandler simulates real work. Two personalities exercise the failure
// paths: "flaky" fails twice then succeeds (retry path), "doomed" always
// fails (DLQ path).
func flakyHandler(ctx context.Context, job *pb.Job, attempt int) error {
	switch job.Type {
	case "flaky":
		if attempt <= 2 {
			return fmt.Errorf("transient failure (simulated), attempt %d", attempt)
		}
		return nil
	case "doomed":
		return fmt.Errorf("this job never succeeds (simulated)")
	default:
		time.Sleep(500 * time.Millisecond) // simulate work
		return nil
	}
}

func main() {
	workerID := uuid.New().String()[:8]
	fmt.Printf("[Worker %s] Booting Distributed Node...\n", workerID)

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

		resp, err := client.Dequeue(ctx, &pb.DequeueRequest{WorkerId: workerID})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
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
			time.Sleep(1 * time.Second) // idle backoff
			continue
		}

		attempt := int(job.RetryCount) + 1
		fmt.Printf("[Worker %s] Processing Job [%s] type=%s attempt=%d/%d\n",
			workerID, shortID(job.Id), job.Type, attempt, job.MaxRetries+1)

		err = flakyHandler(ctx, job, attempt)

		if err == nil {
			ackResp, ackErr := client.Complete(ctx, &pb.CompleteRequest{JobId: job.Id, WorkerId: workerID})
			if ackErr != nil || ackResp.Error != "" {
				fmt.Printf("[Worker] Failed to COMPLETE Job [%s]: %v %v\n", shortID(job.Id), ackErr, ackResp.GetError())
			} else {
				fmt.Printf("[Worker %s] Job [%s] done ✓\n", workerID, shortID(job.Id))
			}
			continue
		}

		// The lesson: on failure we no longer drop the job. We report it,
		// and the SERVER decides retry-vs-DLQ atomically.
		failResp, failErr := client.Fail(ctx, &pb.FailRequest{
			JobId:        job.Id,
			ErrorMessage: err.Error(),
			WorkerId:     workerID,
		})
		if failErr != nil || failResp.Error != "" {
			fmt.Printf("[Worker] Failed to report failure for [%s]: %v %v\n", shortID(job.Id), failErr, failResp.GetError())
			continue
		}

		if failResp.Retrying {
			fmt.Printf("[Worker %s] Job [%s] failed → re-queued with backoff (attempt %d/%d)\n",
				workerID, shortID(job.Id), attempt, job.MaxRetries+1)
		} else {
			fmt.Printf("[Worker %s] Job [%s] failed → moved to DLQ ✗\n", workerID, shortID(job.Id))
		}

		if errors.Is(err, context.Canceled) {
			break
		}
	}
	fmt.Println("[Worker] Engine shutdown complete.")
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
