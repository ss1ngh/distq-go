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

// defaultLeaseFor is the fallback for how long this worker assumes it owns a
// claim, used only if the server does not say. It matches the queue's default.
const defaultLeaseFor = 30 * time.Second

// errLeaseLost cancels the handler when the server reports that this worker no
// longer owns the job: it is somebody else's to finish now.
var errLeaseLost = errors.New("lease lost")

// flakyHandler simulates real work. The job types exercise one path each:
//
//	flaky — fails twice, then succeeds (retry path)
//	doomed — always fails (ends up in the DLQ)
//	slow — outlives the lease, so the heartbeat has to keep it alive
//	anything else — succeeds on the first attempt
func flakyHandler(ctx context.Context, job *pb.Job, attempt int) error {
	switch job.Type {
	case "flaky":
		if attempt <= 2 {
			return fmt.Errorf("transient failure (simulated), attempt %d", attempt)
		}
		return nil
	case "doomed":
		return fmt.Errorf("this job never succeeds (simulated)")
	}

	work := 500 * time.Millisecond
	if job.Type == "slow" {
		work = 45 * time.Second
	}

	select {
	case <-time.After(work):
		return nil
	case <-ctx.Done():
		// Shutting down, or the lease was lost. Either way this attempt must
		// not be reported as finished.
		return ctx.Err()
	}
}

// renewLease keeps a job's lease alive while the handler runs, beating a third
// of the way through the lease so that a slow round trip or a missed beat does
// not cost the worker its job. The server answers ok=false once the job has been
// reassigned, which cancels the handler — the work belongs to somebody else now.
//
// A beat that cannot reach the server is simply retried on the next tick. If the
// lease lapses in the meantime the completion is fenced, so the job still goes to
// whoever owns it next.
func renewLease(ctx context.Context, client pb.JobQueueClient, workerID, jobID string, lease time.Duration, cancel context.CancelCauseFunc) {
	interval := lease / 3
	if interval <= 0 {
		interval = time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		resp, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{JobId: jobID, WorkerId: workerID})
		if err != nil {
			if ctx.Err() != nil {
				return // the handler finished and stopped the heartbeat
			}
			fmt.Printf("[Worker] Heartbeat for [%s] failed: %v\n", shortID(jobID), err)
			continue
		}
		if !resp.GetOk() {
			cancel(errLeaseLost)
			return
		}
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

		lease := time.Duration(resp.GetLeaseSeconds()) * time.Second
		if lease <= 0 {
			lease = defaultLeaseFor
		}

		attempt := int(job.RetryCount) + 1
		fmt.Printf("[Worker %s] Processing Job [%s] type=%s attempt=%d/%d (lease %s)\n",
			workerID, shortID(job.Id), job.Type, attempt, job.MaxRetries+1, lease)

		// The handler runs under a context the heartbeat can cancel, so a worker
		// that has lost its job stops working on it instead of racing the worker
		// that now owns it.
		workCtx, cancelWork := context.WithCancelCause(ctx)
		go renewLease(workCtx, client, workerID, job.Id, lease, cancelWork)

		err = flakyHandler(workCtx, job, attempt)
		cancelWork(nil)

		switch {
		case ctx.Err() != nil:
			// Shutting down. Leave the job to the lease reaper.
		case errors.Is(context.Cause(workCtx), errLeaseLost):
			fmt.Printf("[Worker %s] Job [%s] lease lost — abandoned\n", workerID, shortID(job.Id))
		case err == nil:
			completeResp, completeErr := client.Complete(ctx, &pb.CompleteRequest{JobId: job.Id, WorkerId: workerID})
			if completeErr != nil || completeResp.GetError() != "" {
				fmt.Printf("[Worker] Failed to COMPLETE Job [%s]: %v %s\n", shortID(job.Id), completeErr, completeResp.GetError())
			} else {
				fmt.Printf("[Worker %s] Job [%s] done ✓\n", workerID, shortID(job.Id))
			}
		default:
			// The lesson: on failure we no longer drop the job. We report it,
			// and the SERVER decides retry-vs-DLQ atomically.
			failResp, failErr := client.Fail(ctx, &pb.FailRequest{
				JobId:        job.Id,
				ErrorMessage: err.Error(),
				WorkerId:     workerID,
			})
			if failErr != nil || failResp.GetError() != "" {
				fmt.Printf("[Worker] Failed to report failure for [%s]: %v %s\n", shortID(job.Id), failErr, failResp.GetError())
				continue
			}

			if failResp.Retrying {
				fmt.Printf("[Worker %s] Job [%s] failed → re-queued with backoff (attempt %d/%d)\n",
					workerID, shortID(job.Id), attempt, job.MaxRetries+1)
			} else {
				fmt.Printf("[Worker %s] Job [%s] failed → moved to DLQ ✗\n", workerID, shortID(job.Id))
			}
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
