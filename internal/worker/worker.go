package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/ss1ngh/distq-go/internal/job"
	"github.com/ss1ngh/distq-go/internal/queue"
)

// Handler defines the execution signature for processing dequeued jobs.
type Handler func(ctx context.Context, j *job.Job) error

// Worker acts as an autonomous polling engine that manages the job lifecycle.
type Worker struct {
	q       *queue.Queue
	handler Handler
}

// New initializes a worker with the provided queue and handler dependencies.
func New(q *queue.Queue, h Handler) *Worker {
	return &Worker{
		q:       q,
		handler: h,
	}
}

// Start begins the blocking polling loop. It yields gracefully upon context cancellation.
func (w *Worker) Start(ctx context.Context) {
	for {
		// Intercept shutdown signals to prevent pulling new work while terminating.
		if ctx.Err() != nil {
			fmt.Println("Worker: context canceled, shutting down cleanly")
			return
		}

		j, err := w.q.Dequeue(ctx)

		// Apply static backoff on storage failures to prevent cascading DB load.
		if err != nil {
			fmt.Printf("Worker: failed to dequeue: %v\n", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// Apply idle backoff when the queue is exhausted to minimize CPU and DB I/O.
		if j == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		// Execute injected business logic.
		err = w.handler(ctx, j)

		if err != nil {
			fmt.Printf("Worker: job %s failed: %v\n", j.ID[:8], err)

			stateCtx, cancelState := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelState()

			if j.RetryCount < j.MaxRetries {
				fmt.Printf("Worker: requeuing job %s (attempt %d/%d)\n", j.ID[:8], j.RetryCount+1, j.MaxRetries)
				if reqErr := w.q.Requeue(stateCtx, j.ID); reqErr != nil {
					fmt.Printf("Worker: critical error - failed to requeue: v%n", reqErr)
				}
			} else {
				fmt.Printf("Worker: job %s exceeded max retries, moving to DLQ\n", j.ID[:8])
				if failErr := w.q.Fail(stateCtx, j.ID, err.Error()); failErr != nil {
					fmt.Printf("Worker: critical error - failed to move to DLQ: %v\n", failErr)
				}
			}
			continue
		}

		// Acknowledge success using a detached context to prevent dropped ACKs during shutdown.
		ackCtx, cancelAck := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancelAck()

		if err := w.q.Ack(ackCtx, j.ID); err != nil {
			fmt.Printf("Worker: ack failed for job %s: %v\n", j.ID[:8], err)
		} else {
			fmt.Printf("Worker: job %s done\n", j.ID[:8])
		}
	}
}
