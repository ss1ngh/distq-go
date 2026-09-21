package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/ss1ngh/distq-go/internal/pb"
	"github.com/ss1ngh/distq-go/internal/storage"
)

// DefaultRetryBaseDelay is the backoff base: delay for attempt n is
// base * 2^n (1s, 2s, 4s, ... capped by the store).
const DefaultRetryBaseDelay = 1 * time.Second

// DefaultLeaseFor is how long a claimed job stays owned by its worker before the
// reaper assumes the worker died. It has to comfortably outlast a normal job
// while staying short enough that a crash does not stall the job for long.
const DefaultLeaseFor = 30 * time.Second

type Options struct {
	Store storage.Store
	// RetryBaseDelay is the exponential backoff base. Zero uses the default.
	RetryBaseDelay time.Duration
	// LeaseFor is how long a worker owns a job it claimed. Zero uses the
	// default.
	LeaseFor time.Duration
}

// Queue bridges the RPC layer to the storage layer and owns retry and lease
// policy parameters. It deliberately contains no job state itself — the DB is
// the single source of truth.
type Queue struct {
	store      storage.Store
	retryDelay time.Duration
	leaseFor   time.Duration
}

func New(opts Options) (*Queue, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("Store is required")
	}

	delay := opts.RetryBaseDelay
	if delay <= 0 {
		delay = DefaultRetryBaseDelay
	}
	lease := opts.LeaseFor
	if lease <= 0 {
		lease = DefaultLeaseFor
	}

	return &Queue{store: opts.Store, retryDelay: delay, leaseFor: lease}, nil
}

func (q *Queue) Enqueue(ctx context.Context, j *pb.Job) error {
	if err := q.store.CreateJob(ctx, j); err != nil {
		return fmt.Errorf("persist job: %w", err)
	}
	return nil
}

// Dequeue leases the next visible job to workerID.
func (q *Queue) Dequeue(ctx context.Context, workerID string) (*pb.Job, error) {
	return q.store.DequeueJob(ctx, workerID, q.leaseFor)
}

// Complete marks a job done on behalf of the worker holding its lease.
func (q *Queue) Complete(ctx context.Context, jobID, workerID string) error {
	return q.store.Complete(ctx, jobID, workerID)
}

// Fail delegates the retry-vs-DLQ decision to the store (atomic) and reports
// the outcome so the worker can log what happened to the job.
func (q *Queue) Fail(ctx context.Context, jobID, workerID, errMsg string) (retrying bool, err error) {
	return q.store.FailJob(ctx, jobID, workerID, errMsg, q.retryDelay)
}

// ReapExpiredLeases hands jobs whose worker went quiet back to the queue.
func (q *Queue) ReapExpiredLeases(ctx context.Context) (int64, error) {
	return q.store.ReapExpiredLeases(ctx)
}
