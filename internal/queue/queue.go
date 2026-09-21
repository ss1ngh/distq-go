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

type Options struct {
	Store storage.Store
	// RetryBaseDelay is the exponential backoff base. Zero uses the default.
	RetryBaseDelay time.Duration
}

// Queue bridges the RPC layer to the storage layer and owns retry policy
// parameters. It deliberately contains no job state itself — the DB is the
// single source of truth.
type Queue struct {
	store      storage.Store
	retryDelay time.Duration
}

func New(opts Options) (*Queue, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("Store is required")
	}
	d := opts.RetryBaseDelay
	if d <= 0 {
		d = DefaultRetryBaseDelay
	}
	return &Queue{store: opts.Store, retryDelay: d}, nil
}

func (q *Queue) Enqueue(ctx context.Context, j *pb.Job) error {
	if err := q.store.CreateJob(ctx, j); err != nil {
		return fmt.Errorf("persist job: %w", err)
	}
	return nil
}

func (q *Queue) Dequeue(ctx context.Context) (*pb.Job, error) {
	return q.store.DequeueJob(ctx)
}

func (q *Queue) Complete(ctx context.Context, jobID string) error {
	return q.store.Complete(ctx, jobID)
}

// Fail delegates the retry-vs-DLQ decision to the store (atomic) and reports
// the outcome so the worker can log what happened to the job.
func (q *Queue) Fail(ctx context.Context, jobID, errMsg string) (retrying bool, err error) {
	return q.store.FailJob(ctx, jobID, errMsg, q.retryDelay)
}

func (q *Queue) PendingJobs(ctx context.Context) ([]*pb.Job, error) {
	return q.store.GetPendingJobs(ctx)
}
