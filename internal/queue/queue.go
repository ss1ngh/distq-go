package queue

import (
	"context"
	"fmt"

	"github.com/ss1ngh/distq-go/internal/pb"
	"github.com/ss1ngh/distq-go/internal/storage"
)

// Options no longer needs BufferSize
type Options struct {
	Store storage.Store
}

// Queue now strictly acts as a bridge to the storage layer
type Queue struct {
	store storage.Store
}

func New(opts Options) (*Queue, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("Store is required")
	}
	return &Queue{store: opts.Store}, nil
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

func (q *Queue) Ack(ctx context.Context, jobID string) error {
	return q.store.MarkDone(ctx, jobID)
}

func (q *Queue) Requeue(ctx context.Context, jobID string) error {
	return q.store.MarkPending(ctx, jobID)
}

func (q *Queue) Fail(ctx context.Context, jobID string, errMsg string) error {
	return q.store.MarkFailed(ctx, jobID, errMsg)
}
