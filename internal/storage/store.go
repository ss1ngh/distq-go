package storage

import (
	"context"
	"time"

	"github.com/ss1ngh/distq-go/internal/pb"
)

type Store interface {
	CreateJob(ctx context.Context, j *pb.Job) error
	// DequeueJob atomically claims the next visible job (SKIP LOCKED).
	DequeueJob(ctx context.Context) (*pb.Job, error)
	// Complete marks a job as successfully finished.
	Complete(ctx context.Context, id string) error
	// FailJob records a handler failure and atomically decides the next state:
	// back to pending with backoff (if retries remain) or to the DLQ (failed).
	// Returns true if the job was re-queued for another attempt.
	FailJob(ctx context.Context, id string, errMsg string, baseDelay time.Duration) (bool, error)
	// GetPendingJobs returns in-flight jobs (diagnostics; Lesson 2's reaper
	// will subsume crash recovery).
	GetPendingJobs(ctx context.Context) ([]*pb.Job, error)
	Close() error
}
