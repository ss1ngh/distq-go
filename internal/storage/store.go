package storage

import(
	"context"

	"github.com/ss1ngh/distq-go/internal/job"
)

type Store interface {
	CreateJob(ctx context.Context, j *job.Job) error
	MarkProcessing(ctx context.Context, id string) error
	MarkDone(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id string, errMsg string) error
	MarkPending(ctx context.Context, id string) error
	GetPendingJobs(ctx context.Context) ([]*job.Job, error)
	Close() error
}