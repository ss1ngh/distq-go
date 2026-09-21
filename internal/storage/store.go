package storage

import (
	"context"
	"errors"
	"time"

	"github.com/ss1ngh/distq-go/internal/pb"
)

// ErrLeaseLost is returned when a worker tries to finish a job it is no longer
// the owner of: it stalled past its lease, the job was reassigned, and the lease
// belongs to somebody else now.
var ErrLeaseLost = errors.New("job lease lost or expired")

type Store interface {
	CreateJob(ctx context.Context, j *pb.Job) error

	// DequeueJob atomically claims the next visible job and leases it to
	// workerID for the given duration. It returns (nil, nil) when no job is
	// visible — an empty queue, or every remaining job is still waiting out its
	// backoff window.
	DequeueJob(ctx context.Context, workerID string, lease time.Duration) (*pb.Job, error)

	// Complete marks a job as successfully finished, but only for the worker
	// holding its lease. It returns ErrLeaseLost otherwise.
	Complete(ctx context.Context, id, workerID string) error

	// FailJob records a handler failure and atomically decides the next state:
	// back to pending with backoff if retries remain, or to the DLQ. It reports
	// true when the job was re-queued for another attempt.
	FailJob(ctx context.Context, id, workerID, errMsg string, baseDelay time.Duration) (retrying bool, err error)

	// HeartbeatJob pushes a job's lease expiry forward on behalf of the worker
	// holding it, and reports false once the job is no longer theirs.
	HeartbeatJob(ctx context.Context, id, workerID string, lease time.Duration) (bool, error)

	// NextVisible reports how long until the oldest pending job becomes claimable
	// — how long a dispatcher should sleep before looking again — and whether the
	// queue holds anything pending at all. It is what lets a worker be pushed a
	// delayed retry when it comes due instead of polling for it.
	NextVisible(ctx context.Context) (wait time.Duration, pending bool, err error)

	// ReapExpiredLeases returns jobs whose worker stopped renewing its lease to
	// the queue, and reports how many it released. This is the only thing that
	// has to run for a crashed worker's job to be picked up again.
	ReapExpiredLeases(ctx context.Context) (released int64, err error)

	Close() error
}
