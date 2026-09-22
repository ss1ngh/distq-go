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
	// workerID for the given duration, on behalf of the leadership term that is
	// dispatching. It returns (nil, nil) when no job is visible — an empty
	// queue, or every remaining job is still waiting out its backoff window.
	DequeueJob(ctx context.Context, term int64, workerID string, lease time.Duration) (*pb.Job, error)

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
	// has to run for a crashed worker's job to be picked up again. Like a
	// claim, the sweep is stamped with the caller's leadership term, so a
	// deposed leader's late sweep cannot recover a job the new leader has
	// already decided about.
	ReapExpiredLeases(ctx context.Context, term int64) (released int64, err error)

	// Campaign tries to win leadership of the cluster: exactly one node may
	// dispatch jobs and reap leases at a time. It succeeds only while the
	// previous holder's lease has expired, and the returned term increases with
	// every handover, so a stale leader's decisions can be told apart from a
	// current one's.
	Campaign(ctx context.Context, leaderID string, lease time.Duration) (term int64, won bool, err error)

	// RenewLeadership extends the caller's own leadership lease and reports
	// whether it still holds. False is the definitive answer a deposed leader
	// needs before it dares dispatch again.
	RenewLeadership(ctx context.Context, leaderID string, lease time.Duration) (term int64, leader bool, err error)

	// ResignLeadership gives up leadership immediately instead of making the
	// cluster wait out the lease. A clean shutdown hands over at once; a crash
	// costs the cluster exactly one lease.
	ResignLeadership(ctx context.Context, leaderID string) error

	Close() error
}
