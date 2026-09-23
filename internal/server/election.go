package server

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/ss1ngh/distq-go/internal/queue"
)

// Election is the mechanism that decides which server leads the cluster.
//
// The two implementations differ in kind, which is the point of having both.
// Postgres leases a row and has to be asked again whether it still holds it, so
// a candidate polls and an incumbent discovers it was deposed when its next
// renewal is refused. Etcd leases a key, hands leadership to the next candidate
// the moment a lease lapses, and closes the loser's session channel to say so.
// What they share is the shape the leadership loop is written against:
//
//	Campaign  block until this node leads
//	Hold      block until it stops leading
//	Resign    hand it over instead of making the cluster wait out the lease
type Election interface {
	// Name identifies this node in the election, for logs.
	Name() string

	// Campaign blocks until this node leads, and returns the term fencing its
	// reign. It reports only the end of the context: failures of the mechanism
	// are retried here, because nothing in an election holds state that a retry
	// could corrupt. The term increases with every handover, so a stale leader's
	// decisions can be told apart from a current one's.
	Campaign(ctx context.Context) (term int64, err error)

	// Hold blocks until this node stops leading, and reports whether it lost
	// leadership (true) or the context ended (false). Losing it is the
	// definitive word a deposed leader needs before it dares dispatch again.
	Hold(ctx context.Context) bool

	// Resign gives leadership up immediately, so a clean shutdown hands over at
	// once instead of costing the cluster a lease.
	Resign(ctx context.Context) error

	// Close releases whatever the mechanism holds open.
	Close() error
}

// postgresElection elects by leasing a row, the same discipline a job claim uses
// one level up: win an expired lease, renew it every lease/3, and lose it by
// stalling past expiry. It is the raw mechanism this project started from, and
// its shape is visible in it: the holder of the row is told nothing, so a
// candidate polls and a deposed leader waits to be refused.
type postgresElection struct {
	q          *queue.Queue
	nodeID     string
	lease      time.Duration
	renewEvery time.Duration
}

// NewPostgresElection elects leadership through the store's leadership row. It
// deliberately shares the job-lease duration: one knob to reason about, and the
// renewal rhythm matches the worker heartbeat one.
func NewPostgresElection(q *queue.Queue, lease time.Duration) Election {
	return &postgresElection{
		q:          q,
		nodeID:     newNodeID(),
		lease:      lease,
		renewEvery: lease / 3,
	}
}

func (e *postgresElection) Name() string { return e.nodeID }

// Campaign polls until the lease is won. Postgres cannot tell a waiting
// candidate that its turn has come — winning is a row update that either finds
// the previous lease expired or does not — so the candidate keeps asking, at the
// pace of the renewal it would be making anyway if it had won.
func (e *postgresElection) Campaign(ctx context.Context) (int64, error) {
	for {
		term, won, err := e.q.Campaign(ctx, e.nodeID, e.lease)
		switch {
		case err == nil && won:
			return term, nil
		case ctx.Err() != nil:
			return 0, ctx.Err()
		case err != nil:
			// A database that is down is not a reason to die: nothing here
			// holds state, so retrying is always safe.
			fmt.Fprintf(os.Stderr, "[Leadership] campaign failed, retrying: %v\n", err)
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(e.renewEvery):
		}
	}
}

// Hold renews the lease until it is refused — somebody else took over — or the
// context ends. A failed renewal is not proof of deposition, since the database
// may just be unreachable, so it is logged and survived: the lease lapses,
// somebody campaigns, and the next renewal settles the question.
func (e *postgresElection) Hold(ctx context.Context) bool {
	ticker := time.NewTicker(e.renewEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			_, leader, err := e.q.RenewLeadership(ctx, e.nodeID, e.lease)
			switch {
			case ctx.Err() != nil:
				return false
			case err != nil:
				fmt.Fprintf(os.Stderr, "[Leadership] renewal failed: %v\n", err)
			case !leader:
				return true
			}
		}
	}
}

// Resign clears the row so the next candidate wins on its next attempt. It is
// safe no matter the state — the SQL only clears leadership if this node still
// holds it — so a crash costs the cluster exactly one lease instead.
func (e *postgresElection) Resign(ctx context.Context) error {
	return e.q.ResignLeadership(ctx, e.nodeID)
}

// Close is a no-op: the database belongs to the store, and this only borrows it.
func (e *postgresElection) Close() error { return nil }

// newNodeID is the short, human-readable node identity: it names the node in
// election logs, in the leadership row, and in an etcd election.
func newNodeID() string { return uuid.New().String()[:8] }
