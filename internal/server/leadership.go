package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ss1ngh/distq-go/internal/queue"
)

// Leadership elects and keeps exactly one server as the cluster's leader — the
// only node that dispatches jobs and reaps expired leases. Every server runs
// one; the losers stay out of policy work but keep serving Enqueue, which is a
// plain database write and safe anywhere.
//
// The mechanism is a lease in Postgres, the same discipline a job claim uses,
// one level up: win an expired lease with Campaign, keep it alive by renewing
// every lease/3, and lose it by stalling past expiry. A deposed leader is not
// told that it lost — it finds out the next time its renewal is refused — so
// nothing here is trusted on its own word: dispatch checks Leading() before it
// acts, worker streams die with the reign that admitted them, and the fencing
// in the store's claims makes whatever slips through in the meantime harmless.
// That double check is what "handling split-brain" reduces to: you cannot stop
// two leaders from momentarily believing, so you make the deposed one's actions
// refuse to land.
type Leadership struct {
	q        *queue.Queue
	leaderID string

	lease time.Duration // how long a won leadership is good for
	// campaignEvery is the renewal rhythm while leading and the retry cadence
	// while campaigning; loseEvery is how long a deposed leader stands down
	// before campaigning again, long enough for the takeover to settle.
	campaignEvery time.Duration
	loseEvery     time.Duration
	reapEvery     time.Duration

	mu      sync.Mutex
	isLead  bool
	term    int64           // fencing token of the reign leadCtx belongs to
	leadCtx context.Context // live only while leading; cancelled on demotion
	cancel  context.CancelFunc

	stopped chan struct{}
}

// NewLeadership prepares an idle — not yet leading — leadership loop. The lease
// deliberately shares the job-lease duration: one knob to reason about, and the
// renewal rhythm matches the worker heartbeat one.
func NewLeadership(q *queue.Queue, leaderID string, lease, reapEvery time.Duration) *Leadership {
	// Seed leadCtx already-cancelled, so a worker stream that asks before the
	// first win is turned away instead of waiting on a channel nobody closes.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return &Leadership{
		q:             q,
		leaderID:      leaderID,
		lease:         lease,
		campaignEvery: lease / 3,
		loseEvery:     lease / 2,
		reapEvery:     reapEvery,
		leadCtx:       ctx,
		stopped:       make(chan struct{}),
	}
}

// Run campaigns until this node wins leadership, then holds it — renewing the
// lease and reaping expired job leases — until the process shuts down or a
// renewal comes back refused. It returns once this node has stopped leading.
func (l *Leadership) Run(ctx context.Context) {
	defer close(l.stopped)
	defer l.resign()

	for ctx.Err() == nil {
		term, won := l.campaign(ctx)
		if !won {
			return // shutting down before ever winning
		}
		fmt.Printf("[Leadership] node %s won term %d — dispatching and reaping here\n", l.leaderID, term)
		l.becomeLead(term)
		go l.reapExpiredLeases(l.LeadCtx())

		if l.holdUntilDeposedOrShutdown(ctx) {
			return // shutting down; resign runs via defer
		}

		// Deposed: stand down, let the takeover settle, and campaign again
		// like everybody else. Dropping the streams has already sent this
		// node's workers hunting for the new leader.
		l.demote()
		fmt.Printf("[Leadership] node %s demoted — campaigning again in %s\n", l.leaderID, l.loseEvery)
		select {
		case <-ctx.Done():
			return
		case <-time.After(l.loseEvery):
		}
	}
}

// campaign retries until the lease is won or the process is shutting down. The
// database being down is not a reason to die: nothing here holds state, so
// retrying is always safe.
func (l *Leadership) campaign(ctx context.Context) (int64, bool) {
	for {
		term, won, err := l.q.Campaign(ctx, l.leaderID, l.lease)
		if err == nil && won {
			return term, true
		}
		if ctx.Err() != nil {
			return 0, false
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "[Leadership] campaign failed, retrying: %v\n", err)
		}

		select {
		case <-ctx.Done():
			return 0, false
		case <-time.After(l.campaignEvery):
		}
	}
}

// holdUntilDeposedOrShutdown renews the leadership lease until it is refused —
// somebody else took over — or the process shuts down, and reports which. A
// failed renewal is not proof of deposition (the database may just be
// unreachable), so it is logged and survived: the lease lapses, somebody
// campaigns, and the next renewal settles the question.
func (l *Leadership) holdUntilDeposedOrShutdown(ctx context.Context) bool {
	ticker := time.NewTicker(l.campaignEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return true
		case <-ticker.C:
			_, leader, err := l.q.RenewLeadership(ctx, l.leaderID, l.lease)
			switch {
			case ctx.Err() != nil:
				return true
			case err != nil:
				fmt.Fprintf(os.Stderr, "[Leadership] renewal failed: %v\n", err)
			case !leader:
				return false
			}
		}
	}
}

// becomeLead opens a new reign: a fresh leadership context that everything
// belonging to this reign — the reaper, the worker streams — is bound to.
func (l *Leadership) becomeLead(term int64) {
	ctx, cancel := context.WithCancel(context.Background())
	l.mu.Lock()
	l.term, l.leadCtx, l.cancel = term, ctx, cancel
	l.isLead = true
	l.mu.Unlock()
}

// demote ends the current reign: the leadership context is cancelled, which
// stops the reaper and drops every worker stream admitted under it. The term
// is deliberately kept — the fencing tokens already stamped into claims were
// real, and Term() is not used to decide anything about the future.
func (l *Leadership) demote() {
	l.mu.Lock()
	cancel := l.cancel
	l.cancel = nil
	l.isLead = false
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Leading reports whether this node currently believes it leads. It is the
// cheap first check a leadership-gated action makes; the authoritative one is
// the renewal coming back refused.
func (l *Leadership) Leading() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.isLead
}

// LeadCtx is the context of the current reign, cancelled the moment leadership
// ends — by demotion or shutdown. Work admitted under a reign must watch it.
func (l *Leadership) LeadCtx() context.Context {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.leadCtx
}

// Term is the leadership term this node won, zero while it leads nobody. It is
// the fencing token stamped into every claim and sweep, so a claim by a
// deposed leader — whose term is behind the row's — refuses to land.
func (l *Leadership) Term() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.term
}

// Stopped is closed once Run has returned and resignation has been attempted.
func (l *Leadership) Stopped() <-chan struct{} { return l.stopped }

// resign gives the lease back so a clean restart does not cost the cluster a
// lease period with nobody leading. It is safe no matter the state — the SQL
// only clears leadership if this node still holds it — and a crash costs the
// cluster exactly one lease instead.
func (l *Leadership) resign() {
	l.demote()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := l.q.ResignLeadership(ctx, l.leaderID); err != nil {
		fmt.Fprintf(os.Stderr, "[Leadership] resign: %v\n", err)
		return
	}
	fmt.Println("[Leadership] resigned — leadership is free immediately")
}

// reapExpiredLeases handles all of our crash recovery. If a worker crashes, it
// stops sending heartbeats and its lease expires; this sweep finds those
// abandoned jobs and puts them back in the queue. There is no special
// "startup" recovery: a rebooting leader simply runs this sweep, grabs
// anything already expired, and catches the rest as leases time out.
//
// It runs only on the leader, under its own reign's context — demotion stops
// it mid-flight — and every sweep is stamped with the reign's term, fenced
// against claims a newer leadership has already made.
func (l *Leadership) reapExpiredLeases(ctx context.Context) {
	ticker := time.NewTicker(l.reapEvery)
	defer ticker.Stop()

	for {
		released, err := l.q.ReapExpiredLeases(ctx, l.Term())
		switch {
		case err != nil && ctx.Err() != nil:
			return // shutting down mid-sweep
		case err != nil:
			fmt.Fprintf(os.Stderr, "[Reaper] sweep failed: %v\n", err)
		case released > 0:
			fmt.Printf("[Reaper] reassigned %d job(s) whose lease expired\n", released)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
