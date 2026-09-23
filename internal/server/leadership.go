package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ss1ngh/distq-go/internal/queue"
)

// resignTimeout bounds the handover on shutdown: giving leadership up is not
// worth holding the process open for.
const resignTimeout = 5 * time.Second

// Leadership elects and keeps exactly one server as the cluster's leader — the
// only node that dispatches jobs and reaps expired leases. Every server runs
// one; the losers stay out of policy work but keep serving Enqueue, which is a
// plain database write and safe anywhere.
//
// Who wins is an Election's business — a leased row in Postgres, or a leased key
// in etcd — and whatever it decides is not taken on trust. A deposed leader is
// not usually told that it lost: it finds out by losing the lease, and until it
// does it still believes it leads. So dispatch checks Leading() before it acts,
// worker streams die with the reign that admitted them, and the fencing in the
// store's claims makes whatever slips through in the meantime refuse to land.
// That is what "handling split-brain" reduces to: two leaders can momentarily
// both believe, so the deposed one's actions are made harmless instead.
type Leadership struct {
	elect Election
	q     *queue.Queue

	// reapEvery is how often the leader sweeps expired job leases.
	reapEvery time.Duration

	mu      sync.Mutex
	isLead  bool
	term    int64           // fencing token of the reign leadCtx belongs to
	leadCtx context.Context // live only while leading; cancelled on demotion
	cancel  context.CancelFunc

	stopped chan struct{}
}

// newLeadership prepares an idle — not yet leading — leadership loop around the
// given election mechanism.
func newLeadership(elect Election, q *queue.Queue, reapEvery time.Duration) *Leadership {
	// Seed leadCtx already-cancelled, so a worker stream that asks before the
	// first win is turned away instead of waiting on a channel nobody closes.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return &Leadership{
		elect:     elect,
		q:         q,
		reapEvery: reapEvery,
		leadCtx:   ctx,
		stopped:   make(chan struct{}),
	}
}

// Run campaigns until this node wins leadership, then holds it until the process
// shuts down or the lease is lost, reaping expired job leases throughout. It
// returns once this node has stopped leading.
func (l *Leadership) Run(ctx context.Context) {
	defer close(l.stopped)
	defer l.resign()

	for ctx.Err() == nil {
		term, err := l.elect.Campaign(ctx)
		if err != nil {
			return // the context ended; there is nothing left to campaign for
		}
		fmt.Printf("[Leadership] node %s won term %d — dispatching and reaping here\n", l.elect.Name(), term)
		l.becomeLead(term)
		go l.reapExpiredLeases(l.LeadCtx())

		if !l.elect.Hold(ctx) {
			return // shutting down; resignation runs via defer
		}

		// Deposed. Dropping the streams has already sent this node's workers
		// hunting for the new leader, and campaigning again is safe immediately:
		// a candidate queues behind the new incumbent rather than fighting it.
		l.demote()
		fmt.Printf("[Leadership] node %s demoted — campaigning again\n", l.elect.Name())
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
// stops the reaper and drops every worker stream admitted under it. The term is
// deliberately kept — the fencing tokens already stamped into claims were real,
// and Term() is not used to decide anything about the future.
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

// Leading reports whether this node currently believes it leads. It is the cheap
// first check a leadership-gated action makes; the authoritative one is the
// election saying the lease is gone.
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
// the fencing token stamped into every claim and sweep, so a claim by a deposed
// leader — whose term is behind the row's — refuses to land.
func (l *Leadership) Term() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.term
}

// Stopped is closed once Run has returned and resignation has been attempted.
func (l *Leadership) Stopped() <-chan struct{} { return l.stopped }

// resign hands leadership over so a clean restart does not cost the cluster a
// lease period with nobody leading. There is nothing to hand over if this node
// never led, or lost the lease before it stopped.
func (l *Leadership) resign() {
	wasLeading := l.Leading()
	l.demote()
	if !wasLeading {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), resignTimeout)
	defer cancel()

	if err := l.elect.Resign(ctx); err != nil {
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
