package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ss1ngh/distq-go/internal/pb"
	"github.com/ss1ngh/distq-go/internal/queue"
)

// minArm is the shortest delay the dispatcher will arm its timer for. A job that
// is already due should be claimable, so arming for zero would mean spinning on a
// row something else is holding.
const minArm = 100 * time.Millisecond

// dispatcher decides which worker gets which job. Workers never ask for work:
// they announce that they are free and the dispatcher dequeues a job for them —
// atomically, under a lease as always — and pushes it down their stream.
//
// It runs on demand — a push is triggered by a new job, by a worker becoming
// free, or by a retry's backoff coming due — so an idle queue and idle workers
// cost nothing but a sleep. That is the whole point of the stream: with polling,
// every worker asked the database for work on a timer whether or not there was
// any.
type dispatcher struct {
	ctx context.Context
	q   *queue.Queue

	// mayDispatch is the leadership gate: only the cluster's leader hands out
	// work. It is a function rather than a bool because the answer changes over
	// a server's lifetime — every server runs one of these, and the losers must
	// not dispatch.
	mayDispatch func() bool

	mu      sync.Mutex
	waiters []*waiter
	timer   *time.Timer
}

// waiter is a worker with nothing to do, and the stream to hand its next job to.
type waiter struct {
	workerID   string
	deliveries chan *pb.Assigned
}

func newDispatcher(ctx context.Context, q *queue.Queue, mayDispatch func() bool) *dispatcher {
	return &dispatcher{ctx: ctx, q: q, mayDispatch: mayDispatch}
}

// register notes that a worker is free and looks for work for it.
func (d *dispatcher) register(w *waiter) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, waiting := range d.waiters {
		if waiting.workerID == w.workerID {
			// Already in line. Registering twice would let this worker be handed
			// two jobs, because it would be popped once per registration.
			return
		}
	}

	d.waiters = append(d.waiters, w)
	d.pushLocked()
}

// forget drops a worker whose stream has ended, so it is not handed work nobody
// is listening for. A job already claimed for it is not lost: its lease runs out
// and the reaper returns it to the queue.
func (d *dispatcher) forget(workerID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i, w := range d.waiters {
		if w.workerID == workerID {
			d.waiters = append(d.waiters[:i], d.waiters[i+1:]...)
			return
		}
	}
}

// Push hands out as much work as there is, then makes sure it will be called
// again when there is more.
func (d *dispatcher) Push() {
	if d.ctx.Err() != nil {
		return // shutting down
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.pushLocked()
}

// pushLocked dequeues a job for each worker that is waiting, stopping as soon as
// the queue has nothing visible left to hand out. Callers must hold d.mu.
func (d *dispatcher) pushLocked() {
	if !d.mayDispatch() {
		// A follower holds nobody in line — its streams were never admitted —
		// but a just-deposed leader may still be draining waiters. It stops
		// handing out work here, and the leadership loop's wake on the next
		// win picks the flow back up.
		return
	}

	for len(d.waiters) > 0 {
		w := d.waiters[0]

		job, err := d.q.Dequeue(d.ctx, w.workerID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[Dispatcher] claim for worker %s: %v\n", w.workerID, err)
			return
		}
		if job == nil {
			break // nothing visible: whoever is waiting stays waiting
		}

		// The worker leaves the line before it is sent anything, so its delivery
		// channel is empty and this send cannot block.
		d.waiters = d.waiters[1:]
		w.deliveries <- &pb.Assigned{Job: job, LeaseSeconds: int32(d.q.LeaseFor().Seconds())}
	}

	d.armLocked()
}

// armLocked makes sure a push happens when the oldest pending job comes due, so a
// job that is only waiting out a retry backoff is delivered when it is due rather
// than whenever the next unrelated job happens to arrive. Callers must hold d.mu.
func (d *dispatcher) armLocked() {
	if len(d.waiters) == 0 {
		return // nobody to hand work to; the next worker to register pushes
	}

	wait, pending, err := d.q.NextVisible(d.ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Dispatcher] next visible job: %v\n", err)
		return
	}
	if !pending {
		return // the queue is empty: stay asleep until something arrives
	}

	if wait < minArm {
		wait = minArm
	}

	// An armed timer may fire while it is being reset, which costs one redundant
	// push. Pushing twice is harmless; not pushing at all would strand a job.
	if d.timer == nil {
		d.timer = time.AfterFunc(wait, d.Push)
		return
	}
	d.timer.Reset(wait)
}
