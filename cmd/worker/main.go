package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ss1ngh/distq-go/internal/pb"
)

const (
	// serverAddr is the queue server every worker dials.
	serverAddr = "localhost:4040"
	// reconnectDelay is how long a worker waits before opening a new stream after
	// one breaks, so a server restart does not turn into a hot reconnect loop.
	reconnectDelay = 2 * time.Second
	// defaultLeaseFor is the fallback for how long this worker assumes it owns a
	// claim, used only if the server does not say. It matches the queue's default.
	defaultLeaseFor = 30 * time.Second
)

// errLeaseLost cancels a handler when the server reports that this worker no
// longer owns the job: it is somebody else's to finish now.
var errLeaseLost = errors.New("lease lost")

// worker is one remote node: a single stream to the server, jobs pushed down it,
// and one job handled at a time.
type worker struct {
	id     string
	client pb.JobQueueClient

	// A stream may only be sent on by one goroutine at a time, and a job's
	// heartbeat and its result both send, so every send goes through send().
	sendMu sync.Mutex
	stream pb.JobQueue_JobStreamClient
}

func main() {
	w := &worker{id: uuid.New().String()[:8], client: dial()}
	fmt.Printf("[Worker %s] Booting Distributed Node...\n", w.id)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	for ctx.Err() == nil {
		if err := w.run(ctx); err != nil && ctx.Err() == nil {
			fmt.Printf("[Worker %s] Stream ended: %v\n", w.id, err)
		}
		wait(ctx, reconnectDelay)
	}
	fmt.Println("[Worker] Engine shutdown complete.")
}

func dial() pb.JobQueueClient {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Worker] Fatal: Failed to connect: %v\n", err)
		os.Exit(1)
	}
	return pb.NewJobQueueClient(conn)
}

// run opens a stream and serves jobs on it until the stream breaks. It is called
// again for every reconnection, so it must leave no job half-reported: a job whose
// stream dies loses only its lease, and the reaper hands it to somebody else.
func (w *worker) run(ctx context.Context) error {
	stream, err := w.client.JobStream(ctx)
	if err != nil {
		return err
	}
	w.stream = stream

	if err := w.send(&pb.WorkerMessage{Message: &pb.WorkerMessage_Hello{Hello: &pb.Hello{WorkerId: w.id}}}); err != nil {
		return err
	}
	fmt.Printf("[Worker %s] Listening for jobs...\n", w.id)

	// The job in flight, tracked here in the read loop rather than shared with the
	// handler, so cancelling it needs no locking.
	var (
		jobID  string
		cancel context.CancelCauseFunc
	)

	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}

		switch m := msg.GetMessage().(type) {
		case *pb.ServerMessage_Assigned:
			jobID, cancel = w.start(ctx, m.Assigned)

		case *pb.ServerMessage_Lost:
			// The server has already given this job to somebody else. Stop now
			// rather than racing the new owner to write the result.
			if lost := m.Lost.GetJobId(); lost == jobID && cancel != nil {
				cancel(errLeaseLost)
				jobID, cancel = "", nil
				fmt.Printf("[Worker %s] Job [%s] lease lost — abandoned\n", w.id, shortID(lost))
			}

		case *pb.ServerMessage_Result:
			logResult(w.id, m.Result)
			if m.Result.GetJobId() == jobID {
				// Settled. Clearing the slot means a Lost for this job arriving
				// late cannot cancel whatever runs next.
				jobID, cancel = "", nil
			}
		}
	}
}

// start begins a job's handler in the background and returns the pair the read
// loop needs to take it away again. The handler runs in its own goroutine so the
// loop stays free to receive a Lost or the next Assigned while it works.
func (w *worker) start(ctx context.Context, assigned *pb.Assigned) (string, context.CancelCauseFunc) {
	job := assigned.GetJob()
	jobCtx, cancel := context.WithCancelCause(ctx)

	go w.work(jobCtx, cancel, job, assigned.GetLeaseSeconds())

	return job.Id, cancel
}

// work runs one job and reports the verdict. It reports nothing if the job was
// taken away from it mid-flight: the server already knows, and the fence would
// refuse the report anyway.
func (w *worker) work(ctx context.Context, cancel context.CancelCauseFunc, job *pb.Job, leaseSeconds int32) {
	lease := time.Duration(leaseSeconds) * time.Second
	if lease <= 0 {
		lease = defaultLeaseFor
	}

	attempt := int(job.RetryCount) + 1
	fmt.Printf("[Worker %s] Processing Job [%s] type=%s attempt=%d/%d (lease %s)\n",
		w.id, shortID(job.Id), job.Type, attempt, job.MaxRetries+1, lease)

	go beatLease(ctx, w, job.Id, lease)

	err := handle(ctx, job, attempt)

	// Stop the heartbeat before reporting. A beat that arrives after the report
	// reads as this worker losing a job it has just finished — and on a retry it
	// would cancel the next attempt of the same job.
	cancel(nil)

	if errors.Is(context.Cause(ctx), errLeaseLost) {
		return
	}

	report := &pb.WorkerMessage{Message: &pb.WorkerMessage_Completed{Completed: &pb.Completed{JobId: job.Id}}}
	if err != nil {
		report = &pb.WorkerMessage{Message: &pb.WorkerMessage_Failed{Failed: &pb.Failed{JobId: job.Id, ErrorMessage: err.Error()}}}
	}
	if err := w.send(report); err != nil {
		fmt.Printf("[Worker] Could not report job [%s]: %v\n", shortID(job.Id), err)
	}
}

// beatLease tells the server this job is still being worked on, so its lease does
// not lapse and the job is not handed to another worker. Being told the job is
// gone is the read loop's job, not this one's: all this side has to say is "still
// here", and stop saying it when the work stops.
func beatLease(ctx context.Context, w *worker, jobID string, lease time.Duration) {
	interval := lease / 3
	if interval <= 0 {
		interval = time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msg := &pb.WorkerMessage{Message: &pb.WorkerMessage_Heartbeat{Heartbeat: &pb.Heartbeat{JobId: jobID}}}
			if err := w.send(msg); err != nil {
				return // the stream is gone; there is nothing left to keep alive
			}
		}
	}
}

// send is the only way this worker writes to its stream.
func (w *worker) send(msg *pb.WorkerMessage) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()

	return w.stream.Send(msg)
}

// logResult reports what the server did with a result this worker sent.
func logResult(workerID string, r *pb.Result) {
	switch r.GetOutcome() {
	case pb.Outcome_OUTCOME_ACCEPTED:
		fmt.Printf("[Worker %s] Job [%s] done ✓\n", workerID, shortID(r.GetJobId()))
	case pb.Outcome_OUTCOME_RETRYING:
		fmt.Printf("[Worker %s] Job [%s] failed → re-queued with backoff\n", workerID, shortID(r.GetJobId()))
	case pb.Outcome_OUTCOME_DEAD_LETTERED:
		fmt.Printf("[Worker %s] Job [%s] failed → moved to DLQ ✗\n", workerID, shortID(r.GetJobId()))
	case pb.Outcome_OUTCOME_REFUSED:
		fmt.Printf("[Worker %s] Job [%s] refused: %s\n", workerID, shortID(r.GetJobId()), r.GetError())
	}
}

// handle simulates real work. The job types exercise one path each:
//
//	flaky — fails twice, then succeeds (retry path)
//	doomed — always fails (ends up in the DLQ)
//	slow — outlives the lease, so the heartbeat has to keep it alive
//	anything else — succeeds on the first attempt
func handle(ctx context.Context, job *pb.Job, attempt int) error {
	switch job.Type {
	case "flaky":
		if attempt <= 2 {
			return fmt.Errorf("transient failure (simulated), attempt %d", attempt)
		}
		return nil
	case "doomed":
		return fmt.Errorf("this job never succeeds (simulated)")
	}

	work := 500 * time.Millisecond
	if job.Type == "slow" {
		work = 45 * time.Second
	}

	select {
	case <-time.After(work):
		return nil
	case <-ctx.Done():
		// Shutting down, or the job was taken away. Either way this attempt must
		// not be reported as finished.
		return ctx.Err()
	}
}

// wait pauses for d, or less if ctx is cancelled first.
func wait(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
