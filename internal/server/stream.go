package server

import (
	"context"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ss1ngh/distq-go/internal/pb"
	"github.com/ss1ngh/distq-go/internal/storage"
)

// JobStream carries jobs down to one worker and reports back up. It is the whole
// of the worker-facing API: there is no Dequeue to poll, no separate call to
// finish a job, and no heartbeat RPC — the connection itself is the conversation.
func (s *Server) JobStream(stream pb.JobQueue_JobStreamServer) error {
	ctx := stream.Context()

	// The first message names the worker. Every job this stream is handed is
	// leased to that id, and every report on it is fenced against that id.
	greeting, err := stream.Recv()
	if err != nil {
		return err
	}
	workerID := greeting.GetHello().GetWorkerId()
	if workerID == "" {
		return status.Error(codes.InvalidArgument, "the first message on a job stream must be a Hello naming the worker")
	}
	if !s.lead.Leading() {
		// Only the leader admits workers: a follower has no dispatcher, so a
		// stream it took could never receive a job. Workers treat Unavailable
		// as "wrong server" and reconnect, which lands them on whoever leads.
		return status.Error(codes.Unavailable, "not the leader; reconnect to be routed to it")
	}
	fmt.Printf("[Server] worker %s connected\n", workerID)

	// Receiving happens in its own goroutine, because this one is busy sending:
	// a gRPC stream allows one send at a time, so keeping every send here makes
	// that a property of the code rather than something to remember.
	incoming := make(chan *pb.WorkerMessage)
	recvErr := make(chan error, 1)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case incoming <- msg:
			case <-done:
				return
			}
		}
	}()

	w := &waiter{workerID: workerID, deliveries: make(chan *pb.Assigned, 1)}
	s.dis.register(w)
	defer s.dis.forget(workerID)

	for {
		select {
		case job := <-w.deliveries:
			if err := stream.Send(&pb.ServerMessage{Message: &pb.ServerMessage_Assigned{Assigned: job}}); err != nil {
				return err
			}
		case msg := <-incoming:
			reply, free := s.answer(ctx, workerID, msg)
			if reply != nil {
				if err := stream.Send(reply); err != nil {
					return err
				}
			}
			if free {
				// Nothing is in flight any more, so this worker goes back in
				// line and may be handed the next job.
				s.dis.register(w)
			}
		case err := <-recvErr:
			return err
		case <-ctx.Done():
			return nil
		case <-s.lead.LeadCtx().Done():
			// This server's reign ended — it was deposed or is shutting down.
			// The worker reconnects and is admitted by whoever leads now.
			return nil
		}
	}
}

// answer applies what a worker says and returns the reply to send, plus whether
// the worker is free for more work.
func (s *Server) answer(ctx context.Context, workerID string, msg *pb.WorkerMessage) (*pb.ServerMessage, bool) {
	switch m := msg.GetMessage().(type) {
	case *pb.WorkerMessage_Hello:
		// This stream was already greeted with one.
		return nil, false

	case *pb.WorkerMessage_Heartbeat:
		jobID := m.Heartbeat.GetJobId()

		renewed, err := s.q.Heartbeat(ctx, jobID, workerID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[Server] heartbeat for job %s from worker %s: %v\n", jobID, workerID, err)
			return nil, false
		}
		if renewed {
			return nil, false
		}

		// The worker stalled past its lease, so the job has been reassigned:
		// take it away from the worker, which frees it for the next one.
		fmt.Fprintf(os.Stderr, "[Server] revoked job %s from worker %s: lease expired\n", jobID, workerID)
		return &pb.ServerMessage{Message: &pb.ServerMessage_Lost{Lost: &pb.Lost{JobId: jobID}}}, true

	case *pb.WorkerMessage_Completed:
		jobID := m.Completed.GetJobId()

		if err := s.q.Complete(ctx, jobID, workerID); err != nil {
			logFenced(err, "completion", jobID, workerID)
			return result(jobID, pb.Outcome_OUTCOME_REFUSED, err), true
		}
		return result(jobID, pb.Outcome_OUTCOME_ACCEPTED, nil), true

	case *pb.WorkerMessage_Failed:
		jobID := m.Failed.GetJobId()

		retrying, err := s.q.Fail(ctx, jobID, workerID, m.Failed.GetErrorMessage())
		if err != nil {
			logFenced(err, "failure", jobID, workerID)
			return result(jobID, pb.Outcome_OUTCOME_REFUSED, err), true
		}

		outcome := pb.Outcome_OUTCOME_DEAD_LETTERED
		if retrying {
			outcome = pb.Outcome_OUTCOME_RETRYING
		}
		return result(jobID, outcome, nil), true
	}

	return nil, false
}

// result builds the reply to a report. reason is nil unless the report was
// refused.
func result(jobID string, outcome pb.Outcome, reason error) *pb.ServerMessage {
	r := &pb.Result{JobId: jobID, Outcome: outcome}
	if reason != nil {
		r.Error = reason.Error()
	}
	return &pb.ServerMessage{Message: &pb.ServerMessage_Result{Result: r}}
}

// logFenced notes that a late result was refused. That is the fence working, not
// a server fault: the job was reassigned while this worker was still working on
// it, and the new owner's result is the one that counts.
func logFenced(err error, what, jobID, workerID string) {
	if errors.Is(err, storage.ErrLeaseLost) {
		fmt.Fprintf(os.Stderr, "[Server] fenced late %s for job %s from worker %s\n", what, jobID, workerID)
	}
}
