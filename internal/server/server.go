package server

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/ss1ngh/distq-go/internal/pb"
	"github.com/ss1ngh/distq-go/internal/queue"
	"github.com/ss1ngh/distq-go/internal/storage"
)

// errNoWorkerID guards every call that has to be attributed to a worker: a job is
// leased to a specific id, so without one there is nothing a job could be
// claimed by, and nothing a late result could be fenced against.
const errNoWorkerID = "worker_id is required"

type Server struct {
	pb.UnimplementedJobQueueServer
	q *queue.Queue
}

// New initializes a server backed by the given queue.
func New(q *queue.Queue) *Server {
	return &Server{
		q: q,
	}
}

func (s *Server) Enqueue(ctx context.Context, req *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
	job := req.GetJob()

	if job == nil {
		return &pb.EnqueueResponse{Error: "missing job data"}, nil
	}

	if job.Id == "" {
		job.Id = uuid.New().String()
	}

	if err := s.q.Enqueue(ctx, job); err != nil {
		return &pb.EnqueueResponse{Error: err.Error()}, nil
	}

	return &pb.EnqueueResponse{JobId: job.Id}, nil
}

func (s *Server) Dequeue(ctx context.Context, req *pb.DequeueRequest) (*pb.DequeueResponse, error) {
	workerID := req.GetWorkerId()
	if workerID == "" {
		return &pb.DequeueResponse{Error: errNoWorkerID}, nil
	}

	job, err := s.q.Dequeue(ctx, workerID)
	if err != nil {
		return &pb.DequeueResponse{Error: err.Error()}, nil
	}

	// Tell the worker how long its claim lasts, so it can renew before the
	// reaper decides it has died.
	return &pb.DequeueResponse{Job: job, LeaseSeconds: int32(s.q.LeaseFor().Seconds())}, nil
}

// Complete marks a job as successfully finished, on behalf of the worker holding
// its lease.
func (s *Server) Complete(ctx context.Context, req *pb.CompleteRequest) (*pb.CompleteResponse, error) {
	workerID := req.GetWorkerId()
	if workerID == "" {
		return &pb.CompleteResponse{Error: errNoWorkerID}, nil
	}

	if err := s.q.Complete(ctx, req.GetJobId(), workerID); err != nil {
		logFenced(err, "completion", req.GetJobId(), workerID)
		return &pb.CompleteResponse{Error: err.Error()}, nil
	}

	return &pb.CompleteResponse{}, nil
}

// Fail reports a handler failure. The queue/store decides retry-vs-DLQ
// atomically and we surface the decision to the worker.
func (s *Server) Fail(ctx context.Context, req *pb.FailRequest) (*pb.FailResponse, error) {
	workerID := req.GetWorkerId()
	if workerID == "" {
		return &pb.FailResponse{Error: errNoWorkerID}, nil
	}

	retrying, err := s.q.Fail(ctx, req.GetJobId(), workerID, req.GetErrorMessage())
	if err != nil {
		logFenced(err, "failure", req.GetJobId(), workerID)
		return &pb.FailResponse{Error: err.Error()}, nil
	}

	return &pb.FailResponse{Retrying: retrying}, nil
}

// Heartbeat renews a job's lease on behalf of the worker holding it. A false
// answer is not an error: it tells a worker that stalled past its lease to stop
// working on a job that has already been handed to somebody else.
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	workerID := req.GetWorkerId()
	if workerID == "" {
		return &pb.HeartbeatResponse{Error: errNoWorkerID}, nil
	}

	renewed, err := s.q.Heartbeat(ctx, req.GetJobId(), workerID)
	if err != nil {
		return &pb.HeartbeatResponse{Error: err.Error()}, nil
	}

	return &pb.HeartbeatResponse{Ok: renewed}, nil
}

// logFenced notes that a late result was refused. That is the fence working, not
// a server fault: the job was reassigned while this worker was still working on
// it, and the new owner's result is the one that counts.
func logFenced(err error, result, jobID, workerID string) {
	if errors.Is(err, storage.ErrLeaseLost) {
		fmt.Fprintf(os.Stderr, "[Server] fenced late %s for job %s from worker %s\n", result, jobID, workerID)
	}
}
