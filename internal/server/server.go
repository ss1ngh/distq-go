package server

import (
	"context"

	"github.com/google/uuid"
	"github.com/ss1ngh/distq-go/internal/pb"
	"github.com/ss1ngh/distq-go/internal/queue"
)

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
	job, err := s.q.Dequeue(ctx)

	if err != nil {
		return &pb.DequeueResponse{Error: err.Error()}, nil
	}

	return &pb.DequeueResponse{Job: job}, nil
}

// Complete marks a job as successfully finished.
func (s *Server) Complete(ctx context.Context, req *pb.CompleteRequest) (*pb.CompleteResponse, error) {
	if err := s.q.Complete(ctx, req.GetJobId()); err != nil {
		return &pb.CompleteResponse{Error: err.Error()}, nil
	}
	return &pb.CompleteResponse{}, nil
}

// Fail reports a handler failure. The queue/store decides retry-vs-DLQ
// atomically and we surface the decision to the worker.
func (s *Server) Fail(ctx context.Context, req *pb.FailRequest) (*pb.FailResponse, error) {
	retrying, err := s.q.Fail(ctx, req.GetJobId(), req.GetErrorMessage())
	if err != nil {
		return &pb.FailResponse{Error: err.Error()}, nil
	}
	return &pb.FailResponse{Retrying: retrying}, nil
}
