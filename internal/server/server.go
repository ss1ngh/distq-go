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

// New initializes a TCP server bound to the specified address.
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

// Ack marks a job as successfully completed.
func (s *Server) Ack(ctx context.Context, req *pb.AckRequest) (*pb.AckResponse, error) {
	if err := s.q.Ack(ctx, req.GetJobId()); err != nil {
		return &pb.AckResponse{Error: err.Error()}, nil
	}
	return &pb.AckResponse{}, nil
}

// Fail marks a job as failed and records the reason.
func (s *Server) Fail(ctx context.Context, req *pb.FailRequest) (*pb.FailResponse, error) {
	if err := s.q.Fail(ctx, req.GetJobId(), req.GetErrorMessage()); err != nil {
		return &pb.FailResponse{Error: err.Error()}, nil
	}
	return &pb.FailResponse{}, nil
}
