package server

import (
	"context"

	"github.com/google/uuid"
	"github.com/ss1ngh/distq-go/internal/pb"
	"github.com/ss1ngh/distq-go/internal/queue"
)

type Server struct {
	pb.UnimplementedJobQueueServer
	q   *queue.Queue
	dis *dispatcher
	// ctx is cancelled to shut the server down. Streams are long-lived by
	// design, so a handler has to watch this as well as its own connection:
	// otherwise a worker holding a stream open would keep shutdown waiting for
	// it to hang up.
	ctx context.Context
}

// New initializes a server backed by the given queue. The context bounds the work
// the server does on its own behalf, such as the dispatcher's timer for a job that
// is not due yet.
func New(ctx context.Context, q *queue.Queue) *Server {
	return &Server{
		q:   q,
		dis: newDispatcher(ctx, q),
		ctx: ctx,
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

	// A new job is the one thing that can turn an idle worker into a busy one, so
	// push here instead of leaving it for something else to notice.
	s.dis.Push()

	return &pb.EnqueueResponse{JobId: job.Id}, nil
}
