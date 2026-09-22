package server

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/ss1ngh/distq-go/internal/pb"
	"github.com/ss1ngh/distq-go/internal/queue"
)

type Server struct {
	pb.UnimplementedJobQueueServer
	q   *queue.Queue
	dis *dispatcher
	// lead runs the cluster's leader election. The leader is the only server
	// that admits worker streams and dispatches; followers keep serving
	// Enqueue, which is a plain database write and safe anywhere.
	lead *Leadership
	// ctx is cancelled to shut the server down. Streams are long-lived by
	// design, so a handler has to watch this as well as its own connection:
	// otherwise a worker holding a stream open would keep shutdown waiting for
	// it to hang up.
	ctx context.Context
}

// New initializes a server backed by the given queue. The context bounds the
// work the server does on its own behalf, such as the dispatcher's timer for a
// job that is not due yet. reapEvery is how often the leader — once elected —
// sweeps expired job leases.
func New(ctx context.Context, q *queue.Queue, reapEvery time.Duration) *Server {
	lead := NewLeadership(q, uuid.New().String()[:8], q.LeaseFor(), reapEvery)
	dis := newDispatcher(ctx, q, lead)

	return &Server{q: q, dis: dis, lead: lead, ctx: ctx}
}

// Leadership is the server's election loop, for the caller to Run.
func (s *Server) Leadership() *Leadership { return s.lead }

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
