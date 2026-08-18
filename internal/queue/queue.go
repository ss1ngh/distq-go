package queue

import(
	"fmt"
	"sync"
	"context"
	"errors"

	"github.com/ss1ngh/distq-go/internal/job"
	"github.com/ss1ngh/distq-go/internal/storage"
)

type Options struct{
	BufferSize int
	Store      storage.Store
}

type Queue struct {
	jobChan chan *job.Job

	mu sync.Mutex
	pending map[string]*job.Job

	store storage.Store
}

//creates a new queue
func New(opts Options) (*Queue, error) {
	if opts.BufferSize <= 0 {
		return nil, errors.New("Buffersize must be positive")
	}
	if opts.Store == nil {
		return nil, errors.New("Store is required")
	}

	return &Queue {
		jobChan : make(chan *job.Job, opts.BufferSize),
		pending : make(map[string]*job.Job),
		store:    opts.Store,
	}, nil
}


func (q *Queue) Enqueue(ctx context.Context, j *job.Job) error {
	if err := q.store.CreateJob(ctx, j); err != nil {
		return fmt.Errorf("persist job: %w", err)
	}

	select {
	case q.jobChan <- j: //send job to a channel
		return nil
	case <- ctx.Done(): //pointing away from channel means receiving from channel
		return ctx.Err()
	}
}

func (q *Queue) Dequeue(ctx context.Context) (*job.Job , error) {
	select {
	case j, ok := <-q.jobChan:
		if !ok {
			return nil, errors.New("queue is closed")
		}
		q.mu.Lock()
		q.pending[j.ID] = j
		q.mu.Unlock()

		if err := q.store.MarkProcessing(ctx, j.ID); err != nil {
			return nil, fmt.Errorf("mark processing: %w", err)
		}
		return j, nil

	case <-ctx.Done():
		return nil, ctx.Err() 
	}
}

func (q *Queue) Ack(jobID string) error {
	q.mu.Lock()
	if _, ok := q.pending[jobID]; !ok {
		q.mu.Unlock()
		return fmt.Errorf("job %s not found in pending set", jobID)
	}
	delete(q.pending, jobID)
	q.mu.Unlock()

	return q.store.MarkDone(context.Background(), jobID)
}

func (q *Queue) Shutdown(ctx context.Context) error {
	close(q.jobChan)

	//drain remaining jobs from channel into pending.
	for j := range q.jobChan {
		q.mu.Lock()
		q.pending[j.ID] = j
		q.mu.Unlock()
	}

	//wait for all pending jobs to be acked.
	for {
		q.mu.Lock()
		remaining := len(q.pending)
		q.mu.Unlock()

		if remaining == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("shutdown timed out with %d pending jobs", remaining)
		default:
		}
	}
}

func (q *Queue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobChan) == 0 && len(q.pending) == 0
}  