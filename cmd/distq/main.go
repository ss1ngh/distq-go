package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ss1ngh/distq-go/internal/job"
	"github.com/ss1ngh/distq-go/internal/queue"
	"github.com/ss1ngh/distq-go/internal/storage"
	"github.com/ss1ngh/distq-go/internal/worker"
)

func main() {
	store, err := storage.NewSQLiteStore("distq.db")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open storage: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	//create queue
	q, err := queue.New(queue.Options{Store: store})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create a queue: %v\n", err)
		os.Exit(1)
	}

	//recover jobs
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nreceived interrupt, shutting down...")
		cancel()
	}()

	w := worker.New(q, func(ctx context.Context, j *job.Job) error {
		fmt.Printf("processing job %s (type : %s)\n", j.ID[:8], j.Type)
		time.Sleep(500 * time.Millisecond)
		return nil
	})

	go w.Start(ctx)

	//enqueue 10 jobs.
	for i := 0; i < 10; i++ {
		j := job.NewJob("demo", []byte(fmt.Sprintf(`{"num":%d}`, i)))
		if err := q.Enqueue(ctx, j); err != nil {
			fmt.Fprintf(os.Stderr, "failed to enqueue: %v\n", err)
			break
		}
		fmt.Printf("enqueued job %s\n", j.ID[:8])
	}

	//wait a bit for the worker to finish all jobs.
	time.Sleep(5 * time.Second)
}
