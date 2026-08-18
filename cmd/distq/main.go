package main

import(
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ss1ngh/distq-go/internal/job"
	"github.com/ss1ngh/distq-go/internal/queue"
	"github.com/ss1ngh/distq-go/internal/storage"
)

func main() {
	store, err := storage.NewSQLiteStore("distq.db")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open storage: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	//create queue
	q, err := queue.New(queue.Options{BufferSize:100, Store: store})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create queue: %v\n", err)
		os.Exit(1)
	}

	//create a context that cancels when ctrl+c  is passed
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nreceived interrupt, shutting down...")
		cancel()
	}()

	//start a worker goroutine that processes jobs until shutdown.
	go func() {
		for {
			job, err := q.Dequeue(ctx)
			if err != nil {
				//context was cancelled — stop working.
				return
			}
			fmt.Printf("processing job %s (type: %s)\n", job.ID[:8], job.Type)
			time.Sleep(500 * time.Millisecond) // pretend to work
			if err := q.Ack(job.ID); err != nil {
				fmt.Printf("ack failed for %s: %v\n", job.ID[:8], err)
			}
			fmt.Printf("job %s done\n", job.ID[:8])
		}
	}()

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

	//shut down gracefully — wait up to 5s for in-flight jobs.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := q.Shutdown(shutdownCtx); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown error: %v\n", err)
	}
	fmt.Println("queue shut down cleanly")
}