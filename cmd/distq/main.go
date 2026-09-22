package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/ss1ngh/distq-go/internal/config"
	"github.com/ss1ngh/distq-go/internal/pb"
	"github.com/ss1ngh/distq-go/internal/queue"
	"github.com/ss1ngh/distq-go/internal/server"
	"github.com/ss1ngh/distq-go/internal/storage"
)

const (
	// leaseFor is how long a worker owns a job it claimed. A worker that dies
	// mid-job holds it for at most this long before it is handed to somebody
	// else, so failover latency is leaseFor + reapInterval.
	leaseFor = 30 * time.Second
	// reapInterval is how often expired leases are swept.
	reapInterval = 5 * time.Second
	// shutdownGrace bounds how long in-flight RPCs get to finish.
	shutdownGrace = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "[Server] %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	dsn, err := config.PostgresDSN()
	if err != nil {
		return err
	}

	store, err := storage.NewPostgresStore(dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	defer store.Close()
	fmt.Println("[Server] Successfully connected to PostgreSQL database!")

	q, err := queue.New(queue.Options{Store: store, LeaseFor: leaseFor})
	if err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go reapExpiredLeases(ctx, q)

	addr := config.ListenAddr()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterJobQueueServer(grpcServer, server.New(ctx, q))

	serveErr := make(chan error, 1)
	go func() {
		fmt.Printf("[Server] gRPC listening on %s with PostgreSQL backing...\n", addr)
		serveErr <- grpcServer.Serve(ln)
	}()

	select {
	case err := <-serveErr:
		// The listener died. Staying up would look healthy while refusing every
		// connection, so report it instead.
		return fmt.Errorf("grpc server stopped: %w", err)
	case <-ctx.Done():
	}

	fmt.Println("\n[Server] Shutdown signal received...")
	gracefulStop(grpcServer)
	fmt.Println("[Server] System shutdown complete.")
	return nil
}

// reapExpiredLeases is the whole of crash recovery: a worker that dies stops
// renewing its lease, and the next sweep returns its job to the queue. There is
// no separate boot-time recovery path — a restarted server sweeps before its
// first tick, so it recovers whatever has already lapsed straight away and the
// rest within one lease.
func reapExpiredLeases(ctx context.Context, q *queue.Queue) {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()

	for {
		released, err := q.ReapExpiredLeases(ctx)
		switch {
		case err != nil && ctx.Err() != nil:
			return // shutting down mid-sweep
		case err != nil:
			fmt.Fprintf(os.Stderr, "[Reaper] sweep failed: %v\n", err)
		case released > 0:
			fmt.Printf("[Reaper] reassigned %d job(s) whose lease expired\n", released)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// gracefulStop drains in-flight RPCs, but not indefinitely: the stop is forced
// after shutdownGrace so a stuck worker cannot keep the process alive forever.
func gracefulStop(grpcServer *grpc.Server) {
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(shutdownGrace):
		fmt.Fprintln(os.Stderr, "[Server] Graceful shutdown timed out, forcing stop")
		grpcServer.Stop()
	}
}
