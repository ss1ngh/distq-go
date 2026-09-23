package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
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
	// leaseFor is how long a worker owns a job it claimed — and, sharing one
	// knob, how long a server owns the leadership. A worker that dies mid-job
	// holds it for at most this long before it is handed to somebody else, so
	// failover latency is leaseFor + reapInterval.
	leaseFor = 30 * time.Second
	// reapInterval is how often expired leases are swept, by the leader.
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

	elect, err := newElection(q, leaseFor)
	if err != nil {
		return err
	}
	defer elect.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := server.New(ctx, q, elect, reapInterval)
	go srv.Leadership().Run(ctx)

	addr := config.ListenAddr()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterJobQueueServer(grpcServer, srv)

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
	<-srv.Leadership().Stopped() // waits for the leadership handover
	fmt.Println("[Server] System shutdown complete.")
	return nil
}

// newElection picks the mechanism that decides which server leads: etcd when a
// cluster is named, the store's own lease row otherwise. The choice is announced
// at startup rather than inferred later, because it changes what has to be
// running for leadership to work at all.
//
// The two are not interchangeable on a live cluster. A term only means "later
// than" the terms of the same mechanism, and etcd numbers reigns from its store
// revision while Postgres counts them up from one, so switching back to Postgres
// under a cluster that has elected in etcd leaves the newer claims unfenced and
// unsweepable. Drain the queue before changing this.
func newElection(q *queue.Queue, lease time.Duration) (server.Election, error) {
	endpoints := config.EtcdEndpoints()
	if len(endpoints) == 0 {
		fmt.Println("[Server] Leadership: leasing a row in Postgres")
		return server.NewPostgresElection(q, lease), nil
	}

	elect, err := server.NewEtcdElection(endpoints, lease)
	if err != nil {
		return nil, err
	}
	fmt.Printf("[Server] Leadership: leasing a key in etcd at %s\n", strings.Join(endpoints, ","))
	return elect, nil
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
