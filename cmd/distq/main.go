package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/ss1ngh/distq-go/internal/pb"
	"github.com/ss1ngh/distq-go/internal/queue"
	"github.com/ss1ngh/distq-go/internal/server"
	"github.com/ss1ngh/distq-go/internal/storage"
)

func main() {
	//connect to PostgreSQL running in Docker

	dsn := "postgres://user:pass@localhost:5432/distq?sslmode=disable"
	store, err := storage.NewPostgresStore(dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to postgres: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()
	fmt.Println("[Server] Successfully connected to PostgreSQL database!")

	//initialize queue logic
	q, err := queue.New(queue.Options{Store: store})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create queue: %v\n", err)
		os.Exit(1)
	}

	// 3. Open the OS network port
	ln, err := net.Listen("tcp", ":4040")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to listen: %v\n", err)
		os.Exit(1)
	}

	// 4. Create the gRPC Engine
	grpcServer := grpc.NewServer()

	// 5. Register our custom Server struct with the gRPC Engine
	srv := server.New(q)
	pb.RegisterJobQueueServer(grpcServer, srv)

	go func() {
		fmt.Println("gRPC Server listening on port 4040 with PostgreSQL backing...")
		if err := grpcServer.Serve(ln); err != nil {
			fmt.Fprintf(os.Stderr, "grpc server error: %v\n", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutdown signal received...")
	grpcServer.GracefulStop()
	fmt.Println("System shutdown complete.")
}
