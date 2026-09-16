package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/google/uuid"
	"github.com/ss1ngh/distq-go/internal/protocol"
	"github.com/ss1ngh/distq-go/internal/queue"
)

// Server manages the TCP listener and routes network commands to the underlying storage queue.
type Server struct {
	addr string //network address
	q    *queue.Queue
	ln   net.Listener //active tcp socket for incoming traffic
	wg   sync.WaitGroup
}

// New initializes a TCP server bound to the specified address.
func New(addr string, q *queue.Queue) *Server {
	return &Server{
		addr: addr,
		q:    q,
	}
}

func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("bind tcp %s: %w", s.addr, err)
	}
	s.ln = ln
	fmt.Printf("Server: listening on %s\n", s.addr)

	// Background goroutine to trigger listener closure on context cancellation.
	go func() {
		<-ctx.Done()
		fmt.Println("Server: context canceled, closing listener")
		s.ln.Close()
	}()

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			// net.ErrClosed triggers when the shutdown goroutine closes the listener
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			fmt.Printf("Server: accept error: %v\n", err)
			continue
		}

		s.wg.Add(1)                      //increment active connection counter
		go s.handleConnection(ctx, conn) //new independent goroutine for this specific client
	}
}

// handleConnection manages the lifecycle of a single TCP client socket.
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close() //free resources when client disconnects

	fmt.Printf("Server: accepted connection from %s\n", conn.RemoteAddr())

	for {
		// 1. Read exactly one length-prefixed protocol frame.
		req, err := protocol.ReadFrame(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Printf("Server: client %s cleanly disconnected\n", conn.RemoteAddr())
				return
			}
			fmt.Printf("Server: frame read error from %s: %v\n", conn.RemoteAddr(), err)
			return
		}

		// 2. Route the protocol command to the SQLite storage engine.
		resp := s.routeCommand(ctx, req)

		// 3. Pack the result into a frame and write it back over the socket.
		if err := protocol.WriteFrame(conn, resp); err != nil {
			fmt.Printf("Server: frame write error to %s: %v\n", conn.RemoteAddr(), err)
			return
		}
	}
}

// routeCommand translates network intents into queue state changes.
func (s *Server) routeCommand(ctx context.Context, req protocol.Message) protocol.Message {
	resp := protocol.Message{Command: req.Command}

	switch req.Command {
	case protocol.CmdDequeue:
		j, err := s.q.Dequeue(ctx)
		if err != nil {
			resp.Error = err.Error()
		} else if j != nil {
			resp.Job = j
		}

	case protocol.CmdEnqueue:
		// 1. Validate the incoming network frame
		if req.Job == nil || req.Job.Type == "" {
			resp.Error = "invalid frame: missing job data for ENQUEUE"
			break
		}

		// 2. Ensure the job has a unique ID before touching the database
		if req.Job.ID == "" {
			req.Job.ID = uuid.New().String()
		}

		// 2. Route the data to the SQLite engine
		err := s.q.Enqueue(ctx, req.Job)
		if err != nil {
			resp.Error = err.Error()
		} else {
			resp.JobID = req.Job.ID
		}

	case protocol.CmdAck:
		if err := s.q.Ack(ctx, req.JobID); err != nil {
			resp.Error = err.Error()
		}

	case protocol.CmdFail:
		if err := s.q.Fail(ctx, req.JobID, req.Error); err != nil {
			resp.Error = err.Error()
		}

	default:
		resp.Error = fmt.Sprintf("unknown command: %s", req.Command)
	}

	return resp
}

// Stop waits for all active client goroutines to finish processing before returning.
func (s *Server) Stop() {
	s.wg.Wait()
	fmt.Println("Server: all connections closed, shutdown complete")
}
