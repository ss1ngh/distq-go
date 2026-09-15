package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/ss1ngh/distq-go/internal/job"
)

// Command represents the allowed operations over the TCP socket.
type Command string

const (
	CmdEnqueue Command = "ENQUEUE"
	CmdDequeue Command = "DEQUEUE"
	CmdAck     Command = "ACK"
	CmdFail    Command = "FAIL"
)

// Message is the standard JSON envelope we will send inside the frame payload.
type Message struct {
	Command Command  `json:"command"`
	JobID   string   `json:"job_id,omitempty"`
	Job     *job.Job `json:"job,omitempty"`
	Error   string   `json:"error,omitempty"`
}

// WriteFrame packs a JSON message into a length-prefixed binary frame and writes it to the socket.
func WriteFrame(w io.Writer, msg Message) error {
	payload, err := json.Marshal(msg)

	if err != nil {
		return fmt.Errorf("marshal payload : %w", err)
	}

	//create 4-byte header buffer
	header := make([]byte, 4)

	binary.BigEndian.PutUint32(header, uint32(len(payload)))

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}

	return nil
}

// ReadFrame blocks until a complete length-prefixed frame is read from the socket.
func ReadFrame(r io.Reader) (Message, error) {
	var msg Message

	//read exactly 4 bytes to determine the incoming payload size
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return msg, err // Returns io.EOF if the client cleanly disconnected
	}

	length := binary.BigEndian.Uint32(header)

	// Protect the server from malicious clients sending massive fake length headers
	if length > 10<<20 { // 10 MB max limit
		return msg, fmt.Errorf("payload too large: %d bytes", length)
	}

	//Read exactly 'length' bytes for the payload
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return msg, fmt.Errorf("read payload: %w", err)
	}

	//Unpack the JSON back into our Message struct
	if err := json.Unmarshal(payload, &msg); err != nil {
		return msg, fmt.Errorf("unmarshal payload: %w", err)
	}

	return msg, nil
}
