package job

import(
	"time"
	"github.com/google/uuid"
)

type State int

const(
	Pending State = iota
	Processing
	Done
	Failed
)

func (s State) String() string {
	switch s {
	case Pending:
		return "Pending"
	case Processing:
		return "Processing"
	case Done:
		return "Done"
	case Failed:
		return "Failed"
	default:
		return "Unknown"

	}
}

type Job struct {
	ID string `json:"id"`
	Type string `json:"type"`
	Payload []byte `json:"payload,omitempty"`
	State State `json:"state"`
	MaxRetries int `json:"max_retries"`
	RetryCount int `json:"retry_count"`
	LastError string `json:"last_error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	StartedAt time.Time `json:"started_at,omitempty"`
	DoneAt time.Time `json:"done_at,omitempty"`
}

func NewJob(jobType string, payload []byte) *Job {
	return &Job {
		ID: uuid.New().String(),
		Type: jobType,
		Payload: payload,
		State: Pending,
		MaxRetries: 3,
		RetryCount: 0,
		CreatedAt: time.Now().UTC(),
	}
}