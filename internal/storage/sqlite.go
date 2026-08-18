package storage

import(
	"context"
	"fmt"
	"database/sql"

	_ "modernc.org/sqlite"
	"github.com/ss1ngh/distq-go/internal/job"
)

type SQLiteStore struct{
	db *sql.DB
}

func NewSQLiteStore(path string) (*SQLiteStore, error){
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	const schema = `
	CREATE TABLE IF NOT EXISTS jobs(
		id TEXT PRIMARY KEY,
		type TEXT NOT NULL,
		payload BLOB, 
		state TEXT NOT NULL,
		retry_count INT NOT NULL,
		max_retries INTEGER NOT NULL,
		last_error TEXT,
		created_at DATETIME NOT NULL,
		started_at DATETIME,
		done_at DATETIME
	);`

	if _,err := db.Exec(schema); err != nil{
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	return &SQLiteStore{db : db}, nil
}


//creates new jobs
func (s *SQLiteStore) CreateJob(ctx context.Context, j *job.Job) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO jobs (id, type, payload, state, max_retries, retry_count, last_error, created_at) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.Type, j.Payload, "pending", j.MaxRetries, j.RetryCount, j.LastError, j.CreatedAt)
	if err != nil {
		return fmt.Errorf("create job: %w", err)
	}
	return nil
}

func(s *SQLiteStore) MarkProcessing(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET state = 'processing', started_at= CURRENT_TIMESTAMP WHERE id = ?`, id)

	if err != nil {
		return fmt.Errorf("mark processing : %w", err)
	}
	return nil
}

func (s *SQLiteStore) MarkDone(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET state = 'done', done_at= CURRENT_TIMESTAMP WHERE id=?`, id)

	if err!= nil {
		return fmt.Errorf("mark done : %w", err)
	}
	return nil
}

func (s *SQLiteStore) MarkFailed(ctx context.Context, id string, errMsg string) error {
	_, err := s.db.ExecContext(ctx,`UPDATE jobs SET state = 'failed', last_error = ?, done_at = CURRENT_TIMESTAMP WHERE id = ?`, errMsg, id)
	
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	return nil
}

func (s *SQLiteStore) MarkPending(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET state = 'pending', retry_count = retry_count + 1 WHERE id = ?`, id)

	if err != nil {
		return fmt.Errorf("mark pending: %w", err)
	}
	return nil
}

//run at startup to get recover after a crash/restart
func (s *SQLiteStore) GetPendingJobs(ctx context.Context) ([]*job.Job, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, type, payload, state, max_retries, retry_count, last_error, created_at
		 FROM jobs WHERE state IN ('pending', 'processing')`)
		 
	if err != nil {
		return nil, fmt.Errorf("query pending jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*job.Job
	for rows.Next() {
		j := &job.Job{}
		var state string
		if err := rows.Scan(&j.ID, &j.Type, &j.Payload, &state, &j.MaxRetries,
			&j.RetryCount, &j.LastError, &j.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		j.State = jobStateFromString(state)
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}
	return jobs, nil
}

//release db file connection
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}


func jobStateFromString(s string) job.State {
	switch s {
	case "pending":
		return job.Pending
	case "processing":
		return job.Processing
	case "done":
		return job.Done
	case "failed":
		return job.Failed
	default:
		return job.Pending
	}
}