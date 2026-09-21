package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ss1ngh/distq-go/internal/pb"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}

	const schema = `
	CREATE TABLE IF NOT EXISTS jobs(
		id TEXT PRIMARY KEY,
		type TEXT NOT NULL,
		payload BYTEA,
		state TEXT NOT NULL DEFAULT 'pending',
		retry_count INT NOT NULL DEFAULT 0,
		max_retries INTEGER NOT NULL DEFAULT 3,
		last_error TEXT,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		started_at TIMESTAMP,
		done_at TIMESTAMP
	);`

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create postgres schema: %w", err)
	}

	// Tiny inline migration: CREATE TABLE IF NOT EXISTS does nothing on an
	// existing table, so new columns must be added separately.
	const migrations = `
	ALTER TABLE jobs ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMP;
	CREATE INDEX IF NOT EXISTS idx_jobs_state_next_run ON jobs(state, next_run_at);
	UPDATE jobs SET next_run_at = created_at WHERE next_run_at IS NULL;`

	if _, err := db.Exec(migrations); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate postgres schema: %w", err)
	}

	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) CreateJob(ctx context.Context, j *pb.Job) error {
	status := j.Status
	if status == "" {
		status = "pending"
	}

	maxRetries := j.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	_, err := s.db.ExecContext(ctx, `INSERT INTO jobs (id, type, payload, state, max_retries, last_error, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)`,
		j.Id, j.Type, j.Payload, status, maxRetries, j.Error)
	if err != nil {
		return fmt.Errorf("create job: %w", err)
	}
	return nil
}

// DequeueJob atomically claims the next visible pending job.
// SKIP LOCKED: two concurrent dequeues can never claim the same row —
// the second one skips past the row the first one locked.
// next_run_at <= now() is the backoff gate: a failed job waiting out its
// backoff window is pending but not yet visible.
func (s *PostgresStore) DequeueJob(ctx context.Context) (*pb.Job, error) {
	query := `
		UPDATE jobs
		SET state = 'processing', started_at = CURRENT_TIMESTAMP
		WHERE id = (
			SELECT id FROM jobs
			WHERE state = 'pending' AND next_run_at <= CURRENT_TIMESTAMP
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, type, payload, state, coalesce(last_error, ''), retry_count, max_retries;`

	j := &pb.Job{}

	err := s.db.QueryRowContext(ctx, query).Scan(
		&j.Id,
		&j.Type,
		&j.Payload,
		&j.Status,
		&j.Error,
		&j.RetryCount,
		&j.MaxRetries,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // Queue is empty
		}
		return nil, fmt.Errorf("dequeue job: %w", err)
	}

	return j, nil
}

// Complete marks a job as successfully finished.
func (s *PostgresStore) Complete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET state = 'done', done_at = CURRENT_TIMESTAMP WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("mark done: %w", err)
	}
	return nil
}

// FailJob records a handler failure and atomically decides the next state.
//
// The whole retry-vs-DLQ decision happens in ONE statement: the CASE is
// evaluated under the row lock taken by the UPDATE, so two concurrent Fail
// calls can never both bump retry_count or disagree about the outcome.
// RETURNING state tells us which way it went.
func (s *PostgresStore) FailJob(ctx context.Context, id, errMsg string, baseDelay time.Duration) (bool, error) {
	query := `
		UPDATE jobs
		SET state = CASE WHEN retry_count < max_retries THEN 'pending' ELSE 'failed' END,
			retry_count = retry_count + 1,
			last_error = $2,
			done_at = CASE WHEN retry_count < max_retries THEN NULL ELSE CURRENT_TIMESTAMP END,
			next_run_at = CASE WHEN retry_count < max_retries
				THEN CURRENT_TIMESTAMP + make_interval(secs => $3 * pow(2, retry_count))
				ELSE next_run_at END
		WHERE id = $1 AND state = 'processing'
		RETURNING state;`

	var state string
	err := s.db.QueryRowContext(ctx, query, id, errMsg, baseDelay.Seconds()).Scan(&state)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Job wasn't in 'processing' — either unknown id or already
			// decided by someone else. Treat as not-retrying.
			return false, fmt.Errorf("fail job %s: not in processing state", id)
		}
		return false, fmt.Errorf("fail job: %w", err)
	}

	return state == "pending", nil
}

// GetPendingJobs returns pending or processing jobs (diagnostics/recovery aid).
func (s *PostgresStore) GetPendingJobs(ctx context.Context) ([]*pb.Job, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, type, payload, state, coalesce(last_error, ''), retry_count, max_retries
		 FROM jobs WHERE state IN ('pending', 'processing')`)

	if err != nil {
		return nil, fmt.Errorf("query pending jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*pb.Job
	for rows.Next() {
		j := &pb.Job{}
		if err := rows.Scan(&j.Id, &j.Type, &j.Payload, &j.Status, &j.Error, &j.RetryCount, &j.MaxRetries); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}
	return jobs, nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
