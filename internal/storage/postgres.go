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

// retryDecision is the one place the retry-vs-DLQ rule lives: a job with
// attempts left goes back to 'pending', one that has exhausted max_retries goes
// to the DLQ as 'failed'. max_retries counts retries AFTER the first attempt, so
// a job runs at most max_retries+1 times.
const retryDecision = `CASE WHEN retry_count < max_retries THEN 'pending' ELSE 'failed' END`

// doneAtDecision keeps done_at set only on the attempt that gave up.
const doneAtDecision = `CASE WHEN retry_count < max_retries THEN NULL ELSE CURRENT_TIMESTAMP END`

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
	//
	// worker_id is '' while nobody owns the job; the lease pair (worker_id,
	// lease_expires_at) is what lets a job be reassigned after its worker dies.
	const migrations = `
	ALTER TABLE jobs ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMP;
	ALTER TABLE jobs ADD COLUMN IF NOT EXISTS worker_id TEXT NOT NULL DEFAULT '';
	ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMP;
	CREATE INDEX IF NOT EXISTS idx_jobs_state_next_run ON jobs(state, next_run_at);
	CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs(state, lease_expires_at);
	UPDATE jobs SET next_run_at = created_at WHERE next_run_at IS NULL;
	UPDATE jobs SET lease_expires_at = created_at WHERE state = 'processing' AND lease_expires_at IS NULL;`

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

// DequeueJob atomically claims the next visible pending job and leases it to
// workerID.
//
// SKIP LOCKED: two concurrent dequeues can never claim the same row — the
// second one skips past the row the first one locked.
// next_run_at <= now() is the backoff gate: a failed job waiting out its
// backoff window is pending but not yet visible.
// worker_id + lease_expires_at are the lease: the claim is proof of ownership
// for this worker, and nothing else, until the lease runs out.
func (s *PostgresStore) DequeueJob(ctx context.Context, workerID string, lease time.Duration) (*pb.Job, error) {
	query := `
		UPDATE jobs
		SET state = 'processing',
			started_at = CURRENT_TIMESTAMP,
			worker_id = $1,
			lease_expires_at = CURRENT_TIMESTAMP + make_interval(secs => $2)
		WHERE id = (
			SELECT id FROM jobs
			WHERE state = 'pending' AND next_run_at <= CURRENT_TIMESTAMP
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, type, payload, state, coalesce(last_error, ''), retry_count, max_retries;`

	j := &pb.Job{}

	err := s.db.QueryRowContext(ctx, query, workerID, lease.Seconds()).Scan(
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
			return nil, nil // nothing visible right now
		}
		return nil, fmt.Errorf("dequeue job: %w", err)
	}

	return j, nil
}

// Complete marks a job as successfully finished. It is fenced on worker_id: the
// row only matches while the caller is still the worker holding the lease, so a
// zombie worker cannot mark a reassigned job done.
func (s *PostgresStore) Complete(ctx context.Context, id, workerID string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs
		 SET state = 'done', done_at = CURRENT_TIMESTAMP, worker_id = '', lease_expires_at = NULL
		 WHERE id = $1 AND worker_id = $2 AND state = 'processing'`, id, workerID)
	if err != nil {
		return fmt.Errorf("mark done: %w", err)
	}

	// No row matched: the id is unknown, or the lease is no longer ours.
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if affected == 0 {
		return ErrLeaseLost
	}
	return nil
}

// FailJob records a handler failure and atomically decides the next state.
//
// The whole decision happens in ONE statement: the CASE is evaluated under the
// row lock taken by the UPDATE, so two concurrent Fail calls can never both bump
// retry_count or disagree about the outcome. RETURNING state tells us which way
// it went. Fenced on worker_id like Complete.
func (s *PostgresStore) FailJob(ctx context.Context, id, workerID, errMsg string, baseDelay time.Duration) (bool, error) {
	query := `
		UPDATE jobs
		SET state = ` + retryDecision + `,
			retry_count = retry_count + 1,
			last_error = $1,
			worker_id = '',
			lease_expires_at = NULL,
			done_at = ` + doneAtDecision + `,
			next_run_at = CASE WHEN retry_count < max_retries
				THEN CURRENT_TIMESTAMP + make_interval(secs => $2 * pow(2, retry_count))
				ELSE next_run_at END
		WHERE id = $3 AND worker_id = $4 AND state = 'processing'
		RETURNING state;`

	var state string
	err := s.db.QueryRowContext(ctx, query, errMsg, baseDelay.Seconds(), id, workerID).Scan(&state)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Not ours to fail: unknown id, already decided, or leased to
			// somebody else by now.
			return false, ErrLeaseLost
		}
		return false, fmt.Errorf("fail job: %w", err)
	}

	return state == "pending", nil
}

// HeartbeatJob pushes the lease expiry of a job the worker is still working on
// forward, so a job that takes longer than one lease is not mistaken for a dead
// worker's. It is fenced on worker_id like Complete, and reports false once the
// job has moved on.
func (s *PostgresStore) HeartbeatJob(ctx context.Context, id, workerID string, lease time.Duration) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs
		 SET lease_expires_at = CURRENT_TIMESTAMP + make_interval(secs => $3)
		 WHERE id = $1 AND worker_id = $2 AND state = 'processing'`, id, workerID, lease.Seconds())
	if err != nil {
		return false, fmt.Errorf("heartbeat job: %w", err)
	}

	// No row matched: the lease is no longer ours, so the worker has to stop.
	// Not an error — it is the answer.
	renewed, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected: %w", err)
	}
	return renewed > 0, nil
}

// NextVisible reports how long until the oldest pending job becomes claimable,
// and whether the queue holds anything pending at all. The wait is measured by
// the database, so a clock difference between the app and the database cannot
// turn a caller's sleep into a spin.
func (s *PostgresStore) NextVisible(ctx context.Context) (time.Duration, bool, error) {
	const query = `
		SELECT coalesce(extract(epoch FROM (min(next_run_at) - CURRENT_TIMESTAMP)), 0)::float8,
		       count(*) > 0
		FROM jobs
		WHERE state = 'pending'`

	var seconds float64
	var pending bool
	if err := s.db.QueryRowContext(ctx, query).Scan(&seconds, &pending); err != nil {
		return 0, false, fmt.Errorf("next visible job: %w", err)
	}

	return time.Duration(seconds * float64(time.Second)), pending, nil
}

// ReapExpiredLeases returns jobs whose worker stopped renewing its lease to the
// queue so another worker can pick them up. A stall counts as an attempt, and
// goes through the same retry-vs-DLQ decision as a reported failure — otherwise
// a job that kills every worker that touches it would be retried forever.
//
// No backoff delay here, unlike FailJob: the worker holding the job is gone, so
// there is nothing to wait for.
func (s *PostgresStore) ReapExpiredLeases(ctx context.Context) (int64, error) {
	query := `
		UPDATE jobs
		SET state = ` + retryDecision + `,
			retry_count = retry_count + 1,
			last_error = 'lease expired',
			worker_id = '',
			lease_expires_at = NULL,
			done_at = ` + doneAtDecision + `,
			next_run_at = CURRENT_TIMESTAMP
		WHERE state = 'processing' AND lease_expires_at < CURRENT_TIMESTAMP`

	res, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("reap expired leases: %w", err)
	}

	released, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return released, nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
