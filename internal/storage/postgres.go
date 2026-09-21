package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

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

	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) CreateJob(ctx context.Context, j *pb.Job) error {
	status := j.Status
	if status == "" {
		status = "pending"
	}

	// Note the $1, $2 Postgres placeholders
	_, err := s.db.ExecContext(ctx, `INSERT INTO jobs (id, type, payload, state, last_error) 
		VALUES ($1, $2, $3, $4, $5)`,
		j.Id, j.Type, j.Payload, status, j.Error)
	if err != nil {
		return fmt.Errorf("create job: %w", err)
	}
	return nil
}

func (s *PostgresStore) DequeueJob(ctx context.Context) (*pb.Job, error) {
	query := `
		UPDATE jobs
		SET state = 'processing', started_at = CURRENT_TIMESTAMP
		WHERE id = (
			SELECT id FROM jobs
			WHERE state = 'pending'
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, type, payload, state, coalesce(last_error, '');`

	j := &pb.Job{}

	err := s.db.QueryRowContext(ctx, query).Scan(
		&j.Id,
		&j.Type,
		&j.Payload,
		&j.Status,
		&j.Error,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // Queue is empty
		}
		return nil, fmt.Errorf("dequeue job: %w", err)
	}

	return j, nil
}

func (s *PostgresStore) MarkDone(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET state = 'done', done_at = CURRENT_TIMESTAMP WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("mark done : %w", err)
	}
	return nil
}

func (s *PostgresStore) MarkFailed(ctx context.Context, id string, errMsg string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET state = 'failed', last_error = $1, done_at = CURRENT_TIMESTAMP WHERE id = $2`, errMsg, id)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	return nil
}

func (s *PostgresStore) MarkPending(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET state = 'pending', retry_count = retry_count + 1 WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("mark pending: %w", err)
	}
	return nil
}

func (s *PostgresStore) MarkProcessing(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET state = 'processing', started_at = CURRENT_TIMESTAMP WHERE id = $1 AND state = 'pending'`, id)
	if err != nil {
		return fmt.Errorf("mark processing : %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if n != 1 {
		return errors.New("job already claimed")
	}
	return nil
}

// GetPendingJobs recovers pending or processing jobs on startup
func (s *PostgresStore) GetPendingJobs(ctx context.Context) ([]*pb.Job, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, type, payload, state, coalesce(last_error, '')
		 FROM jobs WHERE state IN ('pending', 'processing')`)

	if err != nil {
		return nil, fmt.Errorf("query pending jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*pb.Job
	for rows.Next() {
		j := &pb.Job{}
		if err := rows.Scan(&j.Id, &j.Type, &j.Payload, &j.Status, &j.Error); err != nil {
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
