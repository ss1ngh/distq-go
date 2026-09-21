package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ss1ngh/distq-go/internal/pb"
)

// These tests run against a real PostgreSQL, because what they check — atomic
// claiming, fencing, lease reaping — is behaviour of the SQL, not of Go. They
// are skipped unless DISTQ_TEST_DSN names a database they may clear out:
//
//	docker exec distq-postgres createdb -U admin distq_test
//	DISTQ_TEST_DSN='postgres://user:pass@localhost:5432/distq_test?sslmode=disable' \
//	    go test ./internal/storage/
func testStore(t *testing.T) *PostgresStore {
	t.Helper()

	dsn := os.Getenv("DISTQ_TEST_DSN")
	if dsn == "" {
		t.Skip("set DISTQ_TEST_DSN to a throwaway database to run these tests")
	}

	store, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	// Every test starts from an empty queue so the counts below mean something.
	if _, err := store.db.Exec(`DELETE FROM jobs`); err != nil {
		t.Fatalf("clear jobs: %v", err)
	}
	return store
}

func testJob(id string) *pb.Job {
	return &pb.Job{Id: id, Type: "test", Payload: []byte(`{"test": true}`), MaxRetries: 3}
}

// TestDequeueClaimsEachJobOnce is the SKIP LOCKED guarantee: workers racing for
// the same queue must never be handed the same job.
func TestDequeueClaimsEachJobOnce(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	const jobCount, workerCount = 50, 8
	for i := range jobCount {
		if err := store.CreateJob(ctx, testJob(fmt.Sprintf("job-%02d", i))); err != nil {
			t.Fatalf("create job: %v", err)
		}
	}

	var (
		mu      sync.Mutex
		claimed = map[string]int{}
		wg      sync.WaitGroup
	)
	for w := range workerCount {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				job, err := store.DequeueJob(ctx, workerID, time.Minute)
				if err != nil {
					t.Errorf("%s: dequeue: %v", workerID, err)
					return
				}
				if job == nil {
					return // queue drained
				}

				mu.Lock()
				claimed[job.Id]++
				mu.Unlock()
			}
		}(fmt.Sprintf("worker-%d", w))
	}
	wg.Wait()

	if len(claimed) != jobCount {
		t.Errorf("claimed %d distinct jobs, want %d", len(claimed), jobCount)
	}
	for id, times := range claimed {
		if times != 1 {
			t.Errorf("job %s was handed out %d times, want 1", id, times)
		}
	}
}

// TestFinishingIsFencedToTheLeaseHolder covers the zombie worker: somebody who
// lost the lease must not be able to report a result for the job.
func TestFinishingIsFencedToTheLeaseHolder(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	if err := store.CreateJob(ctx, testJob("fenced")); err != nil {
		t.Fatalf("create job: %v", err)
	}
	job, err := store.DequeueJob(ctx, "owner", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("dequeue: job=%v err=%v", job, err)
	}

	_, err = store.FailJob(ctx, job.Id, "impostor", "simulated", 0)
	expectLeaseLost(t, "fail", err)
	expectLeaseLost(t, "complete", store.Complete(ctx, job.Id, "impostor"))

	renewed, err := store.HeartbeatJob(ctx, job.Id, "impostor", time.Minute)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if renewed {
		t.Error("heartbeat from a worker that does not hold the lease was accepted")
	}

	// The owner's own result still lands.
	if err := store.Complete(ctx, job.Id, "owner"); err != nil {
		t.Errorf("complete by the lease holder: %v", err)
	}
	if again, err := store.DequeueJob(ctx, "owner", time.Minute); err != nil {
		t.Fatalf("dequeue: %v", err)
	} else if again != nil {
		t.Errorf("job %s was still claimable after completion", again.Id)
	}
}

// TestReapExpiredLeasesReturnsAJobExactlyOnce covers recovery from a dead
// worker, and that a stall the reaper clears is counted as an attempt.
func TestReapExpiredLeasesReturnsAJobExactlyOnce(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	if err := store.CreateJob(ctx, testJob("stuck")); err != nil {
		t.Fatalf("create job: %v", err)
	}
	// A lease that has already lapsed: the worker that held it is gone.
	job, err := store.DequeueJob(ctx, "ghost", -time.Second)
	if err != nil || job == nil {
		t.Fatalf("dequeue: job=%v err=%v", job, err)
	}

	released, err := store.ReapExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if released != 1 {
		t.Errorf("first sweep released %d jobs, want 1", released)
	}

	// Sweeping is not repeatable: the job is pending now, not processing.
	if released, err = store.ReapExpiredLeases(ctx); err != nil {
		t.Fatalf("reap: %v", err)
	} else if released != 0 {
		t.Errorf("second sweep released %d jobs, want 0", released)
	}

	expectLeaseLost(t, "complete", store.Complete(ctx, job.Id, "ghost"))

	again, err := store.DequeueJob(ctx, "live", time.Minute)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if again == nil {
		t.Fatal("job was not claimable after its lease expired")
	}
	if again.Id != job.Id {
		t.Errorf("claimed job %s, want the reaped job %s", again.Id, job.Id)
	}
	if again.RetryCount != 1 {
		t.Errorf("retry count after one stalled attempt = %d, want 1", again.RetryCount)
	}
	if again.Error != "lease expired" {
		t.Errorf("last error after a stall = %q, want %q", again.Error, "lease expired")
	}
}

// TestHeartbeatExtendsTheLease checks that a worker still working keeps its job
// through a sweep that would otherwise take it away.
func TestHeartbeatExtendsTheLease(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	if err := store.CreateJob(ctx, testJob("slow")); err != nil {
		t.Fatalf("create job: %v", err)
	}
	job, err := store.DequeueJob(ctx, "owner", -time.Second)
	if err != nil || job == nil {
		t.Fatalf("dequeue: job=%v err=%v", job, err)
	}

	renewed, err := store.HeartbeatJob(ctx, job.Id, "owner", time.Minute)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !renewed {
		t.Fatal("heartbeat from the lease holder was refused")
	}

	if released, err := store.ReapExpiredLeases(ctx); err != nil {
		t.Fatalf("reap: %v", err)
	} else if released != 0 {
		t.Errorf("sweep released %d jobs that were being kept alive, want 0", released)
	}
}

// TestFailRetriesUntilAttemptsAreExhausted covers the retry-vs-DLQ decision,
// which the server makes on the worker's behalf.
func TestFailRetriesUntilAttemptsAreExhausted(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	job := testJob("doomed")
	job.MaxRetries = 2
	if err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	// max_retries counts retries after the first attempt: two retries, and the
	// third failure is the one that gives up.
	for attempt := 1; attempt <= 2; attempt++ {
		claimed, err := store.DequeueJob(ctx, "worker", time.Minute)
		if err != nil {
			t.Fatalf("attempt %d: dequeue: %v", attempt, err)
		}
		if claimed == nil {
			t.Fatalf("attempt %d: nothing claimable", attempt)
		}

		retrying, err := store.FailJob(ctx, claimed.Id, "worker", "simulated", 0)
		if err != nil {
			t.Fatalf("attempt %d: fail: %v", attempt, err)
		}
		if !retrying {
			t.Errorf("attempt %d: retrying = false, want true", attempt)
		}
	}

	claimed, err := store.DequeueJob(ctx, "worker", time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("final attempt: job=%v err=%v", claimed, err)
	}

	retrying, err := store.FailJob(ctx, claimed.Id, "worker", "simulated", 0)
	if err != nil {
		t.Fatalf("final attempt: fail: %v", err)
	}
	if retrying {
		t.Error("final attempt: retrying = true, want false so the job lands in the DLQ")
	}

	if left, err := store.DequeueJob(ctx, "worker", time.Minute); err != nil {
		t.Fatalf("dequeue: %v", err)
	} else if left != nil {
		t.Errorf("dead-lettered job %s was claimable again", left.Id)
	}
}

// TestFailDelaysTheNextAttempt is the backoff half of a retry: the job stays
// pending, but it is not visible until its window passes.
func TestFailDelaysTheNextAttempt(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	if err := store.CreateJob(ctx, testJob("backoff")); err != nil {
		t.Fatalf("create job: %v", err)
	}
	job, err := store.DequeueJob(ctx, "worker", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("dequeue: job=%v err=%v", job, err)
	}

	if retrying, err := store.FailJob(ctx, job.Id, "worker", "boom", time.Hour); err != nil {
		t.Fatalf("fail: %v", err)
	} else if !retrying {
		t.Fatal("retrying = false, want the job to be queued for another attempt")
	}

	if waiting, err := store.DequeueJob(ctx, "worker", time.Minute); err != nil {
		t.Fatalf("dequeue: %v", err)
	} else if waiting != nil {
		t.Errorf("job %s was claimable inside its backoff window", waiting.Id)
	}

	var state string
	if err := store.db.QueryRow(`SELECT state FROM jobs WHERE id = $1`, job.Id).Scan(&state); err != nil {
		t.Fatalf("read state: %v", err)
	}
	if state != "pending" {
		t.Errorf("state during backoff = %q, want %q", state, "pending")
	}
}

// expectLeaseLost asserts that a fenced call refused to touch a job.
func expectLeaseLost(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, ErrLeaseLost) {
		t.Errorf("%s by a worker that does not hold the lease = %v, want ErrLeaseLost", what, err)
	}
}
