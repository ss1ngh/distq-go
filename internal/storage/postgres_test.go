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

	// Every test starts from an empty queue and a leaderless cluster so the
	// counts below mean something. leadership is restored to its seed state:
	// one row, no leader, term zero, lease long expired.
	const reset = `DELETE FROM jobs;
		DELETE FROM leadership;
		INSERT INTO leadership (singleton) VALUES (TRUE);`
	if _, err := store.db.Exec(reset); err != nil {
		t.Fatalf("reset state: %v", err)
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
				job, err := store.DequeueJob(ctx, 1, workerID, time.Minute)
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
	job, err := store.DequeueJob(ctx, 1, "owner", time.Minute)
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
	if again, err := store.DequeueJob(ctx, 1, "owner", time.Minute); err != nil {
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
	job, err := store.DequeueJob(ctx, 1, "ghost", -time.Second)
	if err != nil || job == nil {
		t.Fatalf("dequeue: job=%v err=%v", job, err)
	}

	released, err := store.ReapExpiredLeases(ctx, 1)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if released != 1 {
		t.Errorf("first sweep released %d jobs, want 1", released)
	}

	// Sweeping with the same term again releases nothing: the job is pending
	// now, not processing.
	if released, err = store.ReapExpiredLeases(ctx, 1); err != nil {
		t.Fatalf("reap: %v", err)
	} else if released != 0 {
		t.Errorf("second sweep released %d jobs, want 0", released)
	}

	expectLeaseLost(t, "complete", store.Complete(ctx, job.Id, "ghost"))

	again, err := store.DequeueJob(ctx, 1, "live", time.Minute)
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
	job, err := store.DequeueJob(ctx, 1, "owner", -time.Second)
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

	if released, err := store.ReapExpiredLeases(ctx, 1); err != nil {
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
		claimed, err := store.DequeueJob(ctx, 1, "worker", time.Minute)
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

	claimed, err := store.DequeueJob(ctx, 1, "worker", time.Minute)
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

	if left, err := store.DequeueJob(ctx, 1, "worker", time.Minute); err != nil {
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
	job, err := store.DequeueJob(ctx, 1, "worker", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("dequeue: job=%v err=%v", job, err)
	}

	if retrying, err := store.FailJob(ctx, job.Id, "worker", "boom", time.Hour); err != nil {
		t.Fatalf("fail: %v", err)
	} else if !retrying {
		t.Fatal("retrying = false, want the job to be queued for another attempt")
	}

	if waiting, err := store.DequeueJob(ctx, 1, "worker", time.Minute); err != nil {
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

// TestDeposedLeaderCannotClaimOrSweep is the split-brain fence at the claim
// layer: a leadership whose term is behind a job's claim_term must not be able
// to dispatch it or to sweep it back into the queue.
func TestDeposedLeaderCannotClaimOrSweep(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	// The new leader dispatches the job; its term is stamped onto the row.
	if err := store.CreateJob(ctx, testJob("fenced-term")); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if job, err := store.DequeueJob(ctx, 7, "worker", time.Minute); err != nil || job == nil {
		t.Fatalf("new leader claim: job=%v err=%v", job, err)
	}

	// A deposed leader — its term 6 is behind the row's 7 — campaigns after a
	// crash (there is no leadership state to check here; the store is the
	// last line of defence) and tries to dispatch the same job.
	if job, err := store.DequeueJob(ctx, 6, "zombie", time.Minute); err != nil {
		t.Fatalf("deposed claim: %v", err)
	} else if job != nil {
		t.Errorf("a deposed leader's claim landed on job %s", job.Id)
	}

	// Its sweep is fenced the same way: a stalled worker's job is left for the
	// current leadership to handle, not recovered by an old one.
	if _, err := store.db.Exec(`UPDATE jobs SET lease_expires_at = 'epoch'`); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	if released, err := store.ReapExpiredLeases(ctx, 6); err != nil {
		t.Fatalf("deposed sweep: %v", err)
	} else if released != 0 {
		t.Errorf("a deposed leader's sweep released %d job(s), want 0", released)
	}

	// The current leadership's sweep of the very same expired lease works.
	if released, err := store.ReapExpiredLeases(ctx, 7); err != nil {
		t.Fatalf("current sweep: %v", err)
	} else if released != 1 {
		t.Errorf("current leadership sweep released %d job(s), want 1", released)
	}
}

// TestClaimTermOnlyMovesUp pins the fence's direction: a claim by a newer
// leadership moves a job's claim_term up, and no path moves it down.
func TestClaimTermOnlyMovesUp(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	if err := store.CreateJob(ctx, testJob("term-up")); err != nil {
		t.Fatalf("create job: %v", err)
	}

	if _, err := store.DequeueJob(ctx, 5, "worker", time.Minute); err != nil {
		t.Fatalf("claim at term 5: %v", err)
	}
	var term int64
	if err := store.db.QueryRow(`SELECT claim_term FROM jobs WHERE id = $1`, "term-up").Scan(&term); err != nil {
		t.Fatalf("read claim_term: %v", err)
	}
	if term != 5 {
		t.Errorf("claim_term after a term-5 claim = %d, want 5", term)
	}

	// The job comes back through the retry path and is claimed again by an
	// older term: the stamp must not go backwards.
	if _, err := store.db.Exec(`UPDATE jobs SET state='pending', next_run_at='epoch', claim_term=99`); err != nil {
		t.Fatalf("simulate term-99 claim: %v", err)
	}
	if _, err := store.DequeueJob(ctx, 6, "worker", time.Minute); err != nil {
		t.Fatalf("claim at term 6 over term 99: %v", err)
	}
	if err := store.db.QueryRow(`SELECT claim_term FROM jobs WHERE id = $1`, "term-up").Scan(&term); err != nil {
		t.Fatalf("read claim_term: %v", err)
	}
	if term != 99 {
		t.Errorf("claim_term after a term-6 claim over a term-99 row = %d, want 99 (only up)", term)
	}
}

// expectLeaseLost asserts that a fenced call refused to touch a job.
func expectLeaseLost(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, ErrLeaseLost) {
		t.Errorf("%s by a worker that does not hold the lease = %v, want ErrLeaseLost", what, err)
	}
}

// TestCampaignElectsExactlyOneLeader is the election guarantee: nodes racing to
// take an expired lease must not all win, the term must advance once, and a
// node that never won must not be able to renew somebody else's lease.
func TestCampaignElectsExactlyOneLeader(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	const nodes = 6
	var (
		mu       sync.Mutex
		wins     int
		winnerID string
		wg       sync.WaitGroup
	)
	for i := range nodes {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			term, won, err := store.Campaign(ctx, id, time.Minute)
			if err != nil {
				t.Errorf("%s: campaign: %v", id, err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if !won {
				return
			}
			wins++
			winnerID = id
			if term != 1 {
				t.Errorf("first term = %d, want 1", term)
			}
		}(fmt.Sprintf("node-%d", i))
	}
	wg.Wait()

	if wins != 1 {
		t.Errorf("%d nodes won leadership, want exactly 1", wins)
	}

	if _, leader, err := store.RenewLeadership(ctx, "node-outside", time.Minute); err != nil || leader {
		t.Errorf("renew by a node that never won: leader=%v err=%v, want refused", leader, err)
	}
	if _, leader, err := store.RenewLeadership(ctx, winnerID, time.Minute); err != nil || !leader {
		t.Errorf("renew by the leader was refused: leader=%v err=%v", leader, err)
	}

	// A clean resignation frees the lease at once, and the next handover
	// advances the term so the two reigns are distinguishable.
	if err := store.ResignLeadership(ctx, winnerID); err != nil {
		t.Fatalf("resign: %v", err)
	}
	term, won, err := store.Campaign(ctx, "node-next", time.Minute)
	if err != nil || !won {
		t.Fatalf("campaign after resign: won=%v err=%v", won, err)
	}
	if term != 2 {
		t.Errorf("term after one handover = %d, want 2", term)
	}
}

// TestDeposedLeaderCannotResurrectItsLease is the split-brain primitive: a
// leader that stalls past its lease is not quietly made leader again by its own
// renewal. Getting back in means campaigning against everyone else.
func TestDeposedLeaderCannotResurrectItsLease(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	term, won, err := store.Campaign(ctx, "old-leader", time.Minute)
	if err != nil || !won {
		t.Fatalf("campaign: won=%v err=%v", won, err)
	}

	// The leader stalls past its lease — the state a long GC pause or a
	// network partition leaves behind.
	if _, err := store.db.Exec(`UPDATE leadership SET lease_expires_at = 'epoch'`); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	if _, leader, err := store.RenewLeadership(ctx, "old-leader", time.Minute); err != nil || leader {
		t.Errorf("renew after expiry: leader=%v err=%v, want refusal", leader, err)
	}

	// A live node takes over, and the term advances.
	newTerm, won, err := store.Campaign(ctx, "new-leader", time.Minute)
	if err != nil || !won {
		t.Fatalf("takeover campaign: won=%v err=%v", won, err)
	}
	if newTerm != term+1 {
		t.Errorf("term after takeover = %d, want %d", newTerm, term+1)
	}

	// The zombie's renewal is still refused: leader_id no longer matches.
	if _, leader, err := store.RenewLeadership(ctx, "old-leader", time.Minute); err != nil || leader {
		t.Errorf("zombie renew after takeover: leader=%v err=%v, want refusal", leader, err)
	}
}
