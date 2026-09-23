package server

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// These run against a real etcd and are skipped unless DISTQ_TEST_ETCD names
// one, the convention the storage tests use for Postgres, so `go test ./...`
// stays runnable without either. A lease is whole seconds and the assertions
// have to allow for one to lapse, so each test costs a few seconds.
const etcdTestLease = 2 * time.Second

// etcdTestEndpoints returns the etcd to test against, or skips.
func etcdTestEndpoints(t *testing.T) []string {
	t.Helper()

	raw := os.Getenv("DISTQ_TEST_ETCD")
	endpoints := make([]string, 0, 2)
	for _, endpoint := range strings.Split(raw, ",") {
		if endpoint = strings.TrimSpace(endpoint); endpoint != "" {
			endpoints = append(endpoints, endpoint)
		}
	}
	if len(endpoints) == 0 {
		t.Skip("DISTQ_TEST_ETCD is not set: skipping the etcd election tests")
	}
	return endpoints
}

// testElectionKey namespaces a test's election and clears anything a previous
// run left behind, so a stale leader key cannot make this one wait out a lease.
func testElectionKey(t *testing.T, endpoints []string) string {
	t.Helper()

	key := "/distq/test/" + t.Name()
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: dialTimeout})
	if err != nil {
		t.Fatalf("connect to etcd: %v", err)
	}
	defer client.Close()

	// Deadlined, because the client retries an unreachable endpoint rather than
	// failing: without it, a wrong DISTQ_TEST_ETCD would hang the run instead of
	// saying so.
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	if _, err := client.Delete(ctx, key, clientv3.WithPrefix()); err != nil {
		t.Fatalf("reach etcd at %s: %v", strings.Join(endpoints, ","), err)
	}
	return key
}

func newTestElection(t *testing.T, endpoints []string, key string) Election {
	t.Helper()

	elect, err := newEtcdElection(endpoints, key, etcdTestLease)
	if err != nil {
		t.Fatalf("newEtcdElection: %v", err)
	}
	t.Cleanup(func() { _ = elect.Close() })
	return elect
}

// campaignInBackground starts a candidate and reports the term it won on the
// channel, or -1 if it failed — so a test can tell "still queued" from "won".
func campaignInBackground(ctx context.Context, elect Election) <-chan int64 {
	won := make(chan int64, 1)
	go func() {
		term, err := elect.Campaign(ctx)
		if err != nil {
			won <- -1
			return
		}
		won <- term
	}()
	return won
}

// The whole point of an election: however many candidates there are, only one of
// them leads, and the next one is handed leadership when the first hands it back.
func TestEtcdElectsOneLeaderAtATime(t *testing.T) {
	endpoints := etcdTestEndpoints(t)
	key := testElectionKey(t, endpoints)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := newTestElection(t, endpoints, key)
	firstTerm, err := first.Campaign(ctx)
	if err != nil {
		t.Fatalf("first campaign: %v", err)
	}

	// A second candidate queues behind the incumbent's lease and is not told it
	// has won — because it has not.
	second := newTestElection(t, endpoints, key)
	won := campaignInBackground(ctx, second)
	select {
	case term := <-won:
		t.Fatalf("second candidate won term %d while the first still held the lease", term)
	case <-time.After(time.Second):
	}

	// Resigning hands over at once rather than making the cluster wait out the
	// lease, which is the difference between a clean restart and a crash.
	if err := first.Resign(ctx); err != nil {
		t.Fatalf("resign: %v", err)
	}
	select {
	case term := <-won:
		if term <= firstTerm {
			t.Errorf("handover term %d did not increase on the first term %d", term, firstTerm)
		}
	case <-time.After(etcdTestLease + 3*time.Second):
		t.Fatal("resigning did not hand leadership to the queued candidate")
	}
}

// A leader that dies holds the cluster's place open until its lease lapses —
// nothing tells etcd the node is gone. That wait is the failover latency a crash
// costs, and it is the lease that bounds it.
func TestEtcdReplacesALeaderThatDies(t *testing.T) {
	endpoints := etcdTestEndpoints(t)
	key := testElectionKey(t, endpoints)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := newTestElection(t, endpoints, key)
	firstTerm, err := first.Campaign(ctx)
	if err != nil {
		t.Fatalf("first campaign: %v", err)
	}

	second := newTestElection(t, endpoints, key)
	won := campaignInBackground(ctx, second)

	// The leader dies: no resign, so its session's keepalive stops and only the
	// lease's expiry ends the reign.
	if err := first.Close(); err != nil {
		t.Fatalf("close the first leader: %v", err)
	}

	select {
	case term := <-won:
		if term <= firstTerm {
			t.Errorf("takeover term %d did not increase on the dead leader's term %d", term, firstTerm)
		}
	case <-time.After(etcdTestLease + 5*time.Second):
		t.Fatal("a dead leader was never replaced")
	}
}

// What the election buys over polling: a leader whose session dies is told, by
// the closing of its own lease's channel, instead of finding out at its next
// renewal. This is the signal the leadership loop demotes on.
func TestEtcdTellsALeaderItWasDeposed(t *testing.T) {
	endpoints := etcdTestEndpoints(t)
	key := testElectionKey(t, endpoints)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader := newTestElection(t, endpoints, key)
	if _, err := leader.Campaign(ctx); err != nil {
		t.Fatalf("campaign: %v", err)
	}

	held := make(chan bool, 1)
	go func() { held <- leader.Hold(ctx) }()

	// The session dies under it — the node is partitioned from etcd, not shut
	// down. Hold must report the loss rather than the end of the context.
	if err := leader.Close(); err != nil {
		t.Fatalf("close the leader's connection: %v", err)
	}

	select {
	case lost := <-held:
		if !lost {
			t.Error("Hold reported a shutdown rather than lost leadership")
		}
	case <-time.After(etcdTestLease + 5*time.Second):
		t.Fatal("a dead session did not tell the leader it had been deposed")
	}
}
