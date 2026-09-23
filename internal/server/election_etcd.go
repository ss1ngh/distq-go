package server

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	// electionKey is the etcd key the cluster elects on: one key, every candidate
	// queued behind whoever holds it.
	electionKey = "/distq/leadership"
	// dialTimeout bounds the initial connection to etcd.
	dialTimeout = 5 * time.Second
	// campaignRetry is how long Campaign waits before trying again after etcd
	// refused a session or an election outright.
	campaignRetry = 2 * time.Second
)

// etcdElection elects by leasing a key in etcd — the production tool this phase
// was meant to be measured against. Three of the differences from the Postgres
// pass are in kind rather than degree:
//
//   - Winning is a blocking call, not a poll. A candidate queues behind the
//     incumbent and etcd hands it leadership the moment the incumbent's lease
//     lapses, so there is no campaign cadence to tune and no "is it mine yet?"
//     question to ask repeatedly.
//   - Losing is announced. A background keepalive holds the lease alive and the
//     session's Done channel closes when that fails, so a partitioned node is
//     told it has been deposed instead of discovering it at its next renewal.
//   - The fencing term is free. etcd numbers every write with a cluster-wide
//     revision, and an election reports the revision its leader key was created
//     at, so a reign has a monotonic token without the Postgres pass's counter
//     column — nothing to increment, and nothing to trust.
//
// What it does not buy is a cheaper answer to the same question. It is another
// service to run, and its availability becomes the cluster's: with etcd down
// nobody campaigns, so leadership is frozen where it stands rather than
// re-decided. The Postgres pass costs only the database this project already
// needs.
type etcdElection struct {
	client *clientv3.Client
	nodeID string
	key    string
	ttl    int

	// session and elect belong to the reign in progress, and are replaced on
	// every campaign: a session is single-use, and a node that has been deposed
	// has to start clean rather than reuse a lease etcd may have revoked.
	session *concurrency.Session
	elect   *concurrency.Election
}

// NewEtcdElection elects leadership through etcd. The lease duration becomes the
// session TTL: a node that stops renewing it drops out of the election after at
// most that long, which is also how long the cluster keeps a dead leader's place
// open before the next candidate is handed it.
func NewEtcdElection(endpoints []string, lease time.Duration) (Election, error) {
	return newEtcdElection(endpoints, electionKey, lease)
}

func newEtcdElection(endpoints []string, key string, lease time.Duration) (Election, error) {
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: dialTimeout})
	if err != nil {
		return nil, fmt.Errorf("connect to etcd at %s: %w", strings.Join(endpoints, ","), err)
	}

	// A session TTL is whole seconds, and zero would ask for a lease that never
	// expires — the opposite of what a lease is for.
	ttl := int(lease.Seconds())
	if ttl < 1 {
		ttl = 1
	}
	return &etcdElection{client: client, nodeID: newNodeID(), key: key, ttl: ttl}, nil
}

func (e *etcdElection) Name() string { return e.nodeID }

// Campaign joins the election and blocks until it is won — etcd places this node
// in line behind the incumbent's lease and returns when the key becomes this
// node's, which is why there is nothing to poll here. Only outright failures are
// retried: an unreachable etcd is not a reason to die, since nothing here holds
// state.
func (e *etcdElection) Campaign(ctx context.Context) (int64, error) {
	for {
		term, err := e.campaignOnce(ctx)
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if err == nil {
			return term, nil
		}
		fmt.Fprintf(os.Stderr, "[Leadership] etcd campaign failed, retrying: %v\n", err)

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(campaignRetry):
		}
	}
}

// campaignOnce runs a single campaign: a fresh session, a fresh election, and a
// blocking wait for the key. It returns the revision the leader key is created
// at, which is this reign's fencing term.
func (e *etcdElection) campaignOnce(ctx context.Context) (int64, error) {
	e.closeSession()

	// etcd's client retries an unreachable endpoint instead of failing, so a
	// cluster that is down would be waited on in silence. Ask for something
	// cheap under a deadline first, so that Campaign reports it and comes back
	// on its own schedule instead.
	ping, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	if _, err := e.client.Get(ping, e.key, clientv3.WithCountOnly()); err != nil {
		return 0, fmt.Errorf("etcd unreachable: %w", err)
	}

	session, err := concurrency.NewSession(e.client, concurrency.WithTTL(e.ttl), concurrency.WithContext(ctx))
	if err != nil {
		return 0, fmt.Errorf("etcd session: %w", err)
	}

	elect := concurrency.NewElection(session, e.key)
	if err := elect.Campaign(ctx, e.nodeID); err != nil {
		_ = session.Close()
		return 0, fmt.Errorf("etcd campaign: %w", err)
	}

	e.session, e.elect = session, elect
	return elect.Rev(), nil
}

// Hold waits for the session holding the lease to end, which is exactly when
// this node stops leading: the lease is what keeps the leader key alive, so
// nothing else can take the election while it holds, and the moment it lapses
// some other candidate is handed the key.
func (e *etcdElection) Hold(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-e.session.Done():
		return true
	}
}

// Resign deletes the leader key, which lets the next candidate in line win at
// once, and closes the session so its keepalive stops.
func (e *etcdElection) Resign(ctx context.Context) error {
	if e.elect == nil {
		return nil // never led
	}
	err := e.elect.Resign(ctx)
	e.closeSession()
	return err
}

func (e *etcdElection) Close() error { return e.client.Close() }

// closeSession drops the current session, revoking its lease and stopping its
// keepalive.
func (e *etcdElection) closeSession() {
	if e.session == nil {
		return
	}
	if err := e.session.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "[Leadership] closing etcd session: %v\n", err)
	}
	e.session, e.elect = nil, nil
}
