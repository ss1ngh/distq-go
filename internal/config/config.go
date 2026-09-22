// Package config collects the handful of settings distq takes from the
// environment, so the same binaries run against a local database and a deployed
// one without a rebuild — and so that no credential is ever written into the
// source tree.
package config

import (
	"fmt"
	"os"
	"strings"
)

// Environment variables the distq commands read.
const (
	// PostgresDSNEnv names the DSN of the database jobs are persisted in.
	PostgresDSNEnv = "DISTQ_PG_DSN"
	// ListenAddrEnv names the address the queue server serves on.
	ListenAddrEnv = "DISTQ_LISTEN_ADDR"
	// ServerAddrEnv names the address the client commands dial.
	ServerAddrEnv = "DISTQ_SERVER_ADDR"
)

// Defaults for the two addresses. Neither is a secret, so both are safe to
// hardcode, and they keep the local commands runnable with no setup.
const (
	defaultListenAddr = ":4040"
	defaultServerAddr = "localhost:4040"
)

// PostgresDSN returns the DSN of the jobs database. Unlike the addresses there
// is no default, because a DSN normally carries a password: it has to come from
// the environment rather than from this repository.
func PostgresDSN() (string, error) {
	dsn := os.Getenv(PostgresDSNEnv)
	if dsn == "" {
		return "", fmt.Errorf("%s is not set: it must name the PostgreSQL database jobs are persisted in, e.g. postgres://user:pass@localhost:5432/distq?sslmode=disable", PostgresDSNEnv)
	}
	return dsn, nil
}

// ListenAddr returns the address the queue server serves on.
func ListenAddr() string { return envOr(ListenAddrEnv, defaultListenAddr) }

// ServerAddr returns the address the worker, producer and fenceprobe dial to
// reach the queue server: the first of ServerAddrs, for the clients that only
// ever talk to one.
func ServerAddr() string { return ServerAddrs()[0] }

// ServerAddrs returns the addresses a queue server can be reached on,
// most-preferred first. A comma-separated DISTQ_SERVER_ADDR names a cluster:
// the worker works down the list until it finds the leader.
func ServerAddrs() []string {
	parts := strings.Split(envOr(ServerAddrEnv, defaultServerAddr), ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		if addr := strings.TrimSpace(part); addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
