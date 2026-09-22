// Package config collects the handful of settings distq takes from the
// environment, so the same binaries run against a local database and a deployed
// one without a rebuild — and so that no credential is ever written into the
// source tree.
package config

import (
	"fmt"
	"os"
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
// reach the queue server.
func ServerAddr() string { return envOr(ServerAddrEnv, defaultServerAddr) }

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
