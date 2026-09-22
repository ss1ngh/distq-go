package config

import "testing"

// The point of this package is that no DSN is compiled in, so an unset variable
// must be an error rather than a silent fallback to some developer's database.
func TestPostgresDSNIsRequired(t *testing.T) {
	t.Setenv(PostgresDSNEnv, "")

	if _, err := PostgresDSN(); err == nil {
		t.Fatal("PostgresDSN() accepted an unset environment variable")
	}
}

func TestPostgresDSNComesFromTheEnvironment(t *testing.T) {
	const want = "postgres://user:secret@db:5432/distq"
	t.Setenv(PostgresDSNEnv, want)

	got, err := PostgresDSN()
	if err != nil {
		t.Fatalf("PostgresDSN() error: %v", err)
	}
	if got != want {
		t.Errorf("PostgresDSN() = %q, want %q", got, want)
	}
}

func TestAddresses(t *testing.T) {
	tests := []struct {
		name       string
		env        string
		value      string
		get        func() string
		wantUnset  string
		wantSetEnv string
	}{
		{
			name:       "listen",
			env:        ListenAddrEnv,
			value:      "0.0.0.0:9000",
			get:        ListenAddr,
			wantUnset:  defaultListenAddr,
			wantSetEnv: "0.0.0.0:9000",
		},
		{
			name:       "server",
			env:        ServerAddrEnv,
			value:      "queue.internal:9000",
			get:        ServerAddr,
			wantUnset:  defaultServerAddr,
			wantSetEnv: "queue.internal:9000",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.env, "")
			if got := tc.get(); got != tc.wantUnset {
				t.Errorf("unset %s: got %q, want %q", tc.env, got, tc.wantUnset)
			}

			t.Setenv(tc.env, tc.value)
			if got := tc.get(); got != tc.wantSetEnv {
				t.Errorf("set %s: got %q, want %q", tc.env, got, tc.wantSetEnv)
			}
		})
	}
}
