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

func TestServerAddrsParsesAClusterList(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want []string
	}{
		{name: "single", env: "queue.internal:4040", want: []string{"queue.internal:4040"}},
		{name: "cluster", env: "a:4040, b:4040 ,c:4040", want: []string{"a:4040", "b:4040", "c:4040"}},
		{name: "empty entries dropped", env: "a:4040,,b:4040,", want: []string{"a:4040", "b:4040"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(ServerAddrEnv, tc.env)
			got := ServerAddrs()
			if len(got) != len(tc.want) {
				t.Fatalf("ServerAddrs() = %q, want %q", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("ServerAddrs()[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}

	t.Setenv(ServerAddrEnv, "")
	if got := ServerAddrs(); len(got) != 1 || got[0] != defaultServerAddr {
		t.Errorf("unset %s: got %q, want [%q]", ServerAddrEnv, got, defaultServerAddr)
	}
}

// EtcdEndpoints is the switch between electing leadership in etcd and electing
// it in the database, so unset has to mean empty rather than some endpoint that
// was never configured — every cluster without etcd would otherwise dial one.
func TestEtcdEndpointsSelectTheElectionMechanism(t *testing.T) {
	t.Setenv(EtcdEndpointsEnv, "")
	if got := EtcdEndpoints(); len(got) != 0 {
		t.Errorf("unset %s: got %q, want none so the database elects", EtcdEndpointsEnv, got)
	}

	t.Setenv(EtcdEndpointsEnv, "http://etcd-1:2379, http://etcd-2:2379 ,")
	want := []string{"http://etcd-1:2379", "http://etcd-2:2379"}
	got := EtcdEndpoints()
	if len(got) != len(want) {
		t.Fatalf("EtcdEndpoints() = %q, want %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("EtcdEndpoints()[%d] = %q, want %q", i, got[i], want[i])
		}
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
