package supervisor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestVolumeTokenIsWrittenPrivatelyFromTheReplicaToken(t *testing.T) {
	var auth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tokens" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		auth = r.Header.Get("Authorization")
		w.Write([]byte(`{"token":"aaa.bbb.ccc","expires_at":1}`))
	}))
	defer server.Close()

	v := newVolumeTokens(Config{Master: server.URL, Token: "sfp_secret"})
	v.path = filepath.Join(t.TempDir(), "surfra", "volume-token")
	if err := v.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if auth != "Bearer sfp_secret" {
		t.Fatalf("exchanged with %q", auth)
	}
	data, err := os.ReadFile(v.path)
	if err != nil || string(data) != "aaa.bbb.ccc" {
		t.Fatalf("token file: %q %v", data, err)
	}
	info, _ := os.Stat(v.path)
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("token file mode %v", info.Mode().Perm())
	}
}

func TestARefusedExchangeLeavesNoFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusUnauthorized)
	}))
	defer server.Close()
	v := newVolumeTokens(Config{Master: server.URL, Token: "sfp_gone"})
	v.path = filepath.Join(t.TempDir(), "volume-token")
	if err := v.refresh(context.Background()); err == nil {
		t.Fatal("a 401 was accepted")
	}
	if _, err := os.Stat(v.path); !os.IsNotExist(err) {
		t.Fatal("a file was written for a refused exchange")
	}
}

func TestTheAppEnvironmentNamesTheFileAndTheServiceButNotTheReplicaToken(t *testing.T) {
	v := &volumeTokens{path: "/tmp/surfra/volume-token-1"}
	env := childEnv(v.env([]string{"PATH=/bin", "SURFRA_REPLICA_TOKEN=sfp_secret", "SURFRA_VOLUMES_TOKEN_FILE=/elsewhere"}))
	if !slices.Contains(env, "SURFRA_VOLUMES_TOKEN_FILE=/tmp/surfra/volume-token-1") || !slices.Contains(env, "SURFRA_VOLUMES_URL=https://volumes.surfra.io") {
		t.Fatalf("env %v", env)
	}
	for _, kv := range env {
		if kv == "SURFRA_REPLICA_TOKEN=sfp_secret" || kv == "SURFRA_VOLUMES_TOKEN_FILE=/elsewhere" {
			t.Fatalf("leaked %q", kv)
		}
	}
	custom := v.env([]string{"SURFRA_VOLUMES_URL=http://127.0.0.1:8799/"})
	if !slices.Contains(custom, "SURFRA_VOLUMES_URL=http://127.0.0.1:8799") {
		t.Fatalf("custom url lost: %v", custom)
	}
}
