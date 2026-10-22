package volumes

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

	v := New(server.URL, "sfp_secret", nil)
	v.path = filepath.Join(t.TempDir(), "surfra", "volume-token")
	if err := v.Refresh(context.Background()); err != nil {
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
	v := New(server.URL, "sfp_gone", nil)
	v.path = filepath.Join(t.TempDir(), "volume-token")
	if err := v.Refresh(context.Background()); err == nil {
		t.Fatal("a 401 was accepted")
	}
	if _, err := os.Stat(v.path); !os.IsNotExist(err) {
		t.Fatal("a file was written for a refused exchange")
	}
}

func TestTheAppEnvironmentNamesTheFileAndTheService(t *testing.T) {
	v := &Tokens{path: "/tmp/surfra/volume-token-1", upstream: defaultVolumes}
	env := v.Env([]string{"PATH=/bin", "SURFRA_VOLUMES_TOKEN_FILE=/elsewhere"})
	if !slices.Contains(env, "SURFRA_VOLUMES_TOKEN_FILE=/tmp/surfra/volume-token-1") || !slices.Contains(env, "SURFRA_VOLUMES_URL=https://volumes.surfra.io") {
		t.Fatalf("env %v", env)
	}
	for _, kv := range env {
		if kv == "SURFRA_VOLUMES_TOKEN_FILE=/elsewhere" {
			t.Fatalf("leaked %q", kv)
		}
	}
	custom := New("", "", []string{"SURFRA_VOLUMES_URL=http://127.0.0.1:8799/"})
	if custom.upstream != "http://127.0.0.1:8799" {
		t.Fatalf("custom url lost: %v", custom.upstream)
	}
}

func TestTheLocalProxyAddsTheTokenSoPlainRequestsWork(t *testing.T) {
	master := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"token":"aaa.bbb.ccc"}`))
	}))
	defer master.Close()
	var seen []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path+" "+r.Header.Get("Authorization"))
		if r.URL.Path == "/v1/weights/files/denied" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Write([]byte("file body"))
	}))
	defer upstream.Close()

	v := New(master.URL, "sfp_secret", []string{"SURFRA_VOLUMES_URL=" + upstream.URL})
	v.path = filepath.Join(t.TempDir(), "volume-token")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := v.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err := v.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(v.Env(nil), "SURFRA_VOLUMES_URL="+v.local) {
		t.Fatalf("env does not point at the proxy: %v", v.Env(nil))
	}

	resp, err := http.Get(v.local + "/v1/weights/files/config.json")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "file body" || seen[0] != "GET /v1/weights/files/config.json Bearer aaa.bbb.ccc" {
		t.Fatalf("got %q, upstream saw %v", body, seen)
	}

	req, _ := http.NewRequest(http.MethodPut, v.local+"/v1/weights/files/x", strings.NewReader("data"))
	req.Header.Set("Authorization", "Bearer sfk_own")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if seen[1] != "PUT /v1/weights/files/x Bearer sfk_own" {
		t.Fatalf("an explicit credential was replaced: %v", seen)
	}

	resp, _ = http.Get(v.local + "/v1/weights/files/denied")
	resp.Body.Close()
	select {
	case <-v.wake:
	default:
		t.Fatal("a 401 did not ask for a fresh token")
	}
}
