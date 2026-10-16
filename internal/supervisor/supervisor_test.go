package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("SURFRA_TEST_CHILD") == "1" {
		serveFixture()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func serveFixture() {
	if os.Getenv("FIXTURE_MODE") == "die" {
		fmt.Fprintln(os.Stderr, "fixture died")
		os.Exit(1)
	}
	port := os.Getenv("SURFRA_HTTP_PORT")
	leaked := os.Getenv("SURFRA_REPLICA_TOKEN") != "" || os.Getenv("SURFRA_MASTER") != "" || os.Getenv("SURFRA_ENTRYPOINT") != ""
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		if leaked {
			http.Error(w, "secret leaked", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/live", func(w http.ResponseWriter, _ *http.Request) {
		if os.Getenv("FIXTURE_MODE") == "sick" {
			http.Error(w, "sick", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if path := os.Getenv("FIXTURE_POSTS"); path != "" {
			f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			if err == nil {
				_, _ = f.Write([]byte("x\n"))
				_ = f.Close()
			}
		}
		switch os.Getenv("FIXTURE_MODE") {
		case "400":
			http.Error(w, "nope", http.StatusBadRequest)
		case "500":
			http.Error(w, "boom", http.StatusInternalServerError)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
		}
	})
	_ = http.ListenAndServe("127.0.0.1:"+port, mux)
}

func TestPrepareCallUsesTheRequestedPath(t *testing.T) {
	raw := json.RawMessage(`{"_surfra":{"method":"GET","path":"/v1/models","query":"limit=1"}}`)
	method, path, query, body, explicit, _ := prepareCall(raw, "/v1/chat/completions")
	if !explicit || method != http.MethodGet || path != "/v1/models" || query != "limit=1" || body != nil {
		t.Fatalf("got %s %s ?%s body=%s explicit=%v", method, path, query, body, explicit)
	}
}

func TestPrepareCallKeepsBakedPostsPlain(t *testing.T) {
	raw := json.RawMessage(`{"model":"qwen","upstream":{"x":1}}`)
	method, path, _, body, explicit, _ := prepareCall(raw, "/v1/chat/completions")
	if explicit || method != http.MethodPost || path != "/v1/chat/completions" {
		t.Fatalf("plain post changed: %s %s explicit=%v", method, path, explicit)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["upstream"]; ok {
		t.Fatal("upstream leaked into the container body")
	}
	if got["model"] != "qwen" {
		t.Fatalf("body = %#v", got)
	}
}

func TestPrepareCallRefusesParentPaths(t *testing.T) {
	raw := json.RawMessage(`{"_surfra":{"method":"GET","path":"/v1/../secret"}}`)
	_, _, _, _, explicit, _ := prepareCall(raw, "/")
	if explicit {
		t.Fatal("parent path was accepted")
	}
}

func TestStripUpstream(t *testing.T) {
	got := stripUpstream(json.RawMessage(`{"prompt":"hi","upstream":{"a":1}}`))
	var body map[string]any
	if err := json.Unmarshal(got, &body); err != nil {
		t.Fatal(err)
	}
	if body["prompt"] != "hi" {
		t.Fatalf("prompt = %#v", body["prompt"])
	}
	if _, ok := body["upstream"]; ok {
		t.Fatalf("upstream leaked: %#v", body)
	}
}

func TestFromEnvRequiresBakedValues(t *testing.T) {
	t.Setenv("SURFRA_MASTER", "")
	t.Setenv("SURFRA_REPLICA_TOKEN", "")
	t.Setenv("SURFRA_ENTRYPOINT", "")
	if _, err := FromEnv(); err == nil {
		t.Fatal("expected missing env to fail")
	}
}

func TestHTTPRoundTrip(t *testing.T) {
	master := newFakeMaster(t)
	defer master.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, master.cfg("ok"))
	}()

	select {
	case body := <-master.completed:
		var got map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if got["prompt"] != "hi" {
			t.Fatalf("body = %#v", got)
		}
		if _, ok := got["upstream"]; ok {
			t.Fatal("upstream was posted to the child")
		}
	case err := <-done:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a completion")
	}
	time.Sleep(100 * time.Millisecond)
	logged, err := os.ReadFile(master.postLog)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(logged), "\n") != 1 {
		t.Fatalf("child posts = %q", logged)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	master.mu.Lock()
	defer master.mu.Unlock()
	if len(master.boots) != 0 {
		t.Fatalf("unexpected boot reports: %#v", master.boots)
	}
}

func TestClientErrorDoesNotCount(t *testing.T) {
	master := newFakeMaster(t)
	defer master.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, master.cfg("400")) }()
	select {
	case fail := <-master.failed:
		if fail.Counts {
			t.Fatal("a 4xx response counted as a worker failure")
		}
		if fail.Error == "" {
			t.Fatal("missing error")
		}
	case err := <-done:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestServerErrorCounts(t *testing.T) {
	master := newFakeMaster(t)
	defer master.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, master.cfg("500")) }()
	select {
	case fail := <-master.failed:
		if !fail.Counts {
			t.Fatal("a 5xx response did not count")
		}
	case err := <-done:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestChildExitBeforeReadyReportsBoot(t *testing.T) {
	master := newFakeMaster(t)
	defer master.Close()
	cfg := master.cfg("die")
	cfg.ReadyTimeout = time.Second
	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected the child exit to fail the supervisor")
	}
	select {
	case message := <-master.booted:
		if message == "" {
			t.Fatal("empty boot message")
		}
	default:
		t.Fatal("boot was not reported")
	}
}

func TestLivenessMissesStopTheWorker(t *testing.T) {
	master := newFakeMaster(t)
	defer master.Close()
	cfg := master.cfg("sick")
	cfg.LiveInterval = 30 * time.Millisecond
	cfg.PollInterval = 20 * time.Millisecond
	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected liveness failure")
	}
	select {
	case <-master.booted:
	case <-time.After(2 * time.Second):
		t.Fatal("boot was not reported")
	}
}

type failCall struct {
	Error  string
	Counts bool
}

type fakeMaster struct {
	*httptest.Server
	mu        sync.Mutex
	open      bool
	completed chan []byte
	failed    chan failCall
	booted    chan string
	boots     []string
	bin       string
	postLog   string
	nextFails int
	dropOnce  bool
	gone      bool
	registers int
}

func newFakeMaster(t *testing.T) *fakeMaster {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeMaster{
		completed: make(chan []byte, 4),
		failed:    make(chan failCall, 4),
		booted:    make(chan string, 4),
		open:      true,
		bin:       bin,
		postLog:   filepath.Join(t.TempDir(), "posts"),
	}
	fake.Server = httptest.NewServer(http.HandlerFunc(fake.serve))
	return fake
}

func (f *fakeMaster) cfg(mode string) Config {
	port := freePort()
	return Config{
		Master:       f.URL,
		Token:        "sfp_secret",
		Entrypoint:   f.bin,
		Port:         port,
		Path:         "/",
		ReadyPath:    "/health",
		LivePath:     "/live",
		ReadyTimeout: 3 * time.Second,
		MaxOngoing:   4,
		ExecTimeout:  2 * time.Second,
		PollInterval: 20 * time.Millisecond,
		LiveInterval: time.Hour,
		Env: []string{
			"SURFRA_TEST_CHILD=1",
			"FIXTURE_MODE=" + mode,
			fmt.Sprintf("SURFRA_HTTP_PORT=%d", port),
			"SURFRA_REPLICA_TOKEN=sfp_secret",
			"SURFRA_MASTER=" + f.URL,
			"SURFRA_ENTRYPOINT=" + f.bin,
			"FIXTURE_POSTS=" + f.postLog,
		},
	}
}

func freePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func (f *fakeMaster) serve(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	switch r.URL.Path {
	case "/v1/workers/register":
		f.mu.Lock()
		f.registers++
		gone := f.gone
		f.mu.Unlock()
		if gone {
			http.Error(w, `{"detail":"unknown replica token"}`, http.StatusUnauthorized)
			return
		}
		writeJSON(w, map[string]any{"worker_id": "sfw_test"})
	case "/v1/workers/next":
		f.mu.Lock()
		open := f.open
		fail := f.nextFails > 0
		if fail {
			f.nextFails--
		}
		drop := f.dropOnce
		f.dropOnce = false
		gone := f.gone
		f.mu.Unlock()
		if fail {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		if drop {
			http.Error(w, `{"detail":"worker is not registered on this replica"}`, http.StatusUnauthorized)
			return
		}
		if gone {
			http.Error(w, `{"detail":"unknown replica token"}`, http.StatusUnauthorized)
			return
		}
		if !open {
			writeJSON(w, map[string]any{"stages": []any{}})
			return
		}
		writeJSON(w, map[string]any{"stages": []any{map[string]any{
			"id":    "stage-1",
			"input": map[string]any{"prompt": "hi", "upstream": map[string]any{"x": 1}},
		}}})
	case "/v1/workers/complete":
		f.mu.Lock()
		f.open = false
		f.mu.Unlock()
		raw, _ := json.Marshal(body["output"])
		f.completed <- raw
		writeJSON(w, map[string]any{"ok": true})
	case "/v1/workers/fail":
		f.mu.Lock()
		f.open = false
		f.mu.Unlock()
		counts, _ := body["counts"].(bool)
		f.failed <- failCall{Error: fmt.Sprint(body["error"]), Counts: counts}
		writeJSON(w, map[string]any{"ok": true})
	case "/v1/workers/boot":
		message := fmt.Sprint(body["message"])
		f.mu.Lock()
		f.boots = append(f.boots, message)
		f.mu.Unlock()
		f.booted <- message
		writeJSON(w, map[string]any{"ok": true})
	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func TestControlPlaneErrorsAreRetried(t *testing.T) {
	master := newFakeMaster(t)
	defer master.Close()
	master.nextFails = 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, master.cfg("echo")) }()
	select {
	case <-master.completed:
	case err := <-done:
		t.Fatalf("worker exited on a 500 from the control plane: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("request never completed after the control plane recovered")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("expected a clean stop, got %v", err)
	}
	if len(master.boots) != 0 {
		t.Fatalf("a transient error was reported as a boot failure: %v", master.boots)
	}
}

func TestDroppedWorkerRegistersAgain(t *testing.T) {
	master := newFakeMaster(t)
	defer master.Close()
	master.dropOnce = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, master.cfg("echo")) }()
	select {
	case <-master.completed:
	case err := <-done:
		t.Fatalf("worker exited instead of registering again: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("request never completed")
	}
	cancel()
	<-done
	master.mu.Lock()
	defer master.mu.Unlock()
	if master.registers != 2 {
		t.Fatalf("registers = %d, want 2", master.registers)
	}
}

func TestStoppedReplicaWaitsQuietly(t *testing.T) {
	master := newFakeMaster(t)
	defer master.Close()
	master.gone = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, master.cfg("echo")) }()
	select {
	case err := <-done:
		t.Fatalf("worker exited, so a restart policy would start it again: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil on shutdown, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop on shutdown")
	}
	if len(master.boots) != 0 {
		t.Fatalf("a stopped replica reported a boot failure: %v", master.boots)
	}
}
