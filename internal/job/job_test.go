package job

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeSurfra struct {
	*httptest.Server
	mu       sync.Mutex
	plan     map[string]any
	stop     string
	starts   int
	beats    int
	finished []map[string]any
	auth     string
}

func newFake(t *testing.T, plan map[string]any) *fakeSurfra {
	f := &fakeSurfra{plan: plan}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/v1/jobs/agent/start":
			f.starts++
			json.NewEncoder(w).Encode(f.plan)
		case "/v1/jobs/agent/heartbeat":
			f.beats++
			json.NewEncoder(w).Encode(map[string]any{"stop": f.stop, "tail": false})
		case "/v1/jobs/agent/finish":
			var got map[string]any
			json.Unmarshal(body, &got)
			f.finished = append(f.finished, got)
			w.Write([]byte(`{"status":"ok"}`))
		case "/v1/tokens":
			f.auth = r.Header.Get("Authorization")
			w.Write([]byte(`{"token":"aaa.bbb.ccc"}`))
		case "/v1/workers/logs":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.Close)
	return f
}

func planFor(command string, extra map[string]any) map[string]any {
	p := map[string]any{"run": true, "command": command, "grace_sec": 1, "heartbeat_sec": 0.2, "deadline_at": nil}
	for k, v := range extra {
		p[k] = v
	}
	return p
}

func runAgent(t *testing.T, f *fakeSurfra, env ...string) (int, string) {
	var out bytes.Buffer
	cfg := Config{
		Master:    f.URL,
		Token:     "sfp_test",
		LogDir:    t.TempDir(),
		Heartbeat: 200 * time.Millisecond,
		Env:       append([]string{"PATH=/usr/bin:/bin", "SURFRA_REPLICA_TOKEN=sfp_test", "SURFRA_VOLUMES_URL=" + f.URL}, env...),
		Stdout:    &out,
		Stderr:    &out,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return Run(ctx, cfg), out.String()
}

func TestExitCodeAndOutputReachSurfra(t *testing.T) {
	f := newFake(t, planFor("echo hello; echo oops >&2; exit 3", nil))
	code, out := runAgent(t, f)
	if code != 3 || !strings.Contains(out, "hello") || !strings.Contains(out, "oops") || !strings.Contains(out, "command exited with code 3") {
		t.Fatalf("code %d, output %q", code, out)
	}
	if len(f.finished) != 1 || f.finished[0]["exit_code"] != float64(3) || f.finished[0]["by"] != "" {
		t.Fatalf("finished %v", f.finished)
	}
}

func TestTheDeadlineStopsTheCommand(t *testing.T) {
	past := float64(time.Now().Unix() - 1)
	f := newFake(t, planFor("sleep 30", map[string]any{"deadline_at": past}))
	code, _ := runAgent(t, f)
	if code != 128+15 || f.finished[0]["by"] != "deadline" || f.finished[0]["signal"] != "SIGTERM" {
		t.Fatalf("code %d, finished %v", code, f.finished)
	}
}

func TestAStopFromSurfraEndsTheCommand(t *testing.T) {
	f := newFake(t, planFor("sleep 30", nil))
	f.stop = "user"
	code, _ := runAgent(t, f)
	if code != 128+15 || f.finished[0]["by"] != "control" {
		t.Fatalf("code %d, finished %v", code, f.finished)
	}
}

func TestSigkillFollowsWhenSigtermIsIgnored(t *testing.T) {
	f := newFake(t, planFor("trap '' TERM; sleep 30", nil))
	f.stop = "user"
	code, _ := runAgent(t, f)
	if code != 128+9 || f.finished[0]["signal"] != "SIGKILL" {
		t.Fatalf("code %d, finished %v", code, f.finished)
	}
}

func TestAJobThatAlreadyRanIsNotRunAgain(t *testing.T) {
	f := newFake(t, map[string]any{"run": false})
	code, out := runAgent(t, f)
	if code != 0 || !strings.Contains(out, "not run again") || len(f.finished) != 0 {
		t.Fatalf("code %d, output %q, finished %v", code, out, f.finished)
	}
}

func TestAnyCommandReachesVolumesWithoutTheReplicaToken(t *testing.T) {
	f := newFake(t, planFor(`cat "$SURFRA_VOLUMES_TOKEN_FILE"; echo; echo "url=$SURFRA_VOLUMES_URL"; echo "replica=${SURFRA_REPLICA_TOKEN:-none}"`, nil))
	code, out := runAgent(t, f)
	if code != 0 || !strings.Contains(out, "aaa.bbb.ccc") || !strings.Contains(out, "replica=none") || !strings.Contains(out, "url=http://127.0.0.1:") {
		t.Fatalf("code %d, output %q", code, out)
	}
	if f.auth != "Bearer sfp_test" {
		t.Fatalf("exchanged with %q", f.auth)
	}
}
