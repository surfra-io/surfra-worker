// Package job is the job agent: a job's container command is `surfra job agent`.
//
// The agent asks Surfra for the command, runs it in its own process group, and reads its output all
// the time, so the command never waits on Surfra. Lines go to the container's output and to a capped
// spool on disk that is sent in the background. Heartbeats tell Surfra the job is alive. If Surfra
// cannot be reached, the command keeps running and the agent keeps trying. The command ends on its
// own, on a stop from Surfra, at its deadline, or when the container is told to stop. The agent
// reports the exit code and exits with it.
//
// This is the same agent as the Python package's, so a job image needs only this binary.
package job

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"surfra.io/worker/internal/logs"
	"surfra.io/worker/internal/volumes"
)

const finishRetry = time.Hour

// Config is what the control plane gives the container.
type Config struct {
	Master    string
	Token     string
	LogDir    string
	Heartbeat time.Duration // zero: use the plan's
	Env       []string
	Stdout    io.Writer
	Stderr    io.Writer
}

// FromEnv reads the job container's environment.
func FromEnv() (Config, error) {
	cfg := Config{
		Master: strings.TrimRight(strings.TrimSpace(os.Getenv("SURFRA_MASTER")), "/"),
		Token:  strings.TrimSpace(os.Getenv("SURFRA_REPLICA_TOKEN")),
		LogDir: strings.TrimSpace(os.Getenv("SURFRA_LOG_DIR")),
		Env:    os.Environ(),
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	if raw := strings.TrimSpace(os.Getenv("SURFRA_JOB_HEARTBEAT")); raw != "" {
		if seconds, err := strconv.ParseFloat(raw, 64); err == nil && seconds > 0 {
			cfg.Heartbeat = time.Duration(seconds * float64(time.Second))
		}
	}
	var missing []string
	if cfg.Master == "" {
		missing = append(missing, "SURFRA_MASTER")
	}
	if cfg.Token == "" {
		missing = append(missing, "SURFRA_REPLICA_TOKEN")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("surfra job agent runs inside a job's container. Missing %s", strings.Join(missing, ", "))
	}
	return cfg, nil
}

type plan struct {
	Run          bool     `json:"run"`
	Command      string   `json:"command"`
	GraceSec     float64  `json:"grace_sec"`
	HeartbeatSec float64  `json:"heartbeat_sec"`
	DeadlineAt   *float64 `json:"deadline_at"`
}

type agent struct {
	cfg     Config
	http    *http.Client
	ship    *logs.Shipper
	attempt string
	grace   time.Duration

	mu          sync.Mutex
	cmd         *exec.Cmd
	exited      chan struct{}
	stopping    bool
	by          string
	unreachable bool
	disowned    bool
}

// Run runs the job and returns the command's exit code.
func Run(ctx context.Context, cfg Config) int {
	if cfg.Stdout == nil {
		cfg.Stdout = os.Stdout
	}
	if cfg.Stderr == nil {
		cfg.Stderr = os.Stderr
	}
	id := make([]byte, 16)
	_, _ = rand.Read(id)
	a := &agent{
		cfg:     cfg,
		http:    &http.Client{Timeout: 30 * time.Second},
		attempt: hex.EncodeToString(id),
		grace:   30 * time.Second,
	}
	a.ship = logs.New(logs.Config{Master: cfg.Master, Token: cfg.Token, Dir: cfg.LogDir, Every: 20 * time.Second, Jitter: 10 * time.Second})
	shipping, stopShipping := context.WithCancel(context.Background())
	shipped := make(chan struct{})
	go func() {
		a.ship.Run(shipping)
		close(shipped)
	}()
	endLogs := func() {
		stopShipping()
		<-shipped
	}
	code := a.run(ctx)
	endLogs()
	return code
}

func (a *agent) say(message string) {
	fmt.Fprintf(a.cfg.Stdout, "[surfra] %s\n", message)
	a.ship.Note(message)
}

func (a *agent) post(ctx context.Context, path string, body any) (int, []byte, error) {
	if body == nil {
		body = map[string]any{}
	}
	payload, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.Master+path, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Surfra-Replica-Token", a.cfg.Token)
	resp, err := a.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, data, err
}

func (a *agent) plan(ctx context.Context) (plan, error) {
	delay := 2 * time.Second
	for tries := 1; ; tries++ {
		problem := ""
		status, data, err := a.post(ctx, "/v1/jobs/agent/start", map[string]string{"attempt": a.attempt})
		switch {
		case err != nil:
			problem = err.Error()
		case status == http.StatusUnauthorized:
			return plan{}, nil
		case status < 300:
			var p plan
			if err := json.Unmarshal(data, &p); err != nil {
				return plan{}, err
			}
			return p, nil
		default:
			problem = fmt.Sprintf("HTTP %d", status)
		}
		if tries == 1 || tries == 5 || tries%20 == 0 {
			a.say(fmt.Sprintf("can't reach Surfra at %s yet (%s). Retrying.", a.cfg.Master, problem))
		}
		select {
		case <-ctx.Done():
			return plan{}, ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 30*time.Second)
	}
}

func (a *agent) run(ctx context.Context) int {
	a.say("job agent starting")
	p, err := a.plan(ctx)
	if err != nil || !p.Run {
		if err == nil {
			a.say("this job already ran or was stopped, so the command is not run again")
		}
		return 0
	}
	if p.GraceSec > 0 {
		a.grace = time.Duration(p.GraceSec * float64(time.Second))
	}
	every := a.cfg.Heartbeat
	if every <= 0 {
		every = 15 * time.Second
		if p.HeartbeatSec > 0 {
			every = time.Duration(p.HeartbeatSec * float64(time.Second))
		}
	}

	// The command gets volumes through a loopback proxy and a volumes-only token, never the replica token.
	tokens := volumes.New(a.cfg.Master, a.cfg.Token, a.cfg.Env)
	first, cancelFirst := context.WithTimeout(context.Background(), 10*time.Second)
	_ = tokens.Refresh(first)
	cancelFirst()
	volumesCtx, stopVolumes := context.WithCancel(context.Background())
	defer stopVolumes()
	_ = tokens.Serve(volumesCtx)
	go tokens.Loop(volumesCtx)

	shell := "/bin/sh"
	if _, err := os.Stat("/bin/bash"); err == nil {
		shell = "/bin/bash"
	}
	a.say("running: " + p.Command)
	began := time.Now()
	cmd := exec.Command(shell, "-c", p.Command)
	cmd.Env = tokens.Env(withoutReplicaToken(a.cfg.Env))
	cmd.Stdin = nil
	stdout := a.ship.Writer("out", a.cfg.Stdout)
	stderr := a.ship.Writer("err", a.cfg.Stderr)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		a.say("could not start the command: " + err.Error())
		a.finish(127, "")
		return 127
	}
	exited := make(chan struct{})
	a.mu.Lock()
	a.cmd, a.exited = cmd, exited
	a.mu.Unlock()
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	beating, stopBeating := context.WithCancel(context.Background())
	defer stopBeating()
	go a.beat(beating, every)

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
wait:
	for {
		select {
		case <-exited:
			break wait
		case <-signals:
			a.terminate("container")
		case <-ctx.Done():
			a.terminate("container")
		case <-tick.C:
			if p.DeadlineAt != nil && float64(time.Now().UnixNano())/1e9 > *p.DeadlineAt {
				a.terminate("deadline")
			}
		}
	}
	for _, w := range []io.Writer{stdout, stderr} {
		if c, ok := w.(io.Closer); ok {
			c.Close()
		}
	}
	code, signalName := exitCode(cmd.ProcessState)
	line := fmt.Sprintf("command exited with code %d", code)
	if signalName != "" {
		line += " (" + signalName + ")"
	}
	a.say(fmt.Sprintf("%s after %.1fs", line, time.Since(began).Seconds()))
	flushing, cancelFlush := context.WithTimeout(context.Background(), 30*time.Second)
	a.ship.Flush(flushing)
	cancelFlush()
	a.finish(code, signalName)
	return code
}

func exitCode(state *os.ProcessState) (int, string) {
	if state == nil {
		return 1, ""
	}
	if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		sig := status.Signal()
		return 128 + int(sig), signalName(sig)
	}
	return state.ExitCode(), ""
}

func signalName(sig syscall.Signal) string {
	names := map[syscall.Signal]string{syscall.SIGTERM: "SIGTERM", syscall.SIGKILL: "SIGKILL", syscall.SIGINT: "SIGINT", syscall.SIGHUP: "SIGHUP", syscall.SIGSEGV: "SIGSEGV", syscall.SIGABRT: "SIGABRT"}
	if name, ok := names[sig]; ok {
		return name
	}
	return fmt.Sprintf("signal %d", int(sig))
}

func withoutReplicaToken(env []string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		if key, _, _ := strings.Cut(kv, "="); key == "SURFRA_REPLICA_TOKEN" {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// terminate sends SIGTERM to the command's process group, then SIGKILL after the grace period.
func (a *agent) terminate(by string) {
	a.mu.Lock()
	if a.stopping || a.cmd == nil || a.cmd.Process == nil {
		a.mu.Unlock()
		return
	}
	select {
	case <-a.exited:
		a.mu.Unlock()
		return
	default:
	}
	a.stopping = true
	a.by = by
	pid, exited := a.cmd.Process.Pid, a.exited
	a.mu.Unlock()
	why := map[string]string{"deadline": "it reached its deadline", "control": "Surfra asked it to stop", "container": "the container is stopping"}[by]
	if why == "" {
		why = by
	}
	a.say(fmt.Sprintf("stopping the command because %s: SIGTERM now, SIGKILL in %.0fs", why, a.grace.Seconds()))
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	go func() {
		select {
		case <-exited:
		case <-time.After(a.grace):
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
	}()
}

func (a *agent) beat(ctx context.Context, every time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		status, data, err := a.post(ctx, "/v1/jobs/agent/heartbeat", nil)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if !a.unreachable {
				a.unreachable = true
				a.say("cannot reach Surfra. The command keeps running.")
			}
			continue
		}
		if a.unreachable {
			a.unreachable = false
			a.say("reached Surfra again")
		}
		switch {
		case status == http.StatusUnauthorized:
			// Only a stop or the liveness timeout ends a job, never a refused heartbeat.
			if !a.disowned {
				a.disowned = true
				a.say("Surfra did not accept a heartbeat. The command keeps running.")
			}
		case status < 300:
			a.disowned = false
			var answer struct {
				Stop string `json:"stop"`
				Tail bool   `json:"tail"`
			}
			_ = json.Unmarshal(data, &answer)
			if answer.Stop == "deadline" {
				a.terminate("deadline")
			} else if answer.Stop != "" {
				a.terminate("control")
			}
			if answer.Tail {
				a.ship.Kick()
			}
		}
	}
}

func (a *agent) finish(code int, signalName string) {
	body := map[string]any{"exit_code": code, "signal": signalName, "by": a.by}
	giveUp := time.Now().Add(finishRetry)
	delay := 2 * time.Second
	for time.Now().Before(giveUp) {
		status, _, err := a.post(context.Background(), "/v1/jobs/agent/finish", body)
		if err == nil && (status < 300 || status == 400 || status == 401 || status == 404) {
			return
		}
		time.Sleep(delay)
		delay = min(delay*2, 60*time.Second)
	}
	fmt.Fprintln(a.cfg.Stdout, "[surfra] could not report the exit code to Surfra")
}
