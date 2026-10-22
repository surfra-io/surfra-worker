// Package supervisor is the static container process.
// It starts the image command, waits until that process answers on localhost,
// then posts each queued request to it. The replica token stays in this process.
package supervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"surfra.io/worker/internal/logs"
	"surfra.io/worker/internal/split"
	"surfra.io/worker/internal/usage"
	"surfra.io/worker/internal/volumes"
)

var (
	errStopped = errors.New("stopped")
	// errGone means Surfra stopped this replica. The container waits to be shut down instead of exiting,
	// because a cloud restart policy would start it again with a token that no longer works.
	errGone = errors.New("replica stopped by Surfra")
)

const (
	stderrLimit  = 32 * 1024
	bodyLimit    = 8 << 20
	liveMisses   = 3
	liveTimeout  = 5 * time.Second
	planeGiveUp  = 5 * time.Minute
	backoffFirst = time.Second
	backoffMax   = 30 * time.Second
)

// Config is the contract the control plane injects. Users do not set these.
type Config struct {
	Master       string
	Token        string
	Entrypoint   string
	Port         int
	Path         string
	ReadyPath    string
	LivePath     string
	ReadyTimeout time.Duration
	MaxOngoing   int
	ExecTimeout  time.Duration
	PollInterval time.Duration
	PollWait     time.Duration
	LiveInterval time.Duration
	Env          []string
	ShipLogs     bool
	LogDir       string
	Usage        bool
}

// FromEnv reads the baked worker environment.
func FromEnv() (Config, error) {
	cfg := Config{
		Master:       strings.TrimRight(strings.TrimSpace(os.Getenv("SURFRA_MASTER")), "/"),
		Token:        strings.TrimSpace(os.Getenv("SURFRA_REPLICA_TOKEN")),
		Entrypoint:   strings.TrimSpace(os.Getenv("SURFRA_ENTRYPOINT")),
		Port:         envInt("SURFRA_HTTP_PORT", 8000),
		Path:         envPath("SURFRA_HTTP_PATH", "/"),
		ReadyPath:    envPath("SURFRA_HTTP_READY_PATH", "/health"),
		LivePath:     envPath("SURFRA_HTTP_LIVE_PATH", ""),
		ReadyTimeout: time.Duration(envInt("SURFRA_HTTP_READY_TIMEOUT", 600)) * time.Second,
		MaxOngoing:   envInt("SURFRA_MAX_ONGOING", 4),
		ExecTimeout:  time.Duration(envInt("SURFRA_EXECUTION_TIMEOUT", 1200)) * time.Second,
		PollInterval: 2 * time.Second,
		PollWait:     20 * time.Second,
		LiveInterval: 10 * time.Second,
		Env:          os.Environ(),
		ShipLogs:     strings.TrimSpace(os.Getenv("SURFRA_LOGS")) != "0",
		LogDir:       strings.TrimSpace(os.Getenv("SURFRA_LOG_DIR")),
		Usage:        !slices.Contains([]string{"0", "false", "no", "off"}, strings.ToLower(strings.TrimSpace(os.Getenv("SURFRA_USAGE")))),
	}
	if cfg.Master == "" || cfg.Token == "" || cfg.Entrypoint == "" {
		return Config{}, fmt.Errorf("this process is started with SURFRA_MASTER, SURFRA_REPLICA_TOKEN, and SURFRA_ENTRYPOINT")
	}
	runtime := strings.TrimSpace(os.Getenv("SURFRA_RUNTIME"))
	if runtime == "" {
		runtime = "http"
	}
	if runtime != "http" {
		return Config{}, fmt.Errorf("SURFRA_RUNTIME %q is not supported by this binary", runtime)
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return Config{}, fmt.Errorf("SURFRA_HTTP_PORT must be from 1 to 65535")
	}
	if cfg.LivePath == "" {
		cfg.LivePath = cfg.ReadyPath
	}
	if cfg.MaxOngoing < 1 {
		cfg.MaxOngoing = 1
	}
	if cfg.MaxOngoing > 64 {
		cfg.MaxOngoing = 64
	}
	if cfg.ReadyTimeout <= 0 {
		return Config{}, fmt.Errorf("SURFRA_HTTP_READY_TIMEOUT must be positive")
	}
	return cfg, nil
}

func envInt(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}

func envPath(name, fallback string) string {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		raw = fallback
	}
	if raw == "" {
		return ""
	}
	if !strings.HasPrefix(raw, "/") {
		raw = "/" + raw
	}
	return raw
}

// Run starts the child command and serves until the context is canceled or the child dies.
// A nil error means the process was asked to stop. Any other error is a boot or liveness failure.
func Run(ctx context.Context, cfg Config) error {
	s, err := newSupervisor(cfg)
	if err != nil {
		return err
	}
	defer s.stopLogs()
	stopVolumes := s.startVolumes(ctx)
	defer stopVolumes()
	if err := s.start(); err != nil {
		s.boot(err.Error())
		return err
	}
	defer s.stopChild()
	if err := s.waitReady(ctx); err != nil {
		if errors.Is(err, errStopped) {
			return nil
		}
		s.boot(err.Error())
		return err
	}
	if err := s.register(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, errStopped) {
			return nil
		}
		if errors.Is(err, errGone) {
			return s.idle(ctx)
		}
		s.boot("register: " + err.Error())
		return err
	}
	if s.cfg.Usage {
		metered, stopMeter := context.WithCancel(ctx)
		defer stopMeter()
		go usage.New().Loop(metered, s.client.usage)
	}
	if err := s.loop(ctx); err != nil {
		if errors.Is(err, errGone) {
			return s.idle(ctx)
		}
		s.boot(err.Error())
		return err
	}
	return nil
}

// register retries the control plane until it answers. Only a stopped replica or a refusal ends it.
func (s *supervisor) register(ctx context.Context) error {
	wait := backoffFirst
	deadline := time.Now().Add(planeGiveUp)
	for {
		err := s.client.register(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return context.Canceled
		}
		if gone(err) {
			return errGone
		}
		if !transient(err) || time.Now().After(deadline) {
			return err
		}
		s.note("control plane unavailable, retrying register: " + err.Error())
		if err := s.sleep(ctx, wait); err != nil {
			return err
		}
		wait = min(wait*2, backoffMax)
	}
}

// idle stops the app and waits for the container to be shut down.
func (s *supervisor) idle(ctx context.Context) error {
	s.stopChild()
	s.note("Surfra stopped this replica. Waiting for the container to be shut down.")
	<-ctx.Done()
	return nil
}

func (s *supervisor) sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return errStopped
	case <-s.exited:
		return fmt.Errorf("child exited: %s", s.tail.text())
	case <-time.After(d):
		return nil
	}
}

// planeError is a non-2xx answer from the control plane.
type planeError struct {
	status int
	text   string
}

func (e *planeError) Error() string {
	return fmt.Sprintf("%d %s %s", e.status, http.StatusText(e.status), e.text)
}

// transient errors are worth retrying: the network, timeouts, rate limits, and 5xx.
func transient(err error) bool {
	var pe *planeError
	if errors.As(err, &pe) {
		return pe.status >= 500 || pe.status == http.StatusTooManyRequests || pe.status == http.StatusRequestTimeout
	}
	return true
}

func gone(err error) bool {
	var pe *planeError
	return errors.As(err, &pe) && pe.status == http.StatusUnauthorized && strings.Contains(pe.text, "unknown replica token")
}

func dropped(err error) bool {
	var pe *planeError
	return errors.As(err, &pe) && pe.status == http.StatusUnauthorized && strings.Contains(pe.text, "worker is not registered")
}

type supervisor struct {
	cfg     Config
	argv    []string
	client  *plane
	cmd     *exec.Cmd
	tail    *tail
	exited  chan struct{}
	stop    sync.Once
	ship    *logs.Shipper
	streams []io.Writer
	endLogs func()
	running sync.Map
}

func newSupervisor(cfg Config) (*supervisor, error) {
	argv, err := split.Command(cfg.Entrypoint)
	if err != nil {
		return nil, err
	}
	s := &supervisor{
		cfg:    cfg,
		argv:   argv,
		client: &plane{master: cfg.Master, token: cfg.Token, http: &http.Client{Timeout: 30 * time.Second}},
		tail:   &tail{max: stderrLimit},
	}
	s.endLogs = func() {}
	if cfg.ShipLogs {
		s.ship = logs.New(logs.Config{
			Master: cfg.Master,
			Token:  cfg.Token,
			Dir:    cfg.LogDir,
			Active: s.activeRequests,
			Every:  60 * time.Second,
			Jitter: 30 * time.Second,
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			s.ship.Run(ctx)
			close(done)
		}()
		s.endLogs = func() {
			cancel()
			<-done
		}
	}
	return s, nil
}

func (s *supervisor) activeRequests() []string {
	var ids []string
	s.running.Range(func(key, _ any) bool {
		ids = append(ids, key.(string))
		return true
	})
	return ids
}

func (s *supervisor) note(message string) {
	if s.ship != nil {
		s.ship.Note(message)
	}
}

func (s *supervisor) stopLogs() {
	for _, w := range s.streams {
		if c, ok := w.(io.Closer); ok {
			c.Close()
		}
	}
	s.endLogs()
}

func (s *supervisor) start() error {
	s.cmd = exec.Command(s.argv[0], s.argv[1:]...)
	s.cmd.Env = childEnv(s.cfg.Env)
	s.cmd.Stdout = os.Stdout
	s.cmd.Stderr = io.MultiWriter(os.Stderr, s.tail)
	if s.ship != nil {
		s.cmd.Stdout = s.ship.Writer("out", os.Stdout)
		s.cmd.Stderr = s.ship.Writer("err", io.MultiWriter(os.Stderr, s.tail))
		s.streams = []io.Writer{s.cmd.Stdout, s.cmd.Stderr}
	}
	s.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	s.exited = make(chan struct{})
	if err := s.cmd.Start(); err != nil {
		close(s.exited)
		return fmt.Errorf("start %s: %w", s.argv[0], err)
	}
	s.note("started " + s.argv[0])
	go func() {
		_ = s.cmd.Wait()
		close(s.exited)
	}()
	return nil
}

func childEnv(base []string) []string {
	drop := map[string]bool{
		"SURFRA_REPLICA_TOKEN": true,
		"SURFRA_MASTER":        true,
		"SURFRA_ENTRYPOINT":    true,
	}
	out := make([]string, 0, len(base))
	for _, kv := range base {
		key, _, _ := strings.Cut(kv, "=")
		if drop[key] {
			continue
		}
		out = append(out, kv)
	}
	return out
}

func (s *supervisor) stopChild() {
	if s.exited == nil {
		return
	}
	s.stop.Do(func() {
		if s.cmd.Process != nil {
			_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGTERM)
		}
		select {
		case <-s.exited:
		case <-time.After(2 * time.Second):
			if s.cmd.Process != nil {
				_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
			}
			<-s.exited
		}
	})
}

func (s *supervisor) waitReady(ctx context.Context) error {
	deadline := time.Now().Add(s.cfg.ReadyTimeout)
	probe := &http.Client{Timeout: 2 * time.Second}
	url := s.local(s.cfg.ReadyPath)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	started := time.Now()
	for {
		if err := s.probe(probe, url); err == nil {
			s.note(fmt.Sprintf("ready after %s", time.Since(started).Round(time.Millisecond)))
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ready timeout after %s: %s", s.cfg.ReadyTimeout, s.tail.text())
		}
		select {
		case <-ctx.Done():
			return errStopped
		case <-s.exited:
			return fmt.Errorf("child exited before ready: %s", s.tail.text())
		case <-tick.C:
		}
	}
}

func (s *supervisor) probe(client *http.Client, url string) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func (s *supervisor) local(path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", s.cfg.Port, path)
}

func (s *supervisor) loop(ctx context.Context) error {
	inflight := map[string]*job{}
	var mu sync.Mutex
	misses := 0
	lastLive := time.Now()
	poll := time.NewTicker(s.cfg.PollInterval)
	defer poll.Stop()
	work, cancelWork := context.WithCancel(ctx)
	defer func() { cancelWork() }()
	failing := time.Time{}
	wait := backoffFirst
	full := false

	failRunning := func(reason string) {
		mu.Lock()
		pending := make([]*job, 0, len(inflight))
		ids := make([]string, 0, len(inflight))
		for id, item := range inflight {
			item.mu.Lock()
			if item.phase == "run" {
				item.phase = "report-fail"
				item.counts = true
				item.message = reason
			}
			item.mu.Unlock()
			pending = append(pending, item)
			ids = append(ids, id)
		}
		mu.Unlock()
		reportCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for i, item := range pending {
			s.report(reportCtx, ids[i], item)
		}
	}

	for {
		select {
		case <-ctx.Done():
			cancelWork()
			failRunning("worker stopped")
			return nil
		case <-s.exited:
			cancelWork()
			failRunning("child exited: " + s.tail.text())
			return fmt.Errorf("child exited: %s", s.tail.text())
		case <-poll.C:
			mu.Lock()
			for id, item := range inflight {
				item.mu.Lock()
				done := item.phase == "done"
				item.mu.Unlock()
				if done {
					delete(inflight, id)
				}
			}
			mu.Unlock()
			if time.Since(lastLive) >= s.cfg.LiveInterval {
				lastLive = time.Now()
				mu.Lock()
				busy := len(inflight) > 0
				mu.Unlock()
				err := s.probe(&http.Client{Timeout: liveTimeout}, s.local(s.cfg.LivePath))
				var netErr interface{ Timeout() bool }
				// A busy app can answer health checks slowly. Only refusals and errors count then.
				if err != nil && busy && errors.As(err, &netErr) && netErr.Timeout() {
					err = nil
				}
				if err != nil {
					misses++
					if misses >= liveMisses {
						cancelWork()
						s.stopChild()
						failRunning("liveness failed: " + err.Error())
						return fmt.Errorf("liveness failed: %w", err)
					}
				} else {
					misses = 0
				}
			}
			// An idle poll waits on the server for work. A child exit ends the wait early.
			pollCtx, cancelPoll := context.WithCancel(ctx)
			go func() {
				select {
				case <-s.exited:
					cancelPoll()
				case <-pollCtx.Done():
				}
			}()
			stages, err := s.client.next(pollCtx, s.cfg.MaxOngoing, s.cfg.PollWait, !full)
			cancelPoll()
			select {
			case <-s.exited:
				cancelWork()
				failRunning("child exited: " + s.tail.text())
				return fmt.Errorf("child exited: %s", s.tail.text())
			default:
			}
			if err != nil && ctx.Err() != nil {
				cancelWork()
				failRunning("worker stopped")
				return nil
			}
			if err != nil && (gone(err) || dropped(err)) {
				// Surfra already requeued what this worker held, so drop it and register again.
				cancelWork()
				mu.Lock()
				inflight = map[string]*job{}
				mu.Unlock()
				if err := s.register(ctx); err != nil {
					if errors.Is(err, errStopped) || errors.Is(err, context.Canceled) {
						return nil
					}
					return err
				}
				s.note("registered again as " + s.client.worker)
				work, cancelWork = context.WithCancel(ctx)
				continue
			}
			if err != nil && transient(err) {
				if failing.IsZero() {
					failing = time.Now()
				}
				if time.Since(failing) > planeGiveUp {
					cancelWork()
					failRunning(err.Error())
					return err
				}
				s.note("control plane unavailable, retrying: " + err.Error())
				if err := s.sleep(ctx, wait); err != nil {
					if errors.Is(err, errStopped) {
						cancelWork()
						failRunning("worker stopped")
						return nil
					}
					cancelWork()
					failRunning(err.Error())
					return err
				}
				wait = min(wait*2, backoffMax)
				continue
			}
			if err != nil {
				cancelWork()
				failRunning(err.Error())
				return err
			}
			failing = time.Time{}
			wait = backoffFirst
			var retry []stage
			full = false
			mu.Lock()
			for _, stage := range stages {
				item := inflight[stage.ID]
				if item == nil && stage.Held {
					// Held without an input we know. The next poll asks for the full stage.
					full = true
					continue
				}
				if item == nil {
					item = &job{phase: "run"}
					inflight[stage.ID] = item
					go s.dispatch(work, stage, item)
					continue
				}
				item.mu.Lock()
				done := item.phase == "done"
				item.mu.Unlock()
				if done {
					delete(inflight, stage.ID)
					continue
				}
				retry = append(retry, stage)
			}
			mu.Unlock()
			for _, stage := range retry {
				mu.Lock()
				item := inflight[stage.ID]
				mu.Unlock()
				if item == nil {
					continue
				}
				s.report(ctx, stage.ID, item)
			}
		}
	}
}

func (s *supervisor) dispatch(ctx context.Context, stage stage, item *job) {
	if stage.TransactionID != "" {
		s.running.Store(stage.TransactionID, struct{}{})
		defer s.running.Delete(stage.TransactionID)
	}
	output, code, explicit, err := s.post(ctx, stage.Input, stage.TransactionID)
	item.mu.Lock()
	if item.phase != "run" {
		item.mu.Unlock()
		return
	}
	if err == nil && (code < 400 || (explicit && code < 500)) {
		item.phase = "report-ok"
		item.output = output
	} else {
		item.phase = "report-fail"
		item.counts = code >= 500 || code == 0
		item.message = failMessage(code, err, output)
	}
	item.mu.Unlock()
	s.report(ctx, stage.ID, item)
}

func failMessage(code int, err error, body []byte) string {
	if err != nil {
		return err.Error()
	}
	text := strings.TrimSpace(string(body))
	if len(text) > 1024 {
		text = text[:1024]
	}
	if text == "" {
		return fmt.Sprintf("upstream status %d", code)
	}
	return fmt.Sprintf("upstream status %d: %s", code, text)
}

func (s *supervisor) post(ctx context.Context, raw json.RawMessage, requestID string) ([]byte, int, bool, error) {
	if s.cfg.ExecTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.ExecTimeout)
		defer cancel()
	}
	method, path, query, body, explicit, header := prepareCall(raw, s.cfg.Path)
	target := s.local(path)
	if query != "" {
		target += "?" + query
	}
	var reader io.Reader
	if len(body) > 0 && method != http.MethodGet && method != http.MethodHead {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return nil, 0, explicit, err
	}
	if header != "" {
		req.Header.Set("Content-Type", header)
	} else if reader != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if requestID != "" {
		req.Header.Set("X-Surfra-Request-Id", requestID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, explicit, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, bodyLimit+1))
	if err != nil {
		return nil, resp.StatusCode, explicit, err
	}
	if len(payload) > bodyLimit {
		return nil, resp.StatusCode, explicit, fmt.Errorf("upstream body exceeds %d bytes", bodyLimit)
	}
	if explicit && resp.StatusCode < 500 {
		return httpEnvelope(resp.StatusCode, resp.Header.Get("Content-Type"), payload), resp.StatusCode, true, nil
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return normalize(payload), resp.StatusCode, false, nil
	}
	return payload, resp.StatusCode, false, nil
}

func prepareCall(raw json.RawMessage, fallback string) (method, path, query string, body []byte, explicit bool, contentType string) {
	method = http.MethodPost
	path = fallback
	body = stripUpstream(raw)
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return method, path, "", body, false, ""
	}
	specRaw, ok := obj["_surfra"]
	if !ok {
		return method, path, "", body, false, ""
	}
	var spec struct {
		Method  string            `json:"method"`
		Path    string            `json:"path"`
		Query   string            `json:"query"`
		Headers map[string]string `json:"headers"`
	}
	if err := json.Unmarshal(specRaw, &spec); err != nil {
		return method, path, "", body, false, ""
	}
	cleaned, err := cleanUpstreamPath(spec.Path)
	if err != nil {
		return method, path, "", body, false, ""
	}
	if spec.Method != "" {
		method = strings.ToUpper(spec.Method)
	}
	delete(obj, "_surfra")
	delete(obj, "upstream")
	if nested, ok := obj["_body"]; ok {
		body = append([]byte(nil), nested...)
	} else if method == http.MethodGet || method == http.MethodHead || len(obj) == 0 {
		body = nil
	} else {
		encoded, err := json.Marshal(obj)
		if err != nil {
			body = stripUpstream(raw)
		} else {
			body = encoded
		}
	}
	for key, value := range spec.Headers {
		if strings.EqualFold(key, "content-type") && value != "" {
			contentType = value
		}
	}
	return method, cleaned, spec.Query, body, true, contentType
}

func cleanUpstreamPath(path string) (string, error) {
	if path == "" || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || strings.Contains(path, "..") || strings.Contains(path, "\\") {
		return "", fmt.Errorf("refusing path %q", path)
	}
	return path, nil
}

func httpEnvelope(code int, contentType string, payload []byte) []byte {
	body := json.RawMessage("null")
	trimmed := bytes.TrimSpace(payload)
	if json.Valid(trimmed) && len(trimmed) > 0 {
		body = trimmed
	} else if len(trimmed) > 0 {
		encoded, err := json.Marshal(string(trimmed))
		if err == nil {
			body = encoded
		}
	}
	wrapped, err := json.Marshal(map[string]any{
		"_http": map[string]any{"status": code, "content_type": contentType},
		"body":  body,
	})
	if err != nil {
		return normalize(payload)
	}
	return wrapped
}

func normalize(payload []byte) []byte {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return []byte("null")
	}
	if json.Valid(trimmed) {
		return trimmed
	}
	encoded, err := json.Marshal(string(trimmed))
	if err != nil {
		return []byte("null")
	}
	return encoded
}

func stripUpstream(raw json.RawMessage) []byte {
	if len(bytes.TrimSpace(raw)) == 0 {
		return []byte("null")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}
	delete(obj, "upstream")
	out, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return out
}

func (s *supervisor) report(ctx context.Context, id string, item *job) {
	item.mu.Lock()
	defer item.mu.Unlock()
	if item.phase != "report-ok" && item.phase != "report-fail" {
		return
	}
	var err error
	if item.phase == "report-ok" {
		err = s.client.complete(ctx, id, item.output)
	} else {
		err = s.client.fail(ctx, id, item.message, item.counts)
	}
	if err == nil {
		item.phase = "done"
	}
}

func (s *supervisor) boot(message string) {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "boot failed"
	}
	s.note(message)
	if len(message) > stderrLimit {
		message = message[len(message)-stderrLimit:]
	}
	call, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.client.boot(call, message); err != nil {
		fmt.Fprintf(os.Stderr, "boot report failed: %s\n", err)
	}
}

type job struct {
	mu      sync.Mutex
	phase   string
	output  []byte
	counts  bool
	message string
}

type stage struct {
	ID            string          `json:"id"`
	TransactionID string          `json:"transaction_id"`
	Input         json.RawMessage `json:"input"`
	Held          bool            `json:"held"`
}

type plane struct {
	master string
	token  string
	worker string
	http   *http.Client
}

func (p *plane) register(ctx context.Context) error {
	var out struct {
		WorkerID string `json:"worker_id"`
	}
	err := p.call(ctx, "/v1/workers/register", map[string]any{
		"replica_token":      p.token,
		"concurrent_workers": 1,
	}, &out)
	if err != nil {
		return err
	}
	if out.WorkerID == "" {
		return fmt.Errorf("register returned no worker id")
	}
	p.worker = out.WorkerID
	return nil
}

// next heartbeats and claims. wait lets an idle poll sit on the server until work
// arrives. With ids, stages this worker already holds come back without inputs.
func (p *plane) next(ctx context.Context, limit int, wait time.Duration, ids bool) ([]stage, error) {
	var out struct {
		Stages []stage `json:"stages"`
	}
	body := map[string]any{
		"replica_token": p.token,
		"worker_id":     p.worker,
		"limit":         limit,
		"wait":          wait.Seconds(),
	}
	if ids {
		body["held"] = "ids"
	}
	err := p.call(ctx, "/v1/workers/next", body, &out)
	return out.Stages, err
}

func (p *plane) complete(ctx context.Context, stageID string, output []byte) error {
	body := map[string]any{
		"replica_token": p.token,
		"worker_id":     p.worker,
		"stage_id":      stageID,
		"output":        json.RawMessage(output),
	}
	return p.call(ctx, "/v1/workers/complete", body, nil)
}

func (p *plane) fail(ctx context.Context, stageID, message string, counts bool) error {
	return p.call(ctx, "/v1/workers/fail", map[string]any{
		"replica_token": p.token,
		"worker_id":     p.worker,
		"stage_id":      stageID,
		"error":         message,
		"counts":        counts,
	}, nil)
}

func (p *plane) boot(ctx context.Context, message string) error {
	return p.call(ctx, "/v1/workers/boot", map[string]any{
		"replica_token": p.token,
		"message":       message,
	}, nil)
}

// usage reports a minute of utilization. A failed report is dropped; the next one replaces it.
func (p *plane) usage(ctx context.Context, sample usage.Aggregate) {
	_ = p.call(ctx, "/v1/workers/usage", map[string]any{"replica_token": p.token, "sample": sample}, nil)
}

func (p *plane) call(ctx context.Context, path string, body any, dest any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.master+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// A claim can carry up to limit inputs of 1 MB each.
	data, err := io.ReadAll(io.LimitReader(resp.Body, 80<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		text := strings.TrimSpace(string(data))
		if len(text) > 300 {
			text = text[:300]
		}
		return &planeError{status: resp.StatusCode, text: text}
	}
	if dest != nil && len(bytes.TrimSpace(data)) > 0 {
		return json.Unmarshal(data, dest)
	}
	return nil
}

type tail struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (t *tail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.max {
		t.buf = append([]byte(nil), t.buf[len(t.buf)-t.max:]...)
	}
	return len(p), nil
}

func (t *tail) text() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.TrimSpace(string(t.buf))
}

// startVolumes gives the app volumes access without the replica token: a loopback proxy at
// SURFRA_VOLUMES_URL that adds a volumes-only token, and the token in SURFRA_VOLUMES_TOKEN_FILE.
func (s *supervisor) startVolumes(ctx context.Context) func() {
	tokens := volumes.New(s.cfg.Master, s.cfg.Token, s.cfg.Env)
	first, cancelFirst := context.WithTimeout(ctx, 10*time.Second)
	_ = tokens.Refresh(first)
	cancelFirst()
	running, stop := context.WithCancel(ctx)
	if err := tokens.Serve(running); err != nil {
		s.note("volumes proxy did not start: " + err.Error())
	}
	s.cfg.Env = tokens.Env(s.cfg.Env)
	go tokens.Loop(running)
	return stop
}
