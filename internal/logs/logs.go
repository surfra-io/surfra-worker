// Package logs ships the app's stdout and stderr to the control plane.
//
// Each line becomes one JSON record with a UTC timestamp, the stream, and the
// request it belongs to when that is known. Records are appended to a file on
// local disk and sent in gzip batches every 60 to 90 seconds, or sooner when a
// batch fills. Unsent batches wait on disk, capped at a share of the disk, so a
// control plane outage loses the oldest lines first instead of filling the disk.
package logs

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	batchBytes   = 8 << 20
	lineBytes    = 16 << 10
	minDiskBytes = 16 << 20
	diskShare    = 0.05
)

// Record is one line. The control plane reads the same field names.
type Record struct {
	Time    string `json:"t"`
	Stream  string `json:"s"`
	Message string `json:"m"`
	Request string `json:"r,omitempty"`
}

// Config is where batches go and how often.
type Config struct {
	Master string
	Token  string
	Dir    string
	// Active returns the request ids running right now.
	Active func() []string
	Every  time.Duration
	Jitter time.Duration
	Client *http.Client
}

// Shipper buffers records on disk and posts them in batches.
type Shipper struct {
	cfg     Config
	mu      sync.Mutex
	file    *os.File
	size    int64
	capped  int64
	dropped int
	kick    chan struct{}
	off     bool
}

// New prepares the buffer directory. A shipper that cannot write to disk still
// echoes to the container output, so the app never blocks on logging.
func New(cfg Config) *Shipper {
	if cfg.Dir == "" {
		cfg.Dir = filepath.Join(os.TempDir(), "surfra-logs")
	}
	if cfg.Every <= 0 {
		cfg.Every = 60 * time.Second
	}
	if cfg.Jitter < 0 {
		cfg.Jitter = 0
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 60 * time.Second}
	}
	if cfg.Active == nil {
		cfg.Active = func() []string { return nil }
	}
	s := &Shipper{cfg: cfg, kick: make(chan struct{}, 1)}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		s.off = true
		return s
	}
	s.capped = diskCap(cfg.Dir)
	if err := s.open(); err != nil {
		s.off = true
	}
	return s
}

func diskCap(dir string) int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return minDiskBytes
	}
	total := int64(st.Blocks) * int64(st.Bsize)
	limit := int64(float64(total) * diskShare)
	if limit < minDiskBytes {
		return minDiskBytes
	}
	return limit
}

func (s *Shipper) open() error {
	f, err := os.OpenFile(filepath.Join(s.cfg.Dir, "pending.ndjson"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	s.file = f
	s.size = info.Size()
	return nil
}

// Writer returns an io.Writer for one stream. Partial lines wait for their newline.
func (s *Shipper) Writer(stream string, echo io.Writer) io.Writer {
	return &lineWriter{ship: s, stream: stream, echo: echo}
}

// Note records a line from the supervisor itself.
func (s *Shipper) Note(message string) {
	s.add("sys", message)
}

func (s *Shipper) add(stream, message string) {
	message = strings.TrimRight(message, "\r")
	if message == "" {
		return
	}
	rec := Record{Time: time.Now().UTC().Format(time.RFC3339Nano), Stream: stream, Message: message, Request: s.requestFor(message)}
	line, err := json.Marshal(rec)
	if err != nil {
		return
	}
	line = append(line, '\n')
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.off || s.file == nil {
		return
	}
	if _, err := s.file.Write(line); err != nil {
		return
	}
	s.size += int64(len(line))
	if s.size >= batchBytes {
		s.rotateLocked()
		select {
		case s.kick <- struct{}{}:
		default:
		}
	}
}

// requestFor tags a line with the only running request, or with the running
// request whose id appears in the line. Otherwise the line has no request.
func (s *Shipper) requestFor(message string) string {
	active := s.cfg.Active()
	if len(active) == 1 {
		return active[0]
	}
	for _, id := range active {
		if id != "" && strings.Contains(message, id) {
			return id
		}
	}
	return ""
}

func (s *Shipper) rotateLocked() {
	if s.file == nil || s.size == 0 {
		return
	}
	s.file.Close()
	name := filepath.Join(s.cfg.Dir, fmt.Sprintf("batch-%020d.ndjson", time.Now().UnixNano()))
	_ = os.Rename(filepath.Join(s.cfg.Dir, "pending.ndjson"), name)
	s.file = nil
	s.size = 0
	if err := s.open(); err != nil {
		s.off = true
	}
	s.trimLocked()
}

// trimLocked drops the oldest unsent batches once the buffer passes its cap.
func (s *Shipper) trimLocked() {
	batches := s.batches()
	var total int64
	sizes := make([]int64, len(batches))
	for i, name := range batches {
		if info, err := os.Stat(name); err == nil {
			sizes[i] = info.Size()
			total += info.Size()
		}
	}
	for i := 0; i < len(batches) && total > s.capped; i++ {
		if os.Remove(batches[i]) == nil {
			total -= sizes[i]
			s.dropped++
		}
	}
}

func (s *Shipper) batches() []string {
	names, _ := filepath.Glob(filepath.Join(s.cfg.Dir, "batch-*.ndjson"))
	sort.Strings(names)
	return names
}

// Run flushes on a timer until ctx ends, then makes one last attempt.
func (s *Shipper) Run(ctx context.Context) {
	if s.off {
		<-ctx.Done()
		return
	}
	for {
		wait := s.cfg.Every
		if s.cfg.Jitter > 0 {
			wait += time.Duration(rand.Int63n(int64(s.cfg.Jitter)))
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			final, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			s.Flush(final)
			cancel()
			return
		case <-s.kick:
			timer.Stop()
		case <-timer.C:
		}
		s.Flush(ctx)
	}
}

// Kick sends what is buffered now instead of at the next interval.
func (s *Shipper) Kick() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// Flush sends every batch on disk, oldest first, and stops at the first failure.
func (s *Shipper) Flush(ctx context.Context) {
	s.mu.Lock()
	s.rotateLocked()
	dropped := s.dropped
	s.dropped = 0
	s.mu.Unlock()
	if dropped > 0 {
		s.Note(fmt.Sprintf("dropped %d log batches because the control plane was unreachable and the buffer was full", dropped))
	}
	for _, name := range s.batches() {
		if err := s.send(ctx, name); err != nil {
			return
		}
		_ = os.Remove(name)
	}
}

func (s *Shipper) send(ctx context.Context, name string) error {
	raw, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var body bytes.Buffer
	zw := gzip.NewWriter(&body)
	if _, err := zw.Write(raw); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.Master+"/v1/workers/logs", &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("X-Surfra-Replica-Token", s.cfg.Token)
	resp, err := s.cfg.Client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("logs upload: %s", resp.Status)
	}
	return nil
}

type lineWriter struct {
	ship   *Shipper
	stream string
	echo   io.Writer
	mu     sync.Mutex
	buf    []byte
}

func (w *lineWriter) Write(p []byte) (int, error) {
	if w.echo != nil {
		w.echo.Write(p)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.emit(w.buf[:i])
		w.buf = w.buf[i+1:]
	}
	for len(w.buf) > lineBytes {
		w.emit(w.buf[:lineBytes])
		w.buf = w.buf[lineBytes:]
	}
	if len(w.buf) == 0 {
		w.buf = nil
	}
	return len(p), nil
}

func (w *lineWriter) emit(line []byte) {
	w.ship.add(w.stream, string(line))
}

// Close sends a trailing line that never got its newline.
func (w *lineWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buf) > 0 {
		w.emit(w.buf)
		w.buf = nil
	}
	return nil
}
