package logs

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type sink struct {
	mu      sync.Mutex
	records []Record
	fail    bool
	tokens  []string
}

func (k *sink) handler(w http.ResponseWriter, r *http.Request) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.fail {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	k.tokens = append(k.tokens, r.Header.Get("X-Surfra-Replica-Token"))
	zr, err := gzip.NewReader(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	scan := bufio.NewScanner(zr)
	for scan.Scan() {
		var rec Record
		if json.Unmarshal(scan.Bytes(), &rec) == nil {
			k.records = append(k.records, rec)
		}
	}
	w.WriteHeader(http.StatusOK)
}

func setup(t *testing.T, active func() []string) (*Shipper, *sink) {
	t.Helper()
	k := &sink{}
	server := httptest.NewServer(http.HandlerFunc(k.handler))
	t.Cleanup(server.Close)
	s := New(Config{Master: server.URL, Token: "rtok", Dir: t.TempDir(), Active: active})
	return s, k
}

func TestLinesAreSplitTimestampedAndShipped(t *testing.T) {
	s, k := setup(t, nil)
	out := s.Writer("out", nil)
	out.Write([]byte("first line\nsecond "))
	out.Write([]byte("half\n"))
	s.Writer("err", nil).Write([]byte("boom\n"))
	s.Flush(context.Background())
	if len(k.records) != 3 {
		t.Fatalf("records = %+v", k.records)
	}
	if k.records[1].Message != "second half" || k.records[2].Stream != "err" {
		t.Fatalf("records = %+v", k.records)
	}
	if _, err := time.Parse(time.RFC3339Nano, k.records[0].Time); err != nil {
		t.Fatalf("time %q: %v", k.records[0].Time, err)
	}
	if k.tokens[0] != "rtok" {
		t.Fatalf("token header = %q", k.tokens[0])
	}
	if left, _ := filepath.Glob(filepath.Join(s.cfg.Dir, "batch-*")); len(left) != 0 {
		t.Fatalf("sent batches stay on disk: %v", left)
	}
}

func TestOneRunningRequestTagsEveryLine(t *testing.T) {
	active := []string{"tx_one"}
	s, k := setup(t, func() []string { return active })
	s.Writer("out", nil).Write([]byte("loading weights\n"))
	active = []string{"tx_one", "tx_two"}
	s.Writer("out", nil).Write([]byte("no id here\nhandling tx_two now\n"))
	s.Flush(context.Background())
	got := []string{k.records[0].Request, k.records[1].Request, k.records[2].Request}
	if got[0] != "tx_one" || got[1] != "" || got[2] != "tx_two" {
		t.Fatalf("requests = %v", got)
	}
}

func TestAFailedUploadKeepsTheBatchForTheNextFlush(t *testing.T) {
	s, k := setup(t, nil)
	k.fail = true
	s.Writer("out", nil).Write([]byte("kept\n"))
	s.Flush(context.Background())
	if left, _ := filepath.Glob(filepath.Join(s.cfg.Dir, "batch-*")); len(left) != 1 {
		t.Fatalf("batch should wait on disk, found %v", left)
	}
	k.fail = false
	s.Flush(context.Background())
	if len(k.records) != 1 || k.records[0].Message != "kept" {
		t.Fatalf("records = %+v", k.records)
	}
}

func TestTheOldestBatchesAreDroppedPastTheDiskCap(t *testing.T) {
	s, _ := setup(t, nil)
	s.capped = 1
	for _, name := range []string{"batch-00000000000000000001.ndjson", "batch-00000000000000000002.ndjson"} {
		os.WriteFile(filepath.Join(s.cfg.Dir, name), []byte(`{"t":"x","s":"out","m":"old"}`+"\n"), 0o600)
	}
	s.Writer("out", nil).Write([]byte("new\n"))
	s.mu.Lock()
	s.rotateLocked()
	dropped := s.dropped
	s.mu.Unlock()
	if dropped != 3 {
		t.Fatalf("dropped = %d", dropped)
	}
}

func TestALongLineIsCutIntoPieces(t *testing.T) {
	s, k := setup(t, nil)
	big := make([]byte, lineBytes+10)
	for i := range big {
		big[i] = 'a'
	}
	w := s.Writer("out", nil)
	w.Write(big)
	w.(*lineWriter).Close()
	s.Flush(context.Background())
	if len(k.records) != 2 || len(k.records[0].Message) != lineBytes || len(k.records[1].Message) != 10 {
		t.Fatalf("got %d records", len(k.records))
	}
}
