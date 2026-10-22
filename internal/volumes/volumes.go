// Package volumes gives user code reach to Surfra volumes without the replica token.
package volumes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// The app process never sees the replica token. This process exchanges it for a volumes-only token,
// keeps that fresh, and serves a proxy on a loopback port that adds it to every request. The app gets
//
//	SURFRA_VOLUMES_URL         http://127.0.0.1:<port>, so a plain request from any language works
//	SURFRA_VOLUMES_TOKEN_FILE  the same token in a file, for clients that talk to the service directly
//
// A request that already carries Authorization is passed through unchanged.

const (
	volumeTokenEvery = 10 * time.Minute
	volumeTokenRetry = 30 * time.Second
	defaultVolumes   = "https://volumes.surfra.io"
)

// Tokens holds the volumes-only token and serves the loopback proxy.
type Tokens struct {
	master   string
	token    string
	path     string
	upstream string
	client   *http.Client

	mu      sync.RWMutex
	current string
	local   string
	wake    chan struct{}
}

// New prepares the token holder. env is the process environment, read for SURFRA_VOLUMES_URL.
func New(master, replicaToken string, env []string) *Tokens {
	upstream := defaultVolumes
	for _, kv := range env {
		if key, value, _ := strings.Cut(kv, "="); key == "SURFRA_VOLUMES_URL" && strings.TrimSpace(value) != "" {
			upstream = strings.TrimRight(strings.TrimSpace(value), "/")
		}
	}
	return &Tokens{
		master:   master,
		token:    replicaToken,
		path:     filepath.Join(os.TempDir(), "surfra", fmt.Sprintf("volume-token-%d", os.Getpid())),
		upstream: upstream,
		client:   &http.Client{Timeout: 30 * time.Second},
		wake:     make(chan struct{}, 1),
	}
}

// Env points the app at the proxy when it runs, and at the service itself otherwise.
func (v *Tokens) Env(base []string) []string {
	out := make([]string, 0, len(base)+2)
	for _, kv := range base {
		key, _, _ := strings.Cut(kv, "=")
		if key == "SURFRA_VOLUMES_TOKEN_FILE" || key == "SURFRA_VOLUMES_URL" {
			continue
		}
		out = append(out, kv)
	}
	target := v.upstream
	if v.local != "" {
		target = v.local
	}
	return append(out, "SURFRA_VOLUMES_TOKEN_FILE="+v.path, "SURFRA_VOLUMES_URL="+target)
}

func (v *Tokens) Refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.master+"/v1/tokens", strings.NewReader(`{"audience":"volumes"}`))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+v.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("volume token: HTTP %d", resp.StatusCode)
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(data), &body); err != nil || body.Token == "" {
		return fmt.Errorf("volume token: no token in the answer")
	}
	v.mu.Lock()
	v.current = body.Token
	v.mu.Unlock()
	return writePrivate(v.path, body.Token)
}

func (v *Tokens) bearer() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.current
}

// writePrivate replaces the file in one step, readable only by this user, so a reader never sees half a token.
func writePrivate(path, text string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, ".token-")
	if err != nil {
		return err
	}
	name := temp.Name()
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		os.Remove(name)
		return err
	}
	if _, err := temp.WriteString(text); err != nil {
		temp.Close()
		os.Remove(name)
		return err
	}
	if err := temp.Close(); err != nil {
		os.Remove(name)
		return err
	}
	return os.Rename(name, path)
}

// Loop refreshes the token until ctx ends. A failed refresh, or a 401 seen by the proxy, retries sooner.
func (v *Tokens) Loop(ctx context.Context) {
	for {
		wait := volumeTokenEvery
		if err := v.Refresh(ctx); err != nil {
			wait = volumeTokenRetry
		}
		select {
		case <-ctx.Done():
			os.Remove(v.path)
			return
		case <-v.wake:
		case <-time.After(wait):
		}
	}
}

// Serve starts the loopback proxy. On failure the app still gets the token file and the service URL.
func (v *Tokens) Serve(ctx context.Context) error {
	target, err := url.Parse(v.upstream)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			r.Out.Host = target.Host
			if r.In.Header.Get("Authorization") == "" {
				if token := v.bearer(); token != "" {
					r.Out.Header.Set("Authorization", "Bearer "+token)
				}
			}
		},
		FlushInterval: -1,
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode == http.StatusUnauthorized {
				select {
				case v.wake <- struct{}{}:
				default:
				}
			}
			return nil
		},
	}
	server := &http.Server{Handler: proxy, ReadHeaderTimeout: 30 * time.Second}
	v.local = "http://" + listener.Addr().String()
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shut)
	}()
	go server.Serve(listener)
	return nil
}
