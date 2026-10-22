package supervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The app process never sees the replica token. It reaches volumes with a volumes-only token this
// process keeps fresh in a file, named by SURFRA_VOLUMES_TOKEN_FILE. Any language can read the file
// and send its contents as a bearer token to SURFRA_VOLUMES_URL.

const (
	volumeTokenEvery = 10 * time.Minute
	volumeTokenRetry = 30 * time.Second
	defaultVolumes   = "https://volumes.surfra.io"
)

type volumeTokens struct {
	master string
	token  string
	path   string
	client *http.Client
}

func newVolumeTokens(cfg Config) *volumeTokens {
	return &volumeTokens{
		master: cfg.Master,
		token:  cfg.Token,
		path:   filepath.Join(os.TempDir(), "surfra", fmt.Sprintf("volume-token-%d", os.Getpid())),
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// env adds the token file and the volumes URL to the app's environment.
func (v *volumeTokens) env(base []string) []string {
	out := make([]string, 0, len(base)+2)
	url := defaultVolumes
	for _, kv := range base {
		key, value, _ := strings.Cut(kv, "=")
		switch key {
		case "SURFRA_VOLUMES_TOKEN_FILE":
			continue
		case "SURFRA_VOLUMES_URL":
			if strings.TrimSpace(value) != "" {
				url = strings.TrimRight(strings.TrimSpace(value), "/")
			}
			continue
		}
		out = append(out, kv)
	}
	return append(out, "SURFRA_VOLUMES_TOKEN_FILE="+v.path, "SURFRA_VOLUMES_URL="+url)
}

func (v *volumeTokens) refresh(ctx context.Context) error {
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
	return writePrivate(v.path, body.Token)
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

// loop refreshes the token until ctx ends. A failed refresh retries sooner; the old token stays until it expires.
func (v *volumeTokens) loop(ctx context.Context) {
	for {
		wait := volumeTokenEvery
		if err := v.refresh(ctx); err != nil {
			wait = volumeTokenRetry
		}
		select {
		case <-ctx.Done():
			os.Remove(v.path)
			return
		case <-time.After(wait):
		}
	}
}
