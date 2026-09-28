package instinct

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestZeroExitLoggedOutIsTerminal(t *testing.T) {
	cfg, _ := fixture(t)
	if err := os.WriteFile(filepath.Join(cfg.StoreDir, "session.db"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "fake-wacli")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nfound=0\nfor arg in \"$@\"; do [ \"$arg\" = '--disable-send-delegate' ] && found=1; done\n[ \"$found\" = 1 ] || exit 42\nprintf '%s\\n' '{\"event\":\"logged_out\",\"ts\":1}' >&2\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	cfg.WacliBin = bin
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.ScanInterval = 100 * time.Millisecond
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := NewRunner(cfg, c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	deadline := time.After(5 * time.Second)
	for {
		if r.Health().State == "re_pair_needed" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("no terminal state: %+v", r.Health())
		case <-time.After(20 * time.Millisecond):
		}
	}
	time.Sleep(1200 * time.Millisecond)
	h := r.Health()
	if h.RestartCount != 0 || h.State != "re_pair_needed" {
		t.Fatalf("hot restart or state lost: %+v", h)
	}
	if n := pending(t, c); n != 1 {
		t.Fatalf("repair event count=%d", n)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop")
	}
}

func TestIngestRequiresDurableSuccessAndRecoversHealth(t *testing.T) {
	cfg, source := fixture(t)
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	add(t, source, groupJID, "one", "payload")
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	status := http.StatusServiceUnavailable
	calls := 0
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Idempotency-Key") == "" || r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing authentication/idempotency")
		}
		var e Envelope
		if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
			t.Error(err)
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()
	cfg.IngestURL = srv.URL
	if err := os.WriteFile(cfg.TokenFile, []byte("test-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(cfg, c)
	runner.client = srv.Client()
	runner.set("connected", "")
	runner.mu.Lock()
	runner.health.Connected = true
	runner.mu.Unlock()
	if err := runner.deliverOne(ctx); err == nil {
		t.Fatal("expected ingest failure")
	}
	runner.set("degraded_ingest", "ingest_unavailable")
	if n := pending(t, c); n != 1 {
		t.Fatalf("lost event after 503: %d", n)
	}
	status = http.StatusAccepted
	if err := runner.deliverOne(ctx); err != nil {
		t.Fatal(err)
	}
	if n := pending(t, c); n != 0 {
		t.Fatalf("event still pending: %d", n)
	}
	if got := runner.Health().State; got != "connected" {
		t.Fatalf("health not recovered: %s", got)
	}
	if calls != 2 {
		t.Fatalf("ingest calls=%d", calls)
	}
}

func TestFailureClasses(t *testing.T) {
	cases := map[string]string{"sync storage limit reached: database size": "degraded_disk", "store is locked": "degraded_lock", "database disk image is malformed": "degraded_store", "disk I/O error": "degraded_disk"}
	for in, want := range cases {
		if got := stateForCode(classify(in)); got != want {
			t.Errorf("%q: %s want %s", in, got, want)
		}
	}
}

func TestMissingSourceOnlyAllowedBeforeFirstObservation(t *testing.T) {
	freshCfg, freshSource := fixture(t)
	if err := freshSource.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(freshCfg.StoreDir, "wacli.db")); err != nil {
		t.Fatal(err)
	}
	freshCapture, err := OpenCapture(freshCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer freshCapture.Close()
	if err := NewRunner(freshCfg, freshCapture).reconcile(context.Background()); err != nil {
		t.Fatalf("fresh store should await first source: %v", err)
	}

	cfg, source := fixture(t)
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := NewRunner(cfg, c)
	if err := r.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(cfg.StoreDir, "wacli.db")); err != nil {
		t.Fatal(err)
	}
	for _, runner := range []*Runner{r, NewRunner(cfg, c)} {
		err := runner.reconcile(context.Background())
		if !errors.Is(err, ErrSourceMissing) {
			t.Fatalf("missing observed source error=%v", err)
		}
		code := classify(err.Error())
		runner.set(stateForCode(code), code)
		h := runner.Health()
		if h.State != "degraded_store" || h.ErrorCode != "source_missing" {
			t.Fatalf("missing source health=%+v", h)
		}
	}
}

func TestLostStoreBeforeChildRestartReportsSourceMissing(t *testing.T) {
	cfg, source := fixture(t)
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.ScanInterval = 10 * time.Second // Exercise the start path, not the scan tick.
	if err := os.WriteFile(filepath.Join(cfg.StoreDir, "session.db"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"session.db", "wacli.db"} {
		if err := os.Remove(filepath.Join(cfg.StoreDir, name)); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	cfg.IngestURL = srv.URL
	if err := os.WriteFile(cfg.TokenFile, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	r := NewRunner(cfg, c) // New process: observation must come from durable metadata.
	r.client = srv.Client()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("runner did not stop")
		}
	}()
	deadline := time.After(3 * time.Second)
	for {
		h := r.Health()
		if h.State == "degraded_store" && h.ErrorCode == "source_missing" {
			return
		}
		if h.State == "pairing_required" {
			t.Fatalf("lost store masked as pairing: %+v", h)
		}
		select {
		case <-deadline:
			t.Fatalf("lost source not reported: %+v", h)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestTokenFileMustBePrivateRegularFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(p, []byte("example"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readPrivateToken(p); err == nil {
		t.Fatal("world-readable token accepted")
	}
	if err := os.Chmod(p, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPrivateToken(p); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(filepath.Dir(p), "link")
	if err := os.Symlink(p, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readPrivateToken(link); err == nil {
		t.Fatal("symlink token accepted")
	}
}

func TestMessageDeliveryDoesNotClearPendingDegradationAlert(t *testing.T) {
	cfg, source := fixture(t)
	add(t, source, groupJID, "one", "example")
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	defer srv.Close()
	cfg.IngestURL = srv.URL
	if err := os.WriteFile(cfg.TokenFile, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	r := NewRunner(cfg, c)
	r.client = srv.Client()
	r.set("degraded_disk", "store_cap_reached")
	if n := pending(t, c); n != 2 {
		t.Fatalf("pending before delivery=%d", n)
	}
	if err := r.deliverOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := r.Health().State; got != "degraded_disk" {
		t.Fatalf("degradation cleared by unrelated delivery: %s", got)
	}
	_, b, _, err := c.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatal(err)
	}
	if e.EventType != "worker_degraded" || e.ErrorCode != "store_cap_reached" {
		t.Fatalf("pending health alert lost: %+v", e)
	}
}

func TestOversizeLifecycleLineDrainsAndStillSeesLogout(t *testing.T) {
	cfg, _ := fixture(t)
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := NewRunner(cfg, c)
	stream := strings.Repeat("x", 1024*1024+10) + "\n" + `{"event":"logged_out"}` + "\n"
	r.consumeEvents(strings.NewReader(stream))
	if got := r.Health().State; got != "re_pair_needed" {
		t.Fatalf("oversize line blocked logout: %s", got)
	}
}

func TestStartupFailureCodes(t *testing.T) {
	if got := StartupErrorCode(os.ErrPermission); got != "degraded_disk" {
		t.Fatalf("permission state=%s", got)
	}
	cfg, _ := fixture(t)
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = OpenCapture(cfg)
	if err == nil || StartupErrorCode(err) != "degraded_lock" {
		t.Fatalf("second worker startup=%v, %s", err, StartupErrorCode(err))
	}
}

func TestLifecycleDoesNotClearStorageDegradationBeforeNewWorkerReconciles(t *testing.T) {
	cfg, _ := fixture(t)
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := NewRunner(cfg, c)
	r.set("degraded_disk", "store_cap_reached")
	r.consumeEventLine([]byte(`{"event":"connected"}`))
	r.consumeEventLine([]byte(`{"event":"offline_sync_completed"}`))
	if got := r.Health().State; got != "degraded_disk" {
		t.Fatalf("lifecycle cleared degradation: %s", got)
	}
	if err := r.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := r.Health().State; got != "degraded_disk" {
		t.Fatalf("same worker cleared degradation: %s", got)
	}
	r.mu.Lock()
	r.childGeneration++
	r.mu.Unlock()
	if err := r.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := r.Health().State; got != "connected" {
		t.Fatalf("new worker did not recover: %s", got)
	}
}
