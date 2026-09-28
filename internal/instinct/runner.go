package instinct

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/openclaw/wacli/internal/lock"
)

type Health struct {
	State              string `json:"state"`
	Connected          bool   `json:"connected"`
	LastSuccessfulSync string `json:"last_successful_sync,omitempty"`
	LastReconciliation string `json:"last_reconciliation,omitempty"`
	StoreBytes         int64  `json:"store_bytes"`
	StateBytes         int64  `json:"state_bytes"`
	PendingEvents      int64  `json:"pending_events"`
	RestartCount       int    `json:"restart_count"`
	ErrorCode          string `json:"error_code,omitempty"`
}

type Runner struct {
	Config             Config
	Capture            *Capture
	mu                 sync.Mutex
	health             Health
	trigger            chan struct{}
	client             *http.Client
	childGeneration    uint64
	degradedGeneration uint64
	sourceObserved     bool
}

var ErrSourceMissing = errors.New("source database missing after observation")

func NewRunner(c Config, capture *Capture) *Runner {
	return &Runner{Config: c, Capture: capture, health: Health{State: "starting"}, trigger: make(chan struct{}, 1), client: &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (r *Runner) Health() Health { r.mu.Lock(); defer r.mu.Unlock(); return r.health }
func (r *Runner) set(state, code string) {
	r.mu.Lock()
	if r.health.State == "re_pair_needed" && state != "stopped" {
		r.mu.Unlock()
		return
	}
	changed := r.health.State != state || r.health.ErrorCode != code
	r.health.State = state
	r.health.ErrorCode = code
	if strings.HasPrefix(state, "degraded_") && state != "degraded_ingest" {
		r.degradedGeneration = r.childGeneration
	}
	if state == "disconnected" || state == "connecting" || state == "re_pair_needed" || state == "stopped" {
		r.health.Connected = false
	}
	r.mu.Unlock()
	if changed && strings.HasPrefix(state, "degraded_") && state != "degraded_ingest" && code != "" {
		_ = r.Capture.QueueOperational(context.Background(), code)
	}
}
func (r *Runner) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/healthz" {
		http.NotFound(w, req)
		return
	}
	if req.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	h := r.Health()
	if h.State != "connected" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(h)
}

func (r *Runner) Run(ctx context.Context) (runErr error) {
	if err := PrepareStore(r.Config.StoreDir); err != nil {
		return err
	}
	server := &http.Server{Addr: r.Config.HTTPAddr, Handler: r, ReadHeaderTimeout: 3 * time.Second}
	serverErr := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	var childDone <-chan error
	var cancelChild context.CancelFunc
	defer func() {
		if cancelChild != nil {
			cancelChild()
		}
		if childDone != nil {
			select {
			case <-childDone:
			case <-time.After(15 * time.Second):
				if runErr == nil {
					runErr = errors.New("child shutdown timed out")
				}
			}
		}
	}()
	backoff := time.Second
	nextStart := time.Now()
	deliveryBackoff := time.Second
	nextDelivery := time.Now()
	scanTick := time.NewTicker(r.Config.ScanInterval)
	defer scanTick.Stop()
	deliverTick := time.NewTicker(time.Second)
	defer deliverTick.Stop()
	startTick := time.NewTicker(time.Second)
	defer startTick.Stop()
	for {
		select {
		case <-ctx.Done():
			r.set("stopped", "")
			return nil
		case err := <-serverErr:
			return err
		case <-startTick.C:
			if childDone != nil || time.Now().Before(nextStart) || r.Health().State == "re_pair_needed" {
				continue
			}
			if _, err := os.Stat(filepath.Join(r.Config.StoreDir, "SESSION_REVOKED")); err == nil {
				r.revoked(ctx)
				continue
			} else if !errors.Is(err, os.ErrNotExist) {
				r.set("degraded_store", "revocation_check_failed")
				continue
			}
			if err := PrepareStore(r.Config.StoreDir); err != nil {
				r.set("degraded_disk", "store_permissions_failed")
				continue
			}
			if err := r.reconcile(ctx); err != nil {
				code := classify(err.Error())
				r.set(stateForCode(code), code)
				nextStart = time.Now().Add(backoff)
				backoff = grow(backoff)
				continue
			}
			if _, err := os.Stat(filepath.Join(r.Config.StoreDir, "session.db")); errors.Is(err, os.ErrNotExist) {
				r.set("pairing_required", "")
				continue
			} else if err != nil {
				r.set("degraded_store", "session_check_failed")
				continue
			}
			childCtx, cancel := context.WithCancel(ctx)
			done, err := r.start(childCtx)
			if err != nil {
				cancel()
				r.set("degraded_process", "start_failed")
				nextStart = time.Now().Add(backoff)
				backoff = grow(backoff)
				continue
			}
			cancelChild = cancel
			childDone = done
			r.mu.Lock()
			r.childGeneration++
			r.health.Connected = false
			if !strings.HasPrefix(r.health.State, "degraded_") {
				r.health.State = "connecting"
				r.health.ErrorCode = ""
			}
			r.mu.Unlock()
		case err := <-childDone:
			childDone = nil
			r.mu.Lock()
			r.health.Connected = false
			r.mu.Unlock()
			if cancelChild != nil {
				cancelChild()
				cancelChild = nil
			}
			if _, statErr := os.Stat(filepath.Join(r.Config.StoreDir, "SESSION_REVOKED")); statErr == nil {
				r.revoked(ctx)
				continue
			}
			if r.Health().State == "re_pair_needed" {
				continue
			}
			code := "process_stopped"
			if err != nil {
				code = classify(err.Error())
			}
			if prior := r.Health().ErrorCode; prior != "" && (code == "sync_failure" || code == "process_stopped") {
				code = prior
			}
			r.set(stateForCode(code), code)
			r.mu.Lock()
			r.health.RestartCount++
			r.mu.Unlock()
			nextStart = time.Now().Add(backoff)
			backoff = grow(backoff)
		case <-r.trigger:
			if err := r.reconcile(ctx); err != nil {
				code := classify(err.Error())
				r.set(stateForCode(code), code)
			} else if r.Health().Connected {
				backoff = time.Second
			}
		case <-scanTick.C:
			if err := r.reconcile(ctx); err != nil {
				code := classify(err.Error())
				r.set(stateForCode(code), code)
			}
		case <-deliverTick.C:
			if time.Now().Before(nextDelivery) {
				continue
			}
			if err := r.deliverOne(ctx); err != nil {
				r.set("degraded_ingest", "ingest_unavailable")
				nextDelivery = time.Now().Add(deliveryBackoff)
				deliveryBackoff = grow(deliveryBackoff)
			} else {
				deliveryBackoff = time.Second
				nextDelivery = time.Now()
			}
		}
	}
}

func (r *Runner) start(ctx context.Context) (<-chan error, error) {
	args := []string{"--store", r.Config.StoreDir, "--events", "sync", "--follow", "--disable-send-delegate", "--presence-mode", "quiet", "--max-db-size", r.Config.MaxDBSize, "--max-messages", fmt.Sprint(r.Config.MaxMessages)}
	cmd := exec.CommandContext(ctx, r.Config.WacliBin, args...)
	cmd.Stdout = io.Discard
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	cmd.Env = append(os.Environ(), "WACLI_STORE_DIR="+r.Config.StoreDir)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() {
		// StderrPipe is closed by Wait. Drain lifecycle events to EOF first so
		// a fast child cannot lose a final logged_out event on process exit.
		r.consumeEvents(stderr)
		done <- cmd.Wait()
	}()
	return done, nil
}

func (r *Runner) consumeEvents(reader io.Reader) {
	br := bufio.NewReaderSize(reader, 1024*1024)
	oversized := false
	for {
		line, err := br.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			oversized = true
			continue
		}
		if oversized {
			r.set("degraded_process", "event_stream_oversize")
			oversized = false
		} else if len(line) > 0 {
			r.consumeEventLine(line)
		}
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			r.set("degraded_process", "event_stream_error")
			return
		}
	}
}

func (r *Runner) consumeEventLine(line []byte) {
	var ev struct {
		Event string                     `json:"event"`
		Data  map[string]json.RawMessage `json:"data"`
	}
	if json.Unmarshal(line, &ev) != nil {
		return
	}
	switch ev.Event {
	case "connected":
		r.mu.Lock()
		clearRepair := r.health.State != "re_pair_needed"
		if r.health.State != "re_pair_needed" {
			r.health.Connected = true
			if !strings.HasPrefix(r.health.State, "degraded_") {
				r.health.State = "connected"
				r.health.ErrorCode = ""
			}
		}
		r.mu.Unlock()
		if clearRepair {
			_ = r.Capture.ClearRepair(context.Background())
		}
		r.promptScan()
	case "disconnected", "reconnecting":
		r.mu.Lock()
		r.health.Connected = false
		if !strings.HasPrefix(r.health.State, "degraded_") && r.health.State != "re_pair_needed" {
			r.health.State = "disconnected"
			r.health.ErrorCode = ""
		}
		r.mu.Unlock()
	case "offline_sync_preview":
		if !strings.HasPrefix(r.Health().State, "degraded_") {
			r.set("catching_up", "")
		}
	case "offline_sync_completed":
		r.mu.Lock()
		r.health.LastSuccessfulSync = time.Now().UTC().Format(time.RFC3339)
		r.health.Connected = true
		if !strings.HasPrefix(r.health.State, "degraded_") && r.health.State != "re_pair_needed" {
			r.health.State = "connected"
			r.health.ErrorCode = ""
		}
		r.mu.Unlock()
		r.promptScan()
	case "logged_out":
		r.revoked(context.Background())
	case "error":
		var message string
		_ = json.Unmarshal(ev.Data["message"], &message)
		code := classify(message)
		r.set(stateForCode(code), code)
	}
}

func (r *Runner) promptScan() {
	select {
	case r.trigger <- struct{}{}:
	default:
	}
}
func (r *Runner) revoked(ctx context.Context) {
	r.set("re_pair_needed", "")
	if err := r.Capture.QueueRepair(ctx); err != nil {
		r.mu.Lock()
		r.health.ErrorCode = "repair_outbox_failed"
		r.mu.Unlock()
	}
	r.promptScan()
}

func (r *Runner) reconcile(ctx context.Context) error {
	sourcePath := filepath.Join(r.Config.StoreDir, "wacli.db")
	if _, statErr := os.Stat(sourcePath); statErr == nil {
		r.mu.Lock()
		r.sourceObserved = true
		r.mu.Unlock()
	} else if errors.Is(statErr, os.ErrNotExist) {
		r.mu.Lock()
		observed := r.sourceObserved || r.health.Connected || r.health.LastReconciliation != ""
		r.mu.Unlock()
		if !observed {
			_, metaErr := r.Capture.meta("source_identity")
			if metaErr == nil {
				observed = true
			} else if !errors.Is(metaErr, sql.ErrNoRows) {
				return metaErr
			}
		}
		if observed {
			return ErrSourceMissing
		}
		return nil
	} else {
		return statErr
	}
	_, err := r.Capture.Reconcile(ctx)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrSourceMissing
		}
		return err
	}
	size, err := StoreSize(r.Config.StoreDir)
	if err != nil {
		return err
	}
	stateBytes, err := StateSize(r.Config.StateDir)
	if err != nil {
		return err
	}
	pending, err := r.Capture.PendingCount(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.health.LastReconciliation = time.Now().UTC().Format(time.RFC3339)
	r.health.StoreBytes = size
	r.health.StateBytes = stateBytes
	r.health.PendingEvents = pending
	r.mu.Unlock()
	h := r.Health()
	r.mu.Lock()
	newGeneration := r.childGeneration > r.degradedGeneration
	r.mu.Unlock()
	if h.Connected && h.State == "degraded_store" && h.ErrorCode == "source_missing" {
		r.set("connected", "")
		_ = r.Capture.ClearOperational(ctx)
	} else if h.Connected && newGeneration && h.State != "degraded_ingest" && strings.HasPrefix(h.State, "degraded_") {
		r.set("connected", "")
		_ = r.Capture.ClearOperational(ctx)
	}
	return nil
}

func (r *Runner) deliverOne(ctx context.Context) error {
	id, payload, key, err := r.Capture.Pending(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	token, err := readPrivateToken(r.Config.TokenFile)
	if err != nil {
		return err
	}
	token = bytes.TrimSpace(token)
	if len(token) == 0 || bytes.ContainsAny(token, "\r\n") {
		return errors.New("invalid ingest token file")
	}
	req, err := http.NewRequestWithContext(ctx, "POST", r.Config.IngestURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+string(token))
	req.Header.Set("Idempotency-Key", key)
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ingest rejected event: HTTP %d", resp.StatusCode)
	}
	if err := r.Capture.Accept(ctx, id); err != nil {
		return err
	}
	pending, err := r.Capture.PendingCount(ctx)
	if err == nil {
		r.mu.Lock()
		r.health.PendingEvents = pending
		if r.health.State == "degraded_ingest" {
			if r.health.Connected {
				r.health.State = "connected"
			} else {
				r.health.State = "disconnected"
			}
			r.health.ErrorCode = ""
		}
		r.mu.Unlock()
	}
	return nil
}

func readPrivateToken(path string) ([]byte, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() || fi.Mode().Perm()&0077 != 0 || fi.Size() > 4096 {
		return nil, errors.New("ingest token file must be owner-only regular file under 4 KiB")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(fi, opened) || !opened.Mode().IsRegular() || opened.Mode().Perm()&0077 != 0 || opened.Size() > 4096 {
		return nil, errors.New("ingest token file changed or is not private")
	}
	return io.ReadAll(io.LimitReader(f, 4097))
}

func grow(v time.Duration) time.Duration {
	v *= 2
	if v > time.Minute {
		return time.Minute
	}
	return v
}
func classify(s string) string {
	v := strings.ToLower(s)
	switch {
	case strings.Contains(v, "source database missing after observation"):
		return "source_missing"
	case strings.Contains(v, "schema migration required"):
		return "migration_required"
	case strings.Contains(v, "state database size cap"):
		return "state_cap_reached"
	case strings.Contains(v, "sync storage limit") || strings.Contains(v, "database size") || strings.Contains(v, "max messages"):
		return "store_cap_reached"
	case strings.Contains(v, "locked") || strings.Contains(v, "busy"):
		return "store_lock_conflict"
	case strings.Contains(v, "database disk image is malformed") || strings.Contains(v, "corrupt"):
		return "store_corrupt"
	case strings.Contains(v, "disk") || strings.Contains(v, "space") || strings.Contains(v, "i/o error"):
		return "disk_error"
	case strings.Contains(v, "permission denied") || strings.Contains(v, "read-only file system"):
		return "disk_error"
	case strings.Contains(v, "not authenticated") || strings.Contains(v, "not logged in"):
		return "pairing_required"
	default:
		return "sync_failure"
	}
}
func stateForCode(code string) string {
	switch code {
	case "store_cap_reached", "state_cap_reached", "disk_error":
		return "degraded_disk"
	case "store_lock_conflict":
		return "degraded_lock"
	case "store_corrupt", "migration_required", "source_missing":
		return "degraded_store"
	case "pairing_required":
		return "pairing_required"
	default:
		return "degraded_process"
	}
}
func SafeErrorCode(err error) string {
	if err == nil {
		return ""
	}
	return classify(err.Error())
}
func StartupErrorCode(err error) string {
	if err == nil {
		return ""
	}
	if lock.IsLocked(err) {
		return "degraded_lock"
	}
	if errors.Is(err, os.ErrPermission) {
		return "degraded_disk"
	}
	switch classify(err.Error()) {
	case "store_lock_conflict":
		return "degraded_lock"
	case "disk_error", "state_cap_reached":
		return "degraded_disk"
	default:
		return "degraded_store"
	}
}
