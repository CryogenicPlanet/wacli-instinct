package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	appPkg "github.com/openclaw/wacli/internal/app"
)

func TestSyncDisableSendDelegateLeavesNoSocket(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix sockets are unavailable")
	}
	cmd := newSyncCmd(&rootFlags{})
	if err := cmd.ParseFlags([]string{"--disable-send-delegate"}); err != nil {
		t.Fatal(err)
	}
	disabled, err := cmd.Flags().GetBool("disable-send-delegate")
	if err != nil || !disabled {
		t.Fatalf("flag not wired: %v", err)
	}
	shortDir, err := os.MkdirTemp("/tmp", "wacli-sync-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortDir) })
	socket := filepath.Join(shortDir, ".send.sock")
	started := false
	var listener net.Listener
	start := func(context.Context) error {
		started = true
		var err error
		listener, err = net.Listen("unix", socket)
		return err
	}
	t.Cleanup(func() {
		if listener != nil {
			_ = listener.Close()
		}
	})
	if after := syncSendDelegateAfterConnect(appPkg.SyncModeFollow, disabled, start); after != nil {
		t.Fatal("delegate callback installed in disabled mode")
	}
	if started {
		t.Fatal("delegate started")
	}
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("delegate socket exists: %v", err)
	}
	if after := syncSendDelegateAfterConnect(appPkg.SyncModeFollow, false, start); after == nil {
		t.Fatal("default follow mode lost delegate")
	} else if err := after(context.Background()); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Lstat(socket)
	if err != nil {
		t.Fatal("enabled delegate not invoked:", err)
	}
	if fi.Mode()&os.ModeSocket == 0 {
		t.Fatalf("delegate path is not a socket: %s", fi.Mode())
	}
}

func TestSyncCommandExposesWebhookFlags(t *testing.T) {
	cmd := newSyncCmd(&rootFlags{})
	for _, name := range []string{"webhook", "webhook-secret", "webhook-allow-private", "webhook-events"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Fatalf("missing --%s flag", name)
		}
	}
}

func TestSyncCommandWebhookEventsDefaultsToMessage(t *testing.T) {
	cmd := newSyncCmd(&rootFlags{})
	flag := cmd.Flags().Lookup("webhook-events")
	if flag == nil {
		t.Fatal("missing --webhook-events flag")
	}
	if flag.DefValue != "message" {
		t.Fatalf("--webhook-events default = %q, want \"message\"", flag.DefValue)
	}
}

func TestSyncCommandRejectsUnknownWebhookEvent(t *testing.T) {
	cmd := newSyncCmd(&rootFlags{})
	cmd.SetArgs([]string{"--webhook", "https://example.test/hook", "--webhook-events", "message,presence"})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--webhook-events must be a comma-separated list of") {
		t.Fatalf("expected webhook-events validation error, got %v", err)
	}
}

func TestSyncCommandRequiresWebhookForEvents(t *testing.T) {
	cmd := newSyncCmd(&rootFlags{})
	cmd.SetArgs([]string{"--webhook-events", "receipt"})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--webhook-events requires --webhook") {
		t.Fatalf("expected webhook-events validation error, got %v", err)
	}
}

func TestSyncCommandRequiresWebhookForSecret(t *testing.T) {
	cmd := newSyncCmd(&rootFlags{})
	cmd.SetArgs([]string{"--webhook-secret", "secret"})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--webhook-secret requires --webhook") {
		t.Fatalf("expected webhook-secret validation error, got %v", err)
	}
}

func TestSyncCommandRejectsIneffectiveStaleThreshold(t *testing.T) {
	cmd := newSyncCmd(&rootFlags{})
	cmd.SetArgs([]string{"--stale-threshold", "2m20s"})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--stale-threshold must be less than 2m20s") {
		t.Fatalf("expected stale-threshold validation error, got %v", err)
	}
}

func TestSyncCommandRejectsInvalidPresenceMode(t *testing.T) {
	cmd := newSyncCmd(&rootFlags{})
	cmd.SetArgs([]string{"--presence-mode", "loud"})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--presence-mode must be one of: normal, quiet") {
		t.Fatalf("expected presence-mode validation error, got %v", err)
	}
}
