package instinct

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/openclaw/wacli/internal/lock"

	_ "github.com/mattn/go-sqlite3"
)

const groupJID = "120363000000000000@g.us"
const otherJID = "120363000000000001@g.us"

func fixture(t *testing.T) (Config, *sql.DB) {
	t.Helper()
	root := t.TempDir()
	store := filepath.Join(root, "store")
	if err := os.Mkdir(store, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(store, "wacli.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE messages (rowid INTEGER PRIMARY KEY AUTOINCREMENT, chat_jid TEXT, msg_id TEXT, sender_jid TEXT, ts INTEGER, from_me INTEGER, text TEXT, media_caption TEXT, deleted_at INTEGER, revoked INTEGER DEFAULT 0, payload_purged_at INTEGER, edited_ts INTEGER DEFAULT 0);
	CREATE TABLE message_payload_purges (chat_jid TEXT,msg_id TEXT,deleted_at INTEGER,purged_at INTEGER);`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return Config{AccountID: "example-account", ChatJIDs: []string{groupJID}, StoreDir: store, StateDir: filepath.Join(root, "state"), IngestURL: "https://ingest.example.test/events", TokenFile: filepath.Join(root, "token"), ScanInterval: 30, InitialBackfill: 2, LookbackRows: 20, MaxMessages: 1000, MaxDBSize: "100MB"}, db
}

func add(t *testing.T, db *sql.DB, chat, id, text string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO messages(chat_jid,msg_id,sender_jid,ts,from_me,text) VALUES(?,?,?,100,0,?)`, chat, id, "15550000000@s.whatsapp.net", text)
	if err != nil {
		t.Fatal(err)
	}
}
func pending(t *testing.T, c *Capture) int64 {
	t.Helper()
	n, err := c.PendingCount(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRestartAndMissedPromptReconcileExactlyOnce(t *testing.T) {
	cfg, source := fixture(t)
	add(t, source, groupJID, "old", "old")
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if n := pending(t, c); n != 1 {
		t.Fatalf("initial backfill pending=%d", n)
	}
	id, b, _, err := c.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatal(err)
	}
	if e.TriageEligible {
		t.Fatal("backfill must be silent")
	}
	if err := c.Accept(ctx, id); err != nil {
		t.Fatal(err)
	}
	c.Close()
	add(t, source, groupJID, "missed", "new") // no webhook/prompt occurs
	c, err = OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if n := pending(t, c); n != 1 {
		t.Fatalf("restarted pending=%d", n)
	}
	_, b, _, err = c.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatal(err)
	}
	if e.MessageID != "missed" || !e.TriageEligible {
		t.Fatalf("wrong message after restart: %+v", e)
	}
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if n := pending(t, c); n != 1 {
		t.Fatalf("duplicate pending=%d", n)
	}
}

func TestIngestFailureKeepsDurableOutboxAndCursor(t *testing.T) {
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
	add(t, source, groupJID, "one", "hello")
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if n := pending(t, c); n != 1 {
		t.Fatalf("pending=%d", n)
	}
	if v, err := c.meta("cursor"); err != nil || v != "1" {
		t.Fatalf("local cursor=%q, %v", v, err)
	}
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if n := pending(t, c); n != 1 {
		t.Fatalf("pending after retry=%d", n)
	}
	// Only Accept, which requires a durable remote 2xx, clears the row.
	id, _, _, err := c.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Accept(ctx, id); err != nil {
		t.Fatal(err)
	}
	if n := pending(t, c); n != 0 {
		t.Fatalf("pending after acceptance=%d", n)
	}
}

func TestAllowlistEditsAndDeletion(t *testing.T) {
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
	add(t, source, otherJID, "offscope", "private")
	add(t, source, groupJID, "target", "one")
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if n := pending(t, c); n != 1 {
		t.Fatalf("allowlist pending=%d", n)
	}
	id, _, _, _ := c.Pending(ctx)
	_ = c.Accept(ctx, id)
	if _, err := source.Exec(`UPDATE messages SET text='two',edited_ts=200 WHERE msg_id='target'`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	_, b, _, err := c.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var e Envelope
	_ = json.Unmarshal(b, &e)
	if e.EventType != "message_updated" || e.TextOrCaption != "two" {
		t.Fatalf("edit: %+v", e)
	}
	id, _, _, _ = c.Pending(ctx)
	_ = c.Accept(ctx, id)
	if _, err := source.Exec(`UPDATE messages SET deleted_at=300,text=NULL WHERE msg_id='target'`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	_, b, _, err = c.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e = Envelope{}
	_ = json.Unmarshal(b, &e)
	if e.EventType != "message_deleted" || e.TextOrCaption != "" {
		t.Fatalf("delete: %+v", e)
	}
	id, _, _, _ = c.Pending(ctx)
	_ = c.Accept(ctx, id)
	if _, err := source.Exec(`DELETE FROM messages WHERE msg_id='target'; INSERT INTO message_payload_purges(chat_jid,msg_id,deleted_at,purged_at) VALUES('` + groupJID + `','target',300,301)`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	_, b, _, err = c.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal(b, &e)
	if e.EventType != "message_deleted" {
		t.Fatalf("purge: %+v", e)
	}
}

func TestPermissionsAndReadOnlySource(t *testing.T) {
	cfg, source := fixture(t)
	add(t, source, groupJID, "one", "test")
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := PrepareStore(cfg.StoreDir); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{cfg.StoreDir, cfg.StateDir, filepath.Join(cfg.StoreDir, "wacli.db"), filepath.Join(cfg.StateDir, "instinct.db")} {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		want := os.FileMode(0600)
		if fi.IsDir() {
			want = 0700
		}
		if fi.Mode().Perm() != want {
			t.Fatalf("%s mode %o", p, fi.Mode().Perm())
		}
	}
	if _, err := os.Stat(filepath.Join(cfg.StoreDir, "wacli.db-wal")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

func TestOfflineBackupRestoreAndPermissions(t *testing.T) {
	cfg, source := fixture(t)
	root := filepath.Dir(cfg.StoreDir)
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	session, err := sql.Open("sqlite3", filepath.Join(cfg.StoreDir, "session.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Exec(`CREATE TABLE device (id TEXT); INSERT INTO device VALUES ('linked-device')`); err != nil {
		t.Fatal(err)
	}
	session.Close()
	add(t, source, groupJID, "kept", "example")
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.Close()
	source.Close()
	snapshot := filepath.Join(root, "snapshot")
	active, err := lock.Acquire(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := Backup(context.Background(), root, snapshot); err == nil {
		t.Fatal("backup accepted active worker lock")
	}
	active.Release()
	if err := Backup(context.Background(), root, snapshot); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(root, "restored")
	if err := Restore(context.Background(), snapshot, restored); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{restored, filepath.Join(restored, "store"), filepath.Join(restored, "state"), filepath.Join(restored, "store/session.db"), filepath.Join(restored, "store/wacli.db"), filepath.Join(restored, "state/instinct.db")} {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		want := os.FileMode(0600)
		if fi.IsDir() {
			want = 0700
		}
		if fi.Mode().Perm() != want {
			t.Fatalf("%s mode %o", p, fi.Mode().Perm())
		}
	}
	db, err := sql.Open("sqlite3", filepath.Join(restored, "store/wacli.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var id string
	if err := db.QueryRow(`SELECT msg_id FROM messages`).Scan(&id); err != nil || id != "kept" {
		t.Fatalf("restored message=%q %v", id, err)
	}
	if err := Restore(context.Background(), snapshot, restored); err == nil {
		t.Fatal("restore must reject existing target")
	}
}

func TestPendingTextSupersededByDeletion(t *testing.T) {
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
	add(t, source, groupJID, "one", "sensitive")
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`UPDATE messages SET deleted_at=200,text=NULL WHERE msg_id='one'`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if n := pending(t, c); n != 1 {
		t.Fatalf("pending=%d", n)
	}
	_, b, _, err := c.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatal(err)
	}
	if e.EventType != "message_deleted" || e.TextOrCaption != "" {
		t.Fatalf("stale payload delivered: %+v", e)
	}
}

func TestExclusiveBridgeLockAndFullDurability(t *testing.T) {
	cfg, _ := fixture(t)
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := OpenCapture(cfg); err == nil {
		t.Fatal("second bridge opened same state")
	}
	var synchronous int
	if err := c.DB.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if synchronous != 2 {
		t.Fatalf("synchronous=%d, want FULL=2", synchronous)
	}
}

func TestStoreReplacementWithSameAnchorIsSilent(t *testing.T) {
	cfg, source := fixture(t)
	add(t, source, groupJID, "same-id", "old")
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	id, _, _, err := c.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Accept(ctx, id); err != nil {
		t.Fatal(err)
	}
	source.Close()
	path := filepath.Join(cfg.StoreDir, "wacli.db")
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	replacement, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	_, err = replacement.Exec(`CREATE TABLE messages (rowid INTEGER PRIMARY KEY AUTOINCREMENT, chat_jid TEXT, msg_id TEXT, sender_jid TEXT, ts INTEGER, from_me INTEGER, text TEXT, media_caption TEXT, deleted_at INTEGER, revoked INTEGER DEFAULT 0, payload_purged_at INTEGER, edited_ts INTEGER DEFAULT 0); CREATE TABLE message_payload_purges (chat_jid TEXT,msg_id TEXT,deleted_at INTEGER,purged_at INTEGER);`)
	if err != nil {
		t.Fatal(err)
	}
	add(t, replacement, groupJID, "same-id", "restored-old-copy")
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	_, b, _, err := c.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatal(err)
	}
	if e.TriageEligible {
		t.Fatal("replaced store backfill must be silent")
	}
}

func TestInitialTombstoneSweepDoesNotExpandBackfill(t *testing.T) {
	cfg, source := fixture(t)
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("historical-%d", i)
		add(t, source, groupJID, id, "old")
		if _, err := source.Exec(`UPDATE messages SET text=NULL,deleted_at=200 WHERE msg_id=?`, id); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("purged-%d", i)
		if _, err := source.Exec(`INSERT INTO message_payload_purges(chat_jid,msg_id,deleted_at,purged_at) VALUES(?,?,200,201)`, groupJID, id); err != nil {
			t.Fatal(err)
		}
	}
	add(t, source, groupJID, "recent-1", "first")
	add(t, source, groupJID, "recent-2", "second")
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := pending(t, c); n != 2 {
		t.Fatalf("initial outbox %d exceeds bounded backfill 2", n)
	}
}

func TestPendingEventsDroppedAfterAllowlistChange(t *testing.T) {
	cfg, source := fixture(t)
	add(t, source, groupJID, "old-allowlist", "private")
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := pending(t, c); n != 1 {
		t.Fatalf("pending=%d", n)
	}
	c.Close()
	cfg.ChatJIDs = []string{otherJID}
	c, err = OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if n := pending(t, c); n != 0 {
		t.Fatalf("old-allowlist payload retained: %d", n)
	}
	if _, _, _, err := c.Pending(context.Background()); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unauthorized pending event returned: %v", err)
	}
}

func TestOldSourceSchemaRequiresControlledMigration(t *testing.T) {
	cfg, source := fixture(t)
	if _, err := source.Exec(`ALTER TABLE messages DROP COLUMN edited_ts`); err != nil {
		t.Fatal(err)
	}
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.Reconcile(context.Background())
	if !errors.Is(err, ErrMigrationRequired) {
		t.Fatalf("migration error=%v", err)
	}
	if got := stateForCode(classify(err.Error())); got != "degraded_store" {
		t.Fatalf("migration state=%s", got)
	}
}

func TestStateDBCapIsVisible(t *testing.T) {
	cfg, _ := fixture(t)
	cfg.MaxStateDBBytes = 1
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.Reconcile(context.Background())
	if !errors.Is(err, ErrStateCapReached) {
		t.Fatalf("state cap error=%v", err)
	}
	if got := stateForCode(classify(err.Error())); got != "degraded_disk" {
		t.Fatalf("state cap state=%s", got)
	}
}

func TestStateCapStopsLargeScanBeforeCursorAdvancesPastAllRows(t *testing.T) {
	cfg, source := fixture(t)
	cfg.InitialBackfill = 0
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	baseline, err := StateSize(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	c.Config.MaxStateDBBytes = baseline + 256*1024
	for i := 0; i < 2500; i++ {
		add(t, source, groupJID, fmt.Sprintf("large-%04d", i), strings.Repeat("x", 1024))
	}
	_, err = c.Reconcile(ctx)
	if !errors.Is(err, ErrStateCapReached) {
		t.Fatalf("large scan cap error=%v", err)
	}
	v, err := c.meta("cursor")
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := strconv.Atoi(v)
	if err != nil {
		t.Fatal(err)
	}
	if cursor >= 2500 {
		t.Fatalf("scan committed all rows past cap: cursor=%d", cursor)
	}
}

func TestStateCapRollsBackInitialBackfill(t *testing.T) {
	cfg, source := fixture(t)
	cfg.InitialBackfill = 25
	for i := 0; i < 25; i++ {
		add(t, source, groupJID, fmt.Sprintf("initial-%02d", i), strings.Repeat("x", 16384))
	}
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	baseline, err := StateSize(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	c.Config.MaxStateDBBytes = baseline + 64*1024
	_, err = c.Reconcile(context.Background())
	if !errors.Is(err, ErrStateCapReached) {
		t.Fatalf("initial cap error=%v", err)
	}
	if _, err := c.meta("cursor"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("initial cursor committed after cap: %v", err)
	}
	if n := pending(t, c); n != 0 {
		t.Fatalf("initial outbox committed after cap: %d", n)
	}
}

func TestAcceptedAuditPrunesButSeenDedupeRemains(t *testing.T) {
	cfg, source := fixture(t)
	add(t, source, groupJID, "one", "example")
	c, err := OpenCapture(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	id, _, _, err := c.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Accept(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DB.Exec(`UPDATE outbox SET accepted_at='2020-01-01T00:00:00Z' WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var audits, seen int
	if err := c.DB.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if err := c.DB.QueryRow(`SELECT COUNT(*) FROM seen`).Scan(&seen); err != nil {
		t.Fatal(err)
	}
	if audits != 0 || seen != 1 {
		t.Fatalf("audits=%d seen=%d", audits, seen)
	}
	if n := pending(t, c); n != 0 {
		t.Fatalf("old message replayed: %d", n)
	}
}
