package instinct

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/openclaw/wacli/internal/lock"
)

type Envelope struct {
	EventType      string `json:"event_type"`
	AccountID      string `json:"account_id"`
	ChatJID        string `json:"chat_jid,omitempty"`
	MessageID      string `json:"msg_id,omitempty"`
	SenderJID      string `json:"sender_jid,omitempty"`
	Timestamp      string `json:"timestamp,omitempty"`
	TextOrCaption  string `json:"text_or_caption,omitempty"`
	FromMe         bool   `json:"from_me,omitempty"`
	ObservedAt     string `json:"observed_at"`
	TriageEligible bool   `json:"triage_eligible"`
	Revision       string `json:"revision,omitempty"`
	ErrorCode      string `json:"error_code,omitempty"`
}

type Capture struct {
	Config     Config
	DB         *sql.DB
	bridgeLock *lock.Lock
}

var ErrMigrationRequired = errors.New("wacli schema migration required")
var ErrStateCapReached = errors.New("state database size cap reached")

func OpenCapture(c Config) (*Capture, error) {
	if err := privateDir(c.StateDir); err != nil {
		return nil, err
	}
	bridgeLock, err := lock.Acquire(c.StateDir)
	if err != nil {
		return nil, err
	}
	p := filepath.Join(c.StateDir, "instinct.db")
	if err := rejectSymlink(p); err != nil {
		bridgeLock.Release()
		return nil, err
	}
	db, err := sql.Open("sqlite3", p+"?_busy_timeout=5000&_journal_mode=WAL&_synchronous=FULL")
	if err != nil {
		bridgeLock.Release()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS seen (account_id TEXT NOT NULL, chat_jid TEXT NOT NULL, msg_id TEXT NOT NULL, revision TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(account_id,chat_jid,msg_id));
CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT NOT NULL, chat_jid TEXT NOT NULL DEFAULT '', msg_id TEXT NOT NULL DEFAULT '', revision TEXT NOT NULL DEFAULT '', idempotency_key TEXT NOT NULL UNIQUE, payload BLOB, accepted_at TEXT);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(accepted_at,id);`); err != nil {
		db.Close()
		bridgeLock.Release()
		return nil, err
	}
	for _, name := range []string{"instinct.db", "instinct.db-wal", "instinct.db-shm"} {
		if err := privateFileIfExists(filepath.Join(c.StateDir, name)); err != nil {
			db.Close()
			bridgeLock.Release()
			return nil, err
		}
	}
	capture := &Capture{Config: c, DB: db, bridgeLock: bridgeLock}
	if err := capture.PruneDisallowed(context.Background()); err != nil {
		capture.Close()
		return nil, err
	}
	return capture, nil
}

func (c *Capture) Close() error { err := c.DB.Close(); _ = c.bridgeLock.Release(); return err }

func (c *Capture) Reconcile(ctx context.Context) (int, error) {
	if err := c.PruneAccepted(ctx); err != nil {
		return 0, err
	}
	if err := c.checkStateCap(); err != nil {
		return 0, err
	}
	path := filepath.Join(c.Config.StoreDir, "wacli.db")
	if err := rejectSymlink(path); err != nil {
		return 0, err
	}
	if _, err := os.Stat(path); err != nil {
		return 0, err
	}
	sourceID, err := sourceIdentity(path)
	if err != nil {
		return 0, err
	}
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro&_busy_timeout=5000"}
	source, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return 0, err
	}
	defer source.Close()
	source.SetMaxOpenConns(1)
	stx, err := source.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, err
	}
	defer stx.Rollback()
	if err := preflightSourceSchema(ctx, stx); err != nil {
		return 0, err
	}
	var maxRow int64
	if err := stx.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid),0) FROM messages`).Scan(&maxRow); err != nil {
		return 0, fmt.Errorf("read source messages: %w", err)
	}
	var cursor int64
	var initialized bool
	if v, err := c.meta("cursor"); err == nil {
		initialized = true
		_, err = fmt.Sscan(v, &cursor)
		if err != nil {
			return 0, err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if initialized {
		want, _ := c.meta("source_identity")
		if want != sourceID {
			cursor = 0
			initialized = false
		}
	}
	if initialized && cursor > 0 {
		var anchor string
		_ = stx.QueryRowContext(ctx, `SELECT chat_jid || ':' || msg_id FROM messages WHERE rowid=?`, cursor).Scan(&anchor)
		want, _ := c.meta("anchor")
		if cursor > maxRow || anchor != want {
			cursor = 0
			initialized = false
		}
	}
	if !initialized {
		// New/replaced stores get a bounded, silent sample; historical rows never alert.
		if err := c.initialBackfill(ctx, stx, maxRow, sourceID); err != nil {
			return 0, err
		}
		if err := c.reconcileTombstones(ctx, stx); err != nil {
			return 0, err
		}
		if err := c.reconcilePurges(ctx, stx); err != nil {
			return 0, err
		}
		return c.Config.InitialBackfill, nil
	}
	start := cursor - c.Config.LookbackRows
	if start < 0 {
		start = 0
	}
	count := 0
	for {
		rows, err := stx.QueryContext(ctx, `SELECT rowid, chat_jid, msg_id, COALESCE(sender_jid,''), ts, from_me,
			COALESCE(NULLIF(text,''),NULLIF(media_caption,''),''), COALESCE(deleted_at,0), revoked, COALESCE(payload_purged_at,0), COALESCE(edited_ts,0)
			FROM messages WHERE rowid>? ORDER BY rowid LIMIT 1000`, start)
		if err != nil {
			return count, err
		}
		batch := make([]sourceMessage, 0, 1000)
		for rows.Next() {
			var m sourceMessage
			var fromMe, revoked int
			if err := rows.Scan(&m.RowID, &m.ChatJID, &m.MessageID, &m.SenderJID, &m.TS, &fromMe, &m.Text, &m.DeletedAt, &revoked, &m.PurgedAt, &m.EditedTS); err != nil {
				rows.Close()
				return count, err
			}
			m.FromMe = fromMe != 0
			m.Deleted = m.DeletedAt != 0 || revoked != 0 || m.PurgedAt != 0
			batch = append(batch, m)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return count, err
		}
		if len(batch) == 0 {
			break
		}
		if err := c.checkStateCap(); err != nil {
			return count, err
		}
		tx, err := c.DB.BeginTx(ctx, nil)
		if err != nil {
			return count, err
		}
		batchCount := 0
		for _, m := range batch {
			if c.allowed(m.ChatJID) && !m.FromMe {
				n, err := c.queueMessage(ctx, tx, m, false, false)
				if err != nil {
					tx.Rollback()
					return count, err
				}
				batchCount += n
			}
			start = m.RowID
			if m.RowID > cursor {
				cursor = m.RowID
			}
		}
		if err := setMetaTx(ctx, tx, "cursor", fmt.Sprint(cursor)); err != nil {
			tx.Rollback()
			return count, err
		}
		if cursor > 0 {
			var anchor string
			if err := stx.QueryRowContext(ctx, `SELECT chat_jid || ':' || msg_id FROM messages WHERE rowid=?`, cursor).Scan(&anchor); err != nil {
				tx.Rollback()
				return count, err
			}
			if err := setMetaTx(ctx, tx, "anchor", anchor); err != nil {
				tx.Rollback()
				return count, err
			}
		}
		if err := c.checkStateCapTx(ctx, tx); err != nil {
			tx.Rollback()
			return count, err
		}
		if err := tx.Commit(); err != nil {
			return count, err
		}
		count += batchCount
		if len(batch) < 1000 {
			break
		}
	}
	// The purge ledger survives row deletion. Missing rows alone never imply deletion.
	if err := c.reconcileTombstones(ctx, stx); err != nil {
		return count, err
	}
	if err := c.reconcilePurges(ctx, stx); err != nil {
		return count, err
	}
	return count, nil
}

func preflightSourceSchema(ctx context.Context, tx *sql.Tx) error {
	required := map[string]bool{"chat_jid": false, "msg_id": false, "sender_jid": false, "ts": false, "from_me": false, "text": false, "media_caption": false, "deleted_at": false, "revoked": false, "payload_purged_at": false, "edited_ts": false}
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(messages)`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		if _, ok := required[name]; ok {
			required[name] = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, present := range required {
		if !present {
			return ErrMigrationRequired
		}
	}
	var ledger string
	err = tx.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name='message_payload_purges'`).Scan(&ledger)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrMigrationRequired
	}
	return err
}

type sourceMessage struct {
	RowID                               int64
	ChatJID, MessageID, SenderJID, Text string
	TS, DeletedAt, PurgedAt, EditedTS   int64
	FromMe, Deleted                     bool
}

func (c *Capture) initialBackfill(ctx context.Context, stx *sql.Tx, maxRow int64, sourceID string) error {
	if err := c.checkStateCap(); err != nil {
		return err
	}
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if c.Config.InitialBackfill > 0 {
		args := make([]any, 0, len(c.Config.ChatJIDs)+1)
		for _, jid := range c.Config.ChatJIDs {
			args = append(args, jid)
		}
		args = append(args, c.Config.InitialBackfill)
		query := fmt.Sprintf(`SELECT rowid, chat_jid, msg_id, COALESCE(sender_jid,''), ts, from_me,
			COALESCE(NULLIF(text,''),NULLIF(media_caption,''),''), COALESCE(deleted_at,0), revoked, COALESCE(payload_purged_at,0), COALESCE(edited_ts,0)
			FROM messages WHERE chat_jid IN (%s) AND from_me=0 ORDER BY rowid DESC LIMIT ?`, strings.TrimRight(strings.Repeat("?,", len(c.Config.ChatJIDs)), ","))
		rows, err := stx.QueryContext(ctx, query, args...)
		if err != nil {
			tx.Rollback()
			return err
		}
		n := 0
		for rows.Next() {
			var m sourceMessage
			var fm, rv int
			if err := rows.Scan(&m.RowID, &m.ChatJID, &m.MessageID, &m.SenderJID, &m.TS, &fm, &m.Text, &m.DeletedAt, &rv, &m.PurgedAt, &m.EditedTS); err != nil {
				rows.Close()
				tx.Rollback()
				return err
			}
			m.Deleted = m.DeletedAt != 0 || rv != 0 || m.PurgedAt != 0
			if c.allowed(m.ChatJID) {
				if _, err := c.queueMessage(ctx, tx, m, true, false); err != nil {
					rows.Close()
					tx.Rollback()
					return err
				}
				n++
				if n >= c.Config.InitialBackfill {
					break
				}
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	if err := setMetaTx(ctx, tx, "cursor", fmt.Sprint(maxRow)); err != nil {
		tx.Rollback()
		return err
	}
	if err := setMetaTx(ctx, tx, "source_identity", sourceID); err != nil {
		tx.Rollback()
		return err
	}
	if maxRow > 0 {
		var anchor string
		if err := stx.QueryRowContext(ctx, `SELECT chat_jid || ':' || msg_id FROM messages WHERE rowid=?`, maxRow).Scan(&anchor); err != nil {
			tx.Rollback()
			return err
		}
		if err := setMetaTx(ctx, tx, "anchor", anchor); err != nil {
			tx.Rollback()
			return err
		}
	}
	if err := c.checkStateCapTx(ctx, tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (c *Capture) reconcilePurges(ctx context.Context, stx *sql.Tx) error {
	rows, err := stx.QueryContext(ctx, `SELECT p.chat_jid,p.msg_id,p.deleted_at,p.purged_at FROM message_payload_purges p WHERE NOT EXISTS (SELECT 1 FROM messages m WHERE m.chat_jid=p.chat_jid AND m.msg_id=p.msg_id)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var m sourceMessage
		if err := rows.Scan(&m.ChatJID, &m.MessageID, &m.DeletedAt, &m.PurgedAt); err != nil {
			return err
		}
		if !c.allowed(m.ChatJID) {
			continue
		}
		m.Deleted = true
		if err := c.checkStateCap(); err != nil {
			return err
		}
		tx, err := c.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := c.queueMessage(ctx, tx, m, false, true); err != nil {
			tx.Rollback()
			return err
		}
		if err := c.checkStateCapTx(ctx, tx); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (c *Capture) reconcileTombstones(ctx context.Context, stx *sql.Tx) error {
	rows, err := stx.QueryContext(ctx, `SELECT rowid,chat_jid,msg_id,COALESCE(sender_jid,''),ts,from_me,'',COALESCE(deleted_at,0),revoked,COALESCE(payload_purged_at,0),COALESCE(edited_ts,0) FROM messages WHERE deleted_at IS NOT NULL OR revoked!=0 OR payload_purged_at IS NOT NULL`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var m sourceMessage
		var fm, rv int
		if err := rows.Scan(&m.RowID, &m.ChatJID, &m.MessageID, &m.SenderJID, &m.TS, &fm, &m.Text, &m.DeletedAt, &rv, &m.PurgedAt, &m.EditedTS); err != nil {
			return err
		}
		if !c.allowed(m.ChatJID) || fm != 0 {
			continue
		}
		m.Deleted = true
		if err := c.checkStateCap(); err != nil {
			return err
		}
		tx, err := c.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := c.queueMessage(ctx, tx, m, false, true); err != nil {
			tx.Rollback()
			return err
		}
		if err := c.checkStateCapTx(ctx, tx); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (c *Capture) queueMessage(ctx context.Context, tx *sql.Tx, m sourceMessage, silent, requireSeen bool) (int, error) {
	event := "message"
	if m.Deleted {
		event = "message_deleted"
		m.Text = ""
	}
	revisionData := fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d\x00%d\x00%d\x00%t", event, m.SenderJID, m.Text, m.TS, m.EditedTS, m.DeletedAt, m.PurgedAt, m.FromMe)
	h := sha256.Sum256([]byte(revisionData))
	revision := hex.EncodeToString(h[:])
	var previous string
	var previouslyDeleted int
	err := tx.QueryRowContext(ctx, `SELECT revision,deleted FROM seen WHERE account_id=? AND chat_jid=? AND msg_id=?`, c.Config.AccountID, m.ChatJID, m.MessageID).Scan(&previous, &previouslyDeleted)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if requireSeen && errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if previous == revision {
		return 0, nil
	}
	if previouslyDeleted != 0 && !m.Deleted {
		return 0, nil
	}
	if previous != "" && !m.Deleted {
		event = "message_updated"
	}
	e := Envelope{EventType: event, AccountID: c.Config.AccountID, ChatJID: m.ChatJID, MessageID: m.MessageID, SenderJID: m.SenderJID, Timestamp: time.Unix(m.TS, 0).UTC().Format(time.RFC3339), TextOrCaption: m.Text, FromMe: m.FromMe, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), TriageEligible: !silent && !m.Deleted, Revision: revision}
	if m.TS == 0 {
		e.Timestamp = ""
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return 0, err
	}
	keyHash := sha256.Sum256([]byte(c.Config.AccountID + "\x00" + m.ChatJID + "\x00" + m.MessageID + "\x00" + revision))
	key := hex.EncodeToString(keyHash[:])
	if _, err := tx.ExecContext(ctx, `DELETE FROM outbox WHERE account_id=? AND chat_jid=? AND msg_id=? AND accepted_at IS NULL`, c.Config.AccountID, m.ChatJID, m.MessageID); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO outbox(account_id,chat_jid,msg_id,revision,idempotency_key,payload) VALUES(?,?,?,?,?,?)`, c.Config.AccountID, m.ChatJID, m.MessageID, revision, key, payload); err != nil {
		return 0, err
	}
	deleted := 0
	if m.Deleted {
		deleted = 1
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO seen(account_id,chat_jid,msg_id,revision,deleted) VALUES(?,?,?,?,?) ON CONFLICT(account_id,chat_jid,msg_id) DO UPDATE SET revision=excluded.revision,deleted=excluded.deleted`, c.Config.AccountID, m.ChatJID, m.MessageID, revision, deleted); err != nil {
		return 0, err
	}
	return 1, nil
}

func (c *Capture) QueueRepair(ctx context.Context) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var active string
	_ = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='repair_active'`).Scan(&active)
	if active == "1" {
		return tx.Rollback()
	}
	var counter int64
	_ = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='repair_counter'`).Scan(&counter)
	counter++
	e := Envelope{EventType: "re_pair_needed", AccountID: c.Config.AccountID, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	b, err := json.Marshal(e)
	if err != nil {
		tx.Rollback()
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO outbox(account_id,idempotency_key,payload) VALUES(?,?,?)`, c.Config.AccountID, fmt.Sprintf("repair:%s:%d", c.Config.AccountID, counter), b)
	if err != nil {
		tx.Rollback()
		return err
	}
	if err := setMetaTx(ctx, tx, "repair_counter", fmt.Sprint(counter)); err != nil {
		tx.Rollback()
		return err
	}
	if err := setMetaTx(ctx, tx, "repair_active", "1"); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (c *Capture) ClearRepair(ctx context.Context) error {
	_, err := c.DB.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('repair_active','0') ON CONFLICT(key) DO UPDATE SET value='0'`)
	return err
}

func (c *Capture) QueueOperational(ctx context.Context, code string) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var active string
	_ = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='alert_active'`).Scan(&active)
	if active == code {
		return tx.Rollback()
	}
	var counter int64
	_ = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='alert_counter'`).Scan(&counter)
	counter++
	e := Envelope{EventType: "worker_degraded", AccountID: c.Config.AccountID, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), ErrorCode: code}
	b, err := json.Marshal(e)
	if err != nil {
		tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox(account_id,idempotency_key,payload) VALUES(?,?,?)`, c.Config.AccountID, fmt.Sprintf("health:%s:%d", c.Config.AccountID, counter), b); err != nil {
		tx.Rollback()
		return err
	}
	if err := setMetaTx(ctx, tx, "alert_counter", fmt.Sprint(counter)); err != nil {
		tx.Rollback()
		return err
	}
	if err := setMetaTx(ctx, tx, "alert_active", code); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
func (c *Capture) ClearOperational(ctx context.Context) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM outbox WHERE idempotency_key LIKE 'health:%' AND accepted_at IS NULL`); err != nil {
		tx.Rollback()
		return err
	}
	if err := setMetaTx(ctx, tx, "alert_active", ""); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (c *Capture) Pending(ctx context.Context) (int64, []byte, string, error) {
	for {
		var id int64
		var b []byte
		var key, account, chat string
		err := c.DB.QueryRowContext(ctx, `SELECT id,payload,idempotency_key,account_id,chat_jid FROM outbox WHERE accepted_at IS NULL ORDER BY id LIMIT 1`).Scan(&id, &b, &key, &account, &chat)
		if err != nil {
			return 0, nil, "", err
		}
		if account == c.Config.AccountID && (chat == "" || c.allowed(chat)) {
			return id, b, key, nil
		}
		if _, err := c.DB.ExecContext(ctx, `DELETE FROM outbox WHERE id=? AND accepted_at IS NULL`, id); err != nil {
			return 0, nil, "", err
		}
	}
}

func (c *Capture) PruneDisallowed(ctx context.Context) error {
	args := make([]any, 0, len(c.Config.ChatJIDs)+1)
	args = append(args, c.Config.AccountID)
	for _, jid := range c.Config.ChatJIDs {
		args = append(args, jid)
	}
	query := fmt.Sprintf(`DELETE FROM outbox WHERE accepted_at IS NULL AND (account_id!=? OR (chat_jid!='' AND chat_jid NOT IN (%s)))`, strings.TrimRight(strings.Repeat("?,", len(c.Config.ChatJIDs)), ","))
	_, err := c.DB.ExecContext(ctx, query, args...)
	return err
}
func (c *Capture) Accept(ctx context.Context, id int64) error {
	_, err := c.DB.ExecContext(ctx, `UPDATE outbox SET accepted_at=?,payload=NULL WHERE id=? AND accepted_at IS NULL`, time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}
func (c *Capture) PruneAccepted(ctx context.Context) error {
	_, err := c.DB.ExecContext(ctx, `DELETE FROM outbox WHERE accepted_at IS NOT NULL AND julianday(accepted_at)<julianday('now','-30 days')`)
	return err
}
func (c *Capture) PendingCount(ctx context.Context) (int64, error) {
	var n int64
	err := c.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox WHERE accepted_at IS NULL`).Scan(&n)
	return n, err
}
func (c *Capture) meta(k string) (string, error) {
	var v string
	err := c.DB.QueryRow(`SELECT value FROM meta WHERE key=?`, k).Scan(&v)
	return v, err
}
func setMetaTx(ctx context.Context, tx *sql.Tx, k, v string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, k, v)
	return err
}
func (c *Capture) allowed(jid string) bool {
	for _, j := range c.Config.ChatJIDs {
		if j == jid {
			return true
		}
	}
	return false
}

func privateDir(p string) error {
	if err := rejectSymlink(p); err != nil {
		return err
	}
	if err := os.MkdirAll(p, 0700); err != nil {
		return err
	}
	return os.Chmod(p, 0700)
}
func privateFileIfExists(p string) error {
	if err := rejectSymlink(p); err != nil {
		return err
	}
	_, err := os.Stat(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return os.Chmod(p, 0600)
}
func rejectSymlink(p string) error {
	fi, err := os.Lstat(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("symlink is not permitted: %s", p)
	}
	return nil
}

func StoreSize(dir string) (int64, error) {
	var total int64
	for _, name := range []string{"session.db", "session.db-wal", "session.db-shm", "wacli.db", "wacli.db-wal", "wacli.db-shm"} {
		p := filepath.Join(dir, name)
		fi, err := os.Stat(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		total += fi.Size()
	}
	return total, nil
}
func StateSize(dir string) (int64, error) {
	var total int64
	for _, name := range []string{"instinct.db", "instinct.db-wal", "instinct.db-shm"} {
		fi, err := os.Stat(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		total += fi.Size()
	}
	return total, nil
}

func (c *Capture) checkStateCap() error {
	if c.Config.MaxStateDBBytes <= 0 {
		return nil
	}
	size, err := StateSize(c.Config.StateDir)
	if err != nil {
		return err
	}
	if size >= c.Config.MaxStateDBBytes {
		return ErrStateCapReached
	}
	return nil
}

func (c *Capture) checkStateCapTx(ctx context.Context, tx *sql.Tx) error {
	if err := c.checkStateCap(); err != nil || c.Config.MaxStateDBBytes <= 0 {
		return err
	}
	// SQLite may delay WAL writes until commit, so include pages allocated by
	// this transaction before advancing a cursor or accepting a new revision.
	var pages, pageSize int64
	if err := tx.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pages); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
		return err
	}
	if pages*pageSize >= c.Config.MaxStateDBBytes {
		return ErrStateCapReached
	}
	return nil
}

func sourceIdentity(path string) (string, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	v := reflect.ValueOf(fi.Sys())
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	dev, ino := v.FieldByName("Dev"), v.FieldByName("Ino")
	if dev.IsValid() && ino.IsValid() {
		return fmt.Sprintf("%v:%v", dev, ino), nil
	}
	// Non-Unix builds still use the cursor/anchor check.
	return "unsupported", nil
}
func PrepareStore(dir string) error {
	if err := privateDir(dir); err != nil {
		return err
	}
	for _, name := range []string{"session.db", "session.db-wal", "session.db-shm", "wacli.db", "wacli.db-wal", "wacli.db-shm", "SESSION_REVOKED"} {
		if err := privateFileIfExists(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	return nil
}
