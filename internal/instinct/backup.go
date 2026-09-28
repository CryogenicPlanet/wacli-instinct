package instinct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/openclaw/wacli/internal/lock"
)

// Backup creates a credential-grade offline snapshot. The caller must stop the
// worker first, so all three databases describe the same point in time.
func Backup(ctx context.Context, root, dest string) error {
	if !filepath.IsAbs(root) || !filepath.IsAbs(dest) {
		return errors.New("paths must be absolute")
	}
	if _, err := os.Stat(dest); err == nil {
		return errors.New("destination already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(dest)
	if err := requirePrivateParent(parent); err != nil {
		return err
	}
	// Hold both locks for the whole cross-database snapshot. The bridge owns the
	// state lock while running; wacli owns the store lock while syncing.
	stateLock, err := lock.Acquire(filepath.Join(root, "state"))
	if err != nil {
		return err
	}
	defer stateLock.Release()
	storeLock, err := lock.Acquire(filepath.Join(root, "store"))
	if err != nil {
		return err
	}
	defer storeLock.Release()
	stage, err := os.MkdirTemp(parent, ".instinct-backup-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	if err := os.Chmod(stage, 0700); err != nil {
		return err
	}
	for _, name := range []string{"store/session.db", "store/wacli.db", "state/instinct.db"} {
		source := filepath.Join(root, name)
		target := filepath.Join(stage, name)
		if err := privateDir(filepath.Dir(target)); err != nil {
			return err
		}
		if err := backupSQLite(ctx, source, target); err != nil {
			return fmt.Errorf("backup %s: %w", name, err)
		}
	}
	marker := filepath.Join(root, "store/SESSION_REVOKED")
	if _, err := os.Stat(marker); err == nil {
		if err := copyPrivate(marker, filepath.Join(stage, "store/SESSION_REVOKED")); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(stage, dest); err != nil {
		return err
	}
	return nil
}

// Restore requires a fresh, absent data root. Never restore into a running
// worker or clone the resulting linked-device identity to a second worker.
func Restore(ctx context.Context, snapshot, root string) error {
	if !filepath.IsAbs(snapshot) || !filepath.IsAbs(root) {
		return errors.New("paths must be absolute")
	}
	if _, err := os.Stat(root); err == nil {
		return errors.New("restore target already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(root)
	if err := requirePrivateParent(parent); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(parent, ".instinct-restore-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	if err := os.Chmod(stage, 0700); err != nil {
		return err
	}
	for _, name := range []string{"store/session.db", "store/wacli.db", "state/instinct.db"} {
		from := filepath.Join(snapshot, name)
		to := filepath.Join(stage, name)
		if err := privateDir(filepath.Dir(to)); err != nil {
			return err
		}
		if err := copyPrivate(from, to); err != nil {
			return fmt.Errorf("restore %s: %w", name, err)
		}
		if err := checkSQLite(ctx, to); err != nil {
			return fmt.Errorf("verify %s: %w", name, err)
		}
	}
	marker := filepath.Join(snapshot, "store/SESSION_REVOKED")
	if _, err := os.Stat(marker); err == nil {
		if err := copyPrivate(marker, filepath.Join(stage, "store/SESSION_REVOKED")); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(stage, root)
}

func backupSQLite(ctx context.Context, source, target string) error {
	if err := rejectSymlink(source); err != nil {
		return err
	}
	if err := checkSQLite(ctx, source); err != nil {
		return err
	}
	f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	f.Close()
	u := url.URL{Scheme: "file", Path: source, RawQuery: "mode=ro&_busy_timeout=5000"}
	src, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := sql.Open("sqlite3", target)
	if err != nil {
		return err
	}
	defer dst.Close()
	srcConn, err := src.Conn(ctx)
	if err != nil {
		return err
	}
	defer srcConn.Close()
	dstConn, err := dst.Conn(ctx)
	if err != nil {
		return err
	}
	defer dstConn.Close()
	err = srcConn.Raw(func(s any) error {
		return dstConn.Raw(func(d any) error {
			b, err := d.(*sqlite3.SQLiteConn).Backup("main", s.(*sqlite3.SQLiteConn), "main")
			if err != nil {
				return err
			}
			for {
				done, err := b.Step(100)
				if err != nil {
					_ = b.Finish()
					return err
				}
				if done {
					break
				}
				select {
				case <-ctx.Done():
					_ = b.Finish()
					return ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
			}
			return b.Finish()
		})
	})
	if err != nil {
		return err
	}
	if err := os.Chmod(target, 0600); err != nil {
		return err
	}
	return checkSQLite(ctx, target)
}

func checkSQLite(ctx context.Context, p string) error {
	if err := rejectSymlink(p); err != nil {
		return err
	}
	u := url.URL{Scheme: "file", Path: p, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return err
	}
	defer db.Close()
	var result string
	if err := db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return errors.New("SQLite integrity check failed")
	}
	return nil
}

func copyPrivate(source, target string) error {
	if err := rejectSymlink(source); err != nil {
		return err
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func requirePrivateParent(p string) error {
	if err := rejectSymlink(p); err != nil {
		return err
	}
	fi, err := os.Stat(p)
	if err != nil {
		return err
	}
	if !fi.IsDir() || fi.Mode().Perm()&0077 != 0 {
		return errors.New("destination parent must be a private 0700 directory on encrypted storage")
	}
	return nil
}
