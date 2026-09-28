package instinct

import (
	"path/filepath"
	"testing"
)

func TestConfigReadsMessageAndStateCaps(t *testing.T) {
	root := t.TempDir()
	t.Setenv("INSTINCT_ACCOUNT_ID", "example")
	t.Setenv("INSTINCT_STORE_DIR", filepath.Join(root, "store"))
	t.Setenv("INSTINCT_STATE_DIR", filepath.Join(root, "state"))
	t.Setenv("INSTINCT_CHAT_JIDS", "120363000000000000@g.us")
	t.Setenv("INSTINCT_INGEST_URL", "https://ingest.example.test/events")
	t.Setenv("INSTINCT_INGEST_TOKEN_FILE", filepath.Join(root, "token"))
	t.Setenv("INSTINCT_MAX_MESSAGES", "1234")
	t.Setenv("INSTINCT_MAX_STATE_DB_BYTES", "1048576")
	c, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxMessages != 1234 || c.MaxStateDBBytes != 1048576 {
		t.Fatalf("caps: messages=%d state=%d", c.MaxMessages, c.MaxStateDBBytes)
	}
}
