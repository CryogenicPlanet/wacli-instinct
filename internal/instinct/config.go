package instinct

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config is deliberately explicit: a display name can never select a chat.
type Config struct {
	AccountID       string
	ChatJIDs        []string
	StoreDir        string
	StateDir        string
	WacliBin        string
	IngestURL       string
	TokenFile       string
	HTTPAddr        string
	ScanInterval    time.Duration
	InitialBackfill int
	LookbackRows    int64
	MaxDBSize       string
	MaxMessages     int64
	MaxStateDBBytes int64
}

func ConfigFromEnv() (Config, error) {
	c := Config{
		AccountID: os.Getenv("INSTINCT_ACCOUNT_ID"),
		StoreDir:  os.Getenv("INSTINCT_STORE_DIR"),
		StateDir:  os.Getenv("INSTINCT_STATE_DIR"),
		WacliBin:  os.Getenv("INSTINCT_WACLI_BIN"),
		IngestURL: os.Getenv("INSTINCT_INGEST_URL"),
		TokenFile: os.Getenv("INSTINCT_INGEST_TOKEN_FILE"),
		HTTPAddr:  os.Getenv("INSTINCT_HTTP_ADDR"),
		MaxDBSize: os.Getenv("INSTINCT_MAX_DB_SIZE"),
	}
	if c.WacliBin == "" {
		c.WacliBin = "wacli"
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = "127.0.0.1:8080"
	}
	if c.MaxDBSize == "" {
		c.MaxDBSize = "2GB"
	}
	c.ScanInterval = 30 * time.Second
	c.InitialBackfill = 100
	c.LookbackRows = 2000
	c.MaxMessages = 250000
	c.MaxStateDBBytes = 512 << 20
	var err error
	if s := os.Getenv("INSTINCT_SCAN_INTERVAL"); s != "" {
		c.ScanInterval, err = time.ParseDuration(s)
		if err != nil {
			return c, fmt.Errorf("INSTINCT_SCAN_INTERVAL: %w", err)
		}
	}
	if s := os.Getenv("INSTINCT_CHAT_JIDS"); s != "" {
		c.ChatJIDs = strings.Split(s, ",")
	}
	if s := os.Getenv("INSTINCT_MAX_MESSAGES"); s != "" {
		c.MaxMessages, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			return c, errors.New("INSTINCT_MAX_MESSAGES must be a positive integer")
		}
	}
	if s := os.Getenv("INSTINCT_MAX_STATE_DB_BYTES"); s != "" {
		c.MaxStateDBBytes, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			return c, errors.New("INSTINCT_MAX_STATE_DB_BYTES must be a positive integer")
		}
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.AccountID == "" || strings.ContainsAny(c.AccountID, " \r\n\t") {
		return errors.New("INSTINCT_ACCOUNT_ID must be a nonempty opaque ID")
	}
	if c.StoreDir == "" || c.StateDir == "" {
		return errors.New("INSTINCT_STORE_DIR and INSTINCT_STATE_DIR are required")
	}
	if !filepath.IsAbs(c.StoreDir) || !filepath.IsAbs(c.StateDir) || filepath.Clean(c.StoreDir) == filepath.Clean(c.StateDir) {
		return errors.New("store and state must be distinct absolute paths")
	}
	if filepath.Dir(c.StoreDir) != filepath.Dir(c.StateDir) || filepath.Base(c.StoreDir) != "store" || filepath.Base(c.StateDir) != "state" {
		return errors.New("store and state must be sibling directories named store and state")
	}
	if len(c.ChatJIDs) == 0 {
		return errors.New("INSTINCT_CHAT_JIDS requires verified group JIDs")
	}
	seen := map[string]bool{}
	for _, jid := range c.ChatJIDs {
		if len(jid) <= len("@g.us") || !strings.HasSuffix(jid, "@g.us") || strings.ContainsAny(jid, " /\r\n\t") || seen[jid] {
			return errors.New("INSTINCT_CHAT_JIDS must contain distinct group JIDs")
		}
		seen[jid] = true
	}
	if c.IngestURL == "" || c.TokenFile == "" {
		return errors.New("INSTINCT_INGEST_URL and INSTINCT_INGEST_TOKEN_FILE are required")
	}
	if !strings.HasPrefix(c.IngestURL, "https://") {
		return errors.New("ingest URL must use HTTPS")
	}
	u, err := url.Parse(c.IngestURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return errors.New("invalid HTTPS ingest URL")
	}
	if !filepath.IsAbs(c.TokenFile) {
		return errors.New("ingest token file path must be absolute")
	}
	host, _, err := net.SplitHostPort(c.HTTPAddr)
	if err != nil || (host != "127.0.0.1" && host != "localhost" && host != "::1") {
		return errors.New("health listener must bind to loopback")
	}
	if c.ScanInterval < time.Second || c.InitialBackfill < 0 || c.InitialBackfill > 1000 || c.LookbackRows < 1 || c.LookbackRows > 100000 {
		return errors.New("invalid scan/backfill limits")
	}
	if c.MaxMessages < 1 {
		return errors.New("max messages must be positive")
	}
	if c.MaxStateDBBytes < 1 {
		return errors.New("state DB byte cap must be positive")
	}
	return nil
}
