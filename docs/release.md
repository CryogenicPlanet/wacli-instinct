# Fork release

This fork packages `wacli` and `wacli-instinct` together. It has no dependency on OpenClaw release workflows, signing credentials, Pages, or Homebrew tap. The fork is MIT licensed under the upstream `LICENSE`.

The tag workflow `.github/workflows/release.yml` builds native Linux amd64, macOS arm64, and macOS amd64 archives on GitHub runners, tests the bridge, publishes SHA-256 checksums, and creates a GitHub release. Tags must match `v*-instinct.*`, for example `v0.19.0-instinct.1`. Protect release tags and review the source commit before tagging. These are unsigned binaries; consumers should verify the checksums and build provenance appropriate to their environment.

For a local development build:

```sh
CGO_ENABLED=1 CGO_CFLAGS=-Wno-error=missing-braces go build -tags sqlite_fts5 -o dist/wacli ./cmd/wacli
CGO_ENABLED=1 CGO_CFLAGS=-Wno-error=missing-braces go build -tags sqlite_fts5 -o dist/wacli-instinct ./cmd/wacli-instinct
```

The dedicated worker image is built with `docker build -f Dockerfile.instinct -t wacli-instinct:local .`. Build contexts and release archives exclude databases, token files, QR payloads, pairing codes, and live configuration. Deploy only after reviewing [Instinct deployment](instinct.md).
