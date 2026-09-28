import assert from 'node:assert/strict';
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const releaseWorkflow = readFileSync(`${root}/.github/workflows/release.yml`, 'utf8');
const darwinConfig = readFileSync(`${root}/.goreleaser.yaml`, 'utf8');
const releaseDocs = readFileSync(`${root}/docs/release.md`, 'utf8');

test('fork release builds both binaries without upstream services', () => {
  assert.doesNotMatch(releaseWorkflow, /openclaw\/release-workflows|homebrew-tap|MACOS_SIGNING/);
  assert.match(releaseWorkflow, /go build -tags sqlite_fts5.*cmd\/wacli/);
  assert.match(releaseWorkflow, /go build -tags sqlite_fts5.*cmd\/wacli-instinct/);
  assert.match(releaseWorkflow, /cp -R docs package\/docs/);
  assert.match(releaseWorkflow, /cp -R deploy\/instinct package\/deploy\/instinct/);
  assert.match(releaseWorkflow, /sha256sum \*\.tar\.gz > checksums\.txt/);
});

test('fork release publishes only on a tag with contents permission', () => {
  assert.match(releaseWorkflow, /tags: \["v\*-instinct\.\*"\]/);
  assert.match(releaseWorkflow, /contents: write/);
  assert.doesNotMatch(releaseWorkflow, /workflow_dispatch/);
});

test('legacy GoReleaser config is not used by the fork workflow', () => {
  assert.doesNotMatch(darwinConfig, /^universal_binaries:/m);
  assert.equal(existsSync(`${root}/.github/workflows/release-verify.yml`), false);
  assert.match(releaseDocs, /wacli-instinct/);
});

test('Darwin preparation requires a literally matching dated changelog heading', async (t) => {
  const fixture = realpathSync(mkdtempSync(path.join(tmpdir(), 'wacli-release-source-')));
  t.after(() => rmSync(fixture, { recursive: true, force: true }));
  cpSync(path.join(root, 'scripts'), path.join(fixture, 'scripts'), { recursive: true });
  mkdirSync(path.join(fixture, 'cmd/wacli'), { recursive: true });
  writeFileSync(path.join(fixture, 'cmd/wacli/root.go'), 'const sourceVersion = "0.17.2"\n');
  const goMod = readFileSync(path.join(root, 'go.mod'), 'utf8');
  writeFileSync(path.join(fixture, 'go.mod'), goMod);
  const { prepareDarwinRelease } = await import(
    pathToFileURL(path.join(fixture, 'scripts/release-local.mjs')).href
  );
  const commit = 'a'.repeat(40);

  for (const [name, heading, accepted] of [
    ['exact dated version', '## 0.17.2 - 2026-09-07', true],
    ['letter separators', '## 0x17y2 - 2026-09-07', false],
    ['digit separators', '## 001702 - 2026-09-07', false],
    ['different version', '## 0.17.3 - 2026-09-07', false],
    ['undated version', '## 0.17.2', false],
    ['malformed date', '## 0.17.2 - 2026-9-7', false],
    ['date format without calendar validation', '## 0.17.2 - 2026-99-99', true],
  ]) {
    await t.test(name, () => {
      writeFileSync(path.join(fixture, 'CHANGELOG.md'), `# Changelog\n\n${heading}\n\n- Fix.\n`);
      const sourceAccepted = new Error('source validation passed');
      const executionCalls = [];
      const run = (command, args, options) => {
        assert.equal(options.cwd, fixture);
        if (command === 'git') {
          if (args[0] === 'status' || args[0] === 'merge-base') return { stdout: '' };
          if (args[0] === 'rev-parse') return { stdout: commit };
          if (args[0] === 'show' && args[1] === `${commit}:go.mod`) return { stdout: goMod };
        }
        executionCalls.push([command, ...args]);
        throw sourceAccepted;
      };

      assert.throws(
        () => prepareDarwinRelease({
          tag: 'v0.17.2',
          commit,
          outputDir: path.join(fixture, 'candidate'),
          platform: 'darwin',
          env: {
            MAC_RELEASE_CODESIGN_IDENTITY: 'fixture-signing-identity',
            NOTARYTOOL_KEYCHAIN_PROFILE: 'fixture-notary-profile',
          },
          run,
        }),
        accepted
          ? (error) => error === sourceAccepted
          : /CHANGELOG\.md section 0\.17\.2 must be dated before official preparation/,
      );
      assert.deepEqual(executionCalls, accepted ? [['go', 'env', 'GOVERSION']] : []);
    });
  }
});
