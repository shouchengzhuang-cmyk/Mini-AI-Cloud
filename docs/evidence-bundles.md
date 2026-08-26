# Commit-bound evidence bundles

`mini-cloud evidence collect` creates a credential-safe bundle under
`build/evidence/<full-git-sha>/`. The collector refuses a dirty worktree by default. `--allow-dirty`
is only for non-release diagnostics and records both `dirty: true` and `allow_dirty: true`.

```bash
make evidence
# or
uv run mini-cloud evidence collect --deployment-status NOT_DEPLOYED
```

The bundle contains:

- `manifest.json`: exact commit, dirty marker, execution time, deployment status, claim status,
  contract hashes, artifact hashes, and known limitations;
- `claims.json`: deterministic projection of the committed evidence contract;
- `commands.json`: actual version and Git probes with exit codes and redacted output;
- `environment.json`: Python, OS, architecture, and optional tool versions without environment
  variables;
- `summary.md`: human-readable projection of the same claim status;
- `hashes.sha256`: checksums for every bundle artifact other than the checksum file itself;
- `test-results/` and `diagnostics/`: explicit collection boundaries, never inferred PASS results.

`PENDING`, `NOT_RUN`, missing tools, and an absent artifact are never promoted to `PASS`. The
collector does not read kubeconfig, CLI auth config, database contents, passwords, tokens, or the
process environment. Command output is passed through credential redaction before it is written.

GitHub Actions collects and uploads the exact `${{ github.sha }}` directory as a short-lived CI
artifact. An uploaded bundle is evidence for that commit only; it does not imply deployment or a
GitHub Release.
