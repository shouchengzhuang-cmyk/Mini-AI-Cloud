# Isolated destructive disaster-recovery rehearsal

The DR harness uses a unique `mini-ai-cloud-local-dr-*` Compose project and the standalone
[`deploy/dr-rehearsal.compose.yml`](../deploy/dr-rehearsal.compose.yml). It exposes no host ports and
contains only PostgreSQL, the real Alembic migration path, and a synthetic marker tool.

```bash
CONFIRM_DR=YES make test-dr
```

The rehearsal:

1. builds the current commit and migrates an isolated PostgreSQL volume;
2. writes a marker Project, Task, timeline event, released reservation, usage row, artifact metadata,
   and artifact bytes;
3. performs the existing `backup.sh` flow and retains its manifest/checksum evidence;
4. resolves each target volume with both exact Compose project and logical-volume labels;
5. re-inspects those exact names immediately before deleting only the isolated PostgreSQL and
   artifact volumes;
6. restores through `restore.sh` and verifies schema version, marker identities, task/timeline,
   usage, artifact SHA-256/content, and zero active reservations;
7. removes the entire unique Compose project and all remaining volumes, then verifies no container
   or volume with that project label remains.

No glob or name prefix is used to select a destructive target. Missing confirmation, an unexpected
container, multiple volume matches, label mismatch, unsafe backup path, checksum failure, marker
drift, or incomplete cleanup fails closed. Redacted command logs and `summary.json` remain under
`build/dr-rehearsal/<run-id>/`; the temporary dump and synthetic stack are removed.

This proves one local, single-PostgreSQL backup/restore path. It is not production HA, point-in-time
recovery, multi-region recovery, or a recovery-time objective.
