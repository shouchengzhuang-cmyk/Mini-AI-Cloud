# X0: Mini v2 producer for GPU-Scheduler-Lab

`mini-cloud export scheduler-lab-v2` is a read-only, offline export of persisted Mini workers,
accelerator inventory, and tasks. It emits only
`mini-ai-cloud.gpu-scheduler-lab/v2`; there is no v1 fallback.

The command refuses an export when persisted data is ambiguous or cannot meet the v2 contract:

- a GPU task has no typed `accelerator_request_json`;
- count or memory differs from the normalized typed request;
- a task requests one or more accelerators with non-positive memory per device;
- the selection policy is not `any`, which the Scheduler v2 consumer does not accept;
- an inventory device is fake, has an unknown vendor/kind pair, or invalid text metadata;
- a device UUID appears more than once anywhere in the snapshot;
- inventory has a device whose worker is absent from the same snapshot.

PostgreSQL reads workers, devices and tasks in one read-only `REPEATABLE READ` transaction.
The result uses stable row ordering and canonical JSON keys. Set `--producer` to
`mini-ai-cloud/<exact-mini-sha>`, using the Mini commit that produced the export.

```text
mini-cloud export scheduler-lab-v2 \
  --database-url "$MINI_CLOUD_DATABASE_URL" \
  --producer "mini-ai-cloud/<exact-mini-sha>" \
  --output /path/outside/checkouts/x0/mini-v2.json
```

Run the smoke with clean Mini and Scheduler checkouts whose Git origins point to the canonical
GitHub repositories. Keep the three artifact paths distinct and outside both checkouts so the
artifacts don't dirty either working tree. Install the Scheduler dependencies in the Python
environment passed to `--scheduler-python`.

```text
python scripts/x0_producer_consumer_smoke.py \
  --input /path/outside/checkouts/x0/mini-v2.json \
  --output /path/outside/checkouts/x0/scheduler-scenario.json \
  --report /path/outside/checkouts/x0/x0-smoke.json \
  --scheduler-repo /path/to/GPU-Scheduler-Lab \
  --scheduler-python /path/to/GPU-Scheduler-Lab/.venv/bin/python \
  --mini-sha <exact-mini-sha> \
  --scheduler-sha <exact-scheduler-sha>
```

The smoke verifies both Git HEADs, the input producer label, and the loaded Scheduler entry
point before running the importer. The report records the verified SHAs, v2 contract identifier,
input SHA-256 and result SHA-256. X0 acceptance uses a separately recorded run against Mini's
persisted data.
