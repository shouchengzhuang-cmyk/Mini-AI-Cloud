# X0: Mini v2 producer for GPU-Scheduler-Lab

`mini-cloud export scheduler-lab-v2` is a read-only, offline export of persisted Mini workers,
accelerator inventory, and tasks. It emits only
`mini-ai-cloud.gpu-scheduler-lab/v2`; there is no v1 fallback.

The command refuses an export when persisted data is ambiguous or cannot meet the v2 contract:

- a GPU task has no typed `accelerator_request_json`;
- count or memory differs from the normalized typed request;
- the selection policy is not `any`, which the Scheduler v2 consumer does not accept;
- an inventory device is fake, has an unknown vendor/kind pair, or invalid text metadata;
- inventory has a device whose worker is absent from the same snapshot.

The result uses stable row ordering and canonical JSON keys. Supply a SHA-bound `--producer` value;
for example, `mini-ai-cloud/9ef5dbf34e0e49b2a117d15a541d9cac919dd051`.

```text
mini-cloud export scheduler-lab-v2 \
  --database-url "$MINI_CLOUD_DATABASE_URL" \
  --producer "mini-ai-cloud/<exact-mini-sha>" \
  --output artifacts/x0/mini-v2.json
```

Run the consumer smoke from an independently checked-out Scheduler repository. It writes only a
derived Scheduler scenario and a small identity report; it neither connects the repositories at
runtime nor changes Mini scheduling state.

```text
python scripts/x0_producer_consumer_smoke.py \
  --input artifacts/x0/mini-v2.json \
  --output artifacts/x0/scheduler-scenario.json \
  --report artifacts/x0/x0-smoke.json \
  --scheduler-repo /path/to/GPU-Scheduler-Lab \
  --scheduler-python /path/to/GPU-Scheduler-Lab/.venv/bin/python \
  --mini-sha <exact-mini-sha> \
  --scheduler-sha <exact-scheduler-sha>
```

The report records the exact Mini and Scheduler SHAs, v2 contract identifier, input SHA-256 and
result SHA-256. A successful local smoke establishes only the software producer-to-consumer
contract. It does not create real hardware, deployment, or paid-GPU evidence.
