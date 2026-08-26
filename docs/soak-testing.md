# Bounded restart and fencing soak

The soak harness repeatedly exercises the existing worker-session fencing and real Kind serving
acceptance paths. Each round is bounded and includes:

- stale worker session, execution, lease, log, artifact, secret, and completion fencing tests;
- serving Pod deletion and replacement, controller restart adoption, and scale 2→4→1 with active
  SSE drain through the mandatory Kind E2E;
- an additional API/embedded-controller rollout restart;
- temporary Redis Pod deletion and recovery;
- final checks for managed Pod/Service leaks, active reservations, active service replicas, and
  non-zero active request counters.

Run only against the dedicated fixed Kind cluster name, which must not preexist:

```bash
CONFIRM_SOAK=YES SOAK_ROUNDS=3 make test-soak
```

`SOAK_ROUNDS` is restricted to 1–10. Commands default to a 600-second timeout and the complete run
to 7200 seconds. The harness always attempts credential-safe diagnostics and deletion of the cluster
it created. It refuses to delete a preexisting cluster. Per-round invariant snapshots, redacted logs,
cleanup status, and the evidence boundary are written under `build/soak/<run-id>/`.

The scheduled/manual workflow uses two rounds so it does not burden ordinary PR CI. This is a
single-host Kind/fake-inference reliability exercise, not an unbounded stress test, production SLO,
real GPU result, or proof of multi-node/high-availability behavior.
