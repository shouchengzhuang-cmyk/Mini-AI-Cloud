# Capability comparison and project boundary

This document explains scope, not product superiority. Mini AI Cloud is an evidence-driven
experimental control plane; KServe, Kueue, Volcano, and Ray Serve are mature projects with much
broader communities, integrations, operational history, and production adoption.

| Dimension | Mini AI Cloud | KServe | Kueue | Volcano | Ray Serve |
|---|---|---|---|---|---|
| Primary focus | End-to-end task/serving control-plane correctness experiments | Kubernetes model inference platform | Kubernetes-native queueing and admission | Kubernetes batch scheduling | Python/Ray model and application serving |
| Workload API | Repository-specific tasks and model services | InferenceService and related serving APIs | Workload/LocalQueue/ClusterQueue integrations | Job and scheduler extensions | Python deployment/application APIs |
| Scheduling model | Built-in CPU/memory/GPU device allocator plus experimental fairness | Delegates placement to Kubernetes and integrations | Admission, quotas, cohorts, borrowing | Scheduler plugins, queues, gang and batch policies | Ray resource scheduling and Serve placement |
| State truth | PostgreSQL desired/actual state with leases and fencing | Kubernetes API objects/controllers | Kubernetes API objects/controllers | Kubernetes API objects/controllers | Ray control plane and Serve state |
| Serving data plane | Experimental gateway, Fake and vLLM adapters | Production-oriented inference runtimes and networking integrations | Not a serving data plane | Not a serving data plane | Production-oriented Ray Serve data plane |
| Evidence in this repository | Unit/property, PostgreSQL, Docker, Kind, bounded soak and DR bundles | Not evaluated by this repository | Not evaluated by this repository | Not evaluated by this repository | Not evaluated by this repository |
| Verified scale here | Local/CI fixtures and one real Kind cluster | No comparative benchmark run | No comparative benchmark run | No comparative benchmark run | No comparative benchmark run |
| Real GPU status here | **NOT RUN** | No claim | No claim | No claim | No claim |

## What is intentionally different

- Mini AI Cloud keeps PostgreSQL as the authoritative state source to make execution fencing,
  outbox delivery, reconciliation, and database concurrency directly testable.
- Its built-in scheduling and simulated serving modes make failure cases reproducible on CPU-only
  developer machines. That is a portfolio/research trade-off, not a replacement argument.
- KServe and Ray Serve cover substantially deeper inference-runtime and production serving needs.
- Kueue and Volcano cover substantially deeper Kubernetes scheduling ecosystems and integrations.

## Non-comparability warnings

- No controlled head-to-head performance or reliability benchmark was run.
- Simulator throughput, Fake Serving latency, and Kind fixture results cannot be compared with
  production cluster throughput.
- A single-node Kind cluster is not evidence of multi-node HA, network partitions, storage HA, or
  managed-cloud operations.
- This repository does not claim to replace, outperform, or be production-equivalent to any
  project listed above.
