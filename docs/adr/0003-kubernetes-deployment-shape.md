# ADR 0003: Kubernetes deployment shape

- **Status:** Accepted
- **Date:** 2026-11-19
- **Context:** The service needs a repeatable local Kubernetes deployment
  that demonstrates replica-safe budget enforcement, rolling updates, and
  autoscaling without hiding the moving parts behind a platform.

## Context

The application is a stateless HTTP deployment whose correctness depends on
a shared budget store. Deploying one pod with the in-memory backend is
simple but would not exercise the Redis path that makes multiple replicas
safe. Deploying raw YAML by hand would not give the portfolio reader a
single install/upgrade path.

Options considered:

- **Raw manifests / Kustomize:** transparent, but repeats values across
  objects and makes dependency wiring manual.
- **Helm chart:** one values file, dependency subcharts, `--wait`, and
  release-scoped names. Familiar to reviewers.
- **A managed platform (ECS/App Runner):** fewer Kubernetes concepts, but
  the task is explicitly a Kubernetes deployment and needs HPA/PDB.

## Decision

Ship `deploy/helm/adselector` with:

- `Deployment` with 2 replicas, `maxUnavailable: 0`, `maxSurge: 1`.
- `Service` of type `ClusterIP`; local proof uses `kubectl port-forward`,
  not NodePort or Ingress.
- `PodDisruptionBudget` with `minAvailable: 1`.
- `HorizontalPodAutoscaler` (`autoscaling/v2`, CPU 70%, min 2, max 10).
- `ConfigMap` for non-secret environment; no secrets in the chart.
- Distroless, non-root container (`65532`), read-only root filesystem,
  all capabilities dropped, seccomp `RuntimeDefault`.
- `terminationGracePeriodSeconds: 30` with a 10s in-process drain.
- Redis subchart `20.1.7`: standalone, auth disabled, no persistence, for
  local use only. Kafka subchart `5.7.19` (Redpanda): disabled by default,
  one broker, TLS/SASL off, only when `kafka.enabled` and
  `config.kafkaEnabled` are both true.
- `external.redisAddr` / `external.kafkaBroker` to point at managed services.

There is no `preStop` hook. The image is distroless and has no shell to run
one; correctness comes from the process handling `SIGTERM` and the pod's
grace period. Readiness and liveness both use `/healthz`.

## Consequences

- `helm upgrade --install --wait` is the single install path and the
  Makefile wraps it as `k8s-deploy`.
- The default install is replica-safe: two API pods, shared Redis, Kafka
  off, so no broker is required for the smoke test.
- Auth and persistence are off in Redis for local convenience. A production
  values overlay must enable them or use a managed endpoint; this chart is
  not claimed to be production-hardened.
- HPA needs metrics-server. kind does not include it, so
  `deploy/k8s/metrics-server-kind.yaml` installs v0.7.2 with
  `--kubelet-insecure-tls`. minikube uses its addon.
- No Ingress means the local URL is a port-forward on `18080`, not a stable
  hostname. That is deliberate for a laptop demo.
