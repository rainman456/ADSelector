# Kubernetes scaling results

Status: **template only. No number below has been measured.** Fill this in
only after running the commands in `docs/verify.md` section 7.

Machine / cluster placeholder:

- CPU:
- Memory:
- Nodes / instance type:
- Kubernetes version:
- k6 image: `grafana/k6:0.54.0`
- Date:

Run shape: `scripts/loadtest-k8s.js` sends 70% DE (mobile/desktop, targets
`camp-eu-any-device-1`) and 30% US/mobile (targets `camp-us-mobile-1`), at a
constant arrival rate. The runner resets the seeded Redis budgets, pins the
HPA to the requested replica count, runs the Job, and then checks every
`budget:*` key.

| Replicas | QPS target | p50 (ms) | p95 (ms) | p99 (ms) | RPS achieved | Error rate | Budget invariant | Notes |
|----------|-----------:|---------:|---------:|---------:|-------------:|-----------:|------------------|-------|
| 1        | 200        | not yet measured | not yet measured | not yet measured | not yet measured | not yet measured | not yet measured | |
| 4        | 400        | not yet measured | not yet measured | not yet measured | not yet measured | not yet measured | not yet measured | |

## How to read this

- p50/p95/p99 and RPS come from the k6 summary printed by
  `scripts/k8s-loadtest.sh`.
- Error rate is the k6 `http_req_failed` rate; the Job fails above 1%.
- Budget invariant passes when every Redis `budget:*` value is in
  `[0, seeded]`. Because all replicas share the same Lua-scripted counter,
  this is the distributed no-overspend proof, not a per-pod sample.
- Do not compare rows from different machines. Put the machine details
  above and keep the shape identical between runs.

## Known limits of this harness

- Single-node kind/minikube is not a throughput benchmark. It shows the
  correctness invariant and a usable latency shape, not production
  capacity.
- Redis is a single subchart instance. A production cluster would use a
  managed or replicated Redis and should re-run the invariant against that
  endpoint.
- k6 runs inside the cluster, so it competes with the API for CPU. Keep the
  Job's CPU limit in mind when reading p99.
