// k6 load-test script for AdSelector's POST /v1/select endpoint.
// Alternative to cmd/loadtest (the bespoke Go harness) for anyone who
// prefers k6's built-in percentile reporting, thresholds, and UI.
//
// NOTE: this has not been run as part of scaffolding this project. Once
// the server is up (`docker compose up` then `go run ./cmd/server`, or the
// adselector container from docker-compose), run it with:
//
//   k6 run scripts/loadtest.js
//   k6 run --env BASE_URL=http://localhost:8080 --env VUS=50 --env DURATION=30s scripts/loadtest.js
//
// The default options below aim for a comparable "N qps for 30s" shape to
// cmd/loadtest's defaults so the two harnesses' p99 numbers are roughly
// comparable in the README's demo writeup.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

const filledCounter = new Counter('adselector_filled_total');
const noFillCounter = new Counter('adselector_no_fill_total');
const selectLatency = new Trend('adselector_select_duration', true);

export const options = {
  scenarios: {
    steady_qps: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.QPS || 200),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: Number(__ENV.VUS || 50),
      maxVUs: Number(__ENV.MAX_VUS || 200),
    },
  },
  thresholds: {
    // Mirrors the "p99 latency at N qps" demo number this project's README
    // calls out as the marketing angle. Tune once real numbers exist.
    http_req_duration: ['p(99)<200'],
    http_req_failed: ['rate<0.01'],
  },
};

const GEOS = ['US', 'DE', 'FR', 'CA', 'BR'];
const DEVICES = ['mobile', 'desktop', 'ctv'];

function randomOf(list) {
  return list[Math.floor(Math.random() * list.length)];
}

export default function () {
  const payload = JSON.stringify({
    user_id: `user-${Math.floor(Math.random() * 10000)}`,
    ad_slot_id: 'slot-homepage-top',
    geo: randomOf(GEOS),
    device: randomOf(DEVICES),
  });

  const res = http.post(`${BASE_URL}/v1/select`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  selectLatency.add(res.timings.duration);

  check(res, {
    'status is 200': (r) => r.status === 200,
  });

  if (res.status === 200) {
    const body = res.json();
    if (body && body.filled) {
      filledCounter.add(1);
    } else {
      noFillCounter.add(1);
    }
  }

  sleep(0.01);
}
