// In-cluster k6 script for AdSelector. The runner supplies BASE_URL, QPS and
// DURATION through a ConfigMap (see deploy/k8s/k6-job.yaml and
// scripts/k8s-loadtest.sh).
//
// Traffic is deliberately aimed at campaigns that exist in the seed data:
//   DE + mobile/desktop -> camp-eu-any-device-1 (bid 3,200,000 micros)
//   US + mobile         -> camp-us-mobile-1    (bid 2,500,000 micros)
// camp-catch-all-low-bid is also eligible but has a lower bid, so it wins
// only when the targeted campaign is capped or out of budget.
//
// The budget invariant is NOT asserted here: k6 cannot read Redis. The
// runner checks the shared Redis keys after the Job finishes.

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://adselector:80';

const filledByCampaign = new Counter('adselector_filled_by_campaign');
const noFill = new Counter('adselector_no_fill_total');
const selectLatency = new Trend('adselector_select_duration', true);

export const options = {
  scenarios: {
    steady_qps: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.QPS || 200),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: Number(__ENV.VUS || 50),
      maxVUs: Number(__ENV.MAX_VUS || 300),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<250'],
  },
};

function profile() {
  if (Math.random() < 0.7) {
    return { geo: 'DE', device: Math.random() < 0.5 ? 'mobile' : 'desktop' };
  }
  return { geo: 'US', device: 'mobile' };
}

export default function () {
  const target = profile();
  const payload = JSON.stringify({
    user_id: `k8s-${__VU}-${__ITER}-${Math.floor(Math.random() * 1000000)}`,
    ad_slot_id: 'slot-homepage-top',
    geo: target.geo,
    device: target.device,
  });

  const res = http.post(`${BASE_URL}/v1/select`, payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '3s',
  });

  selectLatency.add(res.timings.duration);

  check(res, {
    'status is 200': (r) => r.status === 200,
  });

  if (res.status === 200) {
    const body = res.json();
    if (body && body.filled) {
      filledByCampaign.add(1, { campaign_id: body.campaign_id || 'unknown' });
    } else {
      noFill.add(1);
    }
  }
}
