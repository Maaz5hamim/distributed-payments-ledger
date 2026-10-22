import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';
const apiLatency = new Trend('transfer_request_duration_seconds');
const completion = new Trend('saga_completion_seconds');
const failures = new Rate('ledger_failures');
const base = __ENV.BASE_URL || 'http://localhost:8080';
const key = __ENV.API_KEY || 'local-development-key';
export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: { transfers: { executor: 'constant-arrival-rate', rate: Number(__ENV.RATE || 20), timeUnit: '1s', duration: __ENV.DURATION || '60s', preAllocatedVUs: 20, maxVUs: 200 } },
  thresholds: { 'transfer_request_duration_seconds{path:local}': ['p(99)<5'], 'transfer_request_duration_seconds{path:saga}': ['p(99)<5'], ledger_failures: ['rate==0'], http_req_failed: ['rate<0.01'], checks: ['rate==1'] },
};
function headers(id) { return { headers: { 'X-API-Key': key, 'Idempotency-Key': id, 'Content-Type': 'application/json' } }; }
function poll(id) {
  for (let i = 0; i < 600; i++) {
    const r = http.get(`${base}/operations/${id}`, headers(''));
    if (r.status === 200) { const j = r.json(); if (['COMPLETED', 'REVERSED', 'FAILED'].includes(j.state)) return j; }
    sleep(0.1);
  }
  throw new Error(`operation ${id} did not finish`);
}
export function setup() {
  const groups = {};
  for (let i = 0; i < 40; i++) {
    const r = http.post(`${base}/accounts`, JSON.stringify({ initial_deposit: 1000000000 }), headers(`setup-${Date.now()}-${i}`));
    if (![201, 202].includes(r.status)) throw new Error(r.body);
    const v = r.json(); const j = poll(v.operation_id);
    if (j.state !== 'COMPLETED') throw new Error(JSON.stringify(j));
    if (!groups[j.to_shard]) groups[j.to_shard] = [];
    groups[j.to_shard].push(v.id);
  }
  return groups;
}
export default function (groups) {
  const shards = Object.keys(groups).filter(s => groups[s].length >= 2);
  const source = shards[Math.floor(Math.random() * shards.length)];
  const cross = shards.length > 1 && Math.random() < 0.2;
  const other = shards.filter(s => s !== source);
  const dest = cross ? other[Math.floor(Math.random() * other.length)] : source;
  const from = groups[source][Math.floor(Math.random() * groups[source].length)];
  const candidates = groups[dest].filter(id => id !== from);
  const to = candidates[Math.floor(Math.random() * candidates.length)];
  const id = `load-${__VU}-${__ITER}-${Date.now()}`;
  const body = JSON.stringify({ from_account: from, to_account: to, amount: 1 });
  const begin = Date.now();
  const r = http.post(`${base}/transfers`, body, { ...headers(id), tags: { path: cross ? 'saga' : 'local' } });
  apiLatency.add(r.timings.duration / 1000, { path: cross ? 'saga' : 'local' });
  if (![201, 202].includes(r.status)) { failures.add(true); check(r, { accepted: () => false }); return; }
  const v = r.json();
  for (let i = 0; i < 3; i++) {
    const retry = http.post(`${base}/transfers`, body, headers(id));
    check(retry, { 'retry response identical': x => x.status === r.status && x.body === r.body });
  }
  const terminal = poll(v.id);
  completion.add((Date.now() - begin) / 1000, { path: cross ? 'saga' : 'local' });
  failures.add(terminal.state !== 'COMPLETED');
  check(terminal, { completed: x => x.state === 'COMPLETED' });
}
