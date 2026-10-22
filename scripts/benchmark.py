#!/usr/bin/env python3
"""Short reproducible 1/2/4-shard experiment, restoring the default stack.
This is a functional scaling comparison, not a saturation/capacity claim.
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / 'results'
DEFAULT = ['docker', 'compose', '-f', str(ROOT / 'deploy/docker-compose.yml')]
parser = argparse.ArgumentParser()
parser.add_argument('--seconds', type=int, default=20)
parser.add_argument('--rate', type=int, default=10)
args = parser.parse_args()
if args.seconds < 1 or args.rate < 1:
    parser.error('seconds and rate must be positive')
RESULTS.mkdir(exist_ok=True)
metadata = {'date_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'seconds': args.seconds, 'arrival_rate': args.rate, 'traffic': '80% local / 20% cross-shard; 1 shard necessarily 100% local', 'replays_per_transfer': 3}
for key, command in [('cpu', ['sysctl', '-n', 'machdep.cpu.brand_string']), ('memory_bytes', ['sysctl', '-n', 'hw.memsize']), ('docker', ['docker', 'info', '--format', '{{.NCPU}} CPUs, {{.MemTotal}} bytes RAM; Docker {{.ServerVersion}}; {{.OSType}}/{{.Architecture}}'])]:
    metadata[key] = subprocess.check_output(command, text=True).strip()
(RESULTS / 'hardware.json').write_text(json.dumps(metadata, indent=2) + '\n')

def run(command, log=None):
    if log:
        with log.open('w') as output:
            result = subprocess.run(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)
        if result.returncode:
            print(log.read_text()[-5000:])
            raise subprocess.CalledProcessError(result.returncode, command)
    else:
        subprocess.run(command, check=True, cwd=ROOT)

def ready():
    for _ in range(180):
        try:
            if urllib.request.urlopen('http://localhost:8080/readyz', timeout=3).status == 200:
                return
        except OSError:
            pass
        time.sleep(1)
    raise RuntimeError('API did not become ready')

active = None
try:
    run(DEFAULT + ['down'])
    for n in (1, 2, 4):
        run([sys.executable, str(ROOT / 'scripts/topology.py'), str(n)])
        active = ['docker', 'compose', '-f', str(ROOT / f'deploy/generated-{n}.json')]
        run(active + ['up', '-d'], RESULTS / f'start-{n}.log')
        ready()
        run(active + ['run', '--rm', '--user', f'{os.getuid()}:{os.getgid()}', '-v', f'{RESULTS}:/results', '-e', f'RATE={args.rate}', '-e', f'DURATION={args.seconds}s', 'k6', 'run', f'--summary-export=/results/shards-{n}.json', '/scripts/transfers.js'], RESULTS / f'load-{n}.log')
        run([sys.executable, str(ROOT / 'scripts/verify.py')], RESULTS / f'verify-{n}.log')
        run([sys.executable, str(ROOT / 'scripts/verify_events.py'), '--compose', str(ROOT / f'deploy/generated-{n}.json')], RESULTS / f'events-{n}.log')
        summary = json.loads((RESULTS / f'shards-{n}.json').read_text())['metrics']
        print(f"PASS: {n} shards: {summary['iterations']['count']} transfers, failures={summary['ledger_failures']['value']}", flush=True)
        run(active + ['down'])
        active = None
finally:
    if active:
        subprocess.run(active + ['down'], cwd=ROOT)
    run(DEFAULT + ['up', '-d'])
    ready()
