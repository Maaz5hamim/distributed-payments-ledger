#!/usr/bin/env python3
"""Run one reversible local fault; run k6 alongside it. Always restore on exit."""
import argparse
import json
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ['docker', 'compose', '-f', str(ROOT / 'deploy/docker-compose.yml')]

def docker(*args):
    subprocess.run(COMPOSE + list(args), check=True)

def proxy(name, **fields):
    request = urllib.request.Request('http://localhost:8474/proxies/' + name,
        data=json.dumps(fields).encode(), headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)

def toxic(name, payload):
    request = urllib.request.Request(f'http://localhost:8474/proxies/{name}/toxics',
        data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)

def remove(name, label):
    request = urllib.request.Request(f'http://localhost:8474/proxies/{name}/toxics/{label}', method='DELETE')
    urllib.request.urlopen(request, timeout=10).close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('scenario', choices=['worker', 'shard', 'latency', 'partition', 'kafka'])
    parser.add_argument('--seconds', type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 300:
        parser.error('--seconds must be between 1 and 300')
    if args.scenario in ('worker', 'shard', 'kafka'):
        target = {'worker': 'worker', 'shard': 'shard-1', 'kafka': 'kafka'}[args.scenario]
        try:
            docker('stop', '-t', '0', target)
            time.sleep(args.seconds)
        finally:
            docker('start', target)
    elif args.scenario == 'partition':
        try:
            proxy('shard-1', enabled=False)
            time.sleep(args.seconds)
        finally:
            proxy('shard-1', enabled=True)
    else:
        try:
            toxic('shard-1', {'name': 'chaos-latency', 'type': 'latency', 'stream': 'downstream', 'attributes': {'latency': 500, 'jitter': 50}})
            # Toxiproxy operates on TCP streams, not IP packets. This resets
            # about 5% of connections; it is not advertised as packet loss.
            toxic('shard-1', {'name': 'chaos-reset', 'type': 'reset_peer', 'stream': 'downstream', 'toxicity': 0.05, 'attributes': {'timeout': 1000}})
            time.sleep(args.seconds)
        finally:
            for name in ('chaos-latency', 'chaos-reset'):
                try:
                    remove('shard-1', name)
                except Exception as exc:
                    print('Cleanup failed:', name, exc)
    print('Fault restored. Run scripts/verify.py after the queue drains.')

if __name__ == '__main__':
    main()
