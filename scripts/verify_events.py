#!/usr/bin/env python3
"""Prove terminal transfer events made it through real Kafka into the audit DB."""
import argparse
import subprocess
import time
from pathlib import Path

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--compose', default=str(root / 'deploy/docker-compose.yml'))
args = parser.parse_args()
compose = ['docker', 'compose', '-f', args.compose]

def query(service, sql):
    text = subprocess.check_output(compose + ['exec', '-T', service, 'psql', '-U', 'ledger', '-d', 'ledger', '-At', '-c', sql], text=True)
    return set(text.split())

expected = query('coordinator', "SELECT id FROM jobs WHERE kind='transfer' AND state IN ('COMPLETED','REVERSED')")
for _ in range(60):
    received = query('coordinator', 'SELECT event_id FROM consumed_events')
    if expected <= received:
        assert received == expected, 'unexpected consumed event IDs'
        print(f'PASS: all {len(expected)} terminal transfer events recorded exactly once in the audit consumer')
        break
    time.sleep(1)
else:
    raise SystemExit(f'FAIL: {len(expected - received)} terminal events not consumed')
