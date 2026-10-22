#!/usr/bin/env python3
"""Exercise the live default stack. Produces real recovery evidence, not a benchmark."""
import importlib.util
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from smoke import call, wait
spec = importlib.util.spec_from_file_location('faults', ROOT / 'chaos/run.py')
faults = importlib.util.module_from_spec(spec)
spec.loader.exec_module(faults)


def terminal(id, expected):
    for _ in range(1200):
        _, job = call('/operations/' + id)
        if job['state'] in ('COMPLETED', 'REVERSED', 'FAILED'):
            assert job['state'] == expected, job
            return job
        time.sleep(.1)
    raise AssertionError('operation did not finish: ' + id)


def transfer(source, target):
    payload = {'from_account': source, 'to_account': target, 'amount': 1}
    key = str(uuid.uuid4())
    response = call('/transfers', payload, key)
    for _ in range(3):
        assert call('/transfers', payload, key) == response
    return response[1]['id']


def balance(id):
    return call('/accounts/' + id)[1]['balance']


def main():
    groups = {}
    for _ in range(30):
        _, a = call('/accounts', {'initial_deposit': 10000})
        j = wait(a['operation_id'])
        groups.setdefault(j['to_shard'], []).append(a['id'])
        if len(groups.get(0, [])) >= 2 and len(groups.get(1, [])) >= 2:
            break
    assert len(groups.get(0, [])) >= 2 and len(groups.get(1, [])) >= 2, groups
    source, local = groups[0][:2]
    target = groups[1][0]
    evidence = []
    # Keep real traffic running throughout each fault. The pair on shard 0
    # remains available during target-shard failures; Kafka/worker faults must
    # also allow the local API path to commit.
    import threading
    load_errors = []
    stop = threading.Event()
    def load():
        while not stop.is_set():
            try:
                id = transfer(source, local)
                terminal(id, 'COMPLETED')
            except Exception as exc:
                load_errors.append(str(exc))
            stop.wait(.2)
    thread = threading.Thread(target=load)
    thread.start()
    try:
        for name, service in [('worker', 'worker'), ('shard', 'shard-1'), ('kafka', 'kafka')]:
            started = time.time()
            try:
                faults.docker('stop', '-t', '0', service)
                id = transfer(source, target)
                if name == 'worker':
                    _, pending = call('/operations/' + id)
                    assert pending['state'] == 'PENDING', pending
                time.sleep(5)
            finally:
                faults.docker('start', service)
            terminal(id, 'COMPLETED')
            evidence.append({'scenario': name, 'outcome': 'COMPLETED', 'seconds': round(time.time() - started, 3)})
            print(name, 'PASS', flush=True)
        try:
            faults.toxic('shard-1', {'name': 'suite-latency', 'type': 'latency', 'stream': 'downstream', 'attributes': {'latency': 500, 'jitter': 50}})
            id = transfer(source, target)
            # High per-round-trip latency can exceed a bounded DB step.
            # Keep it for five seconds, then prove safe recovery.
            time.sleep(5)
        finally:
            faults.remove('shard-1', 'suite-latency')
        terminal(id, 'COMPLETED')
        evidence.append({'scenario': 'latency-500ms', 'outcome': 'COMPLETED'})
        print('latency PASS', flush=True)
        before = balance(target)
        try:
            faults.proxy('shard-1', enabled=False)
            id = transfer(source, target)
            deadline = time.time() + 100
            while time.time() < deadline:
                _, j = call('/operations/' + id)
                if j['state'] == 'CANCELLING':
                    break
                assert j['state'] not in ('COMPLETED', 'REVERSED'), j
                time.sleep(.5)
            else:
                raise AssertionError('retry budget did not enter cancellation')
            # Still partitioned: no refund can occur until cancellation is
            # durably fenced on the receiving shard.
            time.sleep(1)
            _, j = call('/operations/' + id)
            assert j['state'] == 'CANCELLING', j
        finally:
            faults.proxy('shard-1', enabled=True)
        terminal(id, 'REVERSED')
        assert balance(target) == before
        evidence.append({'scenario': 'partition-through-retry-budget', 'outcome': 'REVERSED', 'late_credit': False})
        print('partition PASS', flush=True)
    finally:
        stop.set()
        thread.join(timeout=60)
    assert not thread.is_alive()
    assert not load_errors, load_errors
    subprocess.run([sys.executable, str(ROOT / 'scripts/verify.py')], check=True)
    subprocess.run([sys.executable, str(ROOT / 'scripts/verify_events.py')], check=True)
    result = ROOT / 'results'
    result.mkdir(exist_ok=True)
    (result / 'chaos.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print('PASS: all chaos scenarios; background local transfers continued; three retries per transfer')


if __name__ == '__main__':
    main()
