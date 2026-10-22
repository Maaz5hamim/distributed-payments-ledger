#!/usr/bin/env python3
import json
import os
import time
import urllib.request
import uuid

KEY = os.getenv('API_KEY', 'local-development-key')
BASE = os.getenv('BASE_URL', 'http://localhost:8080')

def call(path, data=None, key=None):
    body = None if data is None else json.dumps(data).encode()
    request = urllib.request.Request(BASE + path, data=body, headers={
        'X-API-Key': KEY, 'Content-Type': 'application/json',
        'Idempotency-Key': key or str(uuid.uuid4())})
    with urllib.request.urlopen(request, timeout=20) as r:
        return r.status, json.load(r)

def wait(id):
    for _ in range(300):
        _, job = call('/operations/' + id)
        if job['state'] in ('COMPLETED', 'REVERSED', 'FAILED'):
            assert job['state'] == 'COMPLETED', job
            return job
        time.sleep(.1)
    raise RuntimeError('operation did not finish')

def main():
    accounts = []
    groups = {}
    for _ in range(12):
        _, account = call('/accounts', {'initial_deposit': 10000})
        job = wait(account['operation_id'])
        accounts.append(account['id'])
        groups.setdefault(job['to_shard'], []).append(account['id'])
    pairs = []
    for ids in groups.values():
        if len(ids) >= 2:
            pairs.append((ids[0], ids[1], 'local'))
            break
    if len(groups) >= 2:
        ids = list(groups.values())
        pairs.append((ids[0][0], ids[1][0], 'saga'))
    for source, target, path in pairs:
        data = {'from_account': source, 'to_account': target, 'amount': 25}
        key = str(uuid.uuid4())
        status, transfer = call('/transfers', data, key)
        for _ in range(3):
            assert call('/transfers', data, key) == (status, transfer)
        wait(transfer['id'])
        print(path, 'transfer completed; retry response identical')
    total = sum(call('/accounts/' + id)[1]['balance'] for id in accounts)
    assert total == 120000, total
    print('PASS: total customer funds preserved:', total)

if __name__ == '__main__':
    main()
