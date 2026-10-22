#!/usr/bin/env python3
"""Wait for a fresh, successful, balanced reconciliation and drained work."""
import time
import urllib.request

started = time.time()
for _ in range(180):
    try:
        text = urllib.request.urlopen('http://localhost:9091/metrics', timeout=5).read().decode()
        metrics = {}
        for line in text.splitlines():
            if line and not line.startswith('#'):
                key, value = line.split()[:2]
                metrics[key] = float(value)
        fresh = metrics.get('ledger_reconciliation_last_success_timestamp_seconds', 0) >= started
        pending = sum(v for k, v in metrics.items() if k.startswith('ledger_sagas{') and not any(f'state="{s}"' in k for s in ('COMPLETED', 'REVERSED', 'FAILED')))
        unsent = sum(v for k, v in metrics.items() if k.startswith('ledger_outbox_unsent_rows{'))
        success = metrics.get('ledger_reconciliation_success') == 1
        if fresh and success and metrics.get('ledger_reconciliation_mismatches') != 0:
            raise SystemExit('FAIL: reconciliation mismatches detected')
        if fresh and success and pending == 0 and unsent == 0:
            print('PASS: fresh reconciliation, zero mismatches, all operations terminal, outboxes drained')
            break
    except (OSError, ValueError) as exc:
        print('Waiting:', exc)
    time.sleep(2)
else:
    raise SystemExit('FAIL: work did not drain or reconciliation unavailable within 6 minutes')
