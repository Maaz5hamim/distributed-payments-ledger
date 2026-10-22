#!/usr/bin/env python3
"""Generate an isolated 1/2/4-shard Compose deployment for scaling experiments.
Run only against NEW volumes. Account ownership changes with the shard map.
"""
import argparse
import copy
import json
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('shards', type=int, choices=[1, 2, 4])
args = parser.parse_args()
n = args.shards
config = json.loads(subprocess.check_output(['docker', 'compose', '-f', str(root / 'deploy/docker-compose.yml'), '--profile', 'tools', 'config', '--format', 'json']))
config['name'] = f'ledger-bench-{n}'
# Let Compose derive new project-scoped resource names instead of sharing data.
for category in ('volumes', 'networks'):
    for resource in config.get(category, {}).values():
        resource.pop('name', None)
services = config['services']
template = copy.deepcopy(services['shard-0'])
for name in list(services):
    if name.startswith('shard-'):
        del services[name]
for i in range(n):
    db = copy.deepcopy(template)
    db['volumes'][0]['source'] = f'shard-{i}-data'
    config['volumes'].setdefault(f'shard-{i}-data', {})
    services[f'shard-{i}'] = db
urls = ','.join(f'postgres://ledger:ledger@toxiproxy:{15432+i}/ledger?sslmode=disable' for i in range(n))
for name in ('api', 'worker', 'relay', 'consumer', 'reconciler', 'migrate'):
    services[name]['environment']['SHARD_URLS'] = urls
services['toxiproxy']['depends_on'] = {f'shard-{i}': {'condition': 'service_healthy', 'required': True} for i in range(n)}
proxy_file = root / f'deploy/generated-proxy-{n}.json'
proxies = [{'name': f'shard-{i}', 'listen': f'0.0.0.0:{15432+i}', 'upstream': f'shard-{i}:5432', 'enabled': True} for i in range(n)]
proxies.append({'name': 'kafka', 'listen': '0.0.0.0:19092', 'upstream': 'kafka:9092', 'enabled': True})
proxy_file.write_text(json.dumps(proxies, indent=2) + '\n')
services['toxiproxy']['volumes'][0]['source'] = str(proxy_file)
output = root / f'deploy/generated-{n}.json'
output.write_text(json.dumps(config, indent=2) + '\n')
print(f'Created {output}')
print(f'docker compose -f {output} up -d')
print('Stop other deployments first: these experiments use the same local ports.')
