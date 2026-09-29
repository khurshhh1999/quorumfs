#!/usr/bin/env python3
"""Bounded real Toxiproxy disconnect/reconnect; requires the Compose fixture."""
import json
from pathlib import Path
import subprocess
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
CLIENT = ROOT / 'modules/client/build/install/client/bin/client'
URL = 'http://127.0.0.1:18474/proxies/node1'

def enabled(value):
    request = urllib.request.Request(URL, json.dumps({'enabled': value}).encode(),
                                     {'Content-Type': 'application/json'}, method='POST')
    with urllib.request.urlopen(request, timeout=5) as response:
        assert response.status == 200

def health(port):
    return subprocess.run([str(CLIENT), 'health', f'127.0.0.1:{port}', '--insecure'],
                          capture_output=True, timeout=10).returncode

assert health(19001) == 0
try:
    enabled(False)
    assert health(19001) != 0, 'Disabled proxy still reachable'
    assert health(19002) == 0, 'Unrelated node unavailable'
finally:
    enabled(True)
assert health(19001) == 0
out = ROOT / 'build/reports/system/proxy'
out.mkdir(parents=True, exist_ok=True)
(out / 'result.json').write_text(json.dumps({'status': 'passed', 'seed': 20260928,
    'disconnection': True, 'reconnection': True, 'object_quorum_tested': False}, indent=2) + '\n')
print('Proxy disconnect/reconnect passed; no object quorum claim.')
