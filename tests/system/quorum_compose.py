#!/usr/bin/env python3
"""Enable authenticated data RPCs on the local Compose fixture and test public CLI."""
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ['docker', 'compose', '-f', str(ROOT / 'infra/compose/compose.yaml')]
CLIENT = ROOT / 'modules/client/build/install/client/bin/client'
OUT = ROOT / 'build/reports/system/quorum-compose'
OUT.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, QUORUMFS_CLIENT_TOKEN=secrets.token_hex(24),
           QUORUMFS_PEER_TOKEN=secrets.token_hex(24), QUORUMFS_NAMESPACES='demo')


def client(command, port, *args):
    return subprocess.check_output([str(CLIENT), command, f'localhost:{port}', '--insecure', *map(str, args)],
                                   env=env, text=True, stderr=subprocess.STDOUT, timeout=25).strip()


with (OUT / 'compose.log').open('w') as log:
    try:
        subprocess.run(COMPOSE + ['up', '--build', '--force-recreate', '--wait', '--wait-timeout', '180'],
                       cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=240)
        for port in range(19001, 19006):
            assert client('info', port).split()[-1] == 'true'
        with tempfile.TemporaryDirectory(prefix='quorumfs-compose-') as work:
            work = Path(work)
            source = work / 'input'
            source.write_bytes(b'quorum-compose-seed-20261007' * 20000)
            key = 'smoke-' + secrets.token_hex(8)
            write = client('put', 19001, 'demo', key, source).splitlines()
            assert write[1] == 'durable_owners=2'
            context = write[0].split()[3]
            client('get', 19002, 'demo', key, work / 'output')
            assert source.read_bytes() == (work / 'output').read_bytes()
            client('resolve', 19003, 'demo', key, source, context)
            assert len(client('head', 19004, 'demo', key).splitlines()) == 1
            client('get', 19005, 'demo', key, work / 'resolved')
            assert source.read_bytes() == (work / 'resolved').read_bytes()
        (OUT / 'result.json').write_text(json.dumps({'status':'passed','seed':20261007,'five_containers':True,
            'authenticated_quorum_cli':True}, indent=2) + '\n')
        print('PASS five-container authenticated quorum CLI')
    finally:
        subprocess.run(COMPOSE + ['logs', '--no-color'], env=env, stdout=log, stderr=subprocess.STDOUT, timeout=30)
        subprocess.run(COMPOSE + ['down'], env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=90)
