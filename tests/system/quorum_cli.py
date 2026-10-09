#!/usr/bin/env python3
"""Authenticated public CLI against five independent JVMs and RocksDB volumes."""
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
SERVER = ROOT / 'modules/server/build/install/server/bin/server'
CLIENT = ROOT / 'modules/client/build/install/client/bin/client'
OUT = ROOT / 'build/reports/system/quorum-cli'
OUT.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, QUORUMFS_CLIENT_TOKEN=secrets.token_hex(24),
           QUORUMFS_PEER_TOKEN=secrets.token_hex(24), QUORUMFS_NAMESPACES='demo')


def command(port, action, *args, success=True, environment=None):
    result = subprocess.run([str(CLIENT), action, f'localhost:{port}', '--insecure', *map(str, args)],
                            env=environment or env, capture_output=True, text=True, timeout=25)
    assert (result.returncode == 0) == success, result.stdout + result.stderr
    return result.stdout.strip()


sockets = [socket.socket() for _ in range(5)]
for s in sockets:
    s.bind(('127.0.0.1', 0))
ports = [s.getsockname()[1] for s in sockets]
for s in sockets:
    s.close()
processes, logs = [], []
with tempfile.TemporaryDirectory(prefix='quorumfs-quorum-') as work:
    work = Path(work)
    try:
        for i, port in enumerate(ports):
            config = work / f'node{i+1}.properties'
            members = ','.join(f'node{j+1}@localhost:{p}' for j, p in enumerate(ports))
            config.write_text(f'cluster.id=quorumfs-dev\ncluster.epoch=1\nnode.id=node{i+1}\nnode.port={port}\n'
                              f'data.dir={work / ("data"+str(i+1))}\nmembers={members}\n'
                              'quorum.n=3\nquorum.r=2\nquorum.w=2\ndevelopment.insecure=true\n')
            log = (OUT / f'node{i+1}.log').open('w')
            logs.append(log)
            processes.append(subprocess.Popen([str(SERVER), str(config)], env=env, stdout=log, stderr=subprocess.STDOUT))
        for process, port in zip(processes, ports):
            until = time.monotonic() + 45
            while True:
                assert process.poll() is None, 'Node exited at startup'
                health = subprocess.run([str(CLIENT), 'health', f'localhost:{port}', '--insecure'], env=env, capture_output=True, timeout=10)
                if health.returncode == 0:
                    break
                assert time.monotonic() < until, 'Startup deadline exceeded'
                time.sleep(.1)
            assert command(port, 'info').split()[-1] == 'true'
        source = work / 'input'
        source.write_bytes(b'phase-4-seed-20261007' * 20000)
        first = command(ports[0], 'put', 'demo', 'cli-key', source).splitlines()
        assert first[1] == 'durable_owners=2'
        version, size, digest, context, kind = first[0].split()
        assert kind == "live"
        output = work / 'download'
        command(ports[1], 'get', 'demo', 'cli-key', output)
        assert source.read_bytes() == output.read_bytes()
        command(ports[1], 'get', 'demo', 'cli-key', output, success=False)
        assert command(ports[2], 'head', 'demo', 'cli-key').split()[0] == version
        assert command(ports[2], 'context', 'demo', 'cli-key') == context
        resolved = command(ports[3], 'resolve', 'demo', 'cli-key', source, context).splitlines()[0].split()[0]
        assert command(ports[4], 'head', 'demo', 'cli-key').split()[0] == resolved
        command(ports[0], 'head', 'private', 'cli-key', success=False)
        command(ports[0], 'head', 'demo', 'cli-key', success=False,
                environment=dict(env, QUORUMFS_CLIENT_TOKEN='invalid'))
        owners = command(ports[0], 'owners', 'demo', 'cli-key').split(',')
        assert len(set(owners)) == 3
        victim = int(owners[0][4:]) - 1
        processes[victim].kill()
        processes[victim].wait(timeout=10)
        survivor = next(i for i in range(5) if i != victim)
        command(ports[survivor], 'get', 'demo', 'cli-key', work / 'after-kill')
        assert source.read_bytes() == (work / 'after-kill').read_bytes()
        context = command(ports[survivor], 'context', 'demo', 'cli-key')
        deleted = command(ports[survivor], 'delete', 'demo', 'cli-key', context).splitlines()[0].split()
        assert deleted[4] == 'tombstone'
        command(ports[survivor], 'get', 'demo', 'cli-key', work / 'deleted', success=False)
        assert not (work / 'deleted').exists()
        assert command(ports[survivor], 'head', 'demo', 'cli-key').split()[4] == 'tombstone'
        # Restart the actual killed owner; durable handoff catches it up in the background.
        processes[victim] = subprocess.Popen([str(SERVER), str(work / f'node{victim+1}.properties')],
                                            env=env, stdout=logs[victim], stderr=subprocess.STDOUT)
        until = time.monotonic() + 35
        while True:
            health = subprocess.run([str(CLIENT), 'health', f'localhost:{ports[victim]}', '--insecure'],
                                    env=env, capture_output=True, timeout=10)
            if health.returncode == 0:
                break
            assert time.monotonic() < until
            time.sleep(.1)
        # The Java TCP fixture verifies target-local catch-up without reads; this is the CLI contract.
        assert command(ports[victim], 'head', 'demo', 'cli-key').split()[4] == 'tombstone'
        command(ports[survivor], 'put', 'demo', 'cli-key', source, deleted[3])
        command(ports[victim], 'get', 'demo', 'cli-key', work / 'recreated')
        assert source.read_bytes() == (work / 'recreated').read_bytes()
        result = {'status': 'passed', 'seed': 20261007, 'five_jvms': True,
                  'authenticated_cli': True, 'quorum_put_get_resolve': True,
                  'owner_process_kill': True, 'delete_restart_recreate': True, 'unauthorized_denied': True}
        (OUT / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result))
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
        for log in logs:
            log.close()
