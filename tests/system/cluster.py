#!/usr/bin/env python3
"""Real gRPC/native RocksDB bootstrap checks. No object durability claims."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
SERVER = ROOT / "modules/server/build/install/server/bin/server"
CLIENT = ROOT / "modules/client/build/install/client/bin/client"
MODE = sys.argv[1]
SEED = 20260928
RESULTS = ROOT / "build/reports/system" / MODE
RESULTS.mkdir(parents=True, exist_ok=True)


def client(command, port, check=True):
    return subprocess.run([str(CLIENT), command, f"127.0.0.1:{port}", "--insecure"],
                          capture_output=True, text=True, timeout=10, check=check)


def ready(port, process=None):
    until = time.monotonic() + 45
    while time.monotonic() < until:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"Node exited during startup: {process.returncode}")
        if client("health", port, False).returncode == 0:
            return
        time.sleep(0.2)
    raise RuntimeError("Health deadline exceeded")


def verify(ports, enabled=False):
    identities = []
    for port in ports:
        ready(port)
        fields = client("info", port).stdout.strip().split()
        assert fields[1:] == ["quorumfs-dev", "1", "5", "3", "2", "2", str(enabled).lower()], fields
        identities.append(fields[0])
        if not enabled:
            assert client("assert-unimplemented", port).stdout.strip() == "UNIMPLEMENTED"
    assert sorted(identities) == [f"node{i}" for i in range(1, 6)], identities
    return identities


def run():
    if MODE == "smoke":
        ports = [int(p) for p in os.environ.get("QUORUMFS_PORTS", "19001,19002,19003,19004,19005").split(",")]
        assert len(ports) == 5
        return {"nodes": verify(ports, bool(os.environ.get("QUORUMFS_CLIENT_TOKEN") and os.environ.get("QUORUMFS_PEER_TOKEN") and os.environ.get("QUORUMFS_NAMESPACES"))), "transport": "external-grpc"}
    assert MODE in ("integration", "fault")
    sockets = []
    for _ in range(5):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        sockets.append(s)
    ports = [s.getsockname()[1] for s in sockets]
    for s in sockets:
        s.close()
    processes = []
    handles = []
    bootstrap_env = {k: v for k, v in os.environ.items() if k not in (
        "QUORUMFS_CLIENT_TOKEN", "QUORUMFS_PEER_TOKEN", "QUORUMFS_NAMESPACES")}
    with tempfile.TemporaryDirectory(prefix="quorumfs-") as work:
        work = Path(work)
        configs = []
        members = ",".join(f"node{i + 1}@localhost:{p}" for i, p in enumerate(ports))
        def start(i):
            log = (RESULTS / f"node{i + 1}.log").open("a")
            handles.append(log)
            process = subprocess.Popen([str(SERVER), str(configs[i])], stdout=log, stderr=subprocess.STDOUT, env=bootstrap_env)
            processes.append(process)
            ready(ports[i], process)
            return process
        try:
            for i, port in enumerate(ports):
                config = work / f"node{i + 1}.properties"
                config.write_text(f"cluster.id=quorumfs-dev\ncluster.epoch=1\nnode.id=node{i + 1}\nnode.port={port}\n"
                                  f"data.dir={work / ('data' + str(i + 1))}\nmembers={members}\n"
                                  "quorum.n=3\nquorum.r=2\nquorum.w=2\ndevelopment.insecure=true\n")
                configs.append(config)
                start(i)
            identities = verify(ports)
            # Restart every node with the same independent volume.
            for process in list(processes):
                process.terminate()
                process.wait(timeout=15)
                assert process.returncode in (0, 143, -15)
            for i in range(5):
                start(i)
            assert verify(ports) == identities
            if MODE == "fault":
                victim = processes[-5]
                victim.kill()
                victim.wait(timeout=10)
                assert client("health", ports[0], False).returncode != 0
                for port in ports[1:]:
                    ready(port)
                start(0)
                assert verify(ports) == identities
            # Reusing a volume with a different epoch must fail closed.
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=15)
            configs[0].write_text(configs[0].read_text().replace("cluster.epoch=1", "cluster.epoch=2"))
            mismatch = subprocess.run([str(SERVER), str(configs[0])], capture_output=True, text=True, timeout=15, env=bootstrap_env)
            assert mismatch.returncode != 0 and "Persisted node/cluster identity" in mismatch.stderr
            return {"nodes": identities, "transport": "real-grpc", "native_storage": "RocksDB",
                    "restart": "passed", "epoch_mismatch": "rejected", "process_kill": MODE == "fault"}
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=10)
            for handle in handles:
                handle.close()


try:
    result = {"status": "passed", "seed": SEED, "mode": MODE, **run()}
except Exception as error:
    (RESULTS / "result.json").write_text(json.dumps({"status": "failed", "error": str(error), "seed": SEED}, indent=2) + "\n")
    raise
(RESULTS / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
