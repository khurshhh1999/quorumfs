#!/usr/bin/env python3
"""Offline CLI round trip, restart, checkpoint/restore, identity and no-overwrite checks."""
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SERVER = ROOT / "modules/server/build/install/server/bin/server"


def command(config, *args, succeeds=True):
    result = subprocess.run([str(SERVER), "storage", str(config), *map(str, args)],
                            capture_output=True, text=True, timeout=30)
    assert (result.returncode == 0) == succeeds, result.stdout + result.stderr
    return result.stdout.strip()


with tempfile.TemporaryDirectory(prefix="quorumfs-cli-") as folder:
    work = Path(folder)
    def config(name, data, identity="node1"):
        target = work / name
        target.write_text(f"cluster.id=quorumfs-dev\ncluster.epoch=1\nnode.id={identity}\nnode.port=19001\n"
                          f"data.dir={data}\nmembers=" + ",".join(f"node{i}@localhost:{19000+i}" for i in range(1, 6)) +
                          "\nquorum.n=3\nquorum.r=2\nquorum.w=2\ndevelopment.insecure=true\n")
        return target
    original = config("original.properties", work / "db")
    versions = []
    for size in (0, 1, 262144, 262145):
        source = work / f"input-{size}"
        source.write_bytes((b"seed-20260930" * (size // 13 + 1))[:size])
        version, sequence, reported = command(original, "put", "test", "object", source).split()
        assert int(reported) == size
        output = work / f"output-{size}"
        command(original, "get", "test", "object", version, output)
        assert hashlib.sha256(source.read_bytes()).digest() == hashlib.sha256(output.read_bytes()).digest()
        command(original, "get", "test", "object", version, output, succeeds=False)
        versions.append(version)
    assert command(original, "verify") == "VERIFIED 4 versions"
    checkpoint = work / "checkpoint"
    command(original, "checkpoint", checkpoint)
    wrong = config("wrong.properties", work / "wrong-db", "node2")
    command(wrong, "restore", checkpoint, succeeds=False)
    assert not (work / "wrong-db").exists()
    restored = config("restored.properties", work / "restored-db")
    command(restored, "restore", checkpoint)
    assert command(restored, "verify") == "VERIFIED 4 versions"
    restored_output = work / "restored-output"
    command(restored, "get", "test", "object", versions[-1], restored_output)
    assert restored_output.read_bytes() == (work / "input-262145").read_bytes()
    command(restored, "restore", checkpoint, succeeds=False)
    assert not list(work.glob(".quorumfs-restore-*"))

out = ROOT / "build/reports/system/storage-cli"
out.mkdir(parents=True, exist_ok=True)
(out / "result.json").write_text(json.dumps({"status": "passed", "seed": 20260930,
    "boundary_sizes": [0, 1, 262144, 262145], "verified_versions": 4,
    "checkpoint_restore": True, "wrong_identity_rejected": True, "overwrite_rejected": True}, indent=2) + "\n")
print("PASS offline CLI: four boundary sizes, restart, checkpoint/restore, identity and overwrite protection")
