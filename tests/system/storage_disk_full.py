#!/usr/bin/env python3
"""Exercise native RocksDB I/O failure on an 8 MiB Linux tmpfs, not a mocked dependency."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
image = (ROOT / "Dockerfile").read_text().splitlines()[0].split()[1]
assert "@sha256:" in image
classpath = ":".join([
    "/workspace/modules/storage/build/classes/java/main",
    "/workspace/modules/storage/build/classes/java/test",
    "/workspace/modules/server/build/install/server/lib/rocksdbjni-10.10.1.jar",
])
result = subprocess.run([
    "docker", "run", "--rm", "--network", "none", "--read-only", "--user", "10001:10001",
    "--memory", "256m", "--cpus", "1",
    "--tmpfs", "/tmp:rw,exec,size=128m,mode=1777",
    "--tmpfs", "/data:rw,size=8m,mode=1777",
    "--mount", f"type=bind,src={ROOT},dst=/workspace,readonly",
    image, "java", "-Xmx24m", "-XX:MaxDirectMemorySize=16m", "-cp", classpath,
    "io.quorumfs.storage.StorageProcess", "/data/db", "disk-full", "/tmp/disk-full.ready",
], capture_output=True, text=True, timeout=120)
out = ROOT / "build/reports/system/storage-disk-full"
out.mkdir(parents=True, exist_ok=True)
(out / "process.log").write_text(result.stdout + result.stderr)
assert result.returncode == 0, result.stdout + result.stderr
assert "PASS disk-full" in result.stdout and "acknowledged=false" in result.stdout
(out / "result.json").write_text(json.dumps({"status": "passed", "seed": 20260930,
    "native_rocksdb_io_error": True, "acknowledged": False, "filesystem_bytes": 8 * 1024 * 1024,
    "image": image}, indent=2) + "\n")
print(result.stdout.strip())
