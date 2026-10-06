#!/usr/bin/env python3
"""Five independent offline node processes agree on ownership; causal CLI survives restarts."""
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

with tempfile.TemporaryDirectory(prefix="quorumfs-causal-") as folder:
    work = Path(folder)
    configs = []
    for i in range(1, 6):
        config = work / f"node{i}.properties"
        members = [f"node{j}@localhost:{19000+j}" for j in range(1, 6)]
        members = members[i:] + members[:i]
        config.write_text(f"cluster.id=quorumfs-dev\ncluster.epoch=1\nnode.id=node{i}\nnode.port={19000+i}\n"
                          f"data.dir={work / ('db'+str(i))}\nmembers={','.join(members)}\n"
                          "quorum.n=3\nquorum.r=2\nquorum.w=2\ndevelopment.insecure=true\n")
        configs.append(config)
    for key in ("object", "another", "unicode-λ"):
        results = [command(config, "owners", "test", key) for config in configs]
        assert len(set(results)) == 1, results
        assert len(set(results[0].split()[1].split(','))) == 3
    config = configs[0]
    source = work / "source"
    source.write_bytes(b"causal-cli-seed-20261006")
    # Synthetic caller contexts represent concurrent observations; these are offline local writes.
    context_a = b"quorumfs-vector-v1\nnode2=1\n".hex()
    context_b = b"quorumfs-vector-v1\nnode3=1\n".hex()
    first = command(config, "put-causal", "test", "causal", source, context_a).split()[0]
    second = command(config, "put-causal", "test", "causal", source, context_b).split()[0]
    siblings = command(config, "siblings", "test", "causal").splitlines()
    assert {s.split()[0] for s in siblings} == {first, second}
    resolved = command(config, "resolve", "test", "causal", source, f"{first},{second}").split()
    assert [s.split()[0] for s in command(config, "siblings", "test", "causal").splitlines()] == [resolved[0]]
    output = work / "output"
    command(config, "get", "test", "causal", resolved[0], output)
    assert output.read_bytes() == source.read_bytes()
    command(config, "put", "test", "causal", source, succeeds=False)
    assert command(config, "verify") == "VERIFIED 3 versions"
    original = config.read_text()
    for changed in (original.replace("cluster.epoch=1", "cluster.epoch=2"),
                    original.replace("node5@localhost:19005", "node5@other:19005")):
        config.write_text(changed)
        command(config, "owners", "test", "object", succeeds=False)
    config.write_text(original)
    assert command(config, "verify") == "VERIFIED 3 versions"

out = ROOT / "build/reports/system/causal-cli"
out.mkdir(parents=True, exist_ok=True)
(out / "result.json").write_text(json.dumps({"status": "passed", "seed": 20261006,
    "five_node_ownership": True, "conflict_and_resolution": True,
    "restart": True, "epoch_and_same_epoch_drift_rejected": True}, indent=2) + "\n")
print("PASS causal CLI: five-node ownership, siblings, resolution, restart and configuration drift")
