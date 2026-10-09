# PR 4 owner commands

Branch `pr-4` already exists, based on merged PR #3. No commit, push or PR was
created by the agent. Review the intended source/test/docs below before committing.
Generated artifacts remain ignored. Commit and PR names match PR 4 throughout.

```bash
cd /Users/khursheed/Documents/Playground/quorumfs
git switch pr-4
git status --short
git diff --check
git diff --stat
git add \
  .github/workflows/ci.yml \
  README.md \
  build.gradle \
  docs/adr/0005-canonical-owner-quorums.md \
  docs/evidence/Q3/README.md \
  docs/handoffs/phase-4-commands.md \
  docs/handoffs/phase-4-pr.md \
  docs/runbooks/quorum.md \
  infra/compose/compose.yaml \
  modules/client/build.gradle \
  modules/client/src/main/java/io/quorumfs/client/ClientMain.java \
  modules/client/src/main/java/io/quorumfs/client/ObjectClient.java \
  modules/coordinator/build.gradle \
  modules/coordinator/gradle.lockfile \
  modules/coordinator/src/main/java/io/quorumfs/coordinator/CoordinatorService.java \
  modules/protocol/build.gradle \
  modules/protocol/src/main/java/io/quorumfs/protocol/Streams.java \
  modules/protocol/src/main/java/io/quorumfs/protocol/Wire.java \
  modules/protocol/src/main/proto/quorumfs/v1/common.proto \
  modules/protocol/src/test/java/io/quorumfs/protocol/WireTest.java \
  modules/replication/build.gradle \
  modules/replication/gradle.lockfile \
  modules/replication/src/main/java/io/quorumfs/replication/PeerClient.java \
  modules/replication/src/main/java/io/quorumfs/replication/ReplicaService.java \
  modules/replication/src/main/java/io/quorumfs/replication/RpcFailure.java \
  modules/replication/src/main/java/io/quorumfs/replication/Spool.java \
  modules/replication/src/main/java/io/quorumfs/replication/UploadObserver.java \
  modules/server/build.gradle \
  modules/server/src/main/java/io/quorumfs/server/Access.java \
  modules/server/src/main/java/io/quorumfs/server/ServerMain.java \
  modules/server/src/test/java/io/quorumfs/server/AccessTest.java \
  modules/server/src/test/java/io/quorumfs/server/QuorumIntegrationTest.java \
  modules/storage/src/main/java/io/quorumfs/storage/ObjectStorage.java \
  modules/storage/src/test/java/io/quorumfs/storage/CausalStorageTest.java \
  tests/system/cluster.py \
  tests/system/quorum_cli.py \
  tests/system/quorum_compose.py
git diff --cached --check
git diff --cached --stat
git diff --cached
git commit -m "PR 4: quorum reads and writes"
git push -u origin pr-4
gh pr create --repo khurshhh1999/quorumfs --base main --head pr-4 \
  --title "PR 4: quorum reads and writes" \
  --body-file docs/handoffs/phase-4-pr.md
```

## Review checkpoint

Run after publication. Review the diff and checks before manually merging.
Resolve the actual PR number from the branch; the title does not assign a number.

```bash
cd /Users/khursheed/Documents/Playground/quorumfs
pr_number=$(gh pr view pr-4 --repo khurshhh1999/quorumfs --json number --jq .number)
gh pr diff "$pr_number" --repo khurshhh1999/quorumfs
gh pr checks "$pr_number" --repo khurshhh1999/quorumfs --watch
```

After review and passing CI, in the same shell:

```bash
gh pr merge "$pr_number" --repo khurshhh1999/quorumfs --squash --delete-branch
git switch main
git pull --ff-only origin main
```

## Changed files

- `.github/workflows/ci.yml`
- `README.md`
- `build.gradle`
- `docs/adr/0005-canonical-owner-quorums.md`
- `docs/evidence/Q3/README.md`
- `docs/handoffs/phase-4-commands.md`
- `docs/handoffs/phase-4-pr.md`
- `docs/runbooks/quorum.md`
- `infra/compose/compose.yaml`
- `modules/client/build.gradle`
- `modules/client/src/main/java/io/quorumfs/client/ClientMain.java`
- `modules/client/src/main/java/io/quorumfs/client/ObjectClient.java`
- `modules/coordinator/build.gradle`
- `modules/coordinator/gradle.lockfile`
- `modules/coordinator/src/main/java/io/quorumfs/coordinator/CoordinatorService.java`
- `modules/protocol/build.gradle`
- `modules/protocol/src/main/java/io/quorumfs/protocol/Streams.java`
- `modules/protocol/src/main/java/io/quorumfs/protocol/Wire.java`
- `modules/protocol/src/main/proto/quorumfs/v1/common.proto`
- `modules/protocol/src/test/java/io/quorumfs/protocol/WireTest.java`
- `modules/replication/build.gradle`
- `modules/replication/gradle.lockfile`
- `modules/replication/src/main/java/io/quorumfs/replication/PeerClient.java`
- `modules/replication/src/main/java/io/quorumfs/replication/ReplicaService.java`
- `modules/replication/src/main/java/io/quorumfs/replication/RpcFailure.java`
- `modules/replication/src/main/java/io/quorumfs/replication/Spool.java`
- `modules/replication/src/main/java/io/quorumfs/replication/UploadObserver.java`
- `modules/server/build.gradle`
- `modules/server/src/main/java/io/quorumfs/server/Access.java`
- `modules/server/src/main/java/io/quorumfs/server/ServerMain.java`
- `modules/server/src/test/java/io/quorumfs/server/AccessTest.java`
- `modules/server/src/test/java/io/quorumfs/server/QuorumIntegrationTest.java`
- `modules/storage/src/main/java/io/quorumfs/storage/ObjectStorage.java`
- `modules/storage/src/test/java/io/quorumfs/storage/CausalStorageTest.java`
- `tests/system/cluster.py`
- `tests/system/quorum_cli.py`
- `tests/system/quorum_compose.py`
