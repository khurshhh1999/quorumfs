# PR 3 owner commands

Branch `pr-3` already exists and contains the local changes. Commit and PR titles
match the requested PR 3 naming. No commit, push or PR has been created by the
agent. Review the diff before publication; staging below lists only intended
source/test/Markdown files. Generated reports and binaries are ignored.

```bash
cd /Users/khursheed/Documents/Playground/quorumfs
git switch pr-3
git status --short
git diff --check
git diff --stat
git add \
  README.md \
  build.gradle \
  docs/adr/0004-ring-and-causal-versions.md \
  docs/evidence/Q2/README.md \
  docs/handoffs/phase-3-commands.md \
  docs/handoffs/phase-3-pr.md \
  docs/runbooks/causal-versions.md \
  modules/hash-ring/build.gradle \
  modules/hash-ring/src/main/java/io/quorumfs/ring/HashRing.java \
  modules/hash-ring/src/test/java/io/quorumfs/ring/HashRingTest.java \
  modules/server/src/main/java/io/quorumfs/server/NodeConfig.java \
  modules/server/src/main/java/io/quorumfs/server/ServerMain.java \
  modules/server/src/main/java/io/quorumfs/server/StorageMain.java \
  modules/storage/build.gradle \
  modules/storage/src/main/java/io/quorumfs/storage/ObjectStorage.java \
  modules/storage/src/test/java/io/quorumfs/storage/CausalStorageTest.java \
  modules/storage/src/test/java/io/quorumfs/storage/ObjectStorageTest.java \
  modules/storage/src/test/java/io/quorumfs/storage/StorageProcess.java \
  modules/storage/src/test/java/io/quorumfs/storage/StorageProcessTest.java \
  modules/versioning/build.gradle \
  modules/versioning/src/main/java/io/quorumfs/versioning/Siblings.java \
  modules/versioning/src/main/java/io/quorumfs/versioning/VectorClock.java \
  modules/versioning/src/test/java/io/quorumfs/versioning/VectorClockTest.java \
  tests/system/causal_cli.py \
  tests/system/storage_disk_full.py
git diff --cached --check
git diff --cached --stat
git commit -m "PR 3: consistent hashing and causal versions"
git push -u origin pr-3
gh pr create --repo khurshhh1999/quorumfs --base main --head pr-3 \
  --title "PR 3: consistent hashing and causal versions" \
  --body-file docs/handoffs/phase-3-pr.md
```

## Review checkpoint and manual merge

Run after publication and review, not automatically with the commands above.
The number is looked up from the branch, not assumed from the PR title.

```bash
cd /Users/khursheed/Documents/Playground/quorumfs
pr_number=$(gh pr view pr-3 --repo khurshhh1999/quorumfs --json number --jq .number)
gh pr diff "$pr_number" --repo khurshhh1999/quorumfs
gh pr checks "$pr_number" --repo khurshhh1999/quorumfs --watch
gh pr merge "$pr_number" --repo khurshhh1999/quorumfs --squash --delete-branch
git switch main
git pull --ff-only origin main
```

## Changed files

- `README.md`
- `build.gradle`
- `docs/adr/0004-ring-and-causal-versions.md`
- `docs/evidence/Q2/README.md`
- `docs/handoffs/phase-3-commands.md`
- `docs/handoffs/phase-3-pr.md`
- `docs/runbooks/causal-versions.md`
- `modules/hash-ring/build.gradle`
- `modules/hash-ring/src/main/java/io/quorumfs/ring/HashRing.java`
- `modules/hash-ring/src/test/java/io/quorumfs/ring/HashRingTest.java`
- `modules/server/src/main/java/io/quorumfs/server/NodeConfig.java`
- `modules/server/src/main/java/io/quorumfs/server/ServerMain.java`
- `modules/server/src/main/java/io/quorumfs/server/StorageMain.java`
- `modules/storage/build.gradle`
- `modules/storage/src/main/java/io/quorumfs/storage/ObjectStorage.java`
- `modules/storage/src/test/java/io/quorumfs/storage/CausalStorageTest.java`
- `modules/storage/src/test/java/io/quorumfs/storage/ObjectStorageTest.java`
- `modules/storage/src/test/java/io/quorumfs/storage/StorageProcess.java`
- `modules/storage/src/test/java/io/quorumfs/storage/StorageProcessTest.java`
- `modules/versioning/build.gradle`
- `modules/versioning/src/main/java/io/quorumfs/versioning/Siblings.java`
- `modules/versioning/src/main/java/io/quorumfs/versioning/VectorClock.java`
- `modules/versioning/src/test/java/io/quorumfs/versioning/VectorClockTest.java`
- `tests/system/causal_cli.py`
- `tests/system/storage_disk_full.py`
