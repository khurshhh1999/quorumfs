# PR 5: deletion and recovery

Branch: `pr-5`. Base: `main` at `a43fc7c`. Commit and PR title both use
`PR 5: deletion and recovery`; GitHub assigns the actual PR number at creation.
No commit, push, PR creation or merge has been performed.

Planning documents and local agent instructions remain ignored and untracked.
The commands below stage only the inspected source, tests and Markdown docs.
Generated artifacts are excluded. Review the staged diff before committing.

```bash
cd /Users/khursheed/Documents/Playground/quorumfs
git switch pr-5
git status --short
git diff --check
git diff
git add -- \
  .gitignore \
  README.md \
  docs/contracts/README.md \
  docs/handoffs/phase-1-commands.md \
  docs/handoffs/phase-2-commands.md \
  docs/handoffs/phase-3-commands.md \
  docs/handoffs/phase-4-commands.md \
  docs/runbooks/quorum.md \
  docs/runbooks/storage.md \
  modules/client/src/main/java/io/quorumfs/client/ClientMain.java \
  modules/client/src/main/java/io/quorumfs/client/ObjectClient.java \
  modules/coordinator/src/main/java/io/quorumfs/coordinator/CoordinatorService.java \
  modules/protocol/src/main/java/io/quorumfs/protocol/Wire.java \
  modules/protocol/src/test/java/io/quorumfs/protocol/WireTest.java \
  modules/replication/src/main/java/io/quorumfs/replication/PeerClient.java \
  modules/replication/src/main/java/io/quorumfs/replication/ReplicaService.java \
  modules/server/src/test/java/io/quorumfs/server/QuorumIntegrationTest.java \
  modules/storage/src/main/java/io/quorumfs/storage/ObjectStorage.java \
  modules/storage/src/test/java/io/quorumfs/storage/StorageProcess.java \
  modules/storage/src/test/java/io/quorumfs/storage/StorageProcessTest.java \
  modules/versioning/src/main/java/io/quorumfs/versioning/Siblings.java \
  tests/system/quorum_cli.py \
  tests/system/quorum_compose.py \
  docs/adr/0006-tombstones-and-durable-recovery.md \
  docs/evidence/Q4/README.md \
  docs/handoffs/phase-5-commands.md \
  docs/handoffs/phase-5-pr.md \
  docs/runbooks/recovery.md \
  modules/replication/src/main/java/io/quorumfs/replication/Recovery.java \
  modules/storage/src/test/java/io/quorumfs/storage/RecoveryStorageTest.java
git diff --cached --check
git diff --cached --stat
git diff --cached
```

After reviewing that exact staged diff:

```bash
git commit -m 'PR 5: deletion and recovery'
git push -u origin pr-5
gh pr create --repo khurshhh1999/quorumfs --base main --head pr-5 --title 'PR 5: deletion and recovery' --body-file docs/handoffs/phase-5-pr.md
```

If a PR already exists for this branch, inspect it instead of creating another:

```bash
gh pr view pr-5 --repo khurshhh1999/quorumfs
gh pr edit pr-5 --repo khurshhh1999/quorumfs --title 'PR 5: deletion and recovery' --body-file docs/handoffs/phase-5-pr.md
```

Review checkpoint: inspect the PR and require passing checks at the final head.
An empty required-check list is not a passing gate.

```bash
pr_number=$(gh pr view pr-5 --repo khurshhh1999/quorumfs --json number --jq .number)
gh pr view "$pr_number" --repo khurshhh1999/quorumfs --web
gh pr diff "$pr_number" --repo khurshhh1999/quorumfs
gh pr checks "$pr_number" --repo khurshhh1999/quorumfs --required --watch
```

Only after review and checks succeed, in the same shell:

```bash
gh pr merge "$pr_number" --repo khurshhh1999/quorumfs --squash --delete-branch
git switch main
git pull --ff-only origin main
git status --short
```

Preserve unrelated changes; do not reset, force-push or discard work if a branch
switch or update reports a conflict. Full local evidence is in
`docs/evidence/Q4/README.md`. Database format and coordinated upgrade details are
in `docs/adr/0006-tombstones-and-durable-recovery.md`.

## Changed files

- `.gitignore`
- `README.md`
- `docs/contracts/README.md`
- `docs/handoffs/phase-1-commands.md`
- `docs/handoffs/phase-2-commands.md`
- `docs/handoffs/phase-3-commands.md`
- `docs/handoffs/phase-4-commands.md`
- `docs/runbooks/quorum.md`
- `docs/runbooks/storage.md`
- `modules/client/src/main/java/io/quorumfs/client/ClientMain.java`
- `modules/client/src/main/java/io/quorumfs/client/ObjectClient.java`
- `modules/coordinator/src/main/java/io/quorumfs/coordinator/CoordinatorService.java`
- `modules/protocol/src/main/java/io/quorumfs/protocol/Wire.java`
- `modules/protocol/src/test/java/io/quorumfs/protocol/WireTest.java`
- `modules/replication/src/main/java/io/quorumfs/replication/PeerClient.java`
- `modules/replication/src/main/java/io/quorumfs/replication/ReplicaService.java`
- `modules/server/src/test/java/io/quorumfs/server/QuorumIntegrationTest.java`
- `modules/storage/src/main/java/io/quorumfs/storage/ObjectStorage.java`
- `modules/storage/src/test/java/io/quorumfs/storage/StorageProcess.java`
- `modules/storage/src/test/java/io/quorumfs/storage/StorageProcessTest.java`
- `modules/versioning/src/main/java/io/quorumfs/versioning/Siblings.java`
- `tests/system/quorum_cli.py`
- `tests/system/quorum_compose.py`
- `docs/adr/0006-tombstones-and-durable-recovery.md`
- `docs/evidence/Q4/README.md`
- `docs/handoffs/phase-5-commands.md`
- `docs/handoffs/phase-5-pr.md`
- `docs/runbooks/recovery.md`
- `modules/replication/src/main/java/io/quorumfs/replication/Recovery.java`
- `modules/storage/src/test/java/io/quorumfs/storage/RecoveryStorageTest.java`
