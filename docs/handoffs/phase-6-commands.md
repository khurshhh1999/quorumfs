# PR 6: Merkle anti-entropy

Branch `pr-6`, base `main` at `50269b8`. Commit and PR title:
`PR 6: Merkle anti-entropy`. No commit, push, PR creation or merge performed.
Planning files, agent instructions and generated artifacts are ignored and excluded.

Inspect and stage the exact source changes:

```bash
cd /Users/khursheed/Documents/Playground/quorumfs
git switch pr-6
git status --short
git diff --check
git diff
git add -- \
  README.md \
  docs/adr/0007-snapshot-merkle-anti-entropy.md \
  docs/contracts/README.md \
  docs/evidence/Q5/README.md \
  docs/handoffs/phase-6-commands.md \
  docs/handoffs/phase-6-pr.md \
  docs/runbooks/anti-entropy.md \
  docs/runbooks/quorum.md \
  docs/runbooks/recovery.md \
  docs/runbooks/storage.md \
  modules/coordinator/src/main/java/io/quorumfs/coordinator/CoordinatorService.java \
  modules/protocol/src/main/proto/quorumfs/v1/internal.proto \
  modules/replication/src/main/java/io/quorumfs/replication/AntiEntropy.java \
  modules/replication/src/main/java/io/quorumfs/replication/Merkle.java \
  modules/replication/src/main/java/io/quorumfs/replication/MerkleService.java \
  modules/replication/src/main/java/io/quorumfs/replication/PeerClient.java \
  modules/replication/src/main/java/io/quorumfs/replication/ReplicaService.java \
  modules/replication/src/test/java/io/quorumfs/replication/MerkleTest.java \
  modules/server/src/test/java/io/quorumfs/server/QuorumIntegrationTest.java \
  modules/storage/src/main/java/io/quorumfs/storage/ObjectStorage.java \
  modules/storage/src/test/java/io/quorumfs/storage/AntiEntropyStorageTest.java \
  modules/storage/src/test/java/io/quorumfs/storage/RecoveryStorageTest.java \
  modules/storage/src/test/java/io/quorumfs/storage/StorageProcess.java \
  modules/storage/src/test/java/io/quorumfs/storage/StorageProcessTest.java
git diff --cached --check
git diff --cached --stat
git diff --cached
```

After reviewing the staged diff:

```bash
git commit -m 'PR 6: Merkle anti-entropy'
git push -u origin pr-6
gh pr create --repo khurshhh1999/quorumfs --base main --head pr-6 --title 'PR 6: Merkle anti-entropy' --body-file docs/handoffs/phase-6-pr.md
```

If the branch already has a PR, inspect/update it instead of creating a duplicate:

```bash
gh pr view pr-6 --repo khurshhh1999/quorumfs
gh pr edit pr-6 --repo khurshhh1999/quorumfs --title 'PR 6: Merkle anti-entropy' --body-file docs/handoffs/phase-6-pr.md
```

Review checkpoint: inspect the PR and require passing checks at its final head.
An empty required-check list is not success; configure required checks before merge.

```bash
pr_number=$(gh pr view pr-6 --repo khurshhh1999/quorumfs --json number --jq .number)
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

Preserve unrelated work if any command reports conflicts; never force-push or reset.
Evidence: `docs/evidence/Q5/README.md`. Storage format 3 requires a coordinated
upgrade and backup; prior binaries cannot open the upgraded data.

## Changed files

- `README.md`
- `docs/adr/0007-snapshot-merkle-anti-entropy.md`
- `docs/contracts/README.md`
- `docs/evidence/Q5/README.md`
- `docs/handoffs/phase-6-commands.md`
- `docs/handoffs/phase-6-pr.md`
- `docs/runbooks/anti-entropy.md`
- `docs/runbooks/quorum.md`
- `docs/runbooks/recovery.md`
- `docs/runbooks/storage.md`
- `modules/coordinator/src/main/java/io/quorumfs/coordinator/CoordinatorService.java`
- `modules/protocol/src/main/proto/quorumfs/v1/internal.proto`
- `modules/replication/src/main/java/io/quorumfs/replication/AntiEntropy.java`
- `modules/replication/src/main/java/io/quorumfs/replication/Merkle.java`
- `modules/replication/src/main/java/io/quorumfs/replication/MerkleService.java`
- `modules/replication/src/main/java/io/quorumfs/replication/PeerClient.java`
- `modules/replication/src/main/java/io/quorumfs/replication/ReplicaService.java`
- `modules/replication/src/test/java/io/quorumfs/replication/MerkleTest.java`
- `modules/server/src/test/java/io/quorumfs/server/QuorumIntegrationTest.java`
- `modules/storage/src/main/java/io/quorumfs/storage/ObjectStorage.java`
- `modules/storage/src/test/java/io/quorumfs/storage/AntiEntropyStorageTest.java`
- `modules/storage/src/test/java/io/quorumfs/storage/RecoveryStorageTest.java`
- `modules/storage/src/test/java/io/quorumfs/storage/StorageProcess.java`
- `modules/storage/src/test/java/io/quorumfs/storage/StorageProcessTest.java`
