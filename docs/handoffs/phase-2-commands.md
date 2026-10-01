# Owner commands — Phase 2 / Q1

Working branch `codex/pr-2` starts from merged PR #1 on `origin/main`.
No agent commit, push, PR creation or merge was performed. Generated artifacts
remain ignored; only source, configuration, tests and Markdown are included below.

```bash
cd /Users/khursheed/Documents/Playground/quorumfs
git branch --show-current
git status --short
git diff --check
git diff
git add -- .github/workflows/ci.yml Dockerfile README.md build.gradle delivery.md implementation.md plan.md modules/storage/build.gradle modules/storage/src modules/server/src/main/java/io/quorumfs/server/ServerMain.java modules/server/src/main/java/io/quorumfs/server/StorageMain.java tests/system/storage_cli.py tests/system/storage_disk_full.py docs/adr/0003-durable-local-storage.md docs/runbooks/storage.md docs/evidence/Q1/README.md docs/handoffs/phase-2-pr.md docs/handoffs/phase-2-commands.md
git diff --cached --check
git diff --cached --stat
git diff --cached
```

Review the staged changes, then:

```bash
git commit -m "PR 2: durable single-node storage"
git push -u origin codex/pr-2
gh pr create --repo khurshhh1999/quorumfs --base main --head codex/pr-2 --title "PR 2: durable single-node storage" --body-file docs/handoffs/phase-2-pr.md
```

Review checkpoint (remote checks have not run on this uncommitted head):

```bash
pr_number=$(gh pr view codex/pr-2 --repo khurshhh1999/quorumfs --json number --jq .number)
gh pr view "$pr_number" --repo khurshhh1999/quorumfs --web
gh pr diff "$pr_number" --repo khurshhh1999/quorumfs
gh pr checks "$pr_number" --repo khurshhh1999/quorumfs --required --watch
```

After required checks pass and owner review is complete, use the same shell:

```bash
gh pr merge "$pr_number" --repo khurshhh1999/quorumfs --squash --delete-branch
git status --short
# Continue only with a clean working tree.
git switch main
git pull --ff-only origin main
```

An empty required-check list does not mean CI passed. Do not force-push or reset
unrelated work. Review the storage-format upgrade/rollback instructions before
running this version against any volume that must be retained.
