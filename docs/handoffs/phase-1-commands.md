# Owner commands: Phase 1 / Q0

Inspected state: dedicated repository initialized, base commit `f72be99` exists,
origin is `https://github.com/khurshhh1999/quorumfs.git`, and the current branch
is `codex/phase-1-bootstrap`. Do not repeat initialization. Generated artifacts
have been removed from the index and are ignored. The Gradle wrapper is required
build tooling; dependency locks and verification metadata remain versioned.

```bash
cd /Users/khursheed/Documents/Playground/quorumfs
git status --short
git add -- .gitignore .gitattributes .dockerignore .github README.md Dockerfile build.gradle settings.gradle gradle.properties gradlew gradlew.bat gradle modules infra tests docs/adr docs/contracts docs/runbooks docs/evidence/Q0/README.md docs/handoffs/phase-1-commands.md docs/handoffs/phase-1-pr.md
git diff --cached --check
git diff --cached --stat
git diff --cached
```

Review the staged diff and visibility of planning files before committing.

```bash
git commit -m "build: bootstrap five-node quorumfs workspace"
git push -u origin codex/phase-1-bootstrap
gh pr create --repo khurshhh1999/quorumfs --base main --head codex/phase-1-bootstrap --title "build: bootstrap five-node quorumfs workspace" --body-file docs/handoffs/phase-1-pr.md
```

PR review checkpoint. Configure the required CI checks on main before merging;
an empty required-check list does not mean CI passed. Required names:
`format-static`, `unit`, `integration-contract`, `fault-smoke`, `build`, `security`,
`ci-gate`. Review the final head and resolve conversations; require a collaborator
review if one is available, otherwise record owner self-review.

```bash
pr_number=$(gh pr view codex/phase-1-bootstrap --repo khurshhh1999/quorumfs --json number --jq .number)
gh pr view "$pr_number" --repo khurshhh1999/quorumfs --web
gh pr diff "$pr_number" --repo khurshhh1999/quorumfs
gh pr checks "$pr_number" --repo khurshhh1999/quorumfs --required --watch
```

Only after successful required checks and completed owner review; use the same
shell as above, or repeat the PR-number lookup:

```bash
gh pr merge "$pr_number" --repo khurshhh1999/quorumfs --squash --delete-branch
git status --short
# Continue only if the working tree is clean.
git switch main
git pull --ff-only origin main
git status --short
```

If a PR already exists, inspect it instead of creating another, and update its
body with `gh pr edit codex/phase-1-bootstrap --repo khurshhh1999/quorumfs
--body-file docs/handoffs/phase-1-pr.md`. Do not force-push or reset user work.
