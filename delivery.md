# Delivery, CI/CD, and owner handoff

The Q0 CI workflow is implemented and locally linted. No remote workflow run or repository protection has been configured by the agent. The owner retains commits, pushes, PR creation and merging.

## Repository setup
Each project must have its own Git repository and remote. These folders initially sit in a larger workspace; `git rev-parse --show-toplevel` must equal the project directory before any staging or branch operations. The owner can initialize an independent repository with the project-specific commands below. Do not accidentally commit the surrounding workspace. Before the first push choose public/private visibility; internal planning files remain visible if committed to a public remote.

## Phase 1 repository setup and handoff

The owner has initialized the dedicated repository and created base commit
`f72be99`. The Phase 1 branch was `codex/phase-1-bootstrap`, and origin is
`https://github.com/khurshhh1999/quorumfs.git`. Do not repeat initial setup.

Use `docs/handoffs/phase-1-commands.md` for the updated staging and review commands.
Generated artifacts have been removed from the index and are ignored. Keep
execution reports in ignored local directories or CI artifact storage; only
Markdown summaries and source contracts belong in the commit. The Gradle wrapper,
dependency lockfiles and checksum metadata remain required build inputs.

The completed PR body is `docs/handoffs/phase-1-pr.md`. The agent has not
committed, pushed, created a PR, configured branch protections, or merged.

## Phase 2 handoff

Phase 2 branch: `codex/pr-2`; PR #2 is now merged into `main`.
Use `docs/handoffs/phase-2-commands.md`; PR body is
`docs/handoffs/phase-2-pr.md`. Local storage tests and fault checks are implemented;
remote CI at the new head remains owner-pending. Generated reports stay ignored.

## Phase 3 handoff

Current branch: `pr-3`, based on merged PR #2 / `origin/main` (`5560c6c`).
Use `docs/handoffs/phase-3-commands.md` and `docs/handoffs/phase-3-pr.md`.
Commit and PR title: **PR 3: consistent hashing and causal versions**.
The branch follows the owner's requested name without a `codex/` prefix.
Only source, tests and Markdown evidence belong in the change; build artifacts
remain ignored. Owner commit, push, PR creation, review and merge remain pending.

## Build command contract
Implement and verify a pinned Gradle wrapper and these commands in milestone 0:

| Command | Required behavior |
| --- | --- |
| `./gradlew spotlessCheck` | Enforce formatting without modifying files |
| `./gradlew test` | Fast unit/property tests, deterministic seed reported |
| `./gradlew integrationTest` | Real dependencies, persistence and contract cases |
| `./gradlew faultTest` | Deterministic recovery and network/process failure cases |
| `./gradlew check` | Formatting, static checks, unit, integration, API/protobuf compatibility |
| `./gradlew assemble` | Reproducible distributable from the checked source |
| `./gradlew smokeTest` | Black-box smoke against the selected local/staging environment |
| `./gradlew benchmark` | Opt-in controlled benchmark; output raw data and environment metadata |

These tasks exist. Integration and fault tests now include local storage, verified CLI backup/restore, five storage kill boundaries and bounded-heap streaming; the Compose proxy test covers disconnect/reconnect. `benchmark` fails explicitly until a controlled distributed workload exists. The real native ENOSPC fixture runs separately in CI. Causal-history and five-node ownership checks now run locally; distributed replication/recovery remain later milestones. Keep long fault/benchmark tasks outside ordinary `check`; the Q0 fault subset runs on every PR.

## Required PR CI
Create `.github/workflows/ci.yml` on `pull_request` and pushes to the protected base branch. Run on a clean checkout with a pinned JDK/toolchain and service images. Cancel superseded PR runs, set job timeouts, cache dependencies using verified lock/build inputs, and always upload test reports even on failure.

Stable required status names: `format-static`, `unit`, `integration-contract`, `fault-smoke`, `build`, `security`, and `ci-gate`. `ci-gate` runs with an always condition and fails if any required job failed, was canceled, or was unexpectedly skipped. Documentation-only changes may use explicit path-aware checks but must not leave required checks pending or silently skip relevant code gates. If a merge queue is enabled, handle `merge_group` with the same gates.

- Formatting/static: formatter, compiler warnings policy, static analysis, schema and deployment manifest validation.
- Unit: domain, concurrency, authorization, validation and property tests. Track coverage of changed behavior; coverage percentage is not a correctness proof.
- Integration/contract: real services, schema migrations from previous release and fresh install, protobuf/OpenAPI backward compatibility, restart persistence.
- Fault smoke: a short deterministic project-specific crash/partition suite. Nightly runs execute the complete matrix with retained seeds and operation histories.
- Build: package binaries and container images, generate SBOM, scan dependencies/images and secrets. No release push from untrusted PR jobs.
- Security: block confirmed critical/high vulnerabilities in shipped paths; exceptions require owner approval, rationale, mitigation and expiry. No blanket suppression.
- Preserve test summaries, fault histories, benchmark manifests, SBOMs and scan reports as artifacts with retention and size limits. Redact credentials and payloads.

Pin third-party GitHub Actions to reviewed full commit SHAs, use `contents: read` by default, restrict token permissions per job, and never run untrusted PR code with privileged `pull_request_target` credentials. Fork PRs receive no deployment secrets. Use cloud OIDC with repository/environment/ref restrictions for approved release jobs instead of long-lived keys. See [GitHub secure use](https://docs.github.com/en/actions/reference/security/secure-use) and [OIDC](https://docs.github.com/en/actions/concepts/security/openid-connect).

## PR and branch rules
Protect the base branch: PR-only changes, all required checks, resolved conversations, stale approvals dismissed after substantive changes, no force pushes or deletion. Require one independent reviewer when collaborators exist; for a solo owner record self-review and do not invent an approval. Require owner review for authentication, storage semantics, migrations, public contracts, infrastructure, and CI permission changes. Configure actual CODEOWNERS only after the owner supplies valid accounts.

One PR should deliver one cohesive milestone or smaller slice. PR description: problem and resulting behavior, scoped changes, contract/schema impact, tests with evidence, operational/migration risk, rollout and rollback, remaining limitations. Do not bundle both projects. Squash merge only after the owner reviews the final head and passing checks. Agents never invoke auto-merge.

## Release/CD rules
Implement `.github/workflows/release.yml` as manual dispatch of an owner-selected commit/tag from protected history. Run the complete release suite, build once, sign/attest immutable artifacts, and promote the same digest through environments. Never rebuild different bytes under the same version. Releases are owner-triggered and require protected-environment approval for production.

Staging: deploy to an isolated environment, run smoke and recovery checks, verify metrics and alerts. Production: owner approval, bounded canary, explicitly defined abort thresholds, then gradual rollout. Record version, digest, config and migration versions. Cloud infrastructure uses reviewed Terraform plans and separately approved applies with locked remote state; never destroy shared infrastructure from CI.

Use expand/contract database migrations. Run compatible additive migrations once under a migration lock; separate destructive changes into a later release after old binaries are retired and backups restored in rehearsal. Rollback means restoring the prior compatible image/config; never assume a database downgrade is safe. For storage-format changes, prove mixed-version reads and recovery before rollout; otherwise require a maintenance migration and verified backup.

Release gates: clean-install smoke, upgrade test, full fault matrix, recovery rehearsal, security checks, public examples verified, known limitations disclosed, owner-approved release notes, and reproducible evidence for any published performance claim. Benchmark failures block performance claims; correctness failures block release entirely.

## Mandatory end-of-work command handoff
The working agent must print concrete commands, not merely tell the user to commit. The templates below are documentation; replace every angle-bracket field with inspected values in the final handoff. Shell-quote literal values correctly. Generate `docs/handoffs/<scope>-pr.md` containing the finished PR body before handing off. Do not include secrets in it.

Owner-only initial setup, if the directory has no dedicated repository:
```bash
cd <absolute-project-directory>
git init -b main
git remote add origin <owner-provided-repository-url>
git switch -c codex/bootstrap
```
For a new, empty remote, first create and push a small base commit on `main` (for example only README), then create the feature branch and commit the remaining scaffold; a PR requires a base branch. The working agent must inspect whether the remote is empty before proposing this sequence. Do not use initialization commands on an existing repository or guess a URL.

Normal owner handoff, with a verified existing base branch and the working branch already checked out:
```bash
cd <absolute-project-directory>
git rev-parse --show-toplevel
git branch --show-current
git status --short
git diff --check
git diff -- <explicit-changed-paths>
git add -- <explicit-changed-paths>
git diff --cached --check
git diff --cached --stat
git diff --cached
git commit -m '<specific-message>'
git push -u origin <actual-feature-branch>
gh pr create --repo <owner/repo> --base <actual-base> --head <actual-feature-branch> --title '<specific-title>' --body-file docs/handoffs/<scope>-pr.md
```
Review checkpoint: the user reviews the staged diff before committing, then the PR and all checks before merging. If a PR already exists, use `gh pr view` and `gh pr edit --body-file` as needed instead of duplicate creation. If the base moved and causes conflicts, prepare a separate reviewed integration step; do not insert an automatic rebase or force-push.

Separate owner-only review and merge block:
```bash
pr_number=$(gh pr view <actual-feature-branch> --repo <owner/repo> --json number --jq .number)
gh pr view "$pr_number" --repo <owner/repo> --web
gh pr diff "$pr_number" --repo <owner/repo>
gh pr checks "$pr_number" --repo <owner/repo> --required --watch
```
Only after checks succeed and review is complete:
```bash
gh pr merge "$pr_number" --repo <owner/repo> --squash --delete-branch
git switch <actual-base>
git pull --ff-only origin <actual-base>
git status --short
```
If required checks are not configured, configure them before merge; an empty check list is not success. Run merge commands in the same shell as the PR lookup or repeat the lookup. Verify the working tree is clean before switching; do not stash or discard unrelated work automatically. Keep local branch deletion optional. GitHub CLI is a convenience; Git-only users can open and merge the pushed branch through the repository UI.
