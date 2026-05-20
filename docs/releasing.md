# Releasing

Boardwalk releases are driven from GitHub Actions through Cocogitto.

Use the **Release Plan** workflow in `plan` mode first. It reports the
current commit, recent CI status, the version Cocogitto will publish,
and a changelog preview. For an exact release, set the optional
`version` input, for example `1.0.0`. After checking the plan, re-run
the same workflow in `release` mode with the same version input.

Release mode creates the Cocogitto version commit and tag, pushes both
to `main`, and dispatches the **Release** workflow for that tag. The
Release workflow publishes `boardwalk-macros` first, waits until that
version is visible on crates.io, publishes `boardwalk`, then creates the
GitHub Release.

## Local helper

```sh
./scripts/run-release-plan.sh
./scripts/run-release-plan.sh plan 1.0.0
./scripts/run-release-plan.sh plan --version 1.0.0
./scripts/run-release-plan.sh release 1.0.0
./scripts/run-release-plan.sh release --version 1.0.0
```

Set `RELEASE_PLAN_DIR=.` to keep the downloaded `release-plan.md`.

## Required repository setup

GitHub repository settings:

- Actions workflow permissions must allow **Read and write permissions**.
- Branch protection on `main` must allow this release workflow to push
  the Cocogitto version commit and tag, or the workflow must run with a
  token/account that is allowed to bypass the protection.

Repository secrets:

- `CARGO_REGISTRY_TOKEN` — crates.io API token with publish access for
  `boardwalk` and `boardwalk-macros`.
- `GPG_PRIVATE_KEY` — private key used by the Release Plan workflow to
  sign the Cocogitto version commit and tag.

No Homebrew, npm, or binary-asset secrets are needed for Boardwalk.

## Cocogitto major bumps

For normal automatic releases, Cocogitto infers a major bump from a
breaking-change conventional commit such as `feat!:` or a commit with a
`BREAKING CHANGE:` footer. For the first Boardwalk `1.0.0` release, use
the explicit workflow version input instead of creating an artificial
breaking-change commit.
