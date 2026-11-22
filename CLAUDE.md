@dev-llm-rules.md

# This fork

beezer-pet/instant is Beezer's fork of
[Instant](https://github.com/instantdb/instant). Nothing goes upstream: no pull
requests, issues or reports.

These `CLAUDE.md` files are the fork's own. `make setup-dev-llm-rules` is not
run here: it replaces them with links to copies of `dev-llm-rules.md`.

## Branches

- `main` is upstream's `main`, only ever fast-forwarded to it.
- `self-hosted`, the default branch, is `main` plus the fork's commits. When
  `main` moves, `self-hosted` is rebased onto it and pushed with
  `--force-with-lease`.
- Upstream's workflows that run on a push to `main` alone are kept disabled in
  the fork's Actions: `publish-self-hosted-images`, `python`, `python-publish`
  and `sync-instant-skills`. GitHub lists such a workflow only once a push to
  `main` has run it, so each is disabled after the first push that runs it.

## Commits

- Each commit is one self-contained change with its tests, in the style of the
  code around it. It may build on code that only this branch has.
- A mistake in a commit is fixed in that commit, by a fixup and a rebase, and
  never by a later commit.
- The commits about the fork itself (release tooling, CI, these files) are
  apart from the changes to Instant.

## Releases

A release is a tag `v<upstream version>-beezer.N` on `self-hosted`, such as
`v1.0.67-beezer.2`. `.github/workflows/beezer-release.yml` builds the images
`ghcr.io/beezer-pet/instant-server` and `ghcr.io/beezer-pet/instant-dashboard`
and the client packages from it, and makes the GitHub release.

The upstream version is the one in `client/packages/version/src/version.ts`,
which stays as upstream has it: the client reports it to the server, which
turns protocol features off for a version that is not plain semver.
