# GitHub keeps disabling the scheduled pipelines

## What happens

GitHub disables scheduled workflows after **60 days with no commit on the
default branch**. This repository is almost entirely scheduled jobs, so it
sits quiet for months by design, and then GitHub switches the pipelines off.

On 2026-08-23 the last commit was **113 days old** and GitHub had already sent
the warning for *House Refresh*.

## Why it matters

It fails silently. Nothing errors, nothing alerts. The jobs simply stop:

| Workflow | Schedule |
|---|---|
| House Refresh | every 15 minutes |
| Senate Refresh | every 30 minutes |
| House Review Refresh | every 6 hours |
| Corpus Refresh | twice daily |
| Senate Reconcile | daily |
| USAspending Refresh | daily |
| Member Bio + Headshot Refresh | weekly |
| Offshore Match Refresh | weekly |

CapitolExposed reads what these produce, so the site keeps serving, just with
data that stops moving. That is worse than an outage because nobody notices.

## The fix

`docs/keepalive-workflow.yml` pushes an empty commit whenever the repo comes
within 15 days of the cutoff, which resets the timer. It runs on the 1st and
the 15th so a single failed or delayed run cannot lose the window, and it does
nothing while normal development is happening.

It is plain shell rather than a third-party keepalive action on purpose: this
repo holds pipeline credentials and the job needs `contents: write`, which is
not somewhere to add an outside dependency for ten lines of script.

`workflow_dispatch` takes a `force` input so the push path can be tested on
demand instead of waiting 45 days to discover the token cannot write.

## Installing it

The file is in `docs/` rather than `.github/workflows/` only because the token
available at the time lacked the `workflow` permission, and GitHub rejects any
push that creates a workflow file without it:

    ! [remote rejected] master -> master (refusing to allow a Personal Access
      Token to create or update workflow `.github/workflows/keepalive.yml`
      without `workflow` scope)

Any one of these lands it:

1. **gh CLI** (asks for workflow scope during login):

       gh auth login
       git mv docs/keepalive-workflow.yml .github/workflows/keepalive.yml
       git commit -m "Add keepalive workflow" && git push

2. **GitHub web UI**: Add file, Create new file,
   `.github/workflows/keepalive.yml`, paste the contents.

3. **Grant the PAT the Workflows permission** in the token settings, then
   `git mv` as above.

After it lands, run it once from the Actions tab with **force: true** to
confirm the token is allowed to push. It should create one empty commit.

## Until then

This commit resets the 60 day timer on its own, so the pipelines are safe
until roughly **2026-10-22**. The keepalive is what stops it recurring.
