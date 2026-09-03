name: Refresh market data

on:
  schedule:
    # v40: tightened from every 30 minutes to every 15 — GitHub's own docs
    # say scheduled runs can be delayed or dropped under load, "especially
    # at the start of every hour," and the 30-min offset schedule below
    # (6/36 past the hour) has now gone quiet for real TWICE: ~2h53m on
    # 2026-08-17, and ~1h45m again just before this change. Firing twice
    # as often doesn't make any single run more likely to fire, but it
    # roughly doubles how many independent chances there are within any
    # given hour, meaningfully shrinking the odds of a long gap. This is
    # deliberately paired with a second, independent fix: a Cloudflare
    # Worker Cron Trigger (see cloudflare-worker/worker.js's
    # triggerMarketDataRefresh in the main Vestly repo) now force-runs
    # this exact workflow every 15 minutes too, on a completely different
    # scheduler — that's the real reliability backstop; this tighter
    # native schedule is cheap, easy defense-in-depth on top of it, not a
    # guarantee on its own. Still offset from the round quarter-hour marks
    # (GitHub's busiest scheduling window), same reasoning as before.
    - cron: '6,21,36,51 * * * *'
  # Lets you trigger it by hand from the Actions tab, e.g. to test it once
  # right after setting this up, without waiting for the schedule. Also
  # what the Cloudflare Worker above calls via the REST API, on its own
  # independent 15-min schedule.
  workflow_dispatch: {}

# Needed so this workflow can commit the updated market_data.json back into
# the repo using the built-in token — no extra secrets or personal access
# tokens required.
permissions:
  contents: write

jobs:
  refresh:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - run: pip install -r requirements.txt

      - run: python update_market_data.py

      - name: Commit updated market_data.json
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add market_data.json
          if git diff --staged --quiet; then
            echo "No changes to commit."
          else
            git commit -m "Auto-refresh market data"
            git push
          fi
