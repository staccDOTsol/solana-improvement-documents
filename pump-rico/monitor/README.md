# pump.fun Coordinated Wash-Trading Cluster Monitor

Live: https://clouter-web.fly.dev/

A dependency-free (stdlib only) Python service that:
- serves a reframed dashboard (`page.html`) — RICO used strictly as an analytical lens, with explicit limits (heuristic FPR unvalidated, MEV/arb not excluded, selection bias, n=1 control, base-rate caveat, frozen hash-pinned snapshot, no named individuals, exchange labels are heuristic terminals);
- runs a forward scanner (`app.py`) that watches the seed fingerprint wallets, scores new launches by (fleet recurrence + coordinated fresh big buyers + wash ratio), and **behaviorally discovers new fingerprint wallets** — any wallet seen exhibiting the signature at >=2 distinct clusters is promoted into the watchlist;
- persists detections to SQLite on a Fly volume (`/data`).

Deploy: `docker build` -> push `registry.fly.io/clouter-web` -> `flyctl deploy --ha=false`.
Secret: `HELIUS_KEY` (set via `flyctl secrets`). Never commit keys/tokens.
