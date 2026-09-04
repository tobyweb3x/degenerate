# degenerate

Cross-platform prediction-market arbitrage bot. Watches the same real-world event
priced on **Kalshi** and **Polymarket**, finds pairs of equivalent markets, and when
you can buy both sides of the same outcome for less than its $1 payout, fires both
orders at once and pockets the spread.

The system is split in two: a **Rust** trading engine (Tokio) that does market-watching,
order execution, and discovery — where a small **ML model** (a sentence-transformer
running locally on-device via `candle`, Metal-accelerated on macOS) embeds every market's
description and semantically matches equivalent events across the two venues — and a
**Go** server that persists everything and serves the review dashboard. The two halves
talk over **protobuf** defined in `protos/`, streamed over gRPC. The dashboard UI is
server-rendered with **templ** and kept live with **htmx** — no JS framework, just
hypermedia.

Personal project — trades **real money** on both venues. Read the whole README
before running anything, and start with tiny `TRADE_FRACTION` / `MIN_TRADE_SIZE`.

## By the numbers

Measured with `tokei`, excluding `target/`, `node_modules/`, `vendor/` and cached `json/` dumps:

| Component | Languages | Code lines |
|---|---|---|
| `src/` — Rust engine | Rust | 4,329 |
| `crates/` — Kalshi, Polymarket & sentence-transformer clients | Rust | 25,807 |
| `web/` — Go server + dashboard | Go 3,598 · Templ 1,316 · SQL 179 · CSS 237 | 5,330 |
| `web/view/` — vendored browser libs | JS (htmx, hyperscript, tippy, popper) | ~13,630 |
| `protos/` | Protocol Buffers | 114 |

**≈ 52k total lines**, of which the first-party engine + server code is a bit over 9.6k —
the rest is the in-repo client crates and vendored UI libraries.

---

## How it works

```
            discovery                              review                     live trading
┌────────────────────────────────┐   ┌──────────────────────┐   ┌──────────────────────────────────┐
│ Kalshi REST backfill + WS      │   │  Go web dashboard    │   │ PickerComms                      │
│ Polymarket Gamma + CLOB WS     │──▶│  (port 8090) shows   │──▶│  subscribe both legs on WS       │
│        │                       │   │  discovered pairs;   │   │  seed top-of-book over HTTP      │
│        ▼                       │   │  you accept a pair   │   │        │                         │
│ sentence-transformer (local,   │   └──────────────────────┘   │        ▼                         │
│ candle / Metal) ──▶ Qdrant     │        gRPC stream           │ PickerExec                       │
│ semantic match, score ≥ 0.75   │        (EsuOdara, :50051)    │  cost < $0.95 → FAK buy both     │
│ cross-platform hits            │                              │  legs, hedge-sell if one misses  │
└────────────────────────────────┘                              └──────────────────────────────────┘
```

### 1. Discovery — *is this the same event?*

Markets are ingested from both platforms (HTTP backfill by tag at startup, then live
WebSocket updates). Each market's text is embedded locally — a sentence-transformer
running on `candle`, Metal-accelerated on macOS — and upserted into a
[Qdrant](https://qdrant.tech) collection. Every new market is also searched against
the store for semantically similar markets **on the other platform**
(similarity threshold `0.75`); hits are emitted as discovery events over gRPC to the
web dashboard. A background task prunes points for expired markets.

The bot never trades on semantic similarity alone — a human closes the loop.

### 2. Review — *you decide*

The Go dashboard (chi + templ + htmx + Postgres/sqlc) lists discovered cross-platform pairs
with both order books. Accepting a pair sends an `Arb` (anchor + match legs, each with
token/ticker and close time) back down the gRPC stream to the bot.

### 3. Watching — *PickerComms*

For every accepted pair the bot subscribes to both legs' live top-of-book
(Polymarket CLOB market channel, Kalshi orderbook WS), seeds prices over HTTP, and
registers an `ArbWatch`. Pairs whose markets close within **1 hour** are refused.

### 4. Evaluation — *the math*

On every tick, the effective ask to own the outcome on each leg is:

| Leg | Effective ask |
|---|---|
| Polymarket | best ask |
| Kalshi (yes side) | best ask |
| Kalshi (complement side) | `1 − best_bid` (sell the other side into the bid) |

If `ask₁ + ask₂ < 0.95` the pair pays $1 at resolution for under 95¢ — at least a
5¢ edge before fees — and an `ExecutionRequest` goes to the executor. Guards:
skip while an execution for the pair is in flight, skip pairs on cooldown after a
failed execution (`ARB_COOLDOWN_SECS`), skip anything within **10 minutes** of close.

### 5. Execution — *PickerExec*

- Refreshes live balances on both venues; budget = `min(kalshi, polymarket) × TRADE_FRACTION`
- Size = `budget / (ask₁ + ask₂)`, capped by available size at top-of-book, skipped if the
  whole trade is under `MIN_TRADE_SIZE`
- Places **FAK** (fill-and-kill) orders on both legs — Polymarket via the CLOB
  (Polygon signer from `privateKey.hex`), Kalshi via its order API (RSA-signed)
- If one leg fills and the other misses, the filled leg is **hedged out** with a sell
  so you don't end up directional
- Execution mode: `EXEC_MODE=http` re-checks the order book before firing,
  `EXEC_MODE=optimistic` trusts the WebSocket top-of-book (faster, riskier)

---

## Repo layout

```
├── src/
│   ├── main.rs               # task supervision, graceful shutdown
│   ├── app.rs                # wiring: clients, channels, task spawns
│   ├── picker.rs             # comms (watch/evaluate) + exec (orders, sizing, hedging)
│   ├── platforms.rs          # shared platform handle + WS event enum
│   ├── platforms/kalshi.rs   # Kalshi WS loop, subscriptions, backfill
│   ├── platforms/polymarket.rs # Polymarket CLOB WS loop, subscriptions, backfill
│   ├── vector_store.rs       # embeddings + Qdrant search/insert/cleanup
│   ├── models.rs             # domain types + proto conversions
│   └── grpc.rs               # gRPC client (reconnecting bidirectional stream)
├── crates/
│   ├── kalshi-rs/            # Kalshi REST + WS client (RSA-PSS auth, rate limiter)
│   ├── polymarket-hft/       # Polymarket CLOB/Gamma client (WS + REST)
│   └── sentence-transformers-rs/ # local embedding inference on candle (Metal/cuda/cpu)
├── web/                      # Go dashboard: HTTP :8090 + gRPC server :50051, templ, sqlc/pgx
├── protos/                   # shared protobuf definitions (bot ⇄ dashboard)
├── json/                     # cached platform market/tag dumps used for backfills
├── examples/                 # scratch examples
├── docker-compose.yaml       # qdrant + go-backend + rust-bot
├── kalshi.pem                # Kalshi RSA private key (never commit)
└── privateKey.hex            # Polymarket wallet key (never commit)
```

## Configuration

| Variable | Used by | Meaning |
|---|---|---|
| `KALSHI_API_KEY_ID` | bot | Kalshi API key ID |
| `KALSHI_PK_FILE_PATH` | bot | Path to the Kalshi RSA private key (PEM) |
| `QDRANT_URL` | bot | Qdrant gRPC endpoint (default `http://localhost:6334`) |
| `GRPC_URL` | bot | Dashboard gRPC endpoint (default `http://127.0.0.1:50051`) |
| `EXEC_MODE` | bot | `http` (default, re-check book) or `optimistic` (trust WS) |
| `ARB_COOLDOWN_SECS` | bot | Cooldown per pair after a failed execution |
| `TRADE_FRACTION` | bot | Fraction of the smaller balance to risk per arb, `(0,1)` |
| `MIN_TRADE_SIZE` | bot | Minimum total trade size in dollars (default `$2`) |
| `POLY_API_KEY` / `POLY_API_SECRET` / `POLY_PASSPHRASE` | polymarket-hft | Optional, for Polymarket authenticated WS channels |

The Polymarket signer is read from `./privateKey.hex` in the bot's working directory.
Both key files are mounted read-only into containers — keep them out of git.

## Running

Everything (dashboard, Qdrant, bot):

```sh
make run          # generate web code (sqlc/templ), docker compose up --build
```

Bot only, against a locally running dashboard + Qdrant:

```sh
make rust         # RUSTFLAGS="-Awarnings" cargo run
make go           # web dashboard with air live-reload
```

Artefacts: `make build` (prod image), `make zip` (deployable archive, secrets excluded).

## Notes & limitations

- Only the long side is automated: the bot buys both sides of the pair; Kalshi
  complement legs are taken by selling into the bid. There is no short-leg ladder
  beyond the single hedge-sell on a partial fill.
- Semantic matching is a heuristic — a ≥ 0.75 similarity hit can still be a
  *different* resolution rule. Always sanity-check resolution criteria in the
  dashboard before accepting a pair.
- Fees on either venue eat directly into the 5¢ buffer; size accordingly.
- Yoruba naming throughout: the gRPC service is *EsuOdara*, the Qdrant collection
  is *Aroni* — the trickster gets his cut.
