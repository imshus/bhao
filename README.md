# Gold Rate Tracker — AWS deployment

Fetches live gold/silver rates from multiple bullion dealers' public rate
feeds every 30 minutes, stores history in DynamoDB, and serves it through a
JSON API + a small live dashboard.

## Architecture

```
  EventBridge rate(30 min)            EventBridge rate(15 min)
         {}                              {"stream": true}
          |                                     |
          v                                     v
     +---------------------------------------------+
     |        Lambda: gold-tracker-fetch           |
     |  scrapes all 4 dealer feeds over plain HTTP |
     +---------------------------------------------+
        |                                   |
        | writes history                    | pushes only what changed
        | (every 30 min)                    | (every 2s, no writes)
        v                                   v
  DynamoDB                        API Gateway WebSocket API
  gold-rate-tracker                   |            ^
        |                             |            | $connect / $disconnect
        v                             v            |
  Lambda: gold-tracker-api         browser    Lambda: gold-tracker-ws
        ^                             ^                  |
        | API Gateway (HTTP)          |                  v
        '------ fallback polling -----'          DynamoDB: ...-connections
                                      ^
       S3 (static site) + CloudFront -'
```

Two paths carry the same data. The HTTP API is the durable one (history in
DynamoDB, a plain JSON endpoint anyone can consume). The WebSocket is the live
one: while a browser is connected, the fetch Lambda polls every 2s and pushes
only what changed. If the socket is unavailable the dashboard silently falls
back to polling the HTTP API, so the WebSocket is an upgrade, never a
dependency.

Everything runs on AWS's always-free or near-free tiers (Lambda, DynamoDB
on-demand, API Gateway, S3, CloudFront). At this scale (4-5 sources, one
fetch every 30 min, occasional dashboard/API traffic) the realistic monthly
cost is close to $0.

## One-time setup (new AWS account)

1. Get AWS credentials for the target account — either your own IAM user's
   Access Key ID + Secret Access Key, or ask whoever administers the
   account to create one for you (IAM -> Users -> Create user -> attach
   `AdministratorAccess` for simplicity -> Security credentials -> Create
   access key -> "Command Line Interface (CLI)").
2. Configure a CLI profile:
   ```bash
   aws configure --profile gold-tracker
   ```
   (Access Key, Secret Key, region `ap-south-1`, output `json`)
3. Open `deploy.sh` and set `BUDGET_EMAIL` at the top to wherever billing
   alerts should go. Optionally adjust `BUDGET_LIMIT_USD` (default $5/month).
4. Run it:
   ```bash
   bash deploy.sh
   ```
   It prints the dashboard URLs and API endpoint at the end. Takes a few
   minutes; CloudFront's HTTPS URL takes ~10-15 min to fully propagate
   (the plain S3 URL works immediately).
5. Re-running `deploy.sh` is safe — it updates existing resources instead
   of failing on "already exists".

## Live updates (WebSocket)

### Why the scrape itself can't be a WebSocket

The dealers' feed is HTTP-only. This was checked directly, not assumed:

- Their own `LiveRates.html` polls - `setInterval("CallWebServiceFromJquery()", 500)`
  in `js/LiveRates3.js`, a jQuery AJAX GET against the same
  `GetLiveRateByTemplateID` URL this project uses.
- A real WebSocket handshake sent to that URL returns `HTTP/1.1 200 OK` with
  the plain text body - not `101 Switching Protocols`. The server ignores the
  `Upgrade` header.
- `/signalr/negotiate`, `/VOTSBroadcastStreaming/signalr/negotiate`, `/hubs`
  and the bare host all return 404. Despite the "BroadcastStreaming" name
  there is no socket, no SSE and no long-poll on their side.

So the scrape stays a GET. The WebSocket is how the scraped result is
delivered onward, which is where it actually helps: the polling happens once,
server-side, and every connected browser is pushed to.

### How it works when deployed

| Piece | Role |
|---|---|
| `gold-tracker-ws` Lambda | handles `$connect` / `$disconnect` / `$default`, stores connection ids |
| `gold-rate-tracker-connections` table | who is connected right now (TTL-swept) |
| `gold-tracker-fetch` Lambda, `{"stream": true}` | polls every 2s and pushes changes |
| `gold-tracker-stream` rule | fires that every 15 min; each run streams for up to 14 |

The streaming run **checks for connections first and returns immediately if
nobody is watching**, so an idle dashboard costs four sub-second invocations
an hour. History is still written only by the 30-minute schedule - at a
2-second cadence, persisting every round would be ~170k DynamoDB writes a day
for data nothing reads back.

Cost while someone *is* watching: WebSocket messages are $1.00/million and
connection-minutes $0.25/million, and the streaming Lambda at 128MB stays
inside the free compute tier. A dashboard left open through a full trading day
lands in the low single-digit dollars a month; an idle one is still ~$0.

### Deploying it

`bash deploy.sh` provisions all of it and prints the `wss://` endpoint, which
it also injects into the uploaded dashboard (replacing the
`wss://WEBSOCKET_ENDPOINT_PLACEHOLDER` literal in `site/index.html`). Until
that runs, the deployed page simply polls as before.

### Live on our own server (jmd.mrpscan.com)

`jmd.mrpscan.com` is an EC2 box running nginx. It doesn't need API Gateway or
Lambda: `local_server.py` already is a complete WebSocket server (one poller
thread per dealer, push on change, heartbeat every 20s), so it runs there as a
systemd service on `127.0.0.1:7005` and nginx proxies the domain to it.

1. On the server: `sudo bash server_setup.sh` (from `deploy/`; it clones or
   pulls the repo into `/opt/bhao`, installs `gold-tracker.service`, starts it).
2. Paste the two `location` blocks from `deploy/nginx-jmd.mrpscan.com.conf`
   into the domain's HTTPS `server {}` block, then
   `sudo nginx -t && sudo systemctl reload nginx`.
3. Optional: put the Mega Bullion login in `/opt/bhao/.env` and
   `sudo systemctl restart gold-tracker` - the server-side poller is the only
   place those 99.50 rows can be fetched from.

Updating later is just re-running step 1.

Live endpoints on the server:

| URL | What it gives |
|---|---|
| `/` | dashboard (WebSocket push) |
| `/ws` | WebSocket: `{"type":"snapshot"}` on connect, then `{"type":"update","source":{...}}` per dealer change, `{"type":"heartbeat"}` every 20s |
| `/api/stream` | the same messages as Server-Sent Events (`data: {...}` lines) over plain HTTPS |
| `/api` | JSON snapshot for scripts; opened in a browser tab it streams off `/api/stream` and stays live. `/api?raw` always returns plain JSON |
| `/api/3min` | every dealer, refetched once every 3 minutes |

`/api/stream` sends `X-Accel-Buffering: no`, so nginx passes each event
through as-is with no extra config.

How the page picks its transport, wherever it's hosted:

| Situation | What the page does |
|---|---|
| `/ws` answers on the same origin (local_server, directly or via nginx) | WebSocket push |
| `deploy.sh` filled in the API Gateway `wss://` URL | WebSocket push via API Gateway |
| No socket (plain static hosting, server restarting) | polls each dealer from the browser every 0.5s, and keeps retrying the socket |
| localhost with `--dynamo` | polls `/api` every 30s |

A socket that stays open but goes silent for 60s (no heartbeat) is dropped and
reconnected, so a stalled proxy can't freeze the numbers. `?direct` forces
browser polling.

## What gets fetched

Every row and every column each dealer publishes is captured. A feed line is
8 tab-separated fields (a blank, then id, label, buy, sell, high, low, then
another blank); all six meaningful ones are kept, for all rows:

```json
{
  "rows":     [ { "label": "Gold Future MCX", "buy": "151890", "sell": "151920",
                  "high": "152963", "low": "151650", "note": null }, ... ],
  "all_rows": [ { "id": "5398", "label": "GOLD($)", "buy": "4309.00",
                  "sell": "4309.10", "high": "4369.05", "low": "4295.65" }, ... ],
  "row_count": 7
}
```

`rows` is the three canonical products mapped per dealer (unchanged, so the
API contract holds). `all_rows` is the untouched feed: spot gold and silver in
USD, the USD-INR rate, silver futures/costing, bank RTGS rows, and day
high/low on every one of them.

Current row counts: jmd_patil 7, mega_bullion 5, shri_sai 7, shri_ganesh 5.

### Blank cells, and why each one is blank

Nothing is ever substituted for a missing rate, but every blank now carries a
`note` saying which kind of missing it is:

| Dealer | Blank | Why |
|---|---|---|
| mega_bullion | 99.50 Cash + RTGS | login-only (see below) |
| shri_sai | 99.50 Cash | dealer publishes no cash row, only RTGS |
| shri_sai | 99.50 RTGS *buy* | dealer publishes a literal `-` there |
| shri_ganesh | 99.50 Cash + RTGS | dealer publishes only `GOLD/SILVER COSTING` |

### Mega Bullion's missing 99.50 rows

Their public `mega` template carries 5 rows and none of the 99.50 ones. Their
own page resolves a **per-customer** template first:

```js
// their js/TemplateID.Chirayu.js
fetch('https://order.megabullion.info:8889/VOTSMobile/Services/xml/getTemplateID/' + user + '/' + password)
```

and only falls back to `mega` when nobody is logged in - which is what we were
getting. With a Mega Bullion account, set:

```bash
MEGA_BULLION_USER=...  MEGA_BULLION_PASSWORD=...  python local_server.py
```

(or the same two variables on the `gold-tracker-fetch` Lambda). The template
id is then resolved once every 15 minutes and the full feed is fetched instead.
Without them nothing changes - the public template is used and those rows stay
blank rather than invented.

### Templates that look promising but aren't

Every dealer page also sets `coinsScripTemplateId`, and all four of those
templates return **0 rows** - configured in the page, never populated on the
server. Checked and empty, so don't re-investigate:

| Dealer | Second template | Result |
|---|---|---|
| jmd_patil | `jmdcoins` | 0 rows |
| mega_bullion | `megasilver` | 0 rows |
| shri_sai | `saicoins` | 0 rows |
| shri_ganesh | `shriganeshbullioncoins` | 0 rows |

## Adding a new bullion dealer source

Almost all of these dealers use the same vendor's ("Chirayu Softech")
software, which exposes a public, unauthenticated rate feed at:

```
https://bcast.<their-domain>:7768/VOTSBroadcastStreaming/Services/xml/GetLiveRateByTemplateID/<template>
```

To find a new dealer's feed URL and template ID:
1. Open their live-rates webpage (usually named `LiveRates.html`) in a
   browser with dev tools open.
2. In the console, run:
   ```js
   JSON.stringify({ip: localStorage.ipAddressBCast, port: localStorage.step3StreamingPort, tmpl: localStorage.defaultScripTemplateId})
   ```
3. If that returns values, fetch the feed URL directly to see their row
   labels (it's plain tab-separated text: `id, label, buy, sell, high, low`
   per line).
4. Add an entry to the `SOURCES` list in `lambda_fetch/lambda_function.py`,
   following the existing examples. Every source must map to exactly 3
   canonical rows — `Gold Future MCX`, `99.50 Gold Cash`, `99.50 Gold RTGS`
   — using `None` for any row the dealer doesn't publish (leave it blank,
   never fabricate a number).
5. Add the source's id to the `SOURCES` list in `lambda_api/lambda_function.py`
   too (so the API/dashboard picks it up).
6. Redeploy: `bash deploy.sh` (or just the two Lambda update commands if
   you don't want to touch anything else).

Some dealers don't fit this pattern (Cloudflare-protected sites, or
app-only rates with no public website) — those need a different approach
(headless browser or app traffic interception) and aren't handled by this
script.

## Running it on localhost

`local_server.py` runs the whole thing on your own machine - dashboard, live
WebSocket stream and API on one port, no AWS account, no DynamoDB, nothing to
deploy:

```bash
pip install -r requirements.txt
python local_server.py
```

Then open <http://localhost:7005>. The dashboard detects it's on localhost,
opens a WebSocket to `ws://localhost:7005/ws`, and re-renders the instant a
rate changes - no 30-second wait.

**Restart the server after editing `local_server.py`** - Python reads the file
once at startup, so a running process keeps serving the old code (a `/ws` that
404s is the usual symptom).

Each dealer is polled on **its own thread and its own clock**. Their servers
randomly stall a request for ~1s (occasionally 3s), rotating between hosts;
with one shared batch loop a single straggler delayed all four cards. Polled
independently, a stalled dealer only delays its own card.

| Route | What it does |
|---|---|
| `/` | the dashboard from `site/` |
| `/ws` | WebSocket; rates are **pushed** as they change |
| `/api` | one-shot JSON snapshot, same shape the deployed API returns |

Options:

- `python local_server.py --port 9000` - different port.
- `python local_server.py --interval 2` - how often the dealer feeds are polled
  server-side. Default is **0.5s**, the same cadence the dealers' own
  `LiveRates.html` uses; raise it if you want to be lighter on their servers.
- `python local_server.py --dynamo` - read the stored history from the real
  DynamoDB table instead of polling feeds. Needs credentials
  (`AWS_PROFILE=gold-tracker AWS_DEFAULT_REGION=ap-south-1`). The WebSocket is
  disabled in this mode and the dashboard falls back to polling `/api`.

### Local behaviour notes

Same reasoning as [Live updates (WebSocket)](#live-updates-websocket) - the
source is HTTP-only, so `local_server.py` polls it and pushes the result on.
Locally that gives you:

- A push the moment a rate moves, instead of the browser re-polling.
- Ten open tabs still cost the dealer one request per tick, not ten.
- A frame only when a value actually changed (a fresh timestamp alone doesn't
  count), so an idle market means an idle socket.
- A dropped socket falls back to polling `/api` every 30s and reconnects with
  exponential backoff (1s -> 15s), so the page self-heals.

The WebSocket server is written against stdlib `socket`/`struct` (RFC 6455
handshake + framing), keeping the project free of runtime dependencies beyond
`boto3`.

Nothing is written to DynamoDB in this mode. Blank (---) cells are normal: a
dealer that isn't publishing a category right now is left empty rather than
filled with a substituted number.

### Testing just the fetch logic

```bash
cd lambda_fetch
python -c "
from lambda_function import build_source_record, SOURCES
import json
for src in SOURCES:
    print(json.dumps(build_source_record(src, '2026-01-01T00:00:00'), indent=2))
"
```

This only prints what *would* be written - it doesn't touch DynamoDB unless
you call `handler({}, {})` instead, which does write (and needs credentials).

SSL on Windows is handled in code: `fetch_feed` uses certifi's CA bundle when
certifi is installed, which avoids the local `CERTIFICATE_VERIFY_FAILED`
quirk, and falls back to the system default on Lambda where certifi isn't
present. `boto3` is likewise imported lazily inside `handler()`, so the module
imports fine with no AWS setup at all.

`requirements.txt` is **only** for this local running - the deployed Lambda
package doesn't need it, since AWS's Python runtime already ships with
`boto3`. `deploy.sh` zips `lambda_function.py` as-is, nothing more.

## Decommissioning the old (personal) AWS account

Once this is confirmed working in the new account, delete these resources
from the old account to stop any further billing there:
- Lambda functions: `gold-tracker-fetch`, `gold-tracker-api`, `gold-tracker-ws`
- DynamoDB tables: `gold-rate-tracker`, `gold-rate-tracker-connections`
- EventBridge rules: `gold-tracker-schedule`, `gold-tracker-stream`
- API Gateway: `gold-tracker-api` (HTTP) and `gold-tracker-ws` (WebSocket)
- S3 bucket: `gold-tracker-dashboard-<old-account-id>`
- CloudFront distribution (disable first, then delete)
- IAM role: `gold-tracker-lambda-role`
