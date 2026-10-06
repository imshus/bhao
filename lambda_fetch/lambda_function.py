import json
import os
import re
import ssl
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

# boto3 is imported lazily inside handler() so this module can be imported
# (and the fetch/parse logic exercised) on a local machine that has no AWS
# credentials or no boto3 installed at all - see local_server.py.

IST = timezone(timedelta(hours=5, minutes=30))
TABLE_NAME = "gold-rate-tracker"

# WebSocket push (deploy.sh sets these on the Lambda). Unset -> push is off and
# the function just stores history, so local imports never need them.
CONNECTIONS_TABLE = os.environ.get("CONNECTIONS_TABLE", "")
WS_ENDPOINT = os.environ.get("WS_ENDPOINT", "")
STREAM_SECONDS = int(os.environ.get("STREAM_SECONDS", "840"))
POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "2"))
CONNECTION_CHECK_SECONDS = int(os.environ.get("CONNECTION_CHECK_SECONDS", "10"))

# Every source shows exactly these 3 rows, in this order. If a dealer's feed
# doesn't publish that exact category, the row is left blank (None) rather
# than substituting something else in.
CANONICAL_LABELS = ["Gold Future MCX", "99.50 Gold Cash", "99.50 Gold RTGS"]

SOURCES = [
    {
        "id": "jmd_patil",
        "name": "JMD Patil",
        "url": "https://bcast.jmdpatil.com:7768/VOTSBroadcastStreaming/Services/xml/GetLiveRateByTemplateID/jmd",
        "site_url": "https://jmdpatil.com/LiveRates.html",
        "rows": [
            {"match": "exact", "label": "GOLD FUTURE"},
            {"match": "exact", "label": "99.50 GOLD"},
            {"match": "exact", "label": "99.50 GOLD (RTGS)"},
        ],
    },
    {
        "id": "mega_bullion",
        "name": "Mega Bullion",
        "url": "https://bcast.megabullion.info:7768/VOTSBroadcastStreaming/Services/xml/GetLiveRateByTemplateID/mega",
        "site_url": "https://megabullion.info/LiveRates.html",
        # The public "mega" template only carries 5 rows - the 99.50 rows below
        # exist only on a logged-in customer's template. Set MEGA_BULLION_USER
        # and MEGA_BULLION_PASSWORD to fetch those too.
        "template_lookup": {
            "url": "https://order.megabullion.info:8889/VOTSMobile/Services/xml/getTemplateID/{user}/{password}",
            "user_env": "MEGA_BULLION_USER",
            "password_env": "MEGA_BULLION_PASSWORD",
            "feed_url": "https://bcast.megabullion.info:7768/VOTSBroadcastStreaming/Services/xml/GetLiveRateByTemplateID/{template}",
        },
        "rows": [
            {"match": "exact", "label": "GOLD FUTURE"},
            # Buy and sell come from two different feed rows here: "GOLD 99.50
            # LIVALI" is the dealer's buy-back price, "GOLD 99.50 SELL" is what
            # they charge to sell. Both use column 3 (column 4 just mirrors
            # Gold Future's own sell price on every "99.50" row in this feed).
            {"split": True, "buy_label": "GOLD 99.50 LIVALI", "sell_label": "GOLD 99.50 SELL"},
            # RTGS only publishes one real number (column 3, the final
            # delivery price). Column 4 just mirrors Gold Future's own sell
            # price - shown here as "Buy" anyway per request, even though
            # it's a duplicate of the Gold Future MCX sell price above, not
            # an independent RTGS buy-back rate. It still updates live since
            # Gold Future's sell price itself moves every fetch.
            {"match": "exact", "label": "GOLD 99.50 RTGS", "sell_col": 3},
        ],
    },
    {
        "id": "shri_sai",
        "name": "Shri Sai Jewels",
        "url": "https://bcast.shrisaijewels.in:7768/VOTSBroadcastStreaming/Services/xml/GetLiveRateByTemplateID/sai",
        "site_url": "http://www.shrisaijewels.in/",
        "rows": [
            {"match": "exact", "label": "GOLD FUTURE"},
            None,  # this dealer has no "99.50 Gold Cash" category
            {"match": "exact", "label": "Gold 9950 RTGS"},
        ],
    },
    {
        "id": "shri_ganesh",
        "name": "Shri Ganesh Bullion",
        "url": "https://bcast.shriganeshbullion.com:7768/VOTSBroadcastStreaming/Services/xml/GetLiveRateByTemplateID/shriganeshbullion",
        "site_url": "https://shriganeshbullion.com/",
        # This dealer has no "GOLD FUTURE" or "99.50" categories at all - only
        # a single "GOLD COSTING" benchmark row, mapped to the Gold Future
        # MCX slot. Cash and RTGS stay blank since there's no matching data.
        "rows": [
            {"match": "exact", "label": "GOLD COSTING"},
            None,
            None,
        ],
    },
]


def _build_ssl_context():
    """Lambda's runtime trusts these feeds out of the box; local Windows Python
    often doesn't (CERTIFICATE_VERIFY_FAILED), so prefer certifi's CA bundle
    when it's installed, on top of the system store - antivirus HTTPS scanning
    (AVG here) re-signs feeds with a root only the Windows store trusts."""
    ctx = ssl.create_default_context()
    try:
        import certifi

        ctx.load_verify_locations(cafile=certifi.where())
    except ImportError:
        pass
    return ctx


SSL_CONTEXT = _build_ssl_context()


def fetch_feed(url: str) -> str:
    req = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=15, context=SSL_CONTEXT) as resp:
        return resp.read().decode("utf-8", errors="replace")


def parse_feed(raw_text: str) -> list:
    """Every row the dealer publishes, with every column.

    A feed line is 8 tab-separated fields: a blank, then
    id, label, buy, sell, high, low, then another blank. Only the outer blanks
    are trimmed - a blank field in the middle is kept in place, so a dealer
    leaving one column empty can't silently shift the rest one to the left.
    """
    parsed = []
    for line in raw_text.splitlines():
        parts = [p.strip() for p in line.split("\t")]
        while parts and parts[0] == "":
            parts.pop(0)
        while parts and parts[-1] == "":
            parts.pop()
        if len(parts) < 6:
            continue
        row_id, label, buy, sell, high, low = parts[0:6]
        parsed.append({
            "id": row_id,
            "label": label,
            "buy": buy,
            "sell": sell,
            "high": high,
            "low": low,
        })
    return parsed


def to_num(s):
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def find_row(feed_rows, label):
    """The whole row for an exact label match, or None."""
    for row in feed_rows:
        if row["label"] == label:
            return row
    return None


# Some dealers publish only a cut-down feed to the public and keep the rest
# behind a customer login. Mega Bullion is one: the public "mega" template
# carries 5 rows (spot gold/silver, USD-INR, gold & silver futures) and none
# of the 99.50 rows. Their own page resolves a per-customer template id from
# order.megabullion.info first, and only falls back to "mega" when nobody is
# logged in - which is exactly what we were getting.
#
# Set the matching env vars (see README) and this resolves that template id
# and fetches the full feed instead. Without them, nothing changes: the public
# template is used and the extra rows stay blank rather than invented.

_TEMPLATE_CACHE = {}
TEMPLATE_CACHE_SECONDS = 15 * 60
# A failed lookup (wrong login, endpoint down) is remembered too, so a 500ms
# poller doesn't hit the dealer's login endpoint on every round.
TEMPLATE_RETRY_SECONDS = 5 * 60


def fetch_template_id(source):
    """The customer-specific template id for a source, or None."""
    cfg = source.get("template_lookup")
    if not cfg:
        return None

    user = os.environ.get(cfg["user_env"], "").strip()
    password = os.environ.get(cfg["password_env"], "").strip()
    if not user or not password:
        return None

    cached = _TEMPLATE_CACHE.get(source["id"])
    if cached:
        ttl = TEMPLATE_CACHE_SECONDS if cached[0] else TEMPLATE_RETRY_SECONDS
        if time.time() - cached[1] < ttl:
            return cached[0]

    template = _lookup_template(source, cfg, user, password)
    _TEMPLATE_CACHE[source["id"]] = (template, time.time())
    return template


def _lookup_template(source, cfg, user, password):
    url = cfg["url"].format(
        user=urllib.parse.quote(user, safe=""),
        password=urllib.parse.quote(password, safe=""),
    )
    try:
        raw = fetch_feed(url).strip()
    except Exception as e:
        print("template lookup failed for %s: %s" % (source["id"], e))
        return None

    # The service answers either a bare string or an XML-wrapped one.
    if raw.startswith("<"):
        try:
            import xml.etree.ElementTree as ET

            raw = (ET.fromstring(raw).text or "").strip()
        except Exception:
            return None
    raw = raw.strip('"').strip()

    if not re.match(r"^[A-Za-z0-9_-]+$", raw) or raw.lower() in ("null", "undefined", "0", "-1"):
        print("template lookup for %s returned no usable template id" % source["id"])
        return None

    return raw


def feed_url_for(source):
    """The URL to actually fetch: the customer feed when credentials resolve a
    template id, otherwise the dealer's public one."""
    template = fetch_template_id(source)
    if template:
        return source["template_lookup"]["feed_url"].format(template=template)
    return source["url"]


def row_note(source, row_cfg, cells):
    """Why a cell is blank. A missing rate is never invented, but it should at
    least say which kind of missing it is - the dealer not publishing that
    product at all is a different fact from the row being login-only."""
    if row_cfg is None:
        return "This dealer does not publish this category"

    if cells["sell"] is None and cells["buy"] is None:
        lookup = source.get("template_lookup")
        if lookup and not fetch_template_id(source):
            return ("Published only on this dealer's customer template - set %s and %s "
                    "to fetch it" % (lookup["user_env"], lookup["password_env"]))
        return "Row not present in the dealer's feed right now"

    if cells["buy"] in (None, "-", ""):
        return "Dealer publishes no buy-back price for this row"

    return None


def build_source_record(source: dict, timestamp: str) -> dict:
    raw_text = fetch_feed(feed_url_for(source))
    feed_rows = parse_feed(raw_text)

    matched_rows = []
    for canonical_label, row_cfg in zip(CANONICAL_LABELS, source["rows"]):
        cells = {"buy": None, "sell": None, "high": None, "low": None}

        if row_cfg is not None and row_cfg.get("split"):
            buy_row = find_row(feed_rows, row_cfg["buy_label"])
            sell_row = find_row(feed_rows, row_cfg["sell_label"])
            cells["buy"] = buy_row["buy"] if buy_row else None     # column 3 of the buy row
            cells["sell"] = sell_row["buy"] if sell_row else None  # column 3 of the sell row
            if sell_row:   # high/low belong to the row the sell price came from
                cells["high"] = sell_row["high"]
                cells["low"] = sell_row["low"]

        elif row_cfg is not None:
            found = None
            for row in feed_rows:
                if row_cfg["match"] == "exact" and row["label"] == row_cfg["label"]:
                    found = row
                    break
                if row_cfg["match"] == "startswith" and row["label"].startswith(row_cfg["label"]):
                    found = row
                    break

            if found:
                cells["buy"], cells["sell"] = found["buy"], found["sell"]
                cells["high"], cells["low"] = found["high"], found["low"]
                if row_cfg.get("sell_col") == 3:
                    # column 3 is the true sell price for this row type;
                    # column 4 (originally read as "sell") becomes "buy" instead.
                    cells["buy"], cells["sell"] = cells["sell"], cells["buy"]
                if row_cfg.get("no_buy"):
                    cells["buy"] = None

        matched = {"label": canonical_label}
        matched.update(cells)
        matched["note"] = row_note(source, row_cfg, cells)
        matched_rows.append(matched)

    # Badla Bhaw: diff1 = Cash sell - Future buy; diff2 = RTGS sell - Future buy.
    # Dealers price Cash/RTGS as Future *sell* + a fixed premium, so measuring
    # against Future sell would never move; Future buy keeps the badla live.
    diffs = {}
    s0 = to_num(matched_rows[0]["buy"]) if len(matched_rows) > 0 else None
    s1 = to_num(matched_rows[1]["sell"]) if len(matched_rows) > 1 else None
    s2 = to_num(matched_rows[2]["sell"]) if len(matched_rows) > 2 else None

    diffs["diff1"] = (s1 - s0) if (s0 is not None and s1 is not None) else None
    diffs["diff2"] = (s2 - s0) if (s2 is not None and s0 is not None) else None

    return {
        "source": source["id"],
        "timestamp": timestamp,
        "name": source["name"],
        "site_url": source["site_url"],
        "rows": matched_rows,
        # Everything else the dealer publishes, untouched: spot gold/silver,
        # the USD-INR rate, silver futures/costing, bank RTGS rows, and the
        # day high/low on every one of them - not just the three canonical
        # products mapped above.
        "all_rows": feed_rows,
        "row_count": len(feed_rows),
        "diff1": diffs["diff1"],
        "diff2": diffs["diff2"],
    }

def ws_enabled():
    return bool(CONNECTIONS_TABLE and WS_ENDPOINT)


def list_connections():
    if not ws_enabled():
        return []
    import boto3

    table = boto3.resource("dynamodb").Table(CONNECTIONS_TABLE)
    items, kwargs = [], {"ProjectionExpression": "connectionId"}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            return [i["connectionId"] for i in items]
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def push_to_connections(message, connection_ids=None):
    """Fan one JSON message out to every open browser. Connections that have
    gone away come back as GoneException - those rows get deleted."""
    if not ws_enabled():
        return 0

    import boto3

    if connection_ids is None:
        connection_ids = list_connections()
    if not connection_ids:
        return 0

    gateway = boto3.client("apigatewaymanagementapi", endpoint_url=WS_ENDPOINT)
    table = boto3.resource("dynamodb").Table(CONNECTIONS_TABLE)
    data = json.dumps(message, default=str).encode("utf-8")

    sent = 0
    for connection_id in connection_ids:
        try:
            gateway.post_to_connection(ConnectionId=connection_id, Data=data)
            sent += 1
        except gateway.exceptions.GoneException:
            table.delete_item(Key={"connectionId": connection_id})
        except Exception as e:
            print("push to %s failed: %s" % (connection_id, e))
    return sent


def fetch_all(timestamp):
    """All sources in parallel. One dealer stalling (theirs do, for ~1s at a
    time) must not hold up the others."""
    from concurrent.futures import ThreadPoolExecutor

    def one(source):
        try:
            return build_source_record(source, timestamp), None
        except Exception as e:
            return None, {"source": source["id"], "error": str(e)}

    with ThreadPoolExecutor(max_workers=len(SOURCES)) as pool:
        return list(pool.map(one, SOURCES))


def signature_of(record):
    # all_rows too, like local_server.rate_signature: the page renders every
    # feed row, so a move in spot gold or USD-INR alone must still push.
    return json.dumps([record["rows"], record.get("all_rows"), record["diff1"], record["diff2"]],
                      sort_keys=True, default=str)


# --------------------------------------------------------------------------
# Handlers
# --------------------------------------------------------------------------

def fetch_and_store(event, context):
    """The original behaviour: one fetch of every source, written to DynamoDB.
    Now it also pushes the result to any connected browser."""
    import boto3

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TABLE_NAME)

    timestamp = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S%z")
    results = []
    errors = []
    records = []

    for record, error in fetch_all(timestamp):
        if error:
            errors.append(error)
            continue
        # DynamoDB doesn't allow float; our numbers are already ints/None, fine as-is.
        table.put_item(Item=json.loads(json.dumps(record)))
        results.append(record["source"])
        records.append(record)

    pushed = push_to_connections({"type": "update", "sources": records}) if records else 0

    return {
        "statusCode": 200,
        "body": json.dumps({"written": results, "errors": errors,
                            "timestamp": timestamp, "pushed_to": pushed}),
    }


def stream(event, context):
    """Poll the dealers continuously and push changes over the WebSocket, for
    as long as somebody is actually connected.

    History is NOT written here. The 30-minute schedule owns persistence; at a
    2-second cadence, writing every round would be ~170k DynamoDB writes a day
    for data nobody reads back. Live viewers get pushes, the table keeps its
    regular history, and the cost stays where it was."""
    if not ws_enabled():
        return {"statusCode": 200, "body": json.dumps({"skipped": "websocket not configured"})}

    connection_ids = list_connections()
    if not connection_ids:
        # Nobody is watching - don't burn Lambda time on an empty room.
        return {"statusCode": 200, "body": json.dumps({"skipped": "no connections"})}

    started = time.time()
    deadline = started + STREAM_SECONDS
    if context is not None and hasattr(context, "get_remaining_time_in_millis"):
        # Leave a few seconds so the function returns instead of being killed.
        deadline = min(deadline, time.time() + context.get_remaining_time_in_millis() / 1000.0 - 5)

    signatures = {}
    latest = {}        # source -> newest record, for viewers who join mid-stream
    rounds = 0
    pushes = 0
    last_connection_check = time.time()

    while time.time() < deadline:
        round_started = time.time()
        timestamp = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S%z")
        changed = []

        for record, error in fetch_all(timestamp):
            if error or record is None:
                continue
            signature = signature_of(record)
            if signatures.get(record["source"]) != signature:
                signatures[record["source"]] = signature
                record["changed_at"] = timestamp
                changed.append(record)
            else:
                record["changed_at"] = latest[record["source"]].get("changed_at", timestamp)
            latest[record["source"]] = record

        rounds += 1
        if changed:
            sent = push_to_connections({"type": "update", "sources": changed}, connection_ids)
            pushes += 1
            if sent == 0:
                connection_ids = list_connections()
                if not connection_ids:
                    break      # everyone left mid-stream

        # Re-read the connection list occasionally so we notice both arrivals
        # and departures without scanning the table every round.
        if time.time() - last_connection_check > CONNECTION_CHECK_SECONDS:
            last_connection_check = time.time()
            previous = set(connection_ids)
            connection_ids = list_connections()
            if not connection_ids:
                break
            # A viewer who joined mid-stream only got the stored (up to 30 min
            # old) snapshot; dealers that haven't moved since would never be
            # pushed to them. Send them every dealer's current record once.
            joined = [c for c in connection_ids if c not in previous]
            if joined and latest:
                push_to_connections({"type": "update", "sources": list(latest.values())}, joined)

        elapsed = time.time() - round_started
        if elapsed < POLL_SECONDS:
            time.sleep(POLL_SECONDS - elapsed)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "streamed_seconds": round(time.time() - started, 1),
            "rounds": rounds,
            "pushes": pushes,
            "connections": len(connection_ids),
        }),
    }


def handler(event, context):
    """EventBridge invokes this two ways:
      {}                  -> every 30 min: fetch, store history, push once
      {"stream": true}    -> every 15 min: stream to live viewers, no writes
    """
    event = event or {}
    if event.get("stream"):
        return stream(event, context)
    return fetch_and_store(event, context)
