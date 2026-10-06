"""Run the whole tracker on localhost - no AWS account, no DynamoDB needed.

    python local_server.py                 # http://localhost:7005
    python local_server.py --port 9000
    python local_server.py --interval 2    # poll the dealer feeds less often
    python local_server.py --dynamo        # read the real DynamoDB table instead

It serves three things on one port:
  * /            -> the static dashboard from site/
  * /ws          -> WebSocket; new rates are PUSHED the moment they change
  * /api         -> the same JSON the deployed API Gateway returns (fallback)
  * /api/3min    -> every dealer, refetched once every 3 minutes
  * /api/stream  -> Server-Sent Events: the full /api JSON on every rate move
  * /api/3min/stream -> Server-Sent Events: the full /api/3min JSON every 3 minutes
                    (opened in a browser tab, any /api URL is one JSON updating in place)

About "live": the dealers' feed (Chirayu / VOTSBroadcastStreaming) has no
WebSocket or SSE endpoint - their own LiveRates page just re-GETs the same URL
every 500ms. So the polling happens here instead, once, centrally, and the
result is fanned out to every connected browser over a WebSocket.

Each dealer gets its OWN poller thread on its OWN clock. That matters: their
servers randomly stall a request for ~1s (sometimes 3s), and with one shared
batch loop a single straggler held up all four cards. Polled independently, a
stalled dealer only delays its own card.

In the default (live) mode nothing is ever written to DynamoDB. With --dynamo
it serves the stored history through /api instead (needs AWS credentials) and
the WebSocket route is disabled, so the dashboard falls back to polling.
"""

import argparse
import base64
import hashlib
import json
import mimetypes
import os
import queue
import re
import socket
import struct
import sys
import threading
import time
import urllib.parse
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))
SITE_DIR = os.path.join(ROOT, "site")


def load_dotenv(path):
    """KEY=VALUE lines from .env into os.environ; real env vars win."""
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_dotenv(os.path.join(ROOT, ".env"))
sys.path.insert(0, os.path.join(ROOT, "lambda_fetch"))

from lambda_function import CANONICAL_LABELS, IST, SOURCES, build_source_record, fetch_all  # noqa: E402

# Matches what the dealers' own LiveRates page does (setInterval(..., 500)).
DEFAULT_INTERVAL = 0.5
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
PING_EVERY_SECONDS = 20
STARTUP_WAIT_SECONDS = 6.0


def now_ist():
    return datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S%z")


def rate_signature(record):
    """Everything except the timestamps - used to detect an actual rate change.

    all_rows is included, so a move in any value the dealer publishes (spot
    gold, USD-INR, silver futures, a day high) pushes an update, not only the
    three canonical products."""
    return json.dumps(
        [record["rows"], record.get("all_rows"), record["diff1"], record["diff2"]],
        sort_keys=True,
        default=str,
    )


# --------------------------------------------------------------------------
# WebSocket (RFC 6455, stdlib only - the project deliberately has no deps)
# --------------------------------------------------------------------------

class PushClient:
    """One connected listener. send() never touches the socket: it queues the
    bytes, and the client's own writer thread drains the queue. A tab that
    stops reading (phone asleep, stalled proxy) therefore only fills its own
    queue - it can't block broadcast() and freeze every other viewer. Past
    QUEUE_LIMIT queued messages the client is dropped."""

    QUEUE_LIMIT = 256

    def __init__(self, sock):
        self.sock = sock
        self.alive = True
        self.closed = threading.Event()
        self._lock = threading.Lock()
        self._queue = queue.Queue(self.QUEUE_LIMIT)
        threading.Thread(target=self._writer, daemon=True, name="push-writer").start()

    @staticmethod
    def encode_message(message):
        """Bytes for one JSON message ({"type": ...}), or None to skip it.
        Static so broadcast() can encode once per client type."""
        raise NotImplementedError

    @staticmethod
    def encode_control(opcode, data):
        """Bytes for a WebSocket control frame (ping/pong), or None."""
        raise NotImplementedError

    def send_message(self, message, encoded=None):
        return self._enqueue(self.encode_message(message) if encoded is None else encoded)

    def send_control(self, opcode, data=b""):
        return self._enqueue(self.encode_control(opcode, data))

    def _enqueue(self, data):
        if not self.alive:
            return False
        if data is None:
            return True
        try:
            self._queue.put_nowait(data)
            return True
        except queue.Full:
            self.close()
            return False

    def _writer(self):
        while True:
            data = self._queue.get()
            if data is None or not self.alive:
                break
            try:
                self.sock.sendall(data)
            except OSError:
                break
        self.close()

    def close(self):
        with self._lock:
            if not self.alive:
                return
            self.alive = False
        self.closed.set()
        try:
            # Wakes a writer stuck in sendall and the reader stuck in recv.
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass


def ws_frame(payload, opcode=0x1):
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    n = len(payload)
    if n < 126:
        header = struct.pack("!BB", 0x80 | opcode, n)
    elif n < (1 << 16):
        header = struct.pack("!BBH", 0x80 | opcode, 126, n)
    else:
        header = struct.pack("!BBQ", 0x80 | opcode, 127, n)
    return header + payload


class WSClient(PushClient):
    """A browser on /ws: a snapshot on connect, then one update per dealer
    change, as WebSocket text frames (the dashboard merges them)."""

    @staticmethod
    def encode_message(message):
        if message.get("type") == "3min":
            return None          # that one is for /api/3min/stream only
        return ws_frame(json.dumps(message, default=str))

    @staticmethod
    def encode_control(opcode, data):
        return ws_frame(data, opcode)


class SSEClient(PushClient):
    """A listener on /api/stream (Server-Sent Events). Every `data:` event is
    the complete /api JSON - the same array, same shape, every time - so a
    consumer just replaces what it has; nothing to merge. Heartbeats are a
    separate named event so the data events stay pure /api JSON."""

    @staticmethod
    def encode_message(message):
        if message.get("type") == "heartbeat":
            return ("event: heartbeat\ndata: " + json.dumps(message) + "\n\n").encode("utf-8")
        if message.get("type") == "3min":
            return None
        return ("data: " + json.dumps(snapshot(), default=str) + "\n\n").encode("utf-8")

    @staticmethod
    def encode_control(opcode, data):
        return b": ping\n\n" if opcode == 0x9 else None


class ThreeMinSSEClient(SSEClient):
    """A listener on /api/3min/stream: every `data:` event is the complete
    /api/3min JSON, sent on connect and again after each 3-minute refetch.
    Live per-dealer moves are not sent here - that's what /api/stream is for."""

    @staticmethod
    def encode_message(message):
        if message.get("type") == "heartbeat":
            return ("event: heartbeat\ndata: " + json.dumps(message) + "\n\n").encode("utf-8")
        if message.get("type") != "3min":
            return None
        with THREE_MIN_LOCK:
            body = json.dumps(THREE_MIN, default=str)
        return ("data: " + body + "\n\n").encode("utf-8")


CLIENTS = set()     # WSClient, SSEClient and ThreeMinSSEClient instances
CLIENTS_LOCK = threading.Lock()


def broadcast(message):
    """Send one message to every connected listener, encoded once per type."""
    with CLIENTS_LOCK:
        targets = list(CLIENTS)
    encoded = {}
    dead = []
    for c in targets:
        kind = type(c)
        if kind not in encoded:
            encoded[kind] = kind.encode_message(message)
        if not c.send_message(message, encoded[kind]):
            dead.append(c)
    if dead:
        with CLIENTS_LOCK:
            for c in dead:
                CLIENTS.discard(c)


def _read_exactly(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return bytes(buf)


# The page only ever sends tiny JSON ({"action": "snapshot"} / "ping") and
# control frames are capped at 125 bytes by the RFC, so anything bigger is
# not our page - drop it rather than buffer whatever length a client claims.
MAX_CLIENT_FRAME = 4096


def ws_read_loop(client):
    """Consume frames from the browser. No application data is expected - this
    exists to answer pings and to notice when the tab closes."""
    sock = client.sock
    while client.alive:
        head = _read_exactly(sock, 2)
        if not head:
            break
        b0, b1 = head[0], head[1]
        opcode = b0 & 0x0F
        masked = b1 & 0x80
        length = b1 & 0x7F

        if length == 126:
            ext = _read_exactly(sock, 2)
            if not ext:
                break
            length = struct.unpack("!H", ext)[0]
        elif length == 127:
            ext = _read_exactly(sock, 8)
            if not ext:
                break
            length = struct.unpack("!Q", ext)[0]

        if length > MAX_CLIENT_FRAME:
            break

        mask = b""
        if masked:
            mask = _read_exactly(sock, 4)
            if mask is None:
                break

        data = b""
        if length:
            data = _read_exactly(sock, length)
            if data is None:
                break
        if masked and data:
            data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))

        if opcode == 0x8:      # close
            break
        if opcode == 0x9:      # ping -> pong
            client.send_control(0xA, data)
        # 0xA (pong) and any data frames are ignored on purpose.


def keepalive_loop(stop_event):
    """Ping idle clients so dead tabs get noticed even in a still market, and
    send a heartbeat the page can see (pings are invisible to browser JS) so a
    socket that silently stopped delivering - e.g. a stalled proxy - gets
    reconnected instead of leaving the numbers frozen."""
    while not stop_event.wait(PING_EVERY_SECONDS):
        with CLIENTS_LOCK:
            targets = list(CLIENTS)
        for c in targets:
            c.send_control(0x9)
        broadcast({"type": "heartbeat", "at": now_ist()})


# --------------------------------------------------------------------------
# One poller per dealer, each on its own clock
# --------------------------------------------------------------------------

class SourcePoller(threading.Thread):
    """Polls one dealer's feed and pushes that dealer's card when it changes."""

    def __init__(self, source, interval):
        super().__init__(daemon=True, name="poll-" + source["id"])
        self.source = source
        self.interval = interval
        self.record = None          # last known record, with status fields
        self.signature = None
        self.ok = True
        self.first_result = threading.Event()
        self._stop = threading.Event()

    def run(self):
        while not self._stop.is_set():
            started = time.time()
            try:
                record = build_source_record(self.source, now_ist())
                # The deployed API adds these aliases on the way out; mirror it
                # so the dashboard sees exactly the same fields locally.
                record["cash_bhaw"] = record.get("diff1")
                record["rtgs_bhaw"] = record.get("diff2")
                record["latency_ms"] = int((time.time() - started) * 1000)
                record["ok"] = True
                record["error"] = None

                signature = rate_signature(record)
                changed = signature != self.signature
                recovered = not self.ok
                previous = self.record

                # changed_at is when the numbers last MOVED; timestamp is when
                # we last polled successfully. The card shows changed_at.
                record["changed_at"] = (
                    record["timestamp"]
                    if changed or previous is None
                    else previous.get("changed_at", record["timestamp"])
                )

                self.signature = signature
                self.record = record
                self.ok = True
                self.first_result.set()

                if changed or recovered:
                    broadcast({"type": "update", "source": record})

            except Exception as e:
                if self.ok:   # announce the first failure, then stay quiet
                    self.ok = False
                    print("  ! %s failed: %s" % (self.source["id"], e))
                    stale = dict(self.record) if self.record else self.placeholder()
                    stale["ok"] = False
                    stale["error"] = str(e)
                    self.record = stale
                    self.first_result.set()
                    broadcast({"type": "update", "source": stale})

            self._stop.wait(max(0.0, self.interval - (time.time() - started)))

    def placeholder(self):
        """Shape a card can still render when a dealer fails on its very first
        poll and there is nothing real to show yet."""
        return {
            "source": self.source["id"],
            "name": self.source["name"],
            "site_url": self.source["site_url"],
            "timestamp": now_ist(),
            "changed_at": now_ist(),
            "rows": [{"label": l, "buy": None, "sell": None} for l in CANONICAL_LABELS],
            "diff1": None,
            "diff2": None,
            "cash_bhaw": None,
            "rtgs_bhaw": None,
            "latency_ms": None,
        }

    def stop(self):
        self._stop.set()


POLLERS = []


# --------------------------------------------------------------------------
# 3-minute snapshot: its own fetch of every dealer, served at /api/3min
# --------------------------------------------------------------------------

THREE_MIN_SECONDS = 180
THREE_MIN = {"fetched_at": None, "next_fetch_at": None, "sources": [], "errors": []}
THREE_MIN_LOCK = threading.Lock()


def three_min_loop(stop_event):
    while True:
        started = time.time()
        timestamp = now_ist()
        sources, errors = [], []
        for record, error in fetch_all(timestamp):
            if error:
                errors.append(error)
                continue
            record["cash_bhaw"] = record.get("diff1")
            record["rtgs_bhaw"] = record.get("diff2")
            sources.append(record)
        next_at = datetime.fromtimestamp(started + THREE_MIN_SECONDS, IST)
        with THREE_MIN_LOCK:
            THREE_MIN.update({
                "fetched_at": timestamp,
                "next_fetch_at": next_at.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "sources": sources,
                "errors": errors,
            })
        broadcast({"type": "3min"})     # /api/3min/stream listeners get the new JSON
        if stop_event.wait(max(0.0, THREE_MIN_SECONDS - (time.time() - started))):
            return


def snapshot():
    """Every dealer's latest card, in the configured source order."""
    return [p.record for p in POLLERS if p.record is not None]


def wait_for_first_results(timeout=STARTUP_WAIT_SECONDS):
    deadline = time.time() + timeout
    for p in POLLERS:
        p.first_result.wait(max(0.0, deadline - time.time()))


def dynamo_payload():
    # lambda_api/lambda_function.py has the same module name as the fetch one,
    # so load it by path to avoid clashing with the already-imported fetch module.
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "gold_api", os.path.join(ROOT, "lambda_api", "lambda_function.py")
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    resp = mod.handler({"requestContext": {"http": {"method": "GET"}}}, None)
    return json.loads(resp["body"])


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

# URLs that, opened in a browser tab, show one live-updating JSON (api.html).
LIVE_JSON_PATHS = (
    "/api", "/api/stream", "/api/streaming",
    "/api/3min", "/api/3min/stream", "/api/3min/streaming",
)


class Handler(BaseHTTPRequestHandler):
    mode = "live"
    protocol_version = "HTTP/1.1"

    def _send(self, status, body, content_type, extra_headers=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    # -- server-sent events --------------------------------------------------

    def _handle_stream(self, client_cls, first_message):
        """Server-Sent Events over plain HTTP.
          /api/stream       SSEClient: each `data:` event is the complete /api
                            JSON - on connect, then the moment any rate moves.
          /api/3min/stream  ThreeMinSSEClient: the complete /api/3min JSON - on
                            connect, then after each 3-minute refetch.
        Both get an `event: heartbeat` every 20s."""
        if self.mode != "live":
            self._send(501, b"Streaming disabled in --dynamo mode", "text/plain")
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("X-Accel-Buffering", "no")   # nginx: pass each event straight through
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True

        client = client_cls(self.connection)
        with CLIENTS_LOCK:
            CLIENTS.add(client)
            count = len(CLIENTS)
        print("sse connect   (%d client(s))" % count)
        try:
            client.send_message(first_message)
            # Nothing to read from an SSE client; a failed write (the next
            # push or heartbeat) is how a closed tab is noticed.
            client.closed.wait()
        finally:
            with CLIENTS_LOCK:
                CLIENTS.discard(client)
                count = len(CLIENTS)
            client.close()
            print("sse disconnect (%d client(s))" % count)

    def do_OPTIONS(self):
        self._send(200, b"", "text/plain")

    # -- websocket ---------------------------------------------------------

    def _handle_websocket(self):
        if self.mode != "live":
            # --dynamo has no live stream; tell the page to fall back to /api.
            self._send(501, b"WebSocket disabled in --dynamo mode", "text/plain")
            return

        key = self.headers.get("Sec-WebSocket-Key")
        if not key:
            self._send(400, b"Missing Sec-WebSocket-Key", "text/plain")
            return

        accept = base64.b64encode(
            hashlib.sha1((key + WS_GUID).encode("utf-8")).digest()
        ).decode("ascii")

        handshake = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
        )
        self.wfile.write(handshake.encode("ascii"))
        self.wfile.flush()

        self.close_connection = True  # we own the socket from here on
        client = WSClient(self.connection)
        with CLIENTS_LOCK:
            CLIENTS.add(client)
            count = len(CLIENTS)
        print("ws connect    (%d client(s))" % count)

        try:
            client.send_message({"type": "snapshot", "sources": snapshot()})
            ws_read_loop(client)
        except OSError:
            pass
        finally:
            with CLIENTS_LOCK:
                CLIENTS.discard(client)
                count = len(CLIENTS)
            client.close()
            print("ws disconnect (%d client(s))" % count)

    # -- routes ------------------------------------------------------------

    def do_GET(self):
        # "/api//3min/stream" and "/api/3min/" mean the same as the clean form.
        path = re.sub(r"/{2,}", "/", self.path.split("?", 1)[0])
        if len(path) > 1:
            path = path.rstrip("/")

        if path == "/ws":
            if "websocket" in self.headers.get("Upgrade", "").lower():
                self._handle_websocket()
            else:
                self._send(426, b"Expected a WebSocket upgrade", "text/plain")
            return

        # A browser tab opening any of the JSON URLs below gets api.html: ONE
        # JSON document - exactly what /api (or /api/3min) returns - updated in
        # place, instead of a frozen snapshot or a growing list of events.
        # Scripts and apps (no text/html in Accept) and ?raw get the real
        # thing: raw JSON, or the event stream from the /stream URLs.
        query = self.path.split("?", 1)[1] if "?" in self.path else ""
        wants_raw = "raw" in urllib.parse.parse_qs(query, keep_blank_values=True)
        is_browser_tab = (not wants_raw and self.mode == "live"
                          and "text/html" in self.headers.get("Accept", ""))

        if path in LIVE_JSON_PATHS and is_browser_tab:
            with open(os.path.join(SITE_DIR, "api.html"), "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8", {"Vary": "Accept"})
            return

        if path in ("/api/stream", "/api/streaming"):
            self._handle_stream(SSEClient, {"type": "snapshot"})
            return

        if path in ("/api/3min/stream", "/api/3min/streaming"):
            self._handle_stream(ThreeMinSSEClient, {"type": "3min"})
            return

        if path == "/api/3min":
            with THREE_MIN_LOCK:
                body = json.dumps(THREE_MIN, default=str).encode("utf-8")
            self._send(200, body, "application/json", {"Vary": "Accept"})
            return

        if path == "/api":
            try:
                data = dynamo_payload() if self.mode == "dynamo" else snapshot()
                body = json.dumps(data, default=str).encode("utf-8")
                self._send(200, body, "application/json", {"Vary": "Accept"})
            except Exception as e:
                body = json.dumps({"error": str(e)}).encode("utf-8")
                self._send(500, body, "application/json")
            return

        if path == "/":
            path = "/index.html"

        # Serve out of site/ only - no traversal outside it.
        target = os.path.normpath(os.path.join(SITE_DIR, path.lstrip("/\\")))
        if not target.startswith(SITE_DIR + os.sep) or not os.path.isfile(target):
            self._send(404, b"Not found", "text/plain")
            return

        ctype = mimetypes.guess_type(target)[0] or "application/octet-stream"
        with open(target, "rb") as f:
            self._send(200, f.read(), ctype)

    def log_message(self, fmt, *args):
        line = fmt % args
        if "/ws" in line:
            return
        print("%s - %s" % (self.address_string(), line))


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--port", type=int, default=7005)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL,
        help="seconds between polls of each dealer (default 0.5, same as the "
             "dealers' own site); every dealer runs on its own clock",
    )
    ap.add_argument(
        "--dynamo",
        action="store_true",
        help="read from the real DynamoDB table (needs AWS credentials) instead of "
             "polling the feeds live; disables the WebSocket stream",
    )
    args = ap.parse_args()

    # Unbuffered-ish output so connect/disconnect lines show up immediately,
    # including when the output is redirected to a log file.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except AttributeError:
        pass

    Handler.mode = "dynamo" if args.dynamo else "live"
    stop_event = threading.Event()
    threading.Thread(target=three_min_loop, args=(stop_event,), daemon=True).start()

    if not args.dynamo:
        for source in SOURCES:
            poller = SourcePoller(source, args.interval)
            POLLERS.append(poller)
            poller.start()
        threading.Thread(target=keepalive_loop, args=(stop_event,), daemon=True).start()
        print("Polling %d dealers, each every %.1fs on its own thread..."
              % (len(POLLERS), args.interval))
        wait_for_first_results()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.daemon_threads = True
    print("Gold Rate Tracker running in %s mode" % Handler.mode)
    print("  dashboard : http://%s:%d/" % (args.host, args.port))
    if args.dynamo:
        print("  api       : http://%s:%d/api  (WebSocket disabled)" % (args.host, args.port))
    else:
        print("  websocket : ws://%s:%d/ws   (per-dealer push on change)" % (args.host, args.port))
        print("  api       : http://%s:%d/api  (fallback snapshot)" % (args.host, args.port))
    print("  3-minute  : http://%s:%d/api/3min  (refetched every 3 minutes)" % (args.host, args.port))
    print("Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping")
        stop_event.set()
        for p in POLLERS:
            p.stop()
        with CLIENTS_LOCK:
            for c in list(CLIENTS):
                c.close()
        server.server_close()


if __name__ == "__main__":
    main()
