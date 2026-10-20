"""Monitoring server node for the fault-tolerant distributed air-quality network.

Several servers run at once. The leader (primary) ingests sensor readings, computes the
CPCB Air Quality Index, raises health alerts, and replicates its state to every backup
twice a second; that stream doubles as the heartbeat. A reading is acknowledged to its
sensor only after a replication round, so an acknowledged reading survives a leader crash.
When the leader stops, backups hold a Bully election and the winner continues from its
replica. Every message carries a Lamport timestamp.
"""
import argparse
import asyncio
import json
import math
import time
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.asyncio.server import broadcast, serve

REPLICATE_EVERY = 0.5  # leader -> backups state push, also the heartbeat (s)
LEADER_TIMEOUT = 2.0   # backup suspects the leader after this much silence (s)
ELECTION_WAIT = 2.0    # wait for a coordinator message after a higher id answered (s)
PEER_TIMEOUT = 0.5
OFFLINE_AFTER = 5.0    # a station with no reading for this long is reported offline (s)
HISTORY = 120          # AQI points kept per station for the trend chart
ALERT_AT = 201         # CPCB "Poor" and worse
CLEAR_BELOW = 180      # hysteresis so an alert does not flap around the threshold
INDEX = Path(__file__).with_name("static") / "index.html"

# CPCB National AQI breakpoints: (conc_lo, conc_hi, index_lo, index_hi); PM in µg/m³, NO2 in µg/m³
BREAKPOINTS = {
    "pm25": [(0, 30, 0, 50), (30, 60, 50, 100), (60, 90, 100, 200), (90, 120, 200, 300), (120, 250, 300, 400), (250, 500, 400, 500)],
    "pm10": [(0, 50, 0, 50), (50, 100, 50, 100), (100, 250, 100, 200), (250, 350, 200, 300), (350, 430, 300, 400), (430, 600, 400, 500)],
    "no2": [(0, 40, 0, 50), (40, 80, 50, 100), (80, 180, 100, 200), (180, 280, 200, 300), (280, 400, 300, 400), (400, 800, 400, 500)],
}
CATEGORIES = [(50, "Good"), (100, "Satisfactory"), (200, "Moderate"), (300, "Poor"), (400, "Very Poor"), (500, "Severe")]


# ---------------- AQI + ingestion (pure functions on a JSON-able state) ----------------

def sub_index(pollutant, c):
    for lo, hi, ilo, ihi in BREAKPOINTS[pollutant]:
        if c <= hi:
            return round(ilo + (ihi - ilo) * (max(c, lo) - lo) / (hi - lo))
    return 500


def aqi(r):
    """CPCB AQI = worst pollutant sub-index."""
    return max(sub_index(p, r[p]) for p in BREAKPOINTS)


def category(a):
    return next(name for top, name in CATEGORIES if a <= top)


def new_state():
    return {"version": 0, "stations": {}, "last_seq": {}, "alerts": [], "readings": 0}


def validate(m):
    """Sensors are untrusted input: check every field before it touches the state."""
    r = {"station": m.get("station"), "name": m.get("name"), "boot": m.get("boot"), "seq": m.get("seq"), "ts": m.get("ts")}
    if not (isinstance(r["station"], str) and 0 < len(r["station"]) <= 32):
        raise ValueError("bad station id")
    if not (isinstance(r["name"], str) and 0 < len(r["name"]) <= 40):
        raise ValueError("bad station name")
    if not all(isinstance(r[k], int) and r[k] >= 0 for k in ("boot", "seq", "ts")):
        raise ValueError("bad boot/seq/ts")
    for p in BREAKPOINTS:
        v = m.get(p)
        if not (isinstance(v, (int, float)) and math.isfinite(v) and 0 <= v <= 2000):
            raise ValueError(f"bad {p}")
        r[p] = float(v)
    return r


def add_alert(state, ts, now, station, aqi_value, level, msg):
    state["alerts"] = ([{"ts": ts, "time": now, "station": station, "aqi": aqi_value, "level": level, "msg": msg}]
                       + state["alerts"])[:50]


def ingest(state, r, now):
    """Apply one validated reading. Returns False for a duplicate (a sensor re-sending after failover).

    Readings are ordered by (boot, seq): seq restarts at 1 when a sensor reboots, boot does not go back.
    """
    sid = r["station"]
    if (r["boot"], r["seq"]) <= tuple(state["last_seq"].get(sid, (0, 0))):
        return False
    state["last_seq"][sid] = [r["boot"], r["seq"]]
    st = state["stations"].setdefault(sid, {"name": r["name"], "history": [], "alerting": False, "offline": False})
    a = aqi(r)
    st.update(pm25=r["pm25"], pm10=r["pm10"], no2=r["no2"], aqi=a, category=category(a), seen=now, lamport=r["ts"])
    st["history"] = (st["history"] + [a])[-HISTORY:]
    state["readings"] += 1
    if st["offline"]:
        st["offline"] = False
        add_alert(state, r["ts"], now, st["name"], a, "info", f"{st['name']} sensor back online")
    if a >= ALERT_AT and not st["alerting"]:
        st["alerting"] = True
        add_alert(state, r["ts"], now, st["name"], a, "alert",
                  f"{category(a)} air at {st['name']} (AQI {a}): sensitive groups should avoid outdoor activity")
    elif a < CLEAR_BELOW and st["alerting"]:
        st["alerting"] = False
        add_alert(state, r["ts"], now, st["name"], a, "clear", f"Air quality at {st['name']} recovered (AQI {a})")
    return True


def check_offline(state, ts, now):
    """Failure detection for sensors: flag stations that stopped reporting."""
    for st in state["stations"].values():
        if not st["offline"] and now - st["seen"] > OFFLINE_AFTER:
            st["offline"] = True
            add_alert(state, ts, now, st["name"], st["aqi"], "warn",
                      f"{st['name']} sensor offline (no data for {OFFLINE_AFTER:.0f}s)")


# ---------------- distributed server node ----------------

class MonitorServer:
    def __init__(self, sid, peers):
        self.id = sid
        self.peers = peers                # id -> "host:port"
        self.clock = 0                    # Lamport clock
        self.leader = None
        self.last_hb = time.monotonic()
        self.electing = False
        self.state = new_state()
        self.pending = {}                 # sensor websocket -> highest seq awaiting ack
        self.dashboards = set()
        self.sensors = set()
        self.conns = {}                   # cached outbound peer connections
        self.retry_at = {}
        self.up = {n: False for n in peers if n != sid}

    def tick(self, received=0):
        self.clock = max(self.clock, int(received)) + 1
        return self.clock

    def log(self, msg):
        print(f"[server {self.id} | L={self.clock:>6}] {msg}", flush=True)

    def others(self):
        return [n for n in self.peers if n != self.id]

    # ---- peer messaging ----

    async def send_peer(self, nid, msg):
        """Send over a cached connection. Also tracks which peers are up."""
        ws = self.conns.get(nid)
        ok = False
        try:
            if ws is None and time.monotonic() >= self.retry_at.get(nid, 0):
                ws = await asyncio.wait_for(connect(f"ws://{self.peers[nid]}/peer", proxy=None), PEER_TIMEOUT)
                self.conns[nid] = ws
            if ws is not None:
                await ws.send(json.dumps({**msg, "ts": self.tick(), "from": self.id}))
                ok = True
        except Exception:
            self.conns.pop(nid, None)
            self.retry_at[nid] = time.monotonic() + 1  # don't stall every round on a dead peer
        if ok != self.up[nid]:
            self.up[nid] = ok
            self.log(f"server {nid} is {'UP' if ok else 'DOWN'}")
        return ok

    async def ask_peer(self, nid, msg):
        """Request/response on a fresh connection; None if the peer is down or slow."""
        try:
            async with asyncio.timeout(PEER_TIMEOUT * 2):
                async with connect(f"ws://{self.peers[nid]}/peer", proxy=None) as ws:
                    await ws.send(json.dumps({**msg, "ts": self.tick(), "from": self.id}))
                    reply = json.loads(await ws.recv())
                    self.tick(reply["ts"])
                    return reply
        except Exception:
            return None

    async def peer_session(self, ws):
        async for raw in ws:
            m = json.loads(raw)
            self.tick(m["ts"])
            t = m["type"]
            if t == "state":
                self.on_state(m)
            elif t == "election":
                self.log(f"election message from server {m['from']}: answering OK (I outrank it)")
                await ws.send(json.dumps({"type": "ok", "ts": self.tick()}))
                self.start_election()
            elif t == "coordinator":
                self.on_coordinator(m["from"])
            elif t == "get_state":
                await ws.send(json.dumps({"type": "state", "ts": self.tick(), "state": self.state}))

    def on_state(self, m):
        if self.leader == self.id:
            return  # stale stream from a deposed leader; it will get our coordinator message
        if m["from"] < self.id:
            self.start_election()  # bully: a live higher id must lead
            return
        if self.leader != m["from"]:
            self.log(f"following leader {m['from']} (replica at version {m['state']['version']})")
        self.leader, self.last_hb = m["from"], time.monotonic()
        if m["state"]["version"] >= self.state["version"]:
            self.state = m["state"]

    def on_coordinator(self, lid):
        self.log(f"server {lid} announced itself leader")
        if lid < self.id:
            self.start_election()
            return
        self.leader, self.last_hb = lid, time.monotonic()

    # ---- Bully election ----

    def start_election(self):
        if not self.electing:
            self.electing = True
            asyncio.ensure_future(self.election())

    async def election(self):
        try:
            while True:
                self.leader = None
                higher = [n for n in self.others() if n > self.id]
                self.log(f"ELECTION: sending election to higher ids {higher}")
                replies = await asyncio.gather(*(self.ask_peer(n, {"type": "election"}) for n in higher))
                if not any(replies):
                    await self.become_leader()
                    return
                deadline = time.monotonic() + ELECTION_WAIT
                while time.monotonic() < deadline:
                    if self.leader is not None:
                        return
                    await asyncio.sleep(0.1)
                self.log("ELECTION: no coordinator message arrived, retrying")
        finally:
            self.electing = False

    async def become_leader(self):
        # continue from the freshest replica any live server holds (a restarted server has none)
        for r in await asyncio.gather(*(self.ask_peer(n, {"type": "get_state"}) for n in self.others())):
            if r and r["state"]["version"] > self.state["version"]:
                self.state = r["state"]
        self.leader = self.id
        self.log(f"*** I am the LEADER (highest live id). Continuing from replica version "
                 f"{self.state['version']} ({self.state['readings']} readings) ***")
        await asyncio.gather(*(self.send_peer(n, {"type": "coordinator"}) for n in self.others()))
        asyncio.ensure_future(self.leader_loop())

    # ---- leader: replicate, then acknowledge ----

    async def leader_loop(self):
        loop = asyncio.get_running_loop()
        while self.leader == self.id:
            start = loop.time()
            check_offline(self.state, self.tick(), time.time())
            self.state["version"] += 1
            pending, self.pending = self.pending, {}
            await asyncio.gather(*(self.send_peer(n, {"type": "state", "state": self.state}) for n in self.others()))
            for ws, seq in pending.items():  # replicated now, safe to let sensors drop these readings
                try:
                    await ws.send(json.dumps({"type": "ack", "seq": seq, "ts": self.tick()}))
                except Exception:
                    pass
            broadcast(self.dashboards, json.dumps({
                "type": "state", "ts": self.tick(), "leader": self.id, "now": time.time(), "state": self.state,
                "servers": {str(n): (n == self.id or self.up[n]) for n in self.peers}}))
            await asyncio.sleep(max(0, REPLICATE_EVERY - (loop.time() - start)))
        self.log("stepping down; redirecting sensors and dashboards to the new leader")
        for ws in list(self.sensors | self.dashboards):
            await ws.close(1012, "leader changed")

    async def watchdog(self):
        while True:
            await asyncio.sleep(0.25)
            if self.leader != self.id and not self.electing and time.monotonic() - self.last_hb > LEADER_TIMEOUT:
                self.log(f"no state from leader {self.leader} for {LEADER_TIMEOUT}s: suspecting failure")
                self.start_election()

    # ---- sensors and dashboards ----

    async def client_session(self, ws, kind):
        station = None
        try:
            async for raw in ws:
                m = json.loads(raw)
                self.tick(m["ts"] if isinstance(m.get("ts"), int) else 0)
                if self.leader != self.id:  # backups only point clients at the leader
                    await ws.send(json.dumps({"type": "redirect", "leader": self.leader, "ts": self.tick()}))
                    return
                if kind == "live":
                    self.dashboards.add(ws)
                elif m.get("type") == "reading":
                    try:
                        r = validate(m)
                    except ValueError as e:
                        self.log(f"rejected reading: {e}")
                        continue
                    if station is None:
                        station = r["station"]
                        self.sensors.add(ws)
                        self.log(f"sensor '{station}' connected")
                    newest = self.state["alerts"][:1]
                    ingest(self.state, r, time.time())
                    if self.state["alerts"][:1] != newest:
                        self.log(f"ALERT: {self.state['alerts'][0]['msg']}")
                    self.pending[ws] = max(self.pending.get(ws, 0), r["seq"])  # duplicates get acked too
        finally:
            self.dashboards.discard(ws)
            self.sensors.discard(ws)
            self.pending.pop(ws, None)

    async def handler(self, ws):
        try:
            path = ws.request.path
            if path == "/peer":
                await self.peer_session(ws)
            else:
                await self.client_session(ws, "live" if path == "/live" else "sensor")
        except Exception:
            pass  # a dropped connection is normal here

    def http(self, conn, request):
        if request.path in ("/peer", "/sensor", "/live"):
            return None  # websocket upgrade
        if request.path == "/servers":
            body, ctype = json.dumps({str(n): a for n, a in self.peers.items()}), "application/json"
        elif request.path == "/":
            body, ctype = INDEX.read_text(), "text/html; charset=utf-8"
        else:
            return conn.respond(404, "not found\n")
        resp = conn.respond(200, body)
        del resp.headers["Content-Type"]
        resp.headers["Content-Type"] = ctype
        return resp


async def main():
    ap = argparse.ArgumentParser(description="Run one monitoring server")
    ap.add_argument("id", type=int)
    ap.add_argument("--config", default="cluster.json")
    ap.add_argument("--host", default="0.0.0.0", help="bind address (0.0.0.0 lets other devices on the LAN connect)")
    a = ap.parse_args()
    with open(a.config) as fh:
        peers = {int(k): v for k, v in json.load(fh)["servers"].items()}
    node = MonitorServer(a.id, peers)
    port = int(peers[a.id].rsplit(":", 1)[1])
    async with serve(node.handler, a.host, port, process_request=node.http):
        node.log(f"listening on {a.host}:{port}; dashboard at http://localhost:{port}/")
        node.start_election()
        await node.watchdog()


if __name__ == "__main__":
    asyncio.run(main())
