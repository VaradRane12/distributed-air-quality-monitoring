"""Simulated IoT air-quality sensor nodes for Pune.

Each station is an independent client with its own WebSocket connection, Lamport clock and
store-and-forward buffer: readings stay in the buffer until the leader acknowledges them, so
nothing is lost while servers fail over. Type a station id + Enter to simulate a pollution
event (e.g. a garbage fire) at that station.

    python sensors.py                  # all stations
    python sensors.py bhosari kothrud  # just these (run others in another terminal)
"""
import argparse
import asyncio
import json
import math
import random
import sys
import time

from websockets.asyncio.client import connect

# id: (display name, typical PM2.5 in µg/m³)
STATIONS = {
    "shivajinagar": ("Shivajinagar", 45),
    "hinjewadi": ("Hinjewadi IT Park", 35),
    "bhosari": ("Bhosari MIDC", 55),
    "hadapsar": ("Hadapsar", 50),
    "kothrud": ("Kothrud", 30),
    "pimpri": ("Pimpri", 55),
}
INTERVAL = 1.0      # one reading per second (real stations: every 15 min; sped up for the demo)
MAX_BUFFER = 3600   # shortcut: oldest readings dropped after an hour offline; persist to disk if needed


class Sensor:
    def __init__(self, sid, servers):
        self.sid = sid
        self.name, self.base = STATIONS[sid]
        self.servers = servers
        self.clock = 0      # Lamport clock
        self.seq = 0        # per-sensor sequence number, lets the server drop duplicates
        self.buffer = []    # readings not yet acknowledged
        self.event = 0      # remaining readings of a simulated pollution event
        self.phase = random.uniform(0, 2 * math.pi)

    def log(self, msg):
        print(f"[sensor {self.sid:13} | L={self.clock:>5}] {msg}", flush=True)

    def measure(self):
        self.seq += 1
        self.clock += 1
        daily = 1 + 0.3 * math.sin(time.time() / 60 + self.phase)  # one simulated "day" every ~6 min
        if not self.event and random.random() < 0.004:
            self.event = 30
        boost = 5.0 if self.event else 1.0
        self.event = max(0, self.event - 1)
        pm25 = max(0.0, self.base * daily * boost + random.gauss(0, self.base * 0.08))
        return {"type": "reading", "station": self.sid, "name": self.name, "seq": self.seq, "ts": self.clock,
                "pm25": round(pm25, 1),
                "pm10": round(max(0.0, pm25 * 1.7 + random.gauss(0, 6)), 1),
                "no2": round(max(0.0, self.base * 0.6 * daily + random.gauss(0, 4)), 1)}

    async def produce(self):
        """Keep measuring whether or not a server is reachable."""
        while True:
            self.buffer = (self.buffer + [self.measure()])[-MAX_BUFFER:]
            await asyncio.sleep(INTERVAL)

    async def run(self):
        asyncio.ensure_future(self.produce())
        ids, rr, hint = list(self.servers), 0, None
        while True:
            sid = hint if hint in self.servers else ids[rr % len(ids)]
            rr, hint = rr + 1, None
            try:
                async with connect(f"ws://{self.servers[sid]}/sensor", proxy=None, open_timeout=1) as ws:
                    sender = asyncio.ensure_future(self.send_loop(ws))
                    try:
                        async for raw in ws:
                            m = json.loads(raw)
                            self.clock = max(self.clock, m["ts"]) + 1
                            if m["type"] == "redirect":
                                hint = m["leader"] and str(m["leader"])
                                break
                            if m["type"] == "ack":
                                self.buffer = [r for r in self.buffer if r["seq"] > m["seq"]]
                    finally:
                        sender.cancel()
            except Exception:
                pass
            if hint is None:
                self.log(f"no leader reachable, buffering ({len(self.buffer)} readings held)")
            await asyncio.sleep(0.05 if hint else 0.5)

    async def send_loop(self, ws):
        """Re-send everything unacknowledged on a new connection, then stream new readings."""
        sent = 0
        while True:
            for r in [r for r in self.buffer if r["seq"] > sent]:
                await ws.send(json.dumps(r))
                sent = r["seq"]
            await asyncio.sleep(0.1)


async def main():
    ap = argparse.ArgumentParser(description="Run simulated air-quality sensors")
    ap.add_argument("stations", nargs="*", help=f"any of {', '.join(STATIONS)} (default: all)")
    ap.add_argument("--config", default="cluster.json")
    a = ap.parse_args()
    if set(a.stations) - set(STATIONS):
        ap.error(f"unknown station(s) {sorted(set(a.stations) - set(STATIONS))}")
    with open(a.config) as fh:
        servers = json.load(fh)["servers"]
    sensors = {sid: Sensor(sid, servers) for sid in (a.stations or STATIONS)}

    if sys.stdin.isatty():
        def on_line():
            sid = sys.stdin.readline().strip()
            if sid in sensors:
                sensors[sid].event = 30
                sensors[sid].log("simulated pollution event started (~30 s)")
            elif sid:
                print(f"unknown station; choose from {', '.join(sensors)}")
        asyncio.get_running_loop().add_reader(sys.stdin.fileno(), on_line)
        print(f"Type a station id + Enter to simulate a pollution event: {', '.join(sensors)}")

    await asyncio.gather(*(s.run() for s in sensors.values()))


if __name__ == "__main__":
    asyncio.run(main())
