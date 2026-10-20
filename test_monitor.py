"""Tests: AQI + ingestion rules, plus a real 3-server cluster where the leader is killed."""
import json
import math
import os
import subprocess
import sys
import tempfile
import time
import unittest

from websockets.sync.client import connect

import server

HERE = os.path.dirname(os.path.abspath(__file__))


def reading(seq, pm25=20.0, station="kothrud", ts=None, boot=1):
    return {"type": "reading", "station": station, "name": "Kothrud", "boot": boot, "seq": seq, "ts": ts or seq,
            "pm25": pm25, "pm10": 10.0, "no2": 10.0}


class RulesTest(unittest.TestCase):
    def test_cpcb_aqi(self):
        self.assertEqual(server.aqi({"pm25": 30, "pm10": 0, "no2": 0}), 50)
        self.assertEqual(server.aqi({"pm25": 45, "pm10": 0, "no2": 0}), 75)
        self.assertEqual(server.aqi({"pm25": 100, "pm10": 0, "no2": 0}), 233)
        self.assertEqual(server.aqi({"pm25": 10, "pm10": 300, "no2": 0}), 250)  # worst pollutant wins
        self.assertEqual(server.aqi({"pm25": 999, "pm10": 0, "no2": 0}), 500)
        self.assertEqual([server.category(a) for a in (50, 51, 200, 201, 401)],
                         ["Good", "Satisfactory", "Moderate", "Poor", "Severe"])

    def test_duplicates_ignored(self):
        st = server.new_state()
        self.assertTrue(server.ingest(st, server.validate(reading(1)), 0))
        self.assertTrue(server.ingest(st, server.validate(reading(2)), 0))
        self.assertFalse(server.ingest(st, server.validate(reading(2)), 0))  # re-sent after failover
        self.assertFalse(server.ingest(st, server.validate(reading(1)), 0))
        self.assertEqual(st["readings"], 2)

    def test_rebooted_sensor_not_mistaken_for_duplicate(self):
        st = server.new_state()
        for seq in (1, 2, 3):
            server.ingest(st, server.validate(reading(seq)), 0)
        self.assertTrue(server.ingest(st, server.validate(reading(1, boot=2)), 0))  # seq restarts after reboot
        self.assertFalse(server.ingest(st, server.validate(reading(3, boot=1)), 0))  # old run's re-send still dropped

    def test_alert_raised_once_then_cleared(self):
        st = server.new_state()
        for seq, pm in enumerate([20, 100, 110, 100, 50], 1):  # AQI 33, 233, 267, 233, 83
            server.ingest(st, server.validate(reading(seq, pm)), seq)
        levels = [a["level"] for a in reversed(st["alerts"])]
        self.assertEqual(levels, ["alert", "clear"])

    def test_sensor_offline_and_back(self):
        st = server.new_state()
        server.ingest(st, server.validate(reading(1)), 0)
        server.check_offline(st, 9, server.OFFLINE_AFTER + 1)
        self.assertTrue(st["stations"]["kothrud"]["offline"])
        server.ingest(st, server.validate(reading(2)), 20)
        self.assertFalse(st["stations"]["kothrud"]["offline"])
        self.assertEqual([a["level"] for a in reversed(st["alerts"])], ["warn", "info"])

    def test_rejects_bad_input(self):
        for bad in ({"pm25": math.nan}, {"pm25": -1}, {"pm25": "high"}, {"seq": "1"}, {"station": "x" * 50}, {"name": None}):
            with self.assertRaises(ValueError, msg=bad):
                server.validate({**reading(1), **bad})


class FailoverTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addrs = {i: f"127.0.0.1:{8300 + i}" for i in (1, 2, 3)}
        self.cfg = os.path.join(self.tmp, "cluster.json")
        with open(self.cfg, "w") as fh:
            json.dump({"servers": {str(k): v for k, v in self.addrs.items()}}, fh)
        self.procs = {i: self.start(i) for i in (1, 2, 3)}

    def start(self, i):
        log = open(os.path.join(self.tmp, f"server{i}.log"), "a")
        return subprocess.Popen([sys.executable, os.path.join(HERE, "server.py"), str(i),
                                 "--config", self.cfg, "--host", "127.0.0.1"], stdout=log, stderr=subprocess.STDOUT)

    def tearDown(self):
        for p in self.procs.values():
            p.kill()
            p.wait()

    def leader_session(self, path, first, timeout=15):
        """Open path on the leader, following redirects like sensors/dashboard do. Returns (ws, first reply)."""
        sid, end = 1, time.time() + timeout
        while time.time() < end:
            try:
                ws = connect(f"ws://{self.addrs[sid]}{path}", proxy=None, open_timeout=1)
                ws.send(json.dumps(first))
                m = json.loads(ws.recv(timeout=2))
                if m["type"] != "redirect":
                    return ws, m
                ws.close()
                sid = m["leader"] or sid
            except Exception:
                sid = sid % 3 + 1  # down or stale redirect: try the next server
            time.sleep(0.3)
        self.fail(f"no leader reachable for {path}")

    def send(self, seqs):
        """Send readings like a sensor and wait until the leader acks the last one."""
        ws, m = self.leader_session("/sensor", reading(seqs[0]))
        for s in seqs[1:]:
            ws.send(json.dumps(reading(s)))
        while not (m["type"] == "ack" and m["seq"] >= seqs[-1]):
            m = json.loads(ws.recv(timeout=3))
        ws.close()

    def dashboard(self):
        ws, m = self.leader_session("/live", {"type": "watch", "ts": 1})
        ws.close()
        return m

    def test_failover_loses_no_acked_reading(self):
        self.send([1, 2, 3, 4, 5])
        m = self.dashboard()
        self.assertEqual(m["leader"], 3, "bully election should pick the highest id")
        self.assertEqual(m["state"]["readings"], 5)

        self.procs[3].kill()
        self.procs[3].wait()
        # the sensor re-sends 4 and 5 (as if their acks were lost) plus a new reading 6
        self.send([4, 5, 6])
        m = self.dashboard()
        self.assertEqual(m["leader"], 2, "server 2 should take over")
        self.assertEqual(m["state"]["readings"], 6, "acked readings kept, duplicates dropped")
        self.assertEqual(len(m["state"]["stations"]["kothrud"]["history"]), 6)
        self.assertFalse(m["servers"]["3"])

        # restarted server 3 bullies its way back and adopts the replicated data
        self.procs[3] = self.start(3)
        time.sleep(4)
        self.send([7])
        m = self.dashboard()
        self.assertEqual((m["leader"], m["state"]["readings"]), (3, 7))


if __name__ == "__main__":
    unittest.main()
