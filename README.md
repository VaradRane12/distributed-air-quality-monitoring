# Pune Air Watch: Fault-Tolerant Distributed Air-Quality Monitoring Network

IoT air-quality sensors across Pune stream PM2.5, PM10 and NO₂ readings to a **cluster of
monitoring servers**. The leader computes the official **CPCB National AQI**, raises health
alerts when air turns *Poor* or worse, and replicates everything to backup servers. Kill the
leader and the backups elect a new one with the **Bully algorithm**. Sensors buffer their
readings during the switch and re-send them, so no acknowledged reading is ever lost and no
alert is missed. A public health warning system should not go silent because one server crashed.

Mini project for *Distributed Computing System Laboratory* (BIT27PC04), B.Tech IT, PCCoE Pune.
Syllabus mini-project #9: *IoT-Based Distributed Sensor Monitoring System*.

![Live dashboard](docs/screenshots/01-dashboard.png)

| Leader crashes: server 2 takes over, no data gap | Recovery + dead sensor detected |
|---|---|
| ![Failover](docs/screenshots/03-failover.png) | ![Recovery](docs/screenshots/04-recovery-sensor-offline.png) |

Full write-up with all figures, demo guide and design rationale: [docs/REPORT.md](docs/REPORT.md)

## Architecture

![Architecture](docs/screenshots/00-architecture.png)

```
  Sensor nodes (6 Pune stations)                         Dashboards (browser / phone)
  Shivajinagar  Hinjewadi  Bhosari                              ▲  live AQI, alerts,
  Hadapsar      Kothrud    Pimpri                               │  cluster status
        │ readings (seq no. + Lamport ts)                       │
        │ ◄── ack only after replication                        │
        ▼                                                       │
  ┌──────────────┐   state replication every 0.5 s (= heartbeat)   ┌──────────────┐
  │ server 3     │ ──────────────────────────────────────────────► │ server 2     │
  │ LEADER       │ ──────────────────────────────────┐             │ backup       │
  │ AQI + alerts │                                   ▼             └──────────────┘
  └──────────────┘                             ┌──────────────┐
        ▲  ELECTION / OK / COORDINATOR         │ server 1     │
        └───────────────────────────────────── │ backup       │
                                               └──────────────┘
```

- **Sensor nodes** (`sensors.py`) measure every second and keep each reading in a
  store-and-forward buffer until the leader acknowledges it. On reconnect they re-send the
  whole buffer; per-sensor sequence numbers let the server drop duplicates.
- **Leader** validates readings, computes the AQI, raises or clears alerts, detects dead
  sensors, and every 0.5 s replicates its state to the backups. It **acknowledges readings
  only after that replication round**, so an acknowledged reading survives a leader crash.
- **Backups** hold the replica. If it stops arriving for 2 s they suspect the leader and run
  a Bully election; the winner continues from the freshest replica.
- **Dashboards and sensors** may connect to any server; backups redirect them to the leader.

## Distributed-systems concepts used

| Concept (syllabus unit) | Where |
|---|---|
| IoT / pervasive system, client-server + primary-backup architecture (Unit 1) | `sensors.py`, `server.py` |
| Message-oriented communication over WebSocket (TCP) (Unit 2) | `client_session`, `peer_session` |
| Lamport logical clocks (Unit 2) | `tick()`; every message and alert carries `ts`; logs show `L=` |
| Bully leader election (Unit 2) | `election`, `become_leader`, `on_coordinator` |
| Store-and-forward + at-least-once delivery with deduplication (Unit 2) | `Sensor.buffer`, `ingest()` (`last_seq`) |
| Primary-backup replication, replicate-then-acknowledge (Unit 4) | `leader_loop` |
| Failure detection: servers (heartbeats) and sensors (offline timeout) (Unit 4) | `watchdog`, `check_offline` |
| Recovery from the freshest replica (Unit 4) | `become_leader` (`get_state`) |
| Web-based distributed system, client redirection (Unit 3) | `static/index.html`, `http()` |

## Run it

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

.venv/bin/python server.py 1      # terminal 1
.venv/bin/python server.py 2      # terminal 2
.venv/bin/python server.py 3      # terminal 3
.venv/bin/python sensors.py shivajinagar hinjewadi hadapsar kothrud pimpri   # terminal 4
.venv/bin/python sensors.py bhosari                                          # terminal 5
```

(`.venv/bin/python sensors.py` with no arguments runs all six stations in one terminal.)

Open the dashboard at <http://localhost:8001/> (or `http://<laptop-ip>:8001/` on a phone).
In the sensors terminal, type a station id (e.g. `kothrud`) + Enter to simulate a pollution
event such as a garbage fire; the station turns *Poor* and an alert appears.

### Demo script

1. Start the 3 servers: server 3 wins the election. Start the sensors; the dashboard fills.
2. Type `kothrud` in the sensors terminal: Kothrud's AQI jumps past 200 and a health alert appears.
3. `Ctrl+C` server 3. Server 2 suspects the failure, wins the election and continues. Sensors
   log "buffering", then re-send. The dashboard keeps updating; no gap appears in any trend line.
4. `Ctrl+C` the Bhosari sensor (terminal 5): after 5 s the dashboard marks it
   **SENSOR OFFLINE** and logs a warning. Restart it and it comes back online.
5. Restart server 3. It bullies its way back to leader and adopts the replicated data.

## Tests

```bash
.venv/bin/python -m unittest -v test_monitor
```

Unit tests: CPCB AQI values, duplicate dropping (including rebooted sensors), alert raise/clear,
sensor-offline detection, and input validation. The end-to-end test starts a real 3-server cluster, kills the leader
mid-stream, re-sends already-acknowledged readings, and checks that every reading is counted
exactly once.

## SDG mapping

| SDG | Target | How this project contributes |
|---|---|---|
| **SDG 3** – Good Health and Well-being (primary) | 3.9 reduce deaths and illness from air pollution | Real-time AQI and health alerts tell people, especially sensitive groups, when to avoid outdoor exposure |
| **SDG 11** – Sustainable Cities and Communities | 11.6 reduce the environmental impact of cities, with special attention to air quality | City-wide monitoring network that pinpoints pollution hotspots (e.g. industrial Bhosari MIDC) for municipal action |
| **SDG 13** – Climate Action | 13.3 awareness and institutional capacity on climate change | Continuous public data on combustion pollutants supports awareness and evidence-based policy |
| **SDG 9** – Industry, Innovation and Infrastructure | 9.1 reliable, resilient infrastructure | Fault-tolerant design keeps a public-health service running through server failures |

**For the submission sheet:** SDG 3 (3.9), SDG 11 (11.6), SDG 13 (13.3), SDG 9 (9.1)

## Limitations

- Sensor data is simulated (realistic Pune baselines, daily cycle, random pollution events);
  real hardware (e.g. an ESP32 with a PMS5003 sensor) would send the same messages.
- AQI is computed on instantaneous readings; CPCB averages PM over 24 h and NO₂ over 24 h.
- A network partition can briefly produce two leaders; Bully assumes crash failures.
- Sensor buffers live in memory; a sensor that reboots loses readings it had not yet sent.
