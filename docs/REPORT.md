# Fault-Tolerant Distributed Air-Quality Monitoring System using Bully Election and Lamport Clocks

**Mini Project Report — Distributed Computing System Laboratory (BIT27PC04)**
Final Year B.Tech Information Technology, Semester VII, PCCoE Pune — 2026–27

## Abstract

This project builds a distributed IoT network that monitors air quality across six Pune locations. Sensor nodes stream PM2.5, PM10 and NO2 readings to a cluster of three monitoring servers. The leader computes the official CPCB Air Quality Index (AQI), raises health alerts, and replicates its state to backup servers every 0.5 s. If the leader crashes, the backups elect a new one with the Bully algorithm and continue from their replica. Sensors buffer unacknowledged readings and re-send them, and duplicates are dropped by sequence number, so no reading is lost or counted twice. All messages carry Lamport timestamps. In tests and live runs, monitoring resumed about 2.5 s after a leader crash with no data lost.

## 1. Introduction and Problem Statement

Air pollution is a major health risk in Indian cities, and warnings are only useful if they arrive on time. A monitoring system with one central server stops working the moment that server fails. The goal is a monitoring service that keeps collecting data and raising alerts when any server crashes, including the one in charge, without losing or double-counting readings. This is item 9 of the syllabus's suggested mini projects: an IoT-based distributed sensor monitoring system.

## 2. Objectives

1. Sensor nodes and servers that communicate by message passing over WebSocket (TCP).
2. CPCB AQI calculation and real-time health alerts.
3. Leader election among servers using the Bully algorithm.
4. Primary-backup replication, with no loss of acknowledged readings and no duplicates.
5. Detection of failed servers (heartbeats) and failed sensors (timeout), with Lamport timestamps on all events.

## 3. Distributed Computing Concepts Used

| Concept | Use in this project | Unit |
|---|---|---|
| Client-server, primary-backup architecture | Sensors are clients of one leader; two backups stand by | 1 |
| Message-oriented communication | JSON messages over WebSocket between sensors, servers and dashboards | 2 |
| Lamport logical clocks | Every message and alert is stamped; clock = max(local, received) + 1 | 2 |
| Bully election algorithm | Highest live server id becomes leader after a failure | 2 |
| Replication and consistency | Leader pushes full state to backups every 0.5 s; backups keep only newer versions | 4 |
| Fault tolerance and recovery | Heartbeat timeout detects a dead leader; the new leader resumes from the freshest replica | 4 |

## 4. System Architecture

<img src="screenshots/00-architecture.png" width="480" alt="Architecture">

*Figure 1: System architecture.*

- **Sensor nodes (sensors.py):** each station has its own connection, Lamport clock, sequence number and a buffer of unacknowledged readings.
- **Monitoring servers (server.py):** identical programs. The leader ingests readings, computes AQI, raises alerts, detects offline sensors and replicates its state. Backups hold the replica and watch for leader failure.
- **Dashboard (index.html):** live AQI per station with trend charts, an alert feed and cluster status. It works on a phone and reconnects to the new leader automatically.

## 5. Key Algorithms

**Bully election** (runs when no heartbeat arrives for 2 s):

```
send ELECTION to all higher ids
if no one answers OK: become leader, adopt freshest replica, announce COORDINATOR
else: wait for COORDINATOR (retry on timeout)
```

**Replicate-then-acknowledge** (leader, every 0.5 s):

```
send STATE to all backups              # replication + heartbeat
send ACK(highest seq) to each sensor   # only after replication
push state to dashboards
```

**Ingestion with deduplication:** a reading is applied only if its (boot id, sequence number) is newer than the last one seen from that sensor. AQI is the worst pollutant's CPCB sub-index. An alert fires at AQI 201 (Poor) and clears below 180.

## 6. Implementation

| | |
|---|---|
| Language | Python 3 (servers, sensors), JavaScript (dashboard) |
| Communication | WebSocket over TCP, JSON messages (asyncio + websockets) |
| Testing | Python unittest, including a real 3-server failover test |
| Run | `python server.py 1` / `2` / `3`, then `python sensors.py`; dashboard at http://localhost:8001 |

## 7. Results

<img src="screenshots/01-dashboard.png" width="450" alt="Dashboard">

*Figure 2: Live dashboard. Server 3 leads; servers 1 and 2 are backups.*

<img src="screenshots/02-health-alert.png" width="450" alt="Health alert">

*Figure 3: A pollution event at Hadapsar triggers a Very Poor health alert.*

<img src="screenshots/03-failover.png" width="450" alt="Failover">

*Figure 4: Leader (server 3) killed. Server 2 takes over; the alert is kept and no data gap appears.*

<img src="screenshots/05-election-logs.png" width="450" alt="Election logs">

*Figure 5: Logs during failover: failure detected, Bully election, new leader continues from replica; sensors buffer readings.*

<img src="screenshots/04-recovery-sensor-offline.png" width="450" alt="Recovery">

*Figure 6: Server 3 restarts and leads again with all data; a stopped sensor is marked OFFLINE within 5 s.*

<img src="screenshots/06-tests.png" width="420" alt="Tests">

*Figure 7: All 7 automated tests pass.*

| Measure | Result |
|---|---|
| Leader failure detection | 2.0 s |
| Total failover time | about 2.5 s |
| Acknowledged readings lost / duplicates counted | 0 / 0 |
| Sensor failure detection | 5 s |

## 8. Why This Design Is Good

| Situation | Single central server | This system |
|---|---|---|
| Server crash | Monitoring stops until restarted by hand | New leader in about 2.5 s, automatically |
| Readings during outage | Lost | Buffered and re-sent; none lost |
| Re-sent data | May be double counted | Deduplicated by sequence number |
| Clients during failover | Must be reconfigured | Redirected to the new leader automatically |
| Dead sensors | Often unnoticed | Detected and reported in 5 s |

## 9. SDG Mapping

| SDG | Target | Contribution |
|---|---|---|
| SDG 3 – Good Health and Well-being | 3.9 reduce illness and deaths from air pollution | Real-time AQI and health alerts warn residents to avoid exposure |
| SDG 11 – Sustainable Cities and Communities | 11.6 reduce cities' environmental impact, especially air quality | City-wide monitoring pinpoints pollution hotspots for action |
| SDG 13 – Climate Action | 13.3 climate awareness and capacity | Continuous public pollution data supports awareness and policy |
| SDG 9 – Industry, Innovation and Infrastructure | 9.1 reliable, resilient infrastructure | Fault-tolerant design keeps a public-health service running |

## 10. Conclusion

The project applies message passing, Lamport clocks, Bully election and primary-backup replication to keep an air-quality warning service running through server crashes. It recovers from a leader failure in about 2.5 s with no lost or duplicated readings, supporting SDG 3.9 and SDG 11.6.

## References

1. M. van Steen and A. S. Tanenbaum, *Distributed Systems*, 3rd ed., 2017.
2. G. Coulouris et al., *Distributed Systems: Concepts and Design*, 5th ed., Addison-Wesley, 2012.
3. L. Lamport, "Time, Clocks, and the Ordering of Events in a Distributed System," CACM 21(7), 1978.
4. H. Garcia-Molina, "Elections in a Distributed Computing System," IEEE Trans. Computers C-31(1), 1982.
5. Central Pollution Control Board, *National Air Quality Index*, Government of India, 2014.
