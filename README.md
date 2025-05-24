# 📃️ Distributed Key-Value Store

This project implements a fully functional distributed key-value store in C++ with a focus on scalability, consistency, and resilience. It includes features such as consistent hashing, replication, leader election, and fault tolerance.

---

## 🚀 Features

* **Node Management** – each node maintains its state and participates in a gossip-based cluster protocol.
* **Consistent Hashing** – virtual nodes for balanced data distribution.
* **Data Replication** – configurable replication factor and quorum-based consistency.
* **Leader Election** – automatic selection and reelection of cluster leader.
* **Storage Engine** – in-memory key-value storage with thread safety.
* **Client API** – supports `PUT`, `GET`, and `REMOVE` operations with optional consistency levels.
* **Fault Tolerance** – automatic failure detection and read-repair mechanisms.
* **Performance Metrics** – operation latency tracking and counts.

---

## 📁 Project Structure

| File        | Description                                                          |
| ----------- | -------------------------------------------------------------------- |
| `main.cpp`  | Core logic including all subsystems and cluster demo                 |

---

## 🧪 Example Run

```sh
===== DISTRIBUTED KEY-VALUE STORE DEMO =====

[12:00:01.123] Creating cluster with 3 nodes
[12:00:01.124] Starting node1 (127.0.0.1:6001)
[12:00:01.125] Node2 joining the cluster via node1
...
[12:00:02.130] Successfully wrote key 'user:1001' with value 'John Doe'
[12:00:03.142] Value from node1: John Doe
```

---

## 📦 Dependencies

* **C++17 or later**
* [Asio (standalone)](https://think-async.com/Asio/)
* [nlohmann/json](https://github.com/nlohmann/json)

> 📌 Be sure to set `/std:c++17` or `/std:c++20` in MSVC.

---

## 🔧 Build Instructions (CMake)

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

Or, for MSVC users:

1. Open the `.sln` file
2. Set the project to use C++17 or later
3. Build and run

---

## 📱 Simulated Behavior

| Scenario                    | Behavior                                 |
| --------------------------- | ---------------------------------------- |
| Node joins a cluster        | Receives gossip and updates cluster view |
| Node goes down              | Marked dead via missing heartbeat        |
| Data written during failure | Replicated to alive nodes                |
| Node restarts               | Triggers read-repair for missing keys    |
| Leader failure              | Triggers new leader election             |

---

## 📊 Metrics Sample

```
===== SYSTEM METRICS =====
Operations performed:
  - write: 5 operations
  - read: 9 operations
Maximum operation latencies:
  - write: 10ms
  - read: 5ms
=========================
```

---

## 🪤 Cleanup

All nodes are gracefully stopped at the end of execution.

---
