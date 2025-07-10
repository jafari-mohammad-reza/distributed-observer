# 🧠 The Observer

_A distributed log and full-text search engine inspired by Elasticsearch, MongoDB Oplog, and Kafka-based event streaming._

---

## 💡 Project Goals

This project is a hands-on implementation to practice distributed systems by building an Elasticsearch-style engine using Go and Kafka. The system is designed to support:

- 🔍 Full-text and keyword search
- 🧠 MMAP-backed in-memory index storage
- 📦 Append-only oplog-based write recovery
- 🛰️ Kafka-driven CDC and node synchronization
- 🌐 Peer-to-peer communication with custom load balancing
- 🧊 Hot/Cold memory-based index lifecycle management

---

## 🧬 High-Level Architecture

```text
                +--------+
Client Request →|  Node  |───Kafka→ Append-only Write Event (partitioned by index)
                +--------+
                    |
              +-------------+
              |  Processor  |←── Consumes event from Kafka
              +-------------+
                    |
       +----------------------------+
       | Index Memory Manager (MMAP)|
       +----------------------------+
                    |
             +--------------+
             | Disk Storage |
             +--------------+
                    |
           +------------------+
           | Oplog (Recovery) |
           +------------------+
```

Write Flow 1. Client sends a JSON payload to POST
**/logs/:index**

```json
{
  "time": "2023/08/12:13:05:10",
  "name": "x",
  "family": "y"
}
```

2. The node publishes a Kafka event (topic = index, partitioned by hash of index name).
3. Background processor consumes the event and transforms the log into a keyword-based map:

```json
{
  "2023/08/12:13:05:10": "time",
  "x": "name",
  "y": "family"
}
```

4. This map is written to a file using memory-mapped I/O:
   • If the index is OPEN, data is appended.
   • If the index doesn’t exist, it’s created and loaded into memory.
5. Indexes that haven’t been written to for a configurable TTL are marked CLOSED and evicted from memory.
6. Every action (write, close, load) is appended to a Kafka-backed oplog, which is later parsed by a Logstash-style consumer and stored in append-only blob logs.
   📦 Disk Format (Maximum Storage Efficiency)

To store logs efficiently while minimizing space:
• Logs will be serialized in compact binary format (e.g., MessagePack, Protobuf, or Gob).
• Chunks of records will be grouped and compressed with Zstandard (Zstd).
• Each chunk will be stored as a .dat.zst file on disk, with optional side dictionaries for keyword-to-ID mapping.

```text
/logs/auth-2025-07-10/
├── chunk-00001.dat.zst   # Zstd-compressed binary logs
├── chunk-00001.dict      # Optional keyword dictionary
```

    •	Index metadata (e.g., offsets, keyword maps) are stored separately.
    •	Cold chunks can be lazily loaded and mapped into memory only on query.

This format provides:
• 🚀 Compact size
• 💾 Lower disk I/O
• 🧊 Support for cold storage and archival
• 💡 Tradeoff: slow recovery/rebuilding time (acceptable for your goals)
🔍 Search Flow
• Clients must specify an index name or index prefix + time range to reduce scan space.
• The system: 1. Resolves the index set using prefix/range. 2. Lazily loads CLOSED indexes back into memory. 3. Merges the relevant MMAP regions into a unified map. 4. Performs keyword/full-text matching.

⸻

🧠 Memory & Index Lifecycle
• MMAP-backed memory means indexes are mapped directly from disk.
• OPEN Index: actively written to and kept in memory.
• CLOSED Index: offloaded to disk when inactive.
• Lifecycle controlled by background goroutines using Go channels and timers.
• Indexes can be time-sharded (logs-2025-07, logs-2025-07-10) for more efficient time range queries.

⸻

🛠️ Planned Features
• MMAP segment compaction and snapshots
• Text analysis pipeline (tokenizer, stemming, stop-word filtering)
• Binary protocol over TCP for low-latency cluster RPC
• Web UI dashboard for observability and debugging
• Peer-to-peer gossip and node discovery
• Raft-style metadata leadership and replication
• Smart index allocator for memory balancing
• Log-based failover and recovery system
