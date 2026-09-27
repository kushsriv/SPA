# Running on low-memory laptops

The whole project is designed to run in **under 1 GB of RAM**, so a laptop with 4 GB total works if you close the browser and other heavy apps while experiments run.

## Measured memory use
Measured in a test environment with a 300,000-message load test (600-byte messages, 20 node keys):

| Component | Idle | Peak under load | Notes |
|---|---|---|---|
| Kafka broker, **no Docker** (`scripts/kafka_native.sh`, 256 MB heap) | 317 MB | 423 MB | About 67k messages/s |
| Kafka broker in Docker (`mem_limit: 512m`, 256 MB heap) | 264 MB | 479 MB (capped at 512 MB) | About 79k messages/s; never killed for running out of memory |
| Python process: producer and consumer | — | about 140 MB | All simulated edge nodes will share **one** Python process |
| Coordinator (Python) | — | 141 MB (measured) | Same with 5 or 50 nodes |
| **Total, 50 nodes, real run** | | **788 MB** (edges 172 + coordinator 141 + broker 475) | Measured in experiment K4 |

## Which option to use

| Your laptop | Recommended setup | Why |
|---|---|---|
| **Linux** | Docker (`docker compose -f docker/docker-compose.yml up -d`) or no Docker | Docker on Linux has no VM overhead |
| **macOS** | **No Docker**: `scripts/kafka_native.sh start` | Docker Desktop runs a VM that reserves 1–2 GB on its own |
| **Windows** | **WSL2 without Docker Desktop**, then `scripts/kafka_native.sh start` inside WSL | Docker Desktop's VM is heavy; WSL2 alone can be capped (see below) |
| Very little RAM (4 GB or less) | No Docker, fewer simulated nodes (5–10), close the browser | |

### No-Docker Kafka (Linux, macOS, WSL2)
Requires Java 17 or newer: `java -version`. Install with `sudo apt install openjdk-17-jre-headless` (Ubuntu/WSL) or `brew install openjdk@17` (macOS).

```bash
scripts/kafka_native.sh start    # first run downloads Kafka (~120 MB) into .kafka/
scripts/kafka_native.sh stop
scripts/kafka_native.sh reset    # stop and delete all Kafka data
KAFKA_HEAP=192M scripts/kafka_native.sh start   # even smaller heap if needed
```

### Windows: cap WSL2 memory
Create `C:\Users\<you>\.wslconfig` with:
```ini
[wsl2]
memory=1536MB
processors=2
swap=2GB
```
Then run `wsl --shutdown` in PowerShell and reopen Ubuntu.

## Design changes made for low memory
1. **All simulated edge nodes run in one Python process**, each with its own lightweight Kafka producer, instead of one container per node. Bytes are still measured per node.
2. **Network conditions are emulated inside the application** (delay, jitter, loss, bandwidth cap and disconnections in the send path) instead of Linux-only `tc netem` on containers. This works on Windows and macOS too and is fully reproducible from a random seed. `tc netem` stays an optional validation on Linux.
3. **Kafka uses a 256 MB heap**, 2 network and 2 I/O threads, 24 h retention and 64 MB segments.
4. **No Flink and no Spark by default.** PySpark is an optional scalability experiment only for a teammate with a larger machine.

## If a laptop is still too small
Free cloud machines can run the long experiment sweeps. The code is identical, so only the run location changes:
- **GitHub Codespaces**: free monthly hours on personal accounts; Docker works inside.
- **Google Colab**: runs no-Docker Kafka with `scripts/kafka_native.sh` in a notebook cell.

Check the current free quotas; they change.
