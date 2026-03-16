# Clustering-Manager-Worker
# Distributed Edge Computing Cluster for Job Scheduling and Inference

## Overview

This project involves leading the collaborative development of a scalable cluster system as a capstone for IoT edge computing, focusing on job scheduling and inference distribution across edge devices. It enables efficient task management in distributed environments, such as IoT networks for sensor data processing or real-time analytics in remote locations. The system supports automatic cluster formation, real-time communication, and local simulation, making it ideal for resource-constrained setups like agricultural monitoring or industrial IoT in areas with limited connectivity.

Key features include:
- Edge server and worker nodes implemented in Go for coordination and execution.
- Integration of membership and discovery mechanisms for seamless cluster setup.
- Docker-based simulation for testing without physical hardware, including job generation and evaluation.
- Configurable scheduling modes and a web dashboard for monitoring.

## Prerequisites

- **Hardware** (Optional for Simulation):
  - Edge devices (e.g., Milk-V boards for real deployment).
  - Host machine for simulation and server.

- **Software**:
  - Docker and Docker Compose (for simulation).
  - Go 1.21+ (for building edge-server and milkv-node).
  - MQTT broker (e.g., Mosquitto, default port 1883).
  - Optional: go2rtc for RTSP playback, ffmpeg for video conversion.

- **Environment**:
  - Embedded Linux on worker nodes (for real deployment, supporting riscv64 cross-compilation).
  - Network connectivity for MQTT and HTTP (e.g., edge IP for Serf join).

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/your-repo/edge-cluster.git
   cd edge-cluster
   ```

2. Build the Go binaries:
   - For edge server:
     ```
     cd edge-server
     go build -o edge-server main.go
     ```
   - For worker node (cross-compile for riscv64 if needed):
     ```
     cd milkv-node
     GOARCH=riscv64 go build -o milkv-node main.go
     ```

3. Transfer binaries to worker nodes (for real deployment):
   ```
   scp milkv-node root@worker-ip:/root/
   ```

4. Place resources (binaries/models) in `RESOURCE_DIR` on the host (default `/home/qht/cluster-docker/resource`).

5. For simulation, navigate to Docker directory:
   ```
   cd cluster-docker
   ```

## Usage

### Simulation Mode (Local Testing without Hardware)

- Start the cluster:
  ```
  cd cluster-docker
  docker compose up -d
  ```
  Wait 10–20 seconds, verify with:
  ```
  curl -s http://localhost:8081/api/jobs/available
  ```

- Configure scheduler (edit docker-compose.yml or override):
  ```
  SCHEDULER_MODE=Greedy docker compose up -d
  ```
  Modes: PSO_ACO_GA, ACO_GA, Greedy, Round_Robin (default).

- Web UI: Open `http://localhost:8080`.

- Run simulation jobs:
  ```
  JOBS=1000 API_BASE=http://localhost:8081 ./tools/eval/run_1000.sh
  ```

- Auto job generator:
  ```
  docker compose --profile loadgen up -d
  ```

- Stop:
  ```
  docker compose down
  ```

### Real Deployment

- On Edge Server (Host):
  ```
  MQTT_BROKER=tcp://localhost:1883 ./edge-server
  ```
  (Configure env vars: FILE_STORAGE_DIR, RESOURCE_DIR).

- On Worker Node (Edge Device):
  ```
  EDGE_SERVER=edge-ip:8000 EDGE_API_BASE=http://edge-ip:8081 ./milkv-node
  ```
  Workers discover edge via mDNS or env, join Serf, and subscribe to MQTT for jobs.

## Architecture and Workflow

The system follows a distributed architecture for edge computing:

1. **Components and Responsibilities**:
   - **Edge Server (Go - edge-server/)**: Coordinates cluster (Serf for membership, mDNS for discovery), handles job creation/distribution (MQTT primary, HTTP fallback), hosts files/resources, provides Web UI/WebSocket, and optional go2rtc proxy.
   - **Worker Node (Go - milkv-node/)**: Discovers/joins cluster, receives/claims jobs, executes workloads (download binaries/models if needed), publishes status/metrics.
   - **External**: MQTT broker for pub/sub, ffmpeg for file tasks.

2. **Discovery & Membership**:
   - mDNS: Edge advertises _edge-serf._tcp, workers _milkv-worker._tcp.
   - Serf: Gossip for alive nodes, metadata (status, memory, IP).

3. **Job Lifecycle**:
   - Create job (REST /api/jobs or WS).
   - Enqueue pending, publish to MQTT cluster/jobs.
   - Workers claim (cluster/claims), edge assigns (cluster/assign/{nodeID}).
   - Execute: Download resources/files, run binary, publish status (cluster/status/{nodeID}) and metrics (cluster/metrics/{nodeID}).
   - Fallback: HTTP polling if MQTT fails.

4. **File-Based Tasks**:
   - Edge downloads/converts MP4 to H.264, serves /api/files.
   - Workers download, process, cleanup.

5. **Resources Distribution**:
   - Workers fetch missing binaries/models from /api/resources.

6. **Observability**:
   - Web UI (8080) with WS for cluster snapshots.
   - REST API (8081) for jobs/files/resources.
