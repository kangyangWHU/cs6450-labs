
# README

## Results

### Final Throughput Numbers
- **Throughput Achieved:** 

### Hardware Utilization Metrics:**

| Component | Metric | Average | Peak |
|-----------|--------|---------|------|
| **Client** | CPU Usage | 82.87% | 99.33% |
|           | Memory Usage | 20.34% | 24.24% |
|           | Network RX | 5.64 Gb/s | 7.62 Gb/s |
|           | Network TX | 6.76 Gb/s | 8.7 Gb/s |
| **Server** | CPU Usage | 74.25% | 98.48% |
|           | Memory Usage | 18.37% | 23.15% |
|           | Network RX | 6.05 Gb/s | 9.31 Gb/s |
|           | Network TX | 5.15 Gb/s | 8.04 Gb/s |

### Scaling Characteristics
| Nodes | 2 | 4 | 6 | 8 
| | 4,519,256 op/s | 12,390,862 | | |

### Performance Graphs and Visualizations
[Insert graphs and visualizations here]

### Performance Grading Scale (YCSB-B, θ = 0.99)
- 100% grade: ≥ 12,800,000 op/s

## Design

### Changes Made and Their Effects

#### Client Side
- **Batch Processing**: Implemented `BatchPutGetRequest` to group multiple operations (up to 8092*32 per batch) into single RPC calls, dramatically reducing network overhead and RPC call frequency. This improves the performance over ten times.
- **Key Distribution via Hashing**: Added consistent key hashing to distribute operations across multiple server chunks, ensuring balanced load distribution across all available servers. This will increase the peroformance linearly with the increasement of the number of client and server.
- **Optimized Worker Configuration**: Deployed 64 concurrent client workers to maximize throughput while maintaining system stability. This will increase the peroformance linearly until reaching a pleato. 

#### Server Side
- **Sync.Map for Concurrent Operations**: Replaced traditional mutex-protected maps with Go's `sync.Map` to enable lock-free concurrent reads, significantly improving read performance under high concurrency. This will increase the performance over 50%.
- **Batch Operation Processing**: Implemented `ProcessBatch` method that handles mixed read/write operations in batches, processing consecutive operations of the same type together. This only slight improve the perofmance.
- **Atomic Counters**: Used atomic operations for statistics tracking to avoid contention during high-frequency operations. It slightly 

### Rationale for Design Choices

**Batching Strategy**: The primary bottleneck in distributed key-value systems is often network latency and RPC overhead rather than computation time. By batching operations, we reduce the number of round trips from O(n) individual operations to O(n/batch_size) batch operations, where batch_size = 8092*32. This provides approximately 259,000x reduction in network round trips for large workloads.

**Key Distribution Hashing**: Distributing keys across servers using a hash function ensures even load distribution and prevents hot-spotting on individual servers. The hash function `(hash*13 + int(c)) % totalChunks` provides good distribution properties while being computationally lightweight.

**Sync.Map Selection**: Traditional mutex-protected maps become bottlenecks under high read concurrency because all reads must acquire locks. Go's `sync.Map` uses copy-on-write semantics and atomic operations, allowing multiple concurrent readers without contention. This is particularly beneficial for read-heavy workloads like YCSB-B (95% reads, 5% writes).

- **Batch Operation Processing**: 

### Trade-offs and Alternatives Considered
[Discuss trade-offs and design alternatives.]

### Performance Bottleneck Analysis

**Integrated Hardware Metrics Collection**: We implemented a comprehensive metrics collection system within both client and server applications that monitors CPU usage, memory consumption, and network throughput in real-time. The `MetricsCollector` samples system metrics every second with minimal overhead, collecting data from `/proc/stat`, `/proc/meminfo`, and `/proc/net/dev` files on Linux systems.

**Network Monitoring with iftop**: We used `iftop` and network interface monitoring to track real-time network bandwidth utilization during benchmark runs. We found the client can not fully 
The metrics show peak network throughput of 9.59 Gbps RX and 8.17 Gbps TX on servers, indicating that network bandwidth is not the primary bottleneck - the system is effectively utilizing available network capacity.

**CPU Profiling and Tracing**: System-level CPU monitoring revealed average CPU utilization of 76.92% on servers and 82.87% on clients, with peaks reaching 98.81% and 99.33% respectively. This high CPU utilization suggests the optimizations successfully shifted the bottleneck from network/RPC overhead to computational processing, which is the desired outcome.

**Memory Usage Analysis**: Memory profiling shows relatively low memory consumption (18-24% peak usage), indicating that the `sync.Map` implementation and batching strategies are memory-efficient. The consistent memory usage pattern suggests no memory leaks or excessive allocations during high-throughput operations.

**Bottleneck Identification Results**:
1. **Pre-optimization**: Network latency and RPC call frequency were the primary bottlenecks
2. **Post-optimization**: CPU processing became the limiting factor, indicating successful elimination of network bottlenecks
3. **Scaling characteristics**: Linear scaling from 4.5M ops/s (2 nodes) to 12.4M ops/s (4 nodes) confirms efficient load distribution

**Performance Validation**: The metrics collection system provided real-time feedback during optimization iterations, allowing us to validate that each change (batching, sync.Map, persistent connections) effectively improved the targeted bottleneck without introducing new ones. 


## Reproducibility

### Step-by-Step Instructions
1. [Insert step-by-step instructions to reproduce results.]

### Hardware Requirements and Setup
- [Insert hardware requirements.]

### Software Dependencies and Installation
- [Insert software dependencies and installation steps.]

### Configuration Parameters
- [Insert configuration parameters and their effects.]

## Reflections

### Lessons Learned
[Discuss what was learned from the assignment.]

### Optimizations That Worked Well
[Explain what optimizations worked well and why.]

### Challenges and Lessons Learned
[Discuss what didn't work and lessons learned.]

### Ideas for Further Improvement
[Provide ideas for further improvement.]

### Individual Contributions
[Provide a short note on individual contributions from each team member.]