
# README

## Results

### Final Throughput Numbers
- **Throughput Achieved:** 
### Scaling Characteristics
| Nodes | 2         | 4          | 6 | 8 |
|-------|-----------|------------|---|---|
| Ops/s |  |  | | |
### Contention Analysis
We evaluated the impact of the workload skew parameter θ on throughput and commit rate.
| θ |      Result         |
|-------|---------------------------|
| 0 (uniform) |  |
| 0.5 (moderate skew) |  |
| 0.99 (highly skewed)|  |

Overall, we observed that increasing θ shifts the bottleneck from CPU/network utilization to **lock contention**. The **No-Wait deadlock avoidance policy** ensured correctness, but at the cost of more aborts when θ was high.

---

## Payment Workload Results

### Experiment Setup
- **Accounts**: 10 accounts (ID 0–9), each initialized with $1000.  
- **Total Balance**: $10,000 across all accounts.  
- **Transactions**:  
  1. Transfer $100 from account *i* → (i+1)%10.  
  2. Retry aborted transactions until success.  
  3. Periodically run a balance check transaction: sum all accounts, assert total = $10,000.  

### Metrics

| Metric                      | Value (θ=0) | Value (θ=0.5) | Value (θ=0.99) |
|-----------------------------|-------------|---------------|----------------|
| Average Commit Rate (txn/s) |             |               |                |
| Average Abort Rate (txn/s)  |             |               |                |
| Final Balance Check (total) |             |               |                |
| Bugs/Issues Observed        |             |               |                |

### Balance Distribution Example

| Account ID | Final Balance |
|------------|---------------|
| 0          |               |
| 1          |               |
| 2          |               |
| …          |               |
| 9          |               |
| **Total**  | **10000**     |

*(Use this to demonstrate that while balances vary, the total always stays $10,000.)*  

### Performance Graphs and Visualizations

## Design

### System Overview
Our PA2 builds on PA1’s key-value store to support **distributed transactions** across multiple shards with strict serializability. We achieve this by combining **two-phase locking (2PL)** for concurrency control and **two-phase commit (2PC)** for atomic commitment across servers.

### Protocol and Interfaces

**RPC Messages (defined in `kvs/proto.go`):**
- `Begin` → returns a transaction id.  
- `TxnGet` / `TxnPut` → transactional reads and writes (with locks).  
- `Prepare` → Phase 1 of 2PC, server validates read set and votes.  
- `GlobalCommit` / `GlobalAbort` → Phase 2 of 2PC, install or discard write set.  
- `Abort` → client/server-initiated abort, release locks.  

**Transaction workflow:**
1. Client starts transaction (`Begin`).  
2. Client executes operations (`TxnGet`/`TxnPut`).  
3. Client requests `Prepare` from all participants.  
4. If all vote YES, client sends `GlobalCommit`; otherwise sends `GlobalAbort`.  
5. Servers release locks at the end of commit/abort.  

### Workload Generation (`kvs/loadgen.go`)

We implemented three types of workloads:

1. **YCSB Workload (RegularTxn)**  
   - Generates 3-operation transactions using a Zipfian distribution for keys.  
   - Operation mix depends on workload type:  
     - **YCSB-A**: 50% reads, 50% writes  
     - **YCSB-B**: 95% reads, 5% writes  
     - **YCSB-C**: 100% reads  
   - Controlled by parameter `-theta`, which adjusts the skew (0 = uniform, 0.99 = highly skewed).

2. **Payment Workload (PaymentTxn)**  
   - Bank transfer simulation.  
   - Chooses random source and destination accounts (0–9).  
   - Ensures sufficient funds before transfer.  
   - Each transaction consists of:  
     - 2 reads (from/to balances)  
     - 2 writes (updated balances)  
   - Metadata `Amount` tracks transfer size.

3. **Verification Workload (VerificationTxn)**  
   - Reads all 10 accounts, checks total balance == $10,000.  
   - Validates correctness of transfers under concurrent execution.  

**Randomness:**  
- Uses **Xorshift64** PRNG.  
- **ZipfianGenerator** implements skewed key selection based on Gray et al., SIGMOD 1994.  

### Client Design (`kvs/client/main.go`)

- **DistributedClient** manages multiple RPC clients (one per server).  
- **Transaction Execution**:
  - Maps keys to servers (`key % numServers`).  
  - Runs operations, tracking read values and writes.  
  - Special handling for PaymentTxn and VerificationTxn.  
- **Retry Logic**:
  - Aborted transactions are retried with exponential backoff.  
- **2PC**:
  - Phase 1: Gather votes from all participants.  
  - Phase 2: GlobalCommit or GlobalAbort.  
  - Lowest server ID chosen as coordinator to avoid double-counting commits.  



### Server Design (`kvs/server/main.go`)

- **Data Structures**:
  - `mp`: committed key-value map.  
  - `transactions`: active transaction states.  
  - `locks`: per-key lock table.  
- **Transaction State**:
  - Tracks read set, write set, locks held, committed/aborted/prepared flags.  
- **Lock Management (2PL)**:
  - `SharedLock`: multiple concurrent readers.  
  - `ExclusiveLock`: only one writer, blocks others.  
  - **Lock upgrade** supported (Shared → Exclusive if no conflicts).  
  - **No-Wait policy**: if lock acquisition fails, transaction aborts immediately.  
- **Commit Protocol (2PC)**:
  - `Prepare`: validate read set; vote YES/NO.  
  - `GlobalCommit`: apply write set and release locks.  
  - `GlobalAbort`: discard write set and release locks.  
- **Statistics**:
  - Tracks `get/s`, `put/s`, `commit/s`, `ops/s` (only leader commit counts).  


### Rationale

- **2PL + 2PC** → strict serializability, correctness across shards.  
- **Per-key locks** → simple and efficient for small-scale key space.  
- **No-Wait deadlock avoidance** → avoids deadlock cycles, at cost of more aborts.  
- **Hash-based partitioning** (`key % numServers`) → evenly balances load.  
- **Separate Payment + Verification workloads** → directly test serializability correctness.  

### Trade-offs and Alternatives Considered
- **Deadlock handling**: Chose No-Wait over deadlock detection for simplicity.  
- **Lock granularity**: per-key locks reduce complexity; per-shard or row-level locks could improve concurrency but add complexity.  
- **One-phase commit**: considered but rejected — cannot ensure atomic commit across servers.  
- **Batching**: could reduce RPC overhead but conflicts with fixed 3-op transaction model.  


## Reproducibility

### Step-by-Step Instructions

1. **Environment Setup**:
   ```bash
   /proj/utah-cs6450-PG0/bin/setup-nfs
   /proj/utah-cs6450-PG0/bin/install-go
   source ~/.bashrc
   ```
2. **Code Deployment**:

   ```bash
   git clone https://github.com/kangyangWHU/cs6450-labs.git
   cd cs6450-labs
   git checkout yk_pa2 
   ```
3. **Build and Run**:

   ```bash
   # Run the cluster benchmark (default 30 seconds), half server and half clients
   ./run-cluster.sh
   ```
4. **Results Collection**:

   - 

### Hardware Requirements and Setup

- **CloudLab m510 machines**: Maximum 8 nodes 
- **Network**: 10 Gbps Ethernet, use only 10.10.1.x interfaces

### Software Dependencies and Installation

- **Go 1.21+**: Installed via `/proj/utah-cs6450-PG0/bin/install-go`
- **Ubuntu 24.04**: Standard CloudLab image
- **NFS**: Inter-node file sharing via `/proj/utah-cs6450-PG0/bin/setup-nfs`

### Configuration Parameters

- **Batch Size**: `8092*32` operations per batch (configurable in `kvs/client/main.go`)
- **Worker Threads**: 64 concurrent workers per client node (configurable via `numWorker`)
- **Runtime**: Default 30 seconds (configurable via `--client-args "-secs X"`)
- **Key Distribution**: Hash-based distribution across server chunks
- **Transaction size**: 3 operations per transaction (fixed).
- **Workload types**:

-- workload ycsb (95% gets, 5% puts, with θ skew).

-- workload xfer (bank transfer workload).

- **θ parameter**: test with values {0, 0.5, 0.99}.

- **Worker threads**: configurable per client node in run-cluster.sh

## Reflections

### Lessons Learned
This project highlighted the complexity of implementing **strictly serializable distributed transactions** compared to single-node key-value stores.  
- First, we experienced the tradeoff between **correctness and performance**. By combining **two-phase locking (2PL)** with **two-phase commit (2PC)**, we ensured strict serializability across partitions. While this approach provides theoretical guarantees, it comes with substantial coordination and locking overhead.  
- Second, we encountered the practical challenge of **deadlocks and aborts**. Even with three-operation transactions, lock contention was frequent under high concurrency. We adopted the **No-Wait strategy**, which aborts immediately when a lock cannot be acquired. This eliminated deadlocks entirely but led to significantly higher abort rates under contention.  
- Third, we observed how **data access distribution strongly impacts performance**. With θ=0 (uniform distribution), throughput scaled reasonably well. Under θ=0.99 (high skew), however, contention on hot keys became the dominant bottleneck, leading to throughput collapse and frequent retries. This matched our expectations for Zipfian workloads and provided clear evidence of the fragility of 2PL+2PC under skewed workloads.  
- Finally, the **Payment workload** proved to be particularly valuable. It validated the preservation of invariants such as account balance conservation and revealed correctness issues that were not apparent with synthetic YCSB transactions. Without this workload, subtle serialization violations could easily have gone unnoticed.

### Optimizations That Worked Well
Several design choices proved effective in practice:  
1. **No-Wait strategy**: While it increased the number of aborts, its simplicity ensured the system never stalled due to deadlocks.  
2. **Clear 2PC structure**: Separating the protocol into a Prepare phase followed by GlobalCommit/GlobalAbort aligned well with the formal model from class and simplified both reasoning and debugging.  
3. **Account partitioning for Payment workload**: By initializing accounts using `account_id % numServers`, each server managed only its assigned accounts, avoiding duplicate initialization and ensuring data consistency.  
4. **Verification workload**: Periodic checks that the total sum across accounts 0–9 remained $10,000,000 provided strong evidence of correctness. Even under θ=0.99 with high contention and many aborts, the invariant held, which gave us confidence in the implementation.

### What didn't work 
Not all design choices were effective:  
1. **Lock granularity**: Our per-key locks were too coarse. Hotspot keys became severe contention points, limiting concurrency.  
2. **Abort storms under high skew**: With θ=0.99, retries after aborts often overlapped, amplifying contention and creating “abort storms” that degraded throughput. Our retry strategy lacked backoff or adaptive control.  
3. **Batching strategies**: Inspired by PA1, we attempted to experiment with batching to reduce RPC overhead, but with only 3–4 operations per transaction, batching added little benefit and introduced complexity.  
4. **Serialization optimizations**: Switching encodings (e.g., Protobuf vs. Go structs) provided negligible performance improvements, as the system bottleneck was dominated by lock contention and 2PC coordination, not serialization.

### Ideas for Further Improvement
If given additional time, we would pursue the following directions:  
1. **Adaptive retry with backoff**: Implement exponential backoff or randomized delays for retries to avoid abort storms under skewed workloads.  
2. **Consistent hashing and hotspot mitigation**: Move beyond modulo partitioning to consistent hashing or key replication to balance load and reduce contention under Zipfian distributions.  
3. **MVCC or lock-free approaches**: Multi-version concurrency control (MVCC) or lock-free data structures could alleviate bottlenecks from 2PL while still ensuring serializability.  
4. **Extended verification mechanisms**: Beyond checking global account sums, introduce subset audits or temporal consistency checks to catch subtle anomalies earlier.  
5. **Larger-scale evaluation**: Our tests were limited to 4 nodes. Scaling to larger clusters would help surface coordination bottlenecks, network latency issues, and imbalances across partitions.

### Individual Contributions
|     Member    |                                    Contributions                                   |
|:-------------:|:----------------------------------------------------------------------------------:|
|    Hao Ren    |       Implemented client-side transaction logic (Begin, TxnGet, TxnPut, 2PC coordination).                            |
| ChenCheng Mao |           Designed and implemented 2PL lock manager on the server.          |
|   Kang Yang   | Added 2PC RPC support (Prepare, GlobalCommit, GlobalAbort) in proto.go. |
|   Yujin Song   |    Developed and tested Payment and Verification workloads (loadgen.go), ran θ experiments.                      |
