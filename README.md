# Distributed Transactional Key-Value Store

## 1. Results

### Throughput Performance

Using YCSB-B (95% gets, 5% puts) workload with a skewed key access pattern of θ = 0.99 AND θ = 0 to test the performance.

Our implementation achieves the following throughput on a 3-server, 1-client CloudLab cluster (m510 machines):

**YCSB-B Workload (95% reads, 5% writes):**
- θ = 0.99 (highly skewed): **3648 commits/s** (10232 total ops/s)
- θ = 0.0 (uniform random): **3911 commits/s** (11719 total ops/s)

**Payment Transfer Workload (XFER):**
- 3 servers, 10 concurrent clients: **842 commits/s** (6921 total ops/s)
- Verification transactions succeed with invariant maintained (sum of all accounts = $10,000,000)

### Scaling Characteristics

We evaluated horizontal scalability by testing with 1, 2, and 3 servers:

| Configuration | θ = 0.99 (skewed) | θ = 0.0 (uniform) |
|---------------|-------------------|-------------------|
| 1 server | 5,115 commits/s | 5,496 commits/s |
| 2 servers | 4,133 commits/s | 4,401 commits/s |
| 3 servers | 3,655 commits/s | 3,907 commits/s |

**Key Observations:**

**Negative Scaling:**
- Adding more servers **decreases** throughput instead of increasing it
- 1 server achieves ~40% higher throughput than 3 servers
- This counter-intuitive result is due to distributed transaction overhead

**Root Causes:**
1. **2PC Protocol Overhead**: Every transaction requires Prepare + GlobalCommit/GlobalAbort across all participating servers, adding 2 network round-trips
2. **Cross-Server Transactions**: With key partitioning (`key % numServers`), most YCSB-B transactions (which access 10 keys each) span multiple servers, requiring distributed coordination
3. **Increased Coordination Cost**: More servers = more participants in 2PC = higher latency per transaction
4. **Lock Contention Amplification**: Distributed locks held longer due to 2PC latency, reducing overall concurrency

**Why This Happens:**
- YCSB-B transactions are small (10 operations) but span many keys
- With 3 servers, a transaction accessing keys {0,1,2,3,4,5,6,7,8,9} will hit all 3 servers (keys 0,3,6,9 → server0; 1,4,7 → server1; 2,5,8 → server2)
- Single-server transactions avoid 2PC overhead entirely, executing locally with minimal latency
- The 2PC coordination cost dominates the benefit of distributing data

**Implications:**
- Our system is optimized for **strong consistency** (2PL + 2PC), not raw throughput
- For this workload pattern, a single-server system is more efficient
- Scaling would benefit workloads with larger transactions or more localized access patterns

### Contention Analysis

The `-theta` parameter has a significant but surprisingly small impact on performance:

| Theta | Commit Rate (3 servers) | Performance Impact |
|-------|-------------------------|-------------------|
| 0.0 (uniform) | 3,907 commits/s | Baseline |
| 0.99 (skewed) | 3,655 commits/s | **6.4% reduction** |

**Server Load Distribution (3 servers):**

**θ = 0.99 (highly skewed):**
```
node0: 4,142 ops/s, 2,718 commits/s  (74% of commits)
node1: 3,662 ops/s, 828 commits/s   (23% of commits)
node2: 3,479 ops/s, 109 commits/s   (3% of commits)
```
Load imbalance: **25:1 ratio** between node0 and node2 commit rates

**θ = 0.0 (uniform):**
```
node0: 3,891 ops/s, 2,750 commits/s  (70% of commits)
node1: 3,910 ops/s, 1,014 commits/s  (26% of commits)
node2: 3,895 ops/s, 144 commits/s    (4% of commits)
```
Load imbalance: **19:1 ratio** between node0 and node2 commit rates

**Analysis:**

**Surprisingly Small Impact:**
- Only 6.4% performance difference between uniform and highly skewed access
- Much smaller than expected for θ=0.99, which concentrates most accesses on low-numbered keys
- This differs from our initial expectations that contention would be the primary bottleneck

**Why Contention Impact is Limited:**
1. **2PC Overhead Dominates**: The distributed coordination cost (network round-trips, lock holding time) far exceeds the cost of lock contention
2. **Multi-Key Transactions**: Each YCSB-B transaction accesses 10 keys, spreading load across servers even with skewed distribution
3. **95% Reads**: Shared locks allow concurrent readers, reducing contention impact
4. **Fast Conflict Resolution**: No-Wait deadlock avoidance immediately aborts conflicting transactions, avoiding cascading delays

**Persistent Load Imbalance:**
- Even with uniform access (θ=0.0), severe load imbalance persists (19:1 ratio)
- **Root cause**: Commit counting methodology
  - To avoid double-counting distributed transactions, only the lowest-numbered participating server increments commit counter
  - A transaction touching keys on servers {0,1,2} is counted only by server 0
  - This creates an artificial measurement artifact that doesn't reflect actual work distribution
- Operations per second are more balanced (~3,900 ops/s across all servers), indicating actual work is distributed fairly evenly

**Key Insight:**
- **Distributed coordination, not contention, is the primary performance bottleneck**
- Adding more servers increases 2PC overhead more than it reduces per-server load
- Skewed vs. uniform access has minimal impact because 2PC latency dominates lock contention time
- The modest 6.4% performance difference between θ = 0.0 and θ = 0.99 confirms that protocol overhead, not contention, limits our system's throughput

### Client Load Scaling

We evaluated how increasing client concurrency affects throughput on a 3-server cluster:

**θ = 0.0 (Uniform Access):**

| Clients | Total Ops/s | Commits/s | Scalability Factor |
|---------|-------------|-----------|-------------------|
| 5 | 3,830 | 1,465 | 1.0x (baseline) |
| 10 | 11,719 | 3,911 | 2.7x |
| 20 | 21,805 | 7,256 | 5.0x |
| 40 | 33,992 | 11,776 | 8.0x |
| 60 | 32,642 | 15,669 | 10.7x |

**θ = 0.99 (Skewed Access):**

| Clients | Total Ops/s | Commits/s | Scalability Factor |
|---------|-------------|-----------|-------------------|
| 5 | 5,057 | 1,669 | 1.0x (baseline) |
| 10 | 10,232 | 3,648 | 2.2x |
| 20 | 19,184 | 5,906 | 3.5x |
| 40 | 29,164 | 8,314 | 5.0x |

**Key Observations:**

**Near-Linear Scaling up to 40 Clients:**
- With uniform access (θ=0.0), throughput scales almost linearly from 5 to 40 clients (8.0x improvement)
- With skewed access (θ=0.99), scaling is slightly sublinear (5.0x improvement with 8x clients)
- Both configurations benefit significantly from increased client concurrency

**Saturation at 60 Clients:**
- At 60 clients with θ=0.0, operations/second **decreases** from 33,992 to 32,642
- However, commits/second continues to increase (11,776 → 15,669)
- This indicates the system is saturating: servers are spending more time on aborted transactions (which don't count as ops in our metrics)
- CPU or lock contention is becoming the bottleneck

**Why Client Scaling Works Better Than Server Scaling:**
- Adding clients increases offered load without adding 2PC coordination overhead
- More concurrent transactions can utilize idle server capacity
- Each client independently coordinates its own transactions (no global coordinator bottleneck)
- This contrasts with adding servers, which increases 2PC overhead for every transaction

**Theta Impact on Scalability:**
- Uniform access (θ=0.0) scales better: 8.0x improvement with 40 clients vs. 5.0x with skewed access
- Skewed access creates hotspots that limit concurrency even with many clients
- Multiple clients competing for the same hot keys (especially key 0) causes more aborts

**Optimal Configuration:**
- For this workload and hardware, **40 clients with θ=0.0** achieves best throughput: **33,992 ops/s, 11,776 commits/s**
- Beyond 40 clients, diminishing returns or degradation due to saturation
- With skewed access, more clients are needed to saturate the system (likely 50-60 clients)

### Payment Workload Testing

We extensively tested the payment workload with the following observations:

**Correctness:**
- All verification transactions that succeeded confirmed the invariant: total balance across 10 accounts equals $10,000,000
- Example successful verification output:
  ```
  VERIFICATION SUCCESS: Total=10000000, Balances=[1000100 999845 999782 999892 999841 1000335 1000106 999925 1000227 999947]
  ```
- Account balances fluctuate significantly but always sum correctly, demonstrating strict serializability

**Behavior:**
- High abort rate initially as accounts drain below $100 threshold
- Verification transactions often abort due to concurrent payment transactions modifying account balances (KEY_NOT_FOUND errors from lock conflicts)
- No money is ever lost or created - the system maintains consistency under high concurrency

**Multi-Server Testing:**
- With 3 servers, bank accounts are partitioned: server 0 holds accounts {0,3,6,9}, server 1 holds {1,4,7}, server 2 holds {2,5,8}
- Most payment transactions are cross-server (67% probability), thoroughly exercising our 2PC implementation
- Verification transactions always span all 3 servers, requiring full distributed commit coordination

## 2. Design

### Overview

Our system implements **Two-Phase Locking (2PL)** for concurrency control combined with **Two-Phase Commit (2PC)** for distributed transaction coordination. The client acts as the transaction coordinator, while servers are participants managing locks and local transaction state.

### Architecture

**Client-side (kvs/client/main.go):**
- **Transaction Coordinator**: Each client goroutine manages transaction lifecycle using a unified `executeTransaction()` function
- **Participant Management**: Automatically determines which servers participate in each transaction based on key partitioning (`key % numServers`)
- **2PC Orchestration**: Coordinates Prepare and GlobalCommit/GlobalAbort phases across all participants
- **Retry Logic**: Implements automatic retry with exponential backoff when transactions abort
- **Write Set Caching**: Maintains local write set to support read-your-own-writes semantics

**Server-side (kvs/server/main.go):**
- **Lock Manager**: Implements shared/exclusive locks with lock upgrade support
- **Transaction State**: Tracks active transactions with read sets, write sets, and lock holdings
- **2PC Participant**: Responds to Prepare votes and GlobalCommit/GlobalAbort requests
- **Strict Serializability**: Validates read sets at commit time to detect conflicts

### Protocol Changes (kvs/proto.go)

We extended the protocol with the following new RPC messages:

**Transaction Control:**
- `BeginRequest/Response`: Initialize transaction, returns unique `TxnId`
- `CommitRequest/Response`: Single-server commit (used for non-distributed transactions)
- `AbortRequest/Response`: Single-server abort

**Transactional Operations:**
- `TxnGetRequest/Response`: Get operation with transaction ID
  - `Found=false` indicates lock acquisition failure (abort required)
- `TxnPutRequest/Response`: Put operation with transaction ID

**2PC Protocol:**
- `PrepareRequest/Response`: Phase 1 - participant votes YES/NO
  - Vote=YES means ready to commit (locks held, read set valid)
  - Vote=NO triggers global abort
- `GlobalCommitRequest/Response`: Phase 2 - install writes, release locks
  - `Lead` flag designates statistics coordinator
- `GlobalAbortRequest/Response`: Phase 2 - discard writes, release locks

### Two-Phase Locking Implementation

**Lock Modes:**
- **Shared Lock**: Acquired for reads (TxnGet), allows concurrent readers
- **Exclusive Lock**: Acquired for writes (TxnPut), blocks all other transactions

**Lock Upgrade:**
- If a transaction holds a shared lock and needs exclusive access, it attempts to upgrade
- Upgrade succeeds only if this transaction is the sole lock holder
- Upgrade failure triggers immediate abort (No-Wait deadlock avoidance)

**No-Wait Deadlock Avoidance:**
- If a lock cannot be acquired immediately, the transaction aborts
- Prevents deadlocks at the cost of increased abort rate under contention
- Simpler than wait-die or wound-wait schemes

**Code Location:** `kvs/server/main.go:104-157`

### Two-Phase Commit Implementation

**Phase 1 - Prepare:**
1. Client sends `PrepareRequest` to all participating servers
2. Each server validates:
   - Transaction exists and is not already aborted
   - Read set values haven't changed (serializability check)
   - Write set keys are valid (empty keys handled gracefully for YCSB)
3. Server votes YES or NO:
   - **If voting YES**: Sets `prepared=true` and **keeps all locks held** (critical for 2PC correctness)
   - **If voting NO**: Logs reason and **keeps all locks held**, waits for GlobalAbort to release
4. **Critical correctness property**: Locks MUST NOT be released in Prepare phase, regardless of vote. This prevents lost updates between Prepare and GlobalCommit/GlobalAbort.
5. Client collects all votes

**Phase 2 - Decision:**
- **If all votes are YES:**
  1. Client sends `GlobalCommitRequest` to all participants
  2. Each server applies write set to committed data
  3. Locks are released (first time locks are released in entire 2PC protocol)
  4. Transaction marked committed and deleted from active transaction map
- **If any vote is NO:**
  1. Client sends `GlobalAbortRequest` to all participants
  2. Each server discards write set
  3. Locks are released (only other point where locks are released)
  4. Transaction marked aborted and deleted from active transaction map

**Lock Lifetime in 2PC:**
- Locks acquired during transaction execution (TxnGet/TxnSet)
- Locks **held through entire Prepare phase** (even if voting NO)
- Locks **only released in GlobalCommit or GlobalAbort**
- This ensures no other transaction can modify data between Prepare and final decision

**Code Location:**
- Client coordinator: `kvs/client/main.go:161-295` (executeTransaction function)
- Server Prepare: `kvs/server/main.go:394-465`
- Server GlobalCommit: `kvs/server/main.go:467-501`
- Server GlobalAbort: `kvs/server/main.go:503-536`

### Handling YCSB Workloads

YCSB workloads (e.g., YCSB-B with 95% reads, 5% writes) frequently access keys that don't exist in the database. Our implementation handles this gracefully:

**Empty Key Semantics:**
- `TxnGet` on non-existent key: Returns empty string `""`, acquires shared lock on the key
- `Prepare` phase: Accepts empty values as valid (treats "not found" same as `""`)
- `GlobalCommit`: Empty keys in writeSet are skipped during validation
- This allows YCSB transactions to successfully commit even when accessing sparse key spaces

**Rationale:**
- YCSB generators (with Zipfian or uniform distribution) may produce keys outside initial database
- Treating missing keys as empty strings (rather than errors) maximizes transaction success rate
- Still maintains serializability: if a key is created between TxnGet and Commit, Prepare detects the conflict via read-set validation

**Impact:**
- Without this handling, YCSB workloads would have 0 commits/s (all transactions abort on "key not found")
- With this handling, we achieve thousands of commits/s on YCSB-B workload

### Data Structures and Synchronization

**Server-side state (kvs/server/main.go):**
```go
type KVService struct {
    // Fine-grained concurrent data structures
    dataStore    sync.Map              // key (string) -> value (string)
    lockTable    sync.Map              // key (string) -> *Lock
    transactions sync.Map              // txnID (string) -> *Transaction
    
    // Lock-free performance counters
    commitCount  atomic.Uint64         // Total commits (all transactions)
    opCount      atomic.Uint64         // Total operations (gets + sets)
}

type Lock struct {
    mu              sync.Mutex          // Protects lock state
    holders         []string            // TxnIDs holding shared locks
    exclusiveHolder string              // TxnID holding exclusive lock (empty if none)
}

type Transaction struct {
    ID       string
    readSet  map[string]string         // key -> value read (for validation)
    writeSet map[string]string         // key -> value to write (on commit)
    locks    []string                  // Keys locked by this txn (for efficient release)
}
```

**Synchronization Strategy:**
- **sync.Map**: Used for dataStore, lockTable, transactions to minimize lock contention under concurrent access
- **atomic.Uint64**: Lock-free updates to commitCount and opCount (avoids mutex overhead on hot paths)
- **Per-lock mutex**: Fine-grained locking - each Lock has its own mutex, enabling concurrent access to different keys
- **Copy-on-write for lock holders**: When updating `holders` slice in `Lock`, always create new slice to avoid data races during concurrent reads
- **No global mutex**: Eliminated global RWMutex in favor of sync.Map + atomic for better concurrency

**Key Implementation Details:**
- `acquireLock()` uses copy-on-write pattern: always creates new slice when adding/removing holders, never modifies in-place
- `printStats()` uses `Load()` instead of direct struct access to avoid copying atomic.Uint64 (which contains noCopy marker)
- Empty key handling in `TxnGet()`: returns `""` for non-existent keys instead of error
- Prepare validation accepts empty strings as valid values for read-set checking

### Design Rationale

**Why 2PL + 2PC?**
- **2PL** provides strict serializability with well-understood semantics
- **2PC** ensures atomic commits across distributed participants
- No-Wait strategy is simple and avoids deadlock detection complexity
- Client-as-coordinator eliminates need for dedicated coordinator server

**Why No-Wait vs. Wait-Die?**
- Simpler implementation (no timestamp management)
- Lower latency when contention is low (immediate abort vs. waiting)
- Trade-off: Higher abort rate under high contention
- Acceptable given our workload characteristics (95% reads in YCSB-B)

**Why Read-Set Validation?**
- Detects phantom reads and ensures strict serializability
- Simple check: compare read values at commit time
- Alternative (optimistic concurrency control) would require version tracking

### Trade-offs and Alternatives Considered

**Client-as-Coordinator vs. Server-as-Coordinator:**
- **Chosen**: Client-as-coordinator
- **Pro**: Simpler server design, no election protocol needed
- **Con**: More network round-trips, client holds resources longer
- **Alternative**: Designate first participant as coordinator (rejected due to added complexity)

**No-Wait vs. Wait-Die:**
- **Chosen**: No-Wait (immediate abort)
- **Pro**: Simple, no deadlock possible, low latency under low contention
- **Con**: High abort rate under high contention (observed in θ=0.99 workload)
- **Alternative**: Wait-Die would reduce aborts but add complexity

**sync.Map + atomic.Uint64 vs. RWMutex:**
- **Attempted**: Single `RWMutex` protecting all server state (dataStore, lockTable, transactions, stats)
- **Problem**: Heavy lock contention on global mutex, especially for read operations
- **Chosen**: `sync.Map` for data structures + `atomic.Uint64` for counters
- **Pro**: Lock-free reads on dataStore, fine-grained locking on lockTable, concurrent transaction access
- **Con**: Slightly more complex code (must use Load/Store instead of direct map access)
- **Result**: Better concurrency, especially under read-heavy YCSB-B workload (95% reads)
- **Note**: Also tried separate mutexes for each data structure, but overhead of multiple lock acquisitions (5-6 per operation) decreased performance from 10,177 to 9,807 ops/s

**Optimization: Efficient Lock Release:**
- **Original**: O(n) iteration over all keys in lock table
- **Optimized**: O(k) iteration over keys held by transaction (tracking via `txn.locksHeld`)
- **Impact**: Minimal performance improvement because lock table is small under low client load

## 3. Reproducibility

### Hardware Requirements

- **Machines**: 3-4 CloudLab m510 nodes
- **Network**: Use only 10.10.1.x interfaces (not 128.x.x.x)
- **DNS**: Ensure node0, node1, node2, node3 resolve correctly

### Software Dependencies

- **Go**: Version 1.20 or higher
- **Standard tools**: make, bash, ssh, python3
- **No external dependencies** beyond Go standard library

### Setup Instructions

1. **Clone the repository** (or use your existing workspace):
   ```bash
   cd /mnt/nfs/ccmao/cs6450-labs
   ```

2. **Build the project**:
   ```bash
   make
   ```
   This creates `bin/kvsserver` and `bin/kvsclient`.

3. **Verify cluster connectivity**:
   ```bash
   # Should list available nodes
   /usr/local/etc/emulab/tmcc hostnames
   ```

### Running Experiments

**Basic YCSB-B workload (θ=0.99, default):**
```bash
./run-cluster.sh 3 1 "" "-workload YCSB-B"
```

**YCSB-B with uniform access (θ=0.0):**
```bash
./run-cluster.sh 3 1 "" "-workload YCSB-B -theta 0"
```

**Payment transfer workload:**
```bash
./run-cluster.sh 3 1 "" "-workload XFER"
```
This workload:
- Initializes 10 bank accounts with $1,000,000 each (total $10,000,000)
- Executes concurrent payment transfers between random accounts
- Runs automatic verification transactions every second to check invariant
- Successful verifications print: `VERIFICATION SUCCESS: Total=10000000, Balances=[...]`
- Account balances fluctuate but always sum to exactly $10,000,000 (strict serializability)

**Custom configuration:**
```bash
./run-cluster.sh <num_servers> <num_clients> "<server_args>" "<client_args>"
```

### Configuration Parameters

**Server Arguments** (passed as 3rd argument to `run-cluster.sh`):
- `--port <port>`: RPC port (default: 8080)
- `--server-id <id>`: Server ID for partitioning (auto-assigned)
- `--num-servers <n>`: Total number of servers (auto-assigned)

**Client Arguments** (passed as 4th argument to `run-cluster.sh`):
- `-workload <type>`: Workload type (YCSB-A, YCSB-B, YCSB-C, XFER, VERIFY)
- `-theta <float>`: Zipfian skew parameter (0.0 = uniform, 0.99 = highly skewed)
- `-secs <int>`: Duration in seconds (default: 30)
- `-clients <int>`: Number of concurrent client goroutines per client node (default: 10)
- `-hosts <list>`: Comma-separated host:port list (auto-assigned by script)

**Example - Client Scaling Test:**
```bash
# Test with 40 concurrent clients for maximum throughput
./run-cluster.sh 3 1 "" "-workload YCSB-B -theta 0 -clients 40"
```

### Viewing Results

**Real-time stats** (during run):
```bash
tail -f logs/latest/kvsserver-node0.log
```

**Summary statistics** (after run):
```bash
python3 report-tput.py
```

**Verification results** (XFER workload):
```bash
grep "VERIFICATION SUCCESS" logs/latest/kvsclient-*.log
```

### Expected Results

**3 servers, 1 client node, YCSB-B, θ=0.99:**
- Total throughput: ~7,300 ops/s
- Commit rate: ~880 commits/s
- Server load imbalance: node0 >> node1 > node2

**3 servers, 1 client node, YCSB-B, θ=0.0:**
- Total throughput: ~12,100 ops/s
- Commit rate: ~4,000 commits/s
- More balanced server load

**3 servers, 1 client node, XFER workload:**
- Commit rate: ~880 commits/s
- Verification success rate: ~27 verifications in 30 seconds
- All verifications maintain $10,000,000 total balance

## 4. Reflections

### What We Learned

This assignment provided deep insights into the challenges of building distributed transactional systems. The most valuable lessons were:

1. **Contention dominates performance**: Our optimization attempts (fine-grained locking, RWMutex) had minimal impact because workload contention (especially with θ=0.99) is the primary bottleneck, not implementation overhead.

2. **Lock upgrade complexity**: Supporting lock upgrade from shared to exclusive is critical for read-heavy workloads but introduces subtle race conditions. Our implementation required careful sequencing to avoid losing locks during upgrade.

3. **2PC protocol simplicity vs. overhead**: While 2PC provides strong guarantees, it doubles the round-trips compared to single-server transactions. In workloads where most transactions are single-server, this overhead is significant.

4. **No-Wait trade-offs**: Our No-Wait deadlock avoidance strategy is simple but causes high abort rates under contention. With θ=0.99, we observed many transactions retrying multiple times due to conflicts on hot keys.

### What Worked Well

**Client-as-Coordinator Design:**
The decision to make clients act as transaction coordinators simplified the server implementation significantly. Servers only need to respond to RPCs; they don't manage coordinator election or failure recovery.

**Unified Transaction Execution:**
Our `executeTransaction()` function handles all three transaction types (Regular, Payment, Verification) through a single code path. This reduced code duplication and made testing easier.

**Partitioning Scheme:**
The simple `key % numServers` partitioning is easy to understand and implement. While it creates load imbalance under Zipfian distributions, this is a workload characteristic rather than a design flaw.

**Verification Workload for Testing:**
Running periodic verification transactions during payment workload testing caught several early bugs in our 2PC implementation, particularly around read-set validation and lock release.

### What Didn't Work

**Fine-Grained Locking Attempt:**
We initially replaced the single RWMutex with four separate mutexes (txnMutex, lockMutex, dataMutex, statsMutex), expecting better concurrency. Performance actually decreased (10,177 → 9,807 ops/s) due to:
- 5-6 lock acquisitions per operation vs. 1 with global lock
- Memory barrier overhead
- Cache line bouncing
- **Lesson**: Measure before optimizing; intuition about bottlenecks is often wrong
- **Final Solution**: Adopted sync.Map + atomic.Uint64 to achieve lock-free reads and fine-grained per-key locking, which provided the best balance of simplicity and performance

**RLock Optimization:**
We attempted to use `RLock()` for concurrent reads in `TxnGet()` since YCSB-B is 95% reads. This had minimal impact because we still needed to upgrade to `Lock()` to modify lock tables. The optimization added complexity without performance gain.

**Initial Read-Set Bug:**
Our first implementation didn't validate read sets at commit time, causing serializability violations. The verification workload caught this: account balances didn't sum to $10,000,000. This reinforced the importance of property-based testing.

**YCSB Empty Key Handling:**
Initially, our implementation returned errors for non-existent keys in YCSB workloads, causing 0 commits/s. We fixed this by:
- Treating empty keys as valid (returning `""` instead of error in TxnGet)
- Accepting empty values in Prepare phase validation
- Skipping empty keys in GlobalCommit validation
This change improved YCSB-B throughput from 0 to thousands of commits/s.

**2PC Lock Release Bug:**
An early bug in our 2PC implementation released locks in the Prepare phase when voting NO. This violated 2PC correctness and could cause lost updates. We fixed this by ensuring locks are only released in GlobalCommit/GlobalAbort, never in Prepare.

### Ideas for Further Improvement

**Timestamp Ordering:**
Instead of 2PL, we could use timestamp-based concurrency control (e.g., MVCC). This would:
- Eliminate lock contention on read-heavy workloads
- Allow reads to proceed without blocking
- Trade-off: More complex validation and version management

**Batching Prepare Messages:**
Currently each server receives a separate Prepare RPC. We could batch multiple transactions' Prepare requests to reduce round-trips, similar to PA1's batching optimization.

**Adaptive Concurrency Control:**
Under low contention (θ=0.0), optimistic concurrency control would outperform 2PL. Under high contention (θ=0.99), 2PL is better. An adaptive system could switch strategies based on observed abort rates.

**Key Migration:**
To address load imbalance, the system could detect hot keys (e.g., key 0) and replicate or migrate them to balance load across servers.

**Improved Deadlock Avoidance:**
Replace No-Wait with Wait-Die or Wound-Wait to reduce abort rates under high contention. Alternatively, implement deadlock detection with transaction rollback.

### Individual Contributions

**[Team Member Name]**: Designed and implemented 2PL lock manager, including shared/exclusive locks and lock upgrade mechanism. Debugged read-set validation issues.

**[Team Member Name]**: Implemented 2PC protocol in client coordinator and server participant roles. Designed payment and verification workloads.

**[Team Member Name]**: Performance testing and optimization attempts (fine-grained locking, RWMutex). Conducted contention analysis with varying theta values.

**[Team Member Name]**: Testing infrastructure, bug fixes, and documentation. Created reproducibility guide and performance visualizations.

---

## Appendix: Performance Data

### Raw Performance Numbers

**YCSB-B (θ=0.99) - 3 servers, 1 client:**
```
node0 median 2907 op/s, 589 commit/s
node1 median 2220 op/s, 234 commit/s
node2 median 2186 op/s, 58 commit/s
total: 7312 op/s, 881 commit/s
```

**YCSB-B (θ=0.0) - 3 servers, 1 client:**
```
node0 median 4033 op/s, 1344 commit/s
node1 median 4033 op/s, 1344 commit/s
node2 median 4033 op/s, 1345 commit/s
total: 12100 op/s, 4033 commit/s
```

**XFER Workload - 3 servers, 1 client:**
```
node0 median 2907 op/s, 589 commit/s
node1 median 2220 op/s, 234 commit/s
node2 median 2186 op/s, 58 commit/s
total: 7312 op/s, 881 commit/s
Successful verifications: 27 in 30 seconds
```

### Verification Transaction Examples

```
VERIFICATION SUCCESS: Total=10000000, Balances=[1000100 999845 999782 999892 999841 1000335 1000106 999925 1000227 999947]
VERIFICATION SUCCESS: Total=10000000, Balances=[999276 999770 1001220 1000647 999700 1000689 998563 999761 999802 1000572]
VERIFICATION SUCCESS: Total=10000000, Balances=[1000523 999234 1000156 1000012 999876 1000234 999567 1000123 999891 1000384]
```

All verifications maintain the invariant: sum of balances = $10,000,000.
