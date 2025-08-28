
# High-Performance Key-Value Store

> **Make sure you've worked through the Programming Assignment Setup first. If you get stuck, ask on the class Discord server.**

## Table of Contents
1. [Overview](#overview)
2. [Ground Rules](#ground-rules)
3. [Setup Instructions](#setup-instructions)
4. [Deliverables](#deliverables)
5. [Grading Rubric](#grading-rubric)
6. [Timeline and Milestones](#timeline-and-milestones)
7. [Useful Tools](#useful-tools)

---

## 1. Overview
In this assignment, you will optimize a basic key-value store implementation to achieve maximum throughput on CloudLab m510 clusters. Working in groups of 4 students, you will compete to see who can achieve the highest performance while adhering to strict ground rules.

You are encouraged to be creative and explore multiple optimization strategies, from low-level system optimizations (OS, threading, networking) to high-level algorithmic approaches for distributed work. Your grade will be based both on achieved throughput and the quality of your technical write-up.

---

## 2. Ground Rules

### Hardware Constraints
- **Maximum 8 CloudLab m510 machines for ranked results**
- Do development on smaller 4 node clusters; only allocate 8 node clusters for short windows or share among groups if needed.
- Additional scale results are welcome but won't count toward rankings
- **All machines must use only the 10.10.1.x network interfaces**
	- DNS for node0, node1, etc. resolves to these interfaces. Do not use other names or IPs (e.g., NOT 128.x.x.x addresses).
	- Violating this restriction may result in experiment suspension.

### Code Requirements
- Any part of the skeleton code may be rewritten in any language
- **Workload integrity must be maintained:**
	- The embedded workload generator must be used and respected
	- All operations specified by the workload generator must be executed to completion in the order specified
	- Request/operation distributions cannot be modified
	- No operations can be discarded or skipped
- Your key-value store must provide linearizable semantics in failure-free cases; your final report must argue this.

### Request Generation Scaling
- Multiple workload generator instances are allowed for scalability
- Reasonable limits: one workload generator per thread/goroutine
- Prohibited: Unbounded generators (e.g., one per request)
- Maintain shared random number generator behavior from skeleton code to prevent convoying effects
	- Each workload generator instance should be seeded to prevent generating requests in the same order.

### Exception Policy
- Any rule may be waived with instructor permission
- Exceptions require written justification in your assignment report and approval

### Workload Specification
- Evaluated using YCSB-B (95% gets, 5% puts) workload with a skewed key access pattern of θ = 0.99 (Zipfian distribution with high skew).

---

## 3. Setup Instructions

### CloudLab Environment
1. Reserve 4 m510 machines (or 8 when ready) on CloudLab
2. Use the standard Ubuntu 24.04 profile
3. Ensure all machines are on the same VLAN
4. Record the 10.1.x.x IP addresses for each machine

### Initial Setup
```bash
# Set up inter-node nfs and install go
/proj/utah-cs6450-PG0/bin/setup-nfs
/proj/utah-cs6450-PG0/bin/install-go
source ~/.bashrc # make sure go is in your path

# Clone your repository
git clone [your-repo-url]
cd [repo-name]

# Install anything else you want (do this last since other scripts update the pkg list)

```
**In normal case, inital setup is set successfully already and you don't need to do it again. Check it only when unexpected error is produced.**

### Network Configuration
- Verify connectivity between all machines using 10.1.x.x addresses
- Test bandwidth:
	- On server: `iperf3 -s`
	- On client: `iperf3 -c [server-ip]`
- Expected bandwidth: ~10 Gbps between m510 machines

### Baseline Testing
```bash
# Run the provided skeleton code
./run-cluster.sh

# Verify workload generator is working
# Record baseline throughput numbers

./run-cluster.sh help
# Shows arguments for different cluster configurations and allows extra command line args to clients/servers.
```

---

## 4. Deliverables

### Repository Setup
- Create a branch named `pa1-turnin` in your group's GitHub repository. All deliverables must be present on this branch at the assignment deadline.

### Required README.md Sections
1. **Results** (1-2 paragraphs)
	- Final throughput numbers
	- Hardware utilization metrics (CPU, memory, network)
	- Scaling characteristics (performance vs. cluster size/client load)
	- Performance graphs/visualizations
	- Performance Grading Scale (YCSB-B, θ = 0.99):
		- 80% grade: ≥ 400,000 op/s
		- 82% grade: ≥ 800,000 op/s
		- 88% grade: ≥ 1,600,000 op/s
		- 92% grade: ≥ 3,200,000 op/s
		- 95% grade: ≥ 6,400,000 op/s
		- 100% grade: ≥ 12,800,000 op/s
2. **Design** (3-4 paragraphs)
	- Changes made and their effect on performance
	- Rationale for design choices
	- Trade-offs and alternatives considered
	- Bottleneck analyses
3. **Reproducibility** (clear steps)
	- Step-by-step instructions
	- Hardware requirements and setup
	- Software dependencies and installation
	- Configuration parameters and their effects
4. **Reflections** (1-4 paragraphs)
	- Lessons learned
	- Optimizations that worked/didn't work
	- Ideas for further improvement
	- Individual contributions from each team member

---

## 5. Grading Rubric

### Performance (50 points)
- Absolute throughput (30 points): Based on scale above
- Scaling efficiency (10 points): How well performance scales with resources
- Resource utilization (10 points): Efficient use of CPU, memory, network

### Technical Quality (20 points)
- Design sophistication (10 points): Complexity and cleverness of optimizations
- Implementation quality (10 points): Code quality, robustness, correctness

### Documentation (30 points)
- Results clarity (10 points): Clear presentation of performance data
- Design explanation (10 points): Thorough explanation of technical choices
- Reproducibility (10 points): Others can reproduce your results

---

## 6. Timeline and Milestones

**Week 1: Setup and Baseline**
- Set up CloudLab environment
- Run baseline measurements
- Identify initial bottlenecks
- Implement basic optimizations

**Week 2: Final Optimization and Documentation**
- Begin performance analysis
- Final performance runs
- Complete documentation

---

## 7. Useful Tools

- Profiling: `perf`, flamegraphs
- Network analysis: `tcpdump`, `Wireshark`, `ss`
- System monitoring: `htop`, `iotop`, `nethogs`, `numactl`
