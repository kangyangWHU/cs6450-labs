package main

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/rstutsman/cs6450-labs/kvs"
)

// DistributionStats holds statistics about key distribution
type DistributionStats struct {
	KeyFrequency map[uint64]uint64 // key -> frequency count
	ShardCounts  map[int]uint64    // shard -> operation count
	TotalOps     uint64
	UniqueKeys   uint64
}

// ShardDistribution calculates how keys are distributed across shards
func (ds *DistributionStats) CalculateShardDistribution(numShards int, shardFunc func(uint64) int) {
	ds.ShardCounts = make(map[int]uint64)

	for key, freq := range ds.KeyFrequency {
		shard := shardFunc(key)
		ds.ShardCounts[shard] += freq
	}
}

// PrintStats prints comprehensive statistics about the distribution
func (ds *DistributionStats) PrintStats(numShards int) {
	fmt.Printf("\n=== WORKLOAD DISTRIBUTION ANALYSIS ===\n")
	fmt.Printf("Total Operations: %d\n", ds.TotalOps)
	fmt.Printf("Unique Keys: %d\n", ds.UniqueKeys)
	fmt.Printf("Average ops per key: %.2f\n", float64(ds.TotalOps)/float64(ds.UniqueKeys))

	// Top 20 most frequent keys
	fmt.Printf("\n=== TOP 20 MOST FREQUENT KEYS ===\n")
	type keyFreq struct {
		key  uint64
		freq uint64
	}

	var keyFreqs []keyFreq
	for k, f := range ds.KeyFrequency {
		keyFreqs = append(keyFreqs, keyFreq{k, f})
	}

	sort.Slice(keyFreqs, func(i, j int) bool {
		return keyFreqs[i].freq > keyFreqs[j].freq
	})

	for i := 0; i < 20 && i < len(keyFreqs); i++ {
		percentage := float64(keyFreqs[i].freq) / float64(ds.TotalOps) * 100
		fmt.Printf("Key %6d: %8d ops (%.2f%%)\n", keyFreqs[i].key, keyFreqs[i].freq, percentage)
	}

	// Shard distribution analysis
	fmt.Printf("\n=== SHARD DISTRIBUTION ANALYSIS ===\n")

	var shardOps []uint64
	totalShardOps := uint64(0)
	for i := 0; i < numShards; i++ {
		ops := ds.ShardCounts[i]
		shardOps = append(shardOps, ops)
		totalShardOps += ops
	}

	// Calculate statistics
	avgOpsPerShard := float64(totalShardOps) / float64(numShards)

	var variance float64
	for _, ops := range shardOps {
		diff := float64(ops) - avgOpsPerShard
		variance += diff * diff
	}
	variance /= float64(numShards)
	stdDev := math.Sqrt(variance)

	// Find min/max
	minOps, maxOps := shardOps[0], shardOps[0]
	minShard, maxShard := 0, 0
	for i, ops := range shardOps {
		if ops < minOps {
			minOps = ops
			minShard = i
		}
		if ops > maxOps {
			maxOps = ops
			maxShard = i
		}
	}

	fmt.Printf("Average ops per shard: %.2f\n", avgOpsPerShard)
	fmt.Printf("Standard deviation: %.2f\n", stdDev)
	fmt.Printf("Coefficient of variation: %.2f%%\n", (stdDev/avgOpsPerShard)*100)
	fmt.Printf("Min shard (shard %d): %d ops (%.2f%% of avg)\n",
		minShard, minOps, float64(minOps)/avgOpsPerShard*100)
	fmt.Printf("Max shard (shard %d): %d ops (%.2f%% of avg)\n",
		maxShard, maxOps, float64(maxOps)/avgOpsPerShard*100)
	fmt.Printf("Load imbalance ratio: %.2fx\n", float64(maxOps)/float64(minOps))

	// Show worst and best balanced shards
	fmt.Printf("\n=== WORST 10 SHARDS (highest load) ===\n")
	type shardLoad struct {
		shard int
		ops   uint64
	}

	var shardLoads []shardLoad
	for i, ops := range shardOps {
		shardLoads = append(shardLoads, shardLoad{i, ops})
	}

	sort.Slice(shardLoads, func(i, j int) bool {
		return shardLoads[i].ops > shardLoads[j].ops
	})

	for i := 0; i < 10 && i < len(shardLoads); i++ {
		sl := shardLoads[i]
		percentage := float64(sl.ops) / avgOpsPerShard * 100
		fmt.Printf("Shard %2d: %8d ops (%.1f%% of avg)\n", sl.shard, sl.ops, percentage)
	}

	fmt.Printf("\n=== BEST 10 SHARDS (lowest load) ===\n")
	for i := len(shardLoads) - 10; i < len(shardLoads) && i >= 0; i++ {
		sl := shardLoads[i]
		percentage := float64(sl.ops) / avgOpsPerShard * 100
		fmt.Printf("Shard %2d: %8d ops (%.1f%% of avg)\n", sl.shard, sl.ops, percentage)
	}
}

// Different shard distribution functions to test
func simpleModulo(key uint64) int {
	return int(key % 64)
}

func fnv64aInt(key uint64) uint64 {
	const (
		offset64 = 1469598103934665603
		prime64  = 1099511628211
	)
	h := uint64(offset64)

	for i := 0; i < 8; i++ {
		h ^= uint64((key >> (i * 8)) & 0xFF)
		h *= prime64
	}
	return h
}

func hashModulo(key uint64) int {
	return int(fnv64aInt(key) % 64)
}

func highOrderBits(key uint64) int {
	return int((key >> 10) % 64)
}

func xorFolding(key uint64) int {
	folded := key ^ (key >> 16) ^ (key >> 32) ^ (key >> 48)
	return int(folded % 64)
}

func multiplicationHash(key uint64) int {
	const multiplier = 0x9e3779b97f4a7c15
	return int(((key * multiplier) >> (64 - 6)) % 64)
}

func xxhashModulo(key uint64) int {
	// Convert key to bytes for xxhash
	keyBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		keyBytes[i] = byte(key >> (i * 8))
	}
	hash := xxhash.Sum64(keyBytes)
	return int(hash % 64)
}

func xxhashOptimized(key uint64) int {
	// Direct uint64 hashing - much faster!
	// Use xxhash's internal algorithm on the key directly
	hash := xxhash.Sum64String(strconv.FormatUint(key, 10))
	return int(hash % 64)
}

func xxhashUint64Direct(key uint64) int {
	// Even better: treat uint64 as 8-byte slice without allocation
	keyBytes := (*[8]byte)(unsafe.Pointer(&key))[:]
	hash := xxhash.Sum64(keyBytes)
	return int(hash % 64)
}

func xxhashSeed(key uint64) int {
	// Use xxhash-inspired fast mixing
	// This avoids the full xxhash computation but uses similar principles
	h := key ^ 0x9E3779B9 // xxhash-like constant
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return int(h % 64)
}

func xxhashInspired(key uint64) int {
	// Use xxhash-inspired mixing without full algorithm
	h := key
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return int(h % 64)
}

func main() {
	if len(os.Args) < 4 {
		fmt.Printf("Usage: %s <workload_type> <theta> <num_operations>\n", os.Args[0])
		fmt.Printf("Example: %s YCSB-B 0.99 1000000\n", os.Args[0])
		os.Exit(1)
	}

	workloadType := os.Args[1]
	theta, err := strconv.ParseFloat(os.Args[2], 64)
	if err != nil {
		fmt.Printf("Invalid theta value: %s\n", os.Args[2])
		os.Exit(1)
	}

	numOps, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Printf("Invalid number of operations: %s\n", os.Args[3])
		os.Exit(1)
	}

	fmt.Printf("Evaluating workload distribution:\n")
	fmt.Printf("Workload: %s\n", workloadType)
	fmt.Printf("Theta: %.2f\n", theta)
	fmt.Printf("Operations: %d\n", numOps)

	// Create workload generator
	workload := kvs.NewWorkload(workloadType, theta)

	// Collect distribution data
	stats := &DistributionStats{
		KeyFrequency: make(map[uint64]uint64),
		TotalOps:     numOps,
	}

	fmt.Printf("\nGenerating %d operations...\n", numOps)
	for i := uint64(0); i < numOps; i++ {
		op := workload.Next()
		stats.KeyFrequency[op.Key]++

		if i%100000 == 0 && i > 0 {
			fmt.Printf("Progress: %d/%d operations (%.1f%%)\n", i, numOps, float64(i)/float64(numOps)*100)
		}
	}

	stats.UniqueKeys = uint64(len(stats.KeyFrequency))

	// Test different shard distribution methods
	shardMethods := map[string]func(uint64) int{
		"Simple Modulo":       simpleModulo,
		"Hash + Modulo":       hashModulo,
		"High-Order Bits":     highOrderBits,
		"XOR Folding":         xorFolding,
		"Multiplication Hash": multiplicationHash,
		"XXHash (Original)":   xxhashModulo,
		"XXHash (String)":     xxhashOptimized,
		"XXHash (Direct)":     xxhashUint64Direct,
		"XXHash (Fast Mix)":   xxhashSeed,
		"XXHash (Inspired)":   xxhashInspired,
	}

	for methodName, shardFunc := range shardMethods {
		fmt.Print("\n" + strings.Repeat("=", 60))
		fmt.Printf("\nSHARD METHOD: %s\n", methodName)
		fmt.Print(strings.Repeat("=", 60))

		stats.CalculateShardDistribution(64, shardFunc)
		stats.PrintStats(64)
	}

	fmt.Print("\n" + strings.Repeat("=", 60))
	fmt.Printf("\nSUMMARY COMPARISON\n")
	fmt.Print(strings.Repeat("=", 60))

	// Compare all methods side by side
	fmt.Printf("%-20s %10s %10s %10s %12s\n", "Method", "Min Ops", "Max Ops", "Std Dev", "CV %")
	fmt.Printf(strings.Repeat("-", 70) + "\n")

	for methodName, shardFunc := range shardMethods {
		stats.CalculateShardDistribution(64, shardFunc)

		var shardOps []uint64
		totalOps := uint64(0)
		for i := 0; i < 64; i++ {
			ops := stats.ShardCounts[i]
			shardOps = append(shardOps, ops)
			totalOps += ops
		}

		avgOps := float64(totalOps) / 64.0
		var variance float64
		minOps, maxOps := shardOps[0], shardOps[0]

		for _, ops := range shardOps {
			if ops < minOps {
				minOps = ops
			}
			if ops > maxOps {
				maxOps = ops
			}
			diff := float64(ops) - avgOps
			variance += diff * diff
		}

		stdDev := math.Sqrt(variance / 64.0)
		cv := (stdDev / avgOps) * 100

		fmt.Printf("%-20s %10d %10d %10.0f %11.1f%%\n",
			methodName, minOps, maxOps, stdDev, cv)
	}
}
