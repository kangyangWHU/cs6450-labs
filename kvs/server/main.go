package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Stats struct {
	puts uint64
	gets uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts = s.puts - prev.puts
	r.gets = s.gets - prev.gets
	return r
}

type KVService struct {
	mp               sync.Map   // key-value store (concurrent-safe)
	gets             uint64     // atomic counter
	puts             uint64     // atomic counter
	prevStats        Stats      // previous snapshot for printing
	statsMu          sync.Mutex // protects prevStats & lastPrint
	lastPrint        time.Time
	metricsCollector *kvs.MetricsCollector // hardware metrics collector
}

func NewKVService() *KVService {
	kvservice := &KVService{}
	// sync.Map doesn't need initialization
	kvservice.lastPrint = time.Now()

	// Initialize metrics collector
	kvservice.metricsCollector = kvs.NewMetricsCollector()
	kvservice.metricsCollector.StartCollection(1 * time.Second) // Collect every second

	return kvservice
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	// Use sync.Map's Load method for concurrent-safe reads
	if value, found := kv.mp.Load(request.Key); found {
		response.Value = value.(string)
	}
	atomic.AddUint64(&kv.gets, 1)
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	// Use sync.Map's Store method for concurrent-safe writes
	kv.mp.Store(request.Key, request.Value)
	atomic.AddUint64(&kv.puts, 1)
	return nil
}

func (kv *KVService) printStats() {
	// Snapshot atomic counters
	curGets := atomic.LoadUint64(&kv.gets)
	curPuts := atomic.LoadUint64(&kv.puts)

	kv.statsMu.Lock()
	prev := kv.prevStats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.prevStats = Stats{gets: curGets, puts: curPuts}
	kv.lastPrint = now
	kv.statsMu.Unlock()

	diffGets := curGets - prev.gets
	diffPuts := curPuts - prev.puts
	deltaS := now.Sub(lastPrint).Seconds()
	if deltaS <= 0 {
		deltaS = 1
	}

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\n\n",
		float64(diffGets)/deltaS,
		float64(diffPuts)/deltaS,
		float64(diffGets+diffPuts)/deltaS)
}

// BatchGet - optimized for multiple reads using sync.Map
func (kv *KVService) BatchGet(keys []string) ([]string, error) {
	values := make([]string, len(keys))
	for i, key := range keys {
		if value, found := kv.mp.Load(key); found {
			values[i] = value.(string)
		}
		// values[i] remains empty string if key not found
	}
	atomic.AddUint64(&kv.gets, uint64(len(keys)))
	return values, nil
}

// BatchPut - optimized for multiple writes using sync.Map
func (kv *KVService) BatchPut(keys []string, values []string) error {
	if len(keys) != len(values) {
		return fmt.Errorf("keys and values length mismatch")
	}
	for i, key := range keys {
		kv.mp.Store(key, values[i])
	}
	atomic.AddUint64(&kv.puts, uint64(len(keys)))
	return nil
}

func (s *KVService) ProcessBatch(req *kvs.BatchPutGetRequest, resp *kvs.BatchPutGetResponse) error {
	// Original simpler strategy: process maximal consecutive runs of reads, then writes, alternating.
	ops := req.Operations
	n := len(ops)
	resp.Values = make([]string, n)

	i := 0
	for i < n {
		// Collect consecutive reads
		if i < n && ops[i].IsRead {
			rKeys := make([]string, 0, 100)
			rIdxs := make([]int, 0, 100)
			for i < n && ops[i].IsRead {
				rKeys = append(rKeys, ops[i].Key)
				rIdxs = append(rIdxs, i)
				i++
			}
			if len(rKeys) > 0 {
				values, err := s.BatchGet(rKeys)
				if err != nil {
					return err
				}
				for j, idx := range rIdxs {
					resp.Values[idx] = values[j]
				}
			}
		}
		// Collect consecutive writes
		if i < n && !ops[i].IsRead {
			wKeys := make([]string, 0, 8)
			wVals := make([]string, 0, 8)
			for i < n && !ops[i].IsRead {
				wKeys = append(wKeys, ops[i].Key)
				wVals = append(wVals, ops[i].Value)
				i++
			}
			if len(wKeys) > 0 {
				if err := s.BatchPut(wKeys, wVals); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	flag.Parse()

	kvs := NewKVService()
	defer func() {
		kvs.metricsCollector.StopCollection()
		// Print final hardware metrics
		fmt.Println("=== SERVER FINAL HARDWARE METRICS ===")
		kvs.metricsCollector.PrintFinalStats()
	}()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	// Handle shutdown gracefully
	go func() {
		<-sigChan
		fmt.Println("\nShutting down server...")
		kvs.metricsCollector.StopCollection()
		fmt.Println("=== SERVER FINAL HARDWARE METRICS ===")
		kvs.metricsCollector.PrintFinalStats()
		os.Exit(0)
	}()

	http.Serve(l, nil)
}
