package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
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
	mu        sync.RWMutex      // protects mp
	mp        map[string]string // key-value store
	gets      uint64            // atomic counter
	puts      uint64            // atomic counter
	prevStats Stats             // previous snapshot for printing
	statsMu   sync.Mutex        // protects prevStats & lastPrint
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.mp = make(map[string]string)
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	// Read path: shared lock
	kv.mu.RLock()
	value, found := kv.mp[request.Key]
	kv.mu.RUnlock()
	atomic.AddUint64(&kv.gets, 1)
	if found {
		response.Value = value
	}
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.mu.Lock()
	kv.mp[request.Key] = request.Value
	kv.mu.Unlock()
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

// BatchGet - optimized for multiple reads
func (kv *KVService) BatchGet(keys []string) ([]string, error) {
	// Shared read lock for all keys
	kv.mu.RLock()
	values := make([]string, len(keys))
	for i, key := range keys {
		if value, found := kv.mp[key]; found {
			values[i] = value
		}
	}
	kv.mu.RUnlock()
	atomic.AddUint64(&kv.gets, uint64(len(keys)))
	return values, nil
}

// BatchPut - optimized for multiple writes
func (kv *KVService) BatchPut(keys []string, values []string) error {
	if len(keys) != len(values) {
		return fmt.Errorf("keys and values length mismatch")
	}
	kv.mu.Lock()
	for i, key := range keys {
		kv.mp[key] = values[i]
	}
	kv.mu.Unlock()
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

	http.Serve(l, nil)
}
