package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
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
	sync.Mutex
	mp        map[string]string
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.mp = make(map[string]string)
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.Lock()
	defer kv.Unlock()

	kv.stats.gets++

	if value, found := kv.mp[request.Key]; found {
		response.Value = value
	}

	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.Lock()
	defer kv.Unlock()

	kv.stats.puts++

	kv.mp[request.Key] = request.Value

	return nil
}

func (kv *KVService) printStats() {
	kv.Lock()
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.Unlock()

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.gets+diff.puts)/deltaS)
}

// BatchGet - optimized for multiple reads
func (kv *KVService) BatchGet(keys []string) ([]string, error) {

	// Update stats
	kv.Lock()
	// Acquire read lock once for all keys
	// kv.RLock()
	values := make([]string, len(keys))
	for i, key := range keys {
		if value, found := kv.mp[key]; found {
			values[i] = value
		}
	}
	kv.stats.gets += uint64(len(keys))
	kv.Unlock()

	return values, nil
}

func (s *KVService) ProcessBatch(req *kvs.BatchPutGetRequest, resp *kvs.BatchPutGetResponse) error {
	// batchStart := time.Now()

	// Initialize response array
	ops := req.Operations
	n := len(ops)
	resp.Values = make([]string, n)

	// Reads are batched until we hit a write
	readKeys := make([]string, 0, n) // Assume most are reads
	readIndices := make([]int, 0, n) // Track original positions

	for i := 0; i < n; i++ {
		op := ops[i]

		if op.IsRead {
			// Collect read operations
			readKeys = append(readKeys, op.Key)
			readIndices = append(readIndices, i)
		} else {
			// Hit a write - process all collected reads first
			if len(readKeys) > 0 {
				values, err := s.BatchGet(readKeys)
				if err != nil {
					return err
				}

				// Copy values to response in original positions
				for j, respIndex := range readIndices {
					resp.Values[respIndex] = values[j]
				}
				// Reset read batch
				readKeys = readKeys[:0]
				readIndices = readIndices[:0]
			}

			// Process the write operation using put method
			putReq := &kvs.PutRequest{Key: op.Key, Value: op.Value}
			putResp := &kvs.PutResponse{}
			if err := s.Put(putReq, putResp); err != nil {
				return err
			}

		}
	}

	// Process any remaining reads
	if len(readKeys) > 0 {
		values, err := s.BatchGet(readKeys)
		if err != nil {
			return err
		}

		// Copy values to response in original positions
		for j, respIndex := range readIndices {
			resp.Values[respIndex] = values[j]
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
