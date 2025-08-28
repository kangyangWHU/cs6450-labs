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

// fnv64a is a small, fast non-crypto hash for partitioning.
func fnv64a(s string) uint64 {
	const (
		offset64 = 1469598103934665603
		prime64  = 1099511628211
	)
	var h uint64 = offset64
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// type KVService struct {
// 	// sync.Mutex
// 	sync.RWMutex // enables more finetuned locking
// 	mp           map[string]string
// 	stats        Stats
// 	prevStats    Stats
// 	lastPrint    time.Time
// }

type Shard struct { // split the big table into multiple shards.
	sync.RWMutex
	mp map[string]string
	// stats    Stats
	// prevStats Stats
	// lastPrint time.Time
}

const numShards = 64

type KVService struct {
	sync.RWMutex
	// mp       map[string]string
	// shards    [numShards]Shard
	shards    [numShards]*Shard // a group of shards
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func (kv *KVService) shardIdx(key string) int {
	return int(fnv64a(key) & (numShards - 1)) // power-of-two shard count
}

func NewShard() *Shard {
	return &Shard{
		mp: make(map[string]string),
	}
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.shards = [numShards]*Shard{}
	for i := 0; i < numShards; i++ {
		kvs.shards[i] = NewShard()
	}
	kvs.lastPrint = time.Now()
	return kvs
}

// func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
// 	kv.Lock()
// 	defer kv.Unlock()

// 	kv.stats.gets++

// 	if value, found := kv.mp[request.Key]; found {
// 		response.Value = value
// 	}

// 	return nil
// }

func (kv *KVService) GetBatch(req *kvs.GetBatchRequest, resp *kvs.GetBatchResponse) error {
	resp.Values = make([]kvs.GetResponse, len(req.Keys))
	// 1) bucket indices by shard (so we keep response order)
	var buckets [numShards][]int
	for i, g := range req.Keys {
		s := kv.shardIdx(g.Key)
		buckets[s] = append(buckets[s], i)
	}
	// 2) read each shard in parallel
	var wg sync.WaitGroup
	for s := 0; s < numShards; s++ {
		if len(buckets[s]) == 0 {
			continue
		}
		wg.Add(1)
		go func(s int, idxs []int) {
			defer wg.Done()
			sh := kv.shards[s]
			sh.RLock()
			for _, i := range idxs {
				k := req.Keys[i].Key
				resp.Values[i] = kvs.GetResponse{Value: sh.mp[k]}
			}
			sh.RUnlock()
		}(s, buckets[s])
	}
	wg.Wait()
	atomic.AddUint64(&kv.stats.gets, uint64(len(req.Keys)))
	return nil
}

// Protobuf-optimized GetBatch for improved performance
func (kv *KVService) GetBatchProto(req *kvs.RPCProtoGetBatchRequest, resp *kvs.RPCProtoGetBatchResponse) error {
	// Convert to protobuf internally for efficient processing
	pbReq := req.ToProtobuf()
	pbResp := &kvs.ProtoGetBatchResponse{Values: make([]string, len(pbReq.Keys))}
	
	// 1) bucket indices by shard (so we keep response order)
	var buckets [numShards][]int
	for i, key := range pbReq.Keys {
		s := kv.shardIdx(key)
		buckets[s] = append(buckets[s], i)
	}
	// 2) read each shard in parallel
	var wg sync.WaitGroup
	for s := 0; s < numShards; s++ {
		if len(buckets[s]) == 0 {
			continue
		}
		wg.Add(1)
		go func(s int, idxs []int) {
			defer wg.Done()
			sh := kv.shards[s]
			sh.RLock()
			for _, i := range idxs {
				key := pbReq.Keys[i]
				pbResp.Values[i] = sh.mp[key]  // Direct string assignment - more efficient
			}
			sh.RUnlock()
		}(s, buckets[s])
	}
	wg.Wait()
	
	// Convert back to RPC-compatible format
	resp.FromProtobuf(pbResp)
	atomic.AddUint64(&kv.stats.gets, uint64(len(pbReq.Keys)))
	return nil
}

// func (kv *KVService) GetBatch(requests *kvs.GetBatchRequest, responses *kvs.GetBatchResponse) error {
// 	responses.Values = make([]kvs.GetResponse, len(requests.Keys))

// 	// kv.Lock()
// 	kv.RLock() // read lock
// 	// defer kv.Unlock()

// 	// kv.stats.gets += uint64(len(requests.Keys))

// 	// for i, g := range requests.Keys {
// 	// 	responses.Values[i] = kvs.GetResponse{Value: kv.mp[g.Key]}
// 	// }
// 	for i, request := range requests.Keys {
// 		if value, found := kv.mp[request.Key]; found {
// 			responses.Values[i].Value = value
// 		}
// 	}
// 	kv.RUnlock()
// 	atomic.AddUint64(&kv.stats.gets, uint64(len(requests.Keys)))
// 	return nil
// }

// func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
// 	kv.Lock()
// 	defer kv.Unlock()

// 	kv.stats.puts++

// 	kv.mp[request.Key] = request.Value

// 	return nil
// }

// func (kv *KVService) PutBatch(requests *kvs.PutBatchRequest, responses *kvs.PutBatchResponse) error {
// 	kv.Lock()
// 	defer kv.Unlock()

// 	for _, item := range requests.Items {
// 		kv.mp[item.Key] = item.Value
// 	}

// 	// kv.stats.puts += uint64(len(requests.Items))
// 	atomic.AddUint64(&kv.stats.puts, uint64(len(requests.Items)))
// 	return nil
// }

func (kv *KVService) PutBatch(req *kvs.PutBatchRequest, _ *kvs.PutBatchResponse) error {
	// 1) bucket items by shard
	var buckets [numShards][]int
	for i, it := range req.Items { // TODO: we can put this operation into client side in the first place.
		s := kv.shardIdx(it.Key)
		buckets[s] = append(buckets[s], i)
	}
	// 2) write each shard in parallel (exclusive)
	var wg sync.WaitGroup
	for s := 0; s < numShards; s++ {
		if len(buckets[s]) == 0 {
			continue
		}
		wg.Add(1)
		go func(s int, idxs []int) {
			defer wg.Done()
			sh := kv.shards[s]
			sh.Lock()
			for _, i := range idxs {
				it := req.Items[i]
				sh.mp[it.Key] = it.Value
			}
			sh.Unlock()
		}(s, buckets[s])
	}
	wg.Wait()
	atomic.AddUint64(&kv.stats.puts, uint64(len(req.Items)))
	return nil
}

// Protobuf-optimized PutBatch for improved performance
func (kv *KVService) PutBatchProto(req *kvs.RPCProtoPutBatchRequest, _ *kvs.RPCProtoPutBatchResponse) error {
	// Convert to protobuf internally for efficient processing
	pbReq := req.ToProtobuf()
	
	// 1) bucket items by shard
	var buckets [numShards][]int
	for i, it := range pbReq.Items {
		s := kv.shardIdx(it.Key)
		buckets[s] = append(buckets[s], i)
	}
	// 2) write each shard in parallel (exclusive)
	var wg sync.WaitGroup
	for s := 0; s < numShards; s++ {
		if len(buckets[s]) == 0 {
			continue
		}
		wg.Add(1)
		go func(s int, idxs []int) {
			defer wg.Done()
			sh := kv.shards[s]
			sh.Lock()
			for _, i := range idxs {
				it := pbReq.Items[i]
				sh.mp[it.Key] = it.Value  // Direct access to protobuf item fields
			}
			sh.Unlock()
		}(s, buckets[s])
	}
	wg.Wait()
	atomic.AddUint64(&kv.stats.puts, uint64(len(pbReq.Items)))
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
