package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
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

// type Shard struct { // split the big table into multiple shards.
// sync.RWMutex
// mp map[string]string
// stats    Stats
// prevStats Stats
// lastPrint time.Time
// }

type ShardActor struct {
	mp    map[string]string
	getCh chan getReq
	putCh chan putReq
}

type getReq struct {
	// idxs []int
	// keys []string
	reqs kvs.GetBatchRequest
	resp chan []string
}

type putReq struct {
	// idxs []int
	// keys []string
	// vals []string
	reqs kvs.PutBatchRequest
	ack  chan struct{} // to signal completion
}

const numShards = 64

type KVService struct {
	shards [numShards]*ShardActor
	stats  *GlobStats // TODO: add a goroutine to avoid locks
}

type GlobStats struct {
	sync.RWMutex
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

// type KVService struct {
// sync.RWMutex
// mp       map[string]string
// shards    [numShards]Shard
// shards    [numShards]*Shard // a group of shards
// stats     Stats
// prevStats Stats
// lastPrint time.Time
// }

// func (kv *KVService) shardIdx(key string) int {
// 	return int(fnv64a(key) & (numShards - 1)) // power-of-two shard count
// }

func NewShard() *ShardActor {
	return &ShardActor{
		mp:    make(map[string]string),
		getCh: make(chan getReq),
		putCh: make(chan putReq),
	}
}

// func NewShard() *Shard {
// return &Shard{
// mp: make(map[string]string),
// }
// }

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.shards = [numShards]*ShardActor{}
	kvs.stats = &GlobStats{}
	for i := 0; i < numShards; i++ {
		shd := NewShard()
		// kvs.shards[i] = NewShard()
		kvs.shards[i] = shd

		// create an Actor goroutine for each shard
		go func(actor *ShardActor) {
			const batchSize = 1024
			actbuffer := make([]string, batchSize)
			for {
				select {
				case getReqs := <-actor.getCh:
					// process get request
					out := make([]string, len(getReqs.reqs.Keys))
					reqLen := len(getReqs.reqs.Keys)
					if cap(actbuffer) < reqLen {
						actbuffer = make([]string, reqLen)
					}
					// else {
					// actbuffer = actbuffer[:reqLen]
					// }
					for i, k := range getReqs.reqs.Keys {
						actbuffer[i] = actor.mp[strconv.FormatInt(int64(k.Key), 10)]
					}
					copy(out, actbuffer[:reqLen])
					getReqs.resp <- out
				case pr := <-actor.putCh:
					// process put request
					for _, k := range pr.reqs.Items {
						actor.mp[strconv.FormatInt(int64(k.Key), 10)] = k.Value
					}
					close(pr.ack) // signal completion by closing channel
				}
			}
		}(shd)
	}
	// kvs.lastPrint = time.Now()
	// globStats := &GlobStats{}
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

// A more detailed implementation: given a specific shard index, we assign this function with a
// func (kv *KVService) GetBatchSharded(req *kvs.GetBatchRequest, resp *kvs.GetBatchResponse, shardIdx int) error {
// 	resp.Values = make([]kvs.GetResponse, len(req.Keys))
// 	sh := kv.shards[shardIdx]
// 	sh.RLock()
// 	defer sh.RUnlock()
// 	for i, g := range req.Keys {
// 		resp.Values[i] = kvs.GetResponse{Value: sh.mp[g.Key]}
// 	}
// 	return nil
// }

// A utility function to get the total number of shards to the client
func (kv *KVService) GetShardTotalNum() int {
	return numShards
}

func (kv *KVService) GetBatchSharded(req *kvs.GetBatchShardedRequest, resp *kvs.GetBatchResponse) error {
	resp.Values = make([]kvs.GetResponse, len(req.Keys)) // allocate outside

	shardIdx := req.Shard
	if shardIdx < 0 || shardIdx >= numShards {
		return fmt.Errorf("invalid shard index: %d", shardIdx)
	}

	rch := make(chan []string, 1) // make a buffered channel

	kv.shards[shardIdx].getCh <- getReq{reqs: kvs.GetBatchRequest{Keys: req.Keys}, resp: rch}

	vals := <-rch
	for i, v := range vals {
		resp.Values[i] = kvs.GetResponse{Value: v}
	}
	atomic.AddUint64(&kv.stats.stats.gets, uint64(len(req.Keys)))
	return nil
}

func (kv *KVService) PutBatchSharded(req *kvs.PutBatchShardedRequest, resp *kvs.PutBatchResponse) error {
	shardIdx := req.Shard
	if shardIdx < 0 || shardIdx >= numShards {
		return fmt.Errorf("invalid shard index: %d", shardIdx)
	}

	rch := make(chan struct{}, 1) // make a buffered channel

	kv.shards[shardIdx].putCh <- putReq{reqs: kvs.PutBatchRequest{Items: req.Items}, ack: rch}

	<-rch
	atomic.AddUint64(&kv.stats.stats.puts, uint64(len(req.Items)))
	return nil
}

// func (kv *KVService) GetBatch(req *kvs.GetBatchRequest, resp *kvs.GetBatchResponse) error {
// 	resp.Values = make([]kvs.GetResponse, len(req.Keys))
// 	// 1) bucket indices by shard (so we keep response order)
// 	var buckets [numShards][]int
// 	for i, g := range req.Keys {
// 		s := kv.shardIdx(g.Key)
// 		buckets[s] = append(buckets[s], i)
// 	}
// 	// 2) read each shard in parallel
// 	var wg sync.WaitGroup
// 	for s := 0; s < numShards; s++ {
// 		if len(buckets[s]) == 0 {
// 			continue
// 		}
// 		wg.Add(1)
// 		go func(s int, idxs []int) {
// 			defer wg.Done()
// 			sh := kv.shards[s]
// 			sh.RLock()
// 			for _, i := range idxs {
// 				k := req.Keys[i].Key
// 				resp.Values[i] = kvs.GetResponse{Value: sh.mp[k]}
// 			}
// 			sh.RUnlock()
// 		}(s, buckets[s])
// 	}
// 	wg.Wait()
// 	atomic.AddUint64(&kv.stats.gets, uint64(len(req.Keys)))
// 	return nil
// }

// func (kv *KVService) GetBatch(req *kvs.GetBatchRequest, resp *kvs.GetBatchResponse) error {
// 	resp.Values = make([]kvs.GetResponse, len(req.Keys))
// 	// 1) bucket indices by shard (so we keep response order)
// 	var buckets [numShards][]int
// 	for i, g := range req.Keys {
// 		s := kv.shardIdx(g.Key)
// 		buckets[s] = append(buckets[s], i)
// 	}
// 	// 2) read each shard in parallel
// 	var wg sync.WaitGroup
// 	for s := 0; s < numShards; s++ {
// 		if len(buckets[s]) == 0 {
// 			continue
// 		}
// 		wg.Add(1)
// 		go func(s int, idxs []int) {
// 			defer wg.Done()
// 			sh := kv.shards[s]
// 			sh.RLock()
// 			for _, i := range idxs {
// 				k := req.Keys[i].Key
// 				resp.Values[i] = kvs.GetResponse{Value: sh.mp[k]}
// 			}
// 			sh.RUnlock()
// 		}(s, buckets[s])
// 	}
// 	wg.Wait()
// 	atomic.AddUint64(&kv.stats.gets, uint64(len(req.Keys)))
// 	return nil
// }

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

// func (kv *KVService) PutBatch(req *kvs.PutBatchRequest, _ *kvs.PutBatchResponse) error {
// 	// 1) bucket by shard
// 	type bucket struct {
// 		idxs       []int
// 		keys, vals []string
// 	}
// 	buckets := make([]bucket, numShards)
// 	for i, it := range req.Items {
// 		s := kv.shardIdx(it.Key)
// 		b := &buckets[s]
// 		b.idxs = append(b.idxs, i)
// 		b.keys = append(b.keys, it.Key)
// 		b.vals = append(b.vals, it.Value)
// 	}

// 	var wg sync.WaitGroup
// 	for s := 0; s < numShards; s++ {
// 		if len(buckets[s].idxs) == 0 {
// 			continue
// 		}
// 		wg.Add(1)
// 		go func(s int, b bucket) {
// 			defer wg.Done()
// 			ack := make(chan struct{})
// 			kv.shards[s].putCh <- putReq{idxs: b.idxs, keys: b.keys, vals: b.vals, ack: ack}
// 			<-ack
// 		}(s, buckets[s])
// 	}
// 	wg.Wait()
// 	atomic.AddUint64(&kv.stats.stats.puts, uint64(len(req.Items)))
// 	return nil
// }

// func (kv *KVService) PutBatch(req *kvs.PutBatchRequest, _ *kvs.PutBatchResponse) error {
// 	// 1) bucket items by shard
// 	var buckets [numShards][]int
// 	for i, it := range req.Items { // TODO: we can put this operation into client side in the first place.
// 		s := kv.shardIdx(it.Key)
// 		buckets[s] = append(buckets[s], i)
// 	}
// 	// 2) write each shard in parallel (exclusive)
// 	var wg sync.WaitGroup
// 	for s := 0; s < numShards; s++ {
// 		if len(buckets[s]) == 0 {
// 			continue
// 		}
// 		wg.Add(1)
// 		go func(s int, idxs []int) {
// 			defer wg.Done()
// 			sh := kv.shards[s]
// 			sh.Lock()
// 			for _, i := range idxs {
// 				it := req.Items[i]
// 				sh.mp[it.Key] = it.Value
// 			}
// 			sh.Unlock()
// 		}(s, buckets[s])
// 	}
// 	wg.Wait()
// 	atomic.AddUint64(&kv.stats.puts, uint64(len(req.Items)))
// 	return nil
// }

func (kv *KVService) printStats() {
	kv.stats.Lock()
	stats := kv.stats.stats
	prevStats := kv.stats.prevStats
	kv.stats.prevStats = stats
	now := time.Now()
	lastPrint := kv.stats.lastPrint
	kv.stats.lastPrint = now
	kv.stats.Unlock()

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
