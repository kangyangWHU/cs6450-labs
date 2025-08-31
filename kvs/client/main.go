package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Client struct {
	rpcClient *rpc.Client
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient}
}

func (client *Client) Get(key string) string {
	request := kvs.GetRequest{
		Key: key,
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response.Value
}

func (client *Client) Put(key string, value string) {
	request := kvs.PutRequest{
		Key:   key,
		Value: value,
	}
	response := kvs.PutResponse{}
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

func runClient(id int, hosts []string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	// Create persistent RPC clients for each host
	clients := make(map[string]*rpc.Client)
	for _, host := range hosts {
		client, err := rpc.DialHTTP("tcp", host)
		if err != nil {
			log.Printf("Warning: Client %d failed to connect to server %s: %v", id, host, err)
			continue
		}
		clients[host] = client
		// Don't close until the function exits
		defer client.Close()
	}

	value := strings.Repeat("x", 128)

	// Start with a moderate batch size
	batchSize := 8092 * 32

	opsCompleted := uint64(0)

	for !done.Load() {
		// Number of chunks per server
		serverCount := len(hosts)
		chunksPerServer := 1
		totalChunks := chunksPerServer * serverCount

		// Create multiple batch requests, one for each chunk
		batchReqs := make([]*kvs.BatchPutGetRequest, totalChunks)
		for i := range batchReqs {
			batchReqs[i] = &kvs.BatchPutGetRequest{
				Operations: make([]kvs.BatchOperation, 0, batchSize),
			}
		}

		// Fill batches with operations, distributing by key hash
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)

			// Hash the key to determine which chunk it goes to
			hash := 0
			for _, c := range key {
				hash = (hash*13 + int(c)) % totalChunks
			}

			// Add operation to the appropriate chunk
			batchReqs[hash].Operations = append(batchReqs[hash].Operations, kvs.BatchOperation{
				Key:    key,
				Value:  value,
				IsRead: op.IsRead,
			})
		}

		// Track results with wait group
		var wg sync.WaitGroup
		errChan := make(chan error, totalChunks)

		// Process each batch in parallel
		for i, batchReq := range batchReqs {
			// Skip if the batch is empty
			if len(batchReq.Operations) == 0 {
				continue
			}

			// Determine which server to send this chunk to (round-robin)
			serverIdx := i % serverCount
			serverAddr := hosts[serverIdx]

			wg.Add(1)
			go func(req *kvs.BatchPutGetRequest, server string) {
				defer wg.Done()

				// Use the persistent RPC client for this server
				client := clients[server]
				if client == nil {
					errChan <- fmt.Errorf("no connection available to server %s", server)
					return
				}
				// Process batch directly with server
				batchResp := &kvs.BatchPutGetResponse{}
				if err := client.Call("KVService.ProcessBatch", req, batchResp); err != nil {
					errChan <- fmt.Errorf("batch operation failed on server %s: %v", server, err)
					return
				}
			}(batchReq, serverAddr)
		}

		// Wait for all operations to complete
		go func() {
			wg.Wait()
			close(errChan)
		}()

		// Check for errors
		hasError := false
		for err := range errChan {
			if err != nil {
				log.Printf("Client %d batch error: %v\n", id, err)
				hasError = true
				break
			}
		}

		if !hasError {
			opsCompleted += uint64(batchSize)
		}
	}

	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- opsCompleted
}

type HostList []string

func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

func main() {
	hosts := HostList{}

	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C)")
	secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
	flag.Parse()

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf(
		"hosts %v\n"+
			"theta %.2f\n"+
			"workload %s\n"+
			"secs %d\n",
		hosts, *theta, *workload, *secs,
	)

	start := time.Now()

	done := atomic.Bool{}
	resultsCh := make(chan uint64)

	clientId := 0
	numWorker := 64
	for i := 0; i < numWorker; i++ {
		go func(clientId int) {
			workload := kvs.NewWorkload(*workload, *theta)
			runClient(clientId, hosts, &done, workload, resultsCh)
			time.Sleep(10 * time.Millisecond)
		}(clientId)
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	opsCompleted := uint64(0)
	for i := 0; i < numWorker; i++ {
		opsCompleted += <-resultsCh
	}

	elapsed := time.Since(start)

	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
