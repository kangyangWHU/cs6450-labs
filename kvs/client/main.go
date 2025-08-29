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
	masterService := NewDistributedService(hosts)
	value := strings.Repeat("x", 128)
	const batchSize = 8092 * 16

	opsCompleted := uint64(0)

	for !done.Load() {
		// Create a batch request
		batchReq := &kvs.BatchPutGetRequest{
			Operations: make([]kvs.BatchOperation, batchSize),
		}

		// Fill the batch with operations
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)
			batchReq.Operations[j] = kvs.BatchOperation{
				Key:    key,
				Value:  value,
				IsRead: op.IsRead,
			}
		}

		// Process batch with automatic distribution
		batchResp := &kvs.BatchPutGetResponse{}
		if err := masterService.distributeKey(batchReq, batchResp); err != nil {
			log.Printf("Client %d batch error: %v\n", id, err)
			continue
		}
		opsCompleted += batchSize
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

// For master server
type MasterServer struct {
	ServerAddrs []string // List of available server addresses
}

func (m *MasterServer) GetServerForKey(key string) string {
	// Simple hash-based distribution
	hash := 0
	for _, c := range key {
		hash = (hash*31 + int(c)) % len(m.ServerAddrs)
	}
	return m.ServerAddrs[hash]
}

type DistributedService struct {
	master  *MasterServer
	clients map[string]*rpc.Client
	stats   struct {
		processingTime time.Duration
	}
}

func NewDistributedService(serverAddrs []string) *DistributedService {
	master := &MasterServer{
		ServerAddrs: serverAddrs,
	}

	clients := make(map[string]*rpc.Client)
	for _, addr := range serverAddrs {
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Printf("Warning: Failed to connect to server %s: %v", addr, err)
			continue
		}
		clients[addr] = client
	}

	return &DistributedService{
		master:  master,
		clients: clients,
	}
}

func (s *DistributedService) distributeKey(req *kvs.BatchPutGetRequest, resp *kvs.BatchPutGetResponse) error {
	start := time.Now()

	// Group operations by key and maintain original order for reads
	serverOps := make(map[string]*kvs.BatchPutGetRequest)

	// Initialize response array
	resp.Values = make([]string, len(req.Operations))
	// Group operations by target server
	for i, op := range req.Operations {
		// Get target server for this key
		server := s.master.GetServerForKey(op.Key)

		// Initialize server's batch request if needed
		if serverOps[server] == nil {
			serverOps[server] = &kvs.BatchPutGetRequest{
				Operations: make([]kvs.BatchOperation, 0),
				Indices:    make([]int, 0), // Track original indices
			}
		}

		// Add operation to server's batch
		serverOps[server].Operations = append(serverOps[server].Operations, op)
		serverOps[server].Indices = append(serverOps[server].Indices, i)
	}

	// Track RPC timing
	// var mu sync.Mutex

	var wg sync.WaitGroup
	errCh := make(chan error, len(serverOps))

	for server, batchReq := range serverOps {
		wg.Add(1)
		go func(server string, req *kvs.BatchPutGetRequest) {
			defer wg.Done()

			// callStart := time.Now()

			// Get RPC client for this server
			client := s.clients[server]
			if client == nil {
				errCh <- fmt.Errorf("no connection to server %s", server)
				return
			}

			// Send batch request to server
			serverResp := &kvs.BatchPutGetResponse{}
			if err := client.Call("KVService.ProcessBatch", req, serverResp); err != nil {
				errCh <- fmt.Errorf("batch operation failed on server %s: %v", server, err)
				return
			}

			// Copy results back to original positions
			for i, val := range serverResp.Values {
				origIndex := req.Indices[i]
				resp.Values[origIndex] = val
			}

		}(server, batchReq)
	}

	// Wait for all operations to complete
	go func() {
		wg.Wait()
		close(errCh)
	}()

	// Check for errors
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	s.stats.processingTime = time.Since(start)
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
	for i := 0; i < 12; i++ {
		go func(clientId int) {
			workload := kvs.NewWorkload(*workload, *theta)
			runClient(clientId, hosts, &done, workload, resultsCh)
		}(clientId)
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	opsCompleted := <-resultsCh

	elapsed := time.Since(start)

	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
