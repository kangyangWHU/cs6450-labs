package kvs

type PutRequest struct {
	// Key   string
	Key   uint64
	Value string
}

type PutBatchRequest struct {
	Items []PutRequest
}

type PutResponse struct {
}

type PutBatchResponse struct {
}

type GetRequest struct {
	// Key string
	Key uint64
}

type GetBatchRequest struct {
	Keys []GetRequest // TODO: add a Len here to speed up.
	// Keys []string
}

type GetBatchShardedRequest struct {
	Shard int
	Keys  []GetRequest
}

type GetResponse struct {
	Value string
}

type GetBatchResponse struct {
	Values []GetResponse
	// Values []string
}

// proto.go (additions)
// type GetBatchShardedRequest struct {
// 	Shard int
// 	Keys  GetBatchRequest
// }
// type GetBatchShardedResponse struct {
// 	Values GetBatchResponse
// }

type PutBatchShardedRequest struct {
	Shard int
	Items []PutRequest
}
type PutBatchShardedResponse struct{}

// type ClientBucket struct {
// 	GetBuffer GetBatchRequest
// 	PutBuffer PutBatchRequest
// }
