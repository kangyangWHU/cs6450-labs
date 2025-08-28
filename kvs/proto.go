package kvs

type PutRequest struct {
	Key   string
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
	Key string
}

type GetBatchRequest struct {
	Keys []GetRequest
	// Keys []string
}

type GetResponse struct {
	Value string
}

type GetBatchResponse struct {
	Values []GetResponse
	// Values []string
}

type ClientBucket struct {
	GetBuffer GetBatchRequest
	PutBuffer PutBatchRequest
}
