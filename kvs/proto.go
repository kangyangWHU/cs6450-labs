package kvs

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct {
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value string
}

// Transaction support
type BeginRequest struct {
}

type BeginResponse struct {
	TxnId string
}

type CommitRequest struct {
	TxnId string
	Lead  bool // true if this participant is the leader/coordinator
}

type CommitResponse struct {
	Success bool
}

type AbortRequest struct {
	TxnId string
}

type AbortResponse struct {
}

// Transactional operations
type TxnGetRequest struct {
	TxnId string
	Key   string
}

type TxnGetResponse struct {
	Value string
	Found bool
}

type TxnPutRequest struct {
	TxnId string
	Key   string
	Value string
}

type TxnPutResponse struct {
}

// 2PC Protocol messages
type PrepareRequest struct {
	TxnId string
}

type PrepareResponse struct {
	TxnId string
	Vote  bool // true = YES (ready to commit), false = NO (abort)
}

type GlobalCommitRequest struct {
	TxnId string
	Lead  bool // true if this participant is the leader/coordinator
}

type GlobalCommitResponse struct {
}

type GlobalAbortRequest struct {
	TxnId string
}

type GlobalAbortResponse struct {
}
