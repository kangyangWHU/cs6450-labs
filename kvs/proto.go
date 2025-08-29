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

type BatchOperation struct {
	Key    string
	Value  string
	IsRead bool
}

type BatchPutGetRequest struct {
	Operations []BatchOperation
	Indices    []int
}

type BatchPutGetResponse struct {
	Values []string // Contains values for Get operations in the same order
}
