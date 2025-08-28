package kvs

import (
	"google.golang.org/protobuf/proto"
)

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
}

type GetResponse struct {
	Value string
}

type GetBatchResponse struct {
	Values []GetResponse
}

type ClientBucket struct {
	GetBuffer GetBatchRequest
	PutBuffer PutBatchRequest
}

// Protobuf optimization functions for high-performance batch operations

// Convert GetBatchRequest to protobuf for efficient serialization
func (req *GetBatchRequest) ToProtobuf() *ProtoGetBatchRequest {
	if req == nil {
		return nil
	}
	
	keys := make([]string, len(req.Keys))
	for i, k := range req.Keys {
		keys[i] = k.Key
	}
	
	return &ProtoGetBatchRequest{Keys: keys}
}

// Convert protobuf GetBatchResponse back to standard format
func ProtoToGetBatchResponse(pbResp *ProtoGetBatchResponse) *GetBatchResponse {
	if pbResp == nil {
		return nil
	}
	
	values := make([]GetResponse, len(pbResp.Values))
	for i, val := range pbResp.Values {
		values[i] = GetResponse{Value: val}
	}
	
	return &GetBatchResponse{Values: values}
}

// Convert PutBatchRequest to protobuf for efficient serialization
func (req *PutBatchRequest) ToProtobuf() *ProtoPutBatchRequest {
	if req == nil {
		return nil
	}
	
	items := make([]*ProtoPutBatchRequest_Item, len(req.Items))
	for i, item := range req.Items {
		items[i] = &ProtoPutBatchRequest_Item{
			Key:   item.Key,
			Value: item.Value,
		}
	}
	
	return &ProtoPutBatchRequest{Items: items}
}

// High-performance serialization functions

// Serialize GetBatchRequest to bytes using protobuf
func (req *GetBatchRequest) MarshalProtobuf() ([]byte, error) {
	pb := req.ToProtobuf()
	return proto.Marshal(pb)
}

// Deserialize GetBatchResponse from protobuf bytes
func UnmarshalProtoGetBatchResponse(data []byte) (*GetBatchResponse, error) {
	var pbResp ProtoGetBatchResponse
	if err := proto.Unmarshal(data, &pbResp); err != nil {
		return nil, err
	}
	
	return ProtoToGetBatchResponse(&pbResp), nil
}

// Serialize PutBatchRequest to bytes using protobuf
func (req *PutBatchRequest) MarshalProtobuf() ([]byte, error) {
	pb := req.ToProtobuf()
	return proto.Marshal(pb)
}

// RPC-compatible wrapper types for protobuf messages
// These have exported fields that work with Go's gob encoding

type RPCProtoGetBatchRequest struct {
	Keys []string
}

type RPCProtoGetBatchResponse struct {
	Values []string
}

type RPCProtoPutBatchRequest struct {
	Items []struct {
		Key   string
		Value string
	}
}

type RPCProtoPutBatchResponse struct {
	// Empty but gob-serializable
}

// Convert between RPC wrappers and protobuf types

func (req *RPCProtoGetBatchRequest) ToProtobuf() *ProtoGetBatchRequest {
	return &ProtoGetBatchRequest{Keys: req.Keys}
}

func (resp *RPCProtoGetBatchResponse) FromProtobuf(pb *ProtoGetBatchResponse) {
	resp.Values = pb.Values
}

func (req *RPCProtoPutBatchRequest) ToProtobuf() *ProtoPutBatchRequest {
	items := make([]*ProtoPutBatchRequest_Item, len(req.Items))
	for i, item := range req.Items {
		items[i] = &ProtoPutBatchRequest_Item{
			Key:   item.Key,
			Value: item.Value,
		}
	}
	return &ProtoPutBatchRequest{Items: items}
}