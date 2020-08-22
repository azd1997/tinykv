package server

import (
	"context"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	if val == nil {		// key not found
		return &kvrpcpb.RawGetResponse{
			NotFound:             true,
		}, nil
	}

	rsp := &kvrpcpb.RawGetResponse{
		Value:                val,
		NotFound:             false,
	}

	return rsp, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).

	modify := storage.Modify{Data: storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}}

	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawPutResponse{
		RegionError:          nil,
		Error:                "",
	}, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).

	modify := storage.Modify{Data: storage.Delete{
		Key:   req.Key,
		Cf:    req.Cf,
	}}

	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawDeleteResponse{
		RegionError:          nil,
		Error:                "",
	}, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.Cf)

	kvPairs := make([]*kvrpcpb.KvPair, 0, req.Limit)
	for iter.Seek(req.StartKey); iter.Valid() && len(kvPairs) < cap(kvPairs); iter.Next() {
		item := iter.Item()
		k := item.Key()
		v, e := item.Value()

		var keyError *kvrpcpb.KeyError
		if e != nil {
			switch e {
			case badger.ErrConflict:
				keyError = &kvrpcpb.KeyError{Conflict:&kvrpcpb.WriteConflict{
					StartTs:              0,
					ConflictTs:           0,
					Key:                  nil,
					Primary:              nil,
				}}
			case badger.ErrRetry:
				keyError = &kvrpcpb.KeyError{Retryable:badger.ErrRetry.Error()}
			// TODO 暂且不清楚

			}
		}

		// 尽管提供了KeyError，但返回的e是badger中定义的，只能根据e是badger中何种错误来决定对应的何种KeyError
		// 但目前不太清楚。 badger的错误码定义在badger/errors.go，但item.Value()返回的错误码却并不是那些制定好的错误
		// 暂时统一看作
		//keyError := &kvrpcpb.KeyError{
		//	Locked:               nil,
		//	Retryable:            "",
		//	Abort:                "",
		//	Conflict:             nil,
		//}

		pair := &kvrpcpb.KvPair{
			Error:                keyError,
			Key:                  k,
			Value:                v,
		}
		kvPairs = append(kvPairs, pair)
	}

	rsp := &kvrpcpb.RawScanResponse{
		RegionError:          nil,
		Error:                "",
		Kvs:                  kvPairs,
	}

	return rsp, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
