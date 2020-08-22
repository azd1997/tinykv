package standalone_storage

import (
	"errors"
	"fmt"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).

	// 单机存储是对BadgerDB API的包装，位于util/engine_util
	engine *engine_util.Engines
}

// NewStandAloneStorage 新建单机存储
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	// 单机存储的创建需先将底层badgerDB创建
	kv := engine_util.CreateDB(conf.DBPath, false)
	fakeraftDir := "/tmp/fakeraft"
	raft := engine_util.CreateDB(fakeraftDir, true)	// 单机版本中raft不需要，这里只是占位
	engine := engine_util.NewEngines(kv, raft, conf.DBPath, fakeraftDir)
	return &StandAloneStorage{
		engine: engine,
	}
}

// Start 启动
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	return nil
}

// Stop 停止
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).

	// 需要在停止时关闭底层的db
	err := s.engine.Close()
	if err != nil {
		return err
	}
	return nil
}

// Destroy 销毁数据库
// 不要在调Stop之后调Destroy，两者都会调用engine.Close()，会关闭badger中的关闭通知channel，不能重复关闭
func (s *StandAloneStorage) Destroy() error {
	return s.engine.Destroy()
}

// Reader 利用BadgerDB的事务特性获取快照数据
// 谨记: defer reader.Close()
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	// 构建事务
	txn := s.engine.Kv.NewTransaction(false)

	// 需要用一个struct实现storage.StorageReader接口，将内容填到其中
	sr := &storageReader{txn: txn}

	return sr, nil
}

// Write 批量写
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	// 构造WriteBatch
	wb := &engine_util.WriteBatch{}
	for _, modify := range batch {
		wb.SetCF(modify.Cf(), modify.Key(), modify.Value())
	}

	return s.engine.WriteKV(wb)
}

///////////////////////////////////////////////////////////////////////////////////////////
// StorageReader 存储快照读 实现

type storageReader struct {
	txn *badger.Txn
}

// GetCF 根据列族名和键名查询值。 ${cf}_${key}
func (r *storageReader) GetCF(cf string, key []byte) ([]byte, error) {
	if cf == "" || len(key) == 0 {
		return nil, errors.New("empty cf or key")
	}
	cfkey := []byte(fmt.Sprintf("%s_%s", cf, string(key)))
	item, err := r.txn.Get(cfkey)	// 注意：txn是badger的API，它没有列族的概念，因此一定是我们先拼完整键名
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	valueCopy, err := item.ValueCopy(nil)	// dst为nil或空间不足，会新键slice来存储，并返回。由于不清楚value长度，直接传nil
	if err != nil {
		return nil, err
	}
	return valueCopy, nil
}

// IterCF 迭代某个CF
func (r *storageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

// Close 关闭
func (r *storageReader) Close() {
	r.txn.Discard()
}
