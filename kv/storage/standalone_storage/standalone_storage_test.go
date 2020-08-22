package standalone_storage_test

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/standalone_storage"
)

func TestStandaloneStorage(t *testing.T) {
	// 新建
	conf := config.NewTestConfig()
	ss := standalone_storage.NewStandAloneStorage(conf)
	err := ss.Start()
	require.Nil(t, err)
	defer func() {
		ss.Destroy()
	}()

	// 批量写
	batch := make([]storage.Modify, 4)
	batch[0] = storage.Modify{Data: storage.Put{Key: []byte("eiger"), Value: []byte("23"), Cf: "test"}}
	batch[1] = storage.Modify{Data: storage.Put{Key: []byte("eiger1997"), Value: []byte("1997"), Cf: "test"}}
	batch[2] = storage.Modify{Data: storage.Put{Key: []byte("eiger2222"), Value: []byte("25"), Cf: "test"}}
	batch[3] = storage.Modify{Data: storage.Delete{Key: []byte("eiger2222"), Cf: "test"}}
	err = ss.Write(&kvrpcpb.Context{}, batch)
	require.Nil(t, err)

	// 快照读
	reader, err := ss.Reader(&kvrpcpb.Context{})
	require.Nil(t, err)
	defer reader.Close()
	// 按键读
	age, err := reader.GetCF("test", []byte("eiger"))
	require.Nil(t, err)
	require.Equal(t, []byte("23"), age)
	t.Log(string(age))
	// 迭代读
	iter := reader.IterCF("test")
	defer iter.Close()
	for iter.(*engine_util.BadgerIterator).Rewind(); iter.Valid(); iter.Next() {
		t.Log(iter.Item())		// item只包括键信息
		v, err := iter.Item().ValueCopy(nil)
		require.Nil(t, err)
		t.Log(string(v))
	}
}
