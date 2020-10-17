package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf *config.Config
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	store := &StandAloneStorage{
		conf:   conf,
	}
	return store
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	dbPath := s.conf.DBPath

	kvPath := filepath.Join(dbPath, "kv")
	kvDB := engine_util.CreateDB("kv", s.conf)

	raftPath := filepath.Join(dbPath, "raft")
	raftDB := engine_util.CreateDB("raft", s.conf)

	s.engine = engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := StandAloneReader{store: s}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	for _, modify := range batch {
		key, val, cf := modify.Key(), modify.Value(), modify.Cf()
		wb.SetCF(cf, key, val)
	}
	if err := s.engine.WriteKV(wb); err != nil {
		return err
	}
	return nil
}

type StandAloneReader struct {
	store *StandAloneStorage
	txn *badger.Txn // 本 reader 的事务
}

// 如果没有查找到 return nil, nil
func (s StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	db := s.store.engine.Kv
	val, err := engine_util.GetCF(db, cf, key)
	if err != nil {
		return nil, nil
	}
	return val, nil
}
// 开始只读事务，返回迭代器
func (s StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	db := s.store.engine.Kv
	txn := db.NewTransaction(false) // false：只读事务
	s.txn = txn
	return engine_util.NewCFIterator(cf, txn)
}

func (s StandAloneReader) Close() {
	if s.txn != nil { // 如果存在事务
		s.txn.Discard() // 关闭事务
	}
}
