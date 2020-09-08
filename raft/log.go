// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).

	// offset 指所存记录的第一条的索引(firstIndex)，故称偏移量
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex + 1)		// [low, high)
	if err != nil {
		panic(err)
	}

	log.Infof("offset(first): %d stabled(last): %d, entries: %v", firstIndex, lastIndex, entries)

	return &RaftLog{
		storage:         storage,
		applied:         firstIndex-1,
		stabled:         lastIndex,
		entries:         entries,
		offset:firstIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).

	// 所有entry均已stabled (之所以把这项单列出来，是因为调用l.Entries()有的错误可以返回空entry数组而不报错，而有的必须报错，比如storage出问题了)
	if l.stabled + 1 > l.LastIndex() {
		return make([]pb.Entry, 0)
	}
	// 取所有unstable的entry
	ents, err := l.Entries(l.stabled + 1, l.committed + 1)
	if err != nil {
		log.Error(err)
		return nil
	}
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).

	// 这样的查询最好返回拷贝，不要使用l.entries内原空间
	// 调用l.Entries()获得的是拷贝，不影响l.entries

	// 所有entry均已apply (之所以把这项单列出来，是因为调用l.Entries()有的错误可以返回空entry数组而不报错，而有的必须报错，比如storage出问题了)
	if l.applied + 1 > l.LastIndex() {
		return make([]pb.Entry, 0)
	}
	// 取所有已committed但没applied的entry
	var err error
	ents, err = l.Entries(l.applied + 1, l.committed + 1)
	if err != nil {
		log.Error(err)
		return nil
	}
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).

	// l.entries是否非空
	if len(l.entries) > 0 {
		return l.offset + uint64(len(l.entries)) - 1
	}
	// l.entries为空
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		log.Panic(err)
		return 0
	}
	return lastIndex
}

// LastTerm 返回日志序列最后一条日志所属的任期Term
func (l *RaftLog) LastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		return 0
	}
	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).

	if i > l.LastIndex() {	// 不存在
		return 0, nil
	}
	if len(l.entries) > 0 && i >= l.offset {	// 待查询的entry在l.entries中
		return l.entries[i-l.offset].Term, nil
	}
	// 在storage中
	return l.storage.Term(i)
}

// Entries 按照[start, lastIndex+1)或者[start, end)范围读取
func (l *RaftLog) Entries(start uint64, end ...uint64) ([]pb.Entry, error) {
	lastIndex := l.LastIndex()
	if len(end) > 1 {
		return nil, fmt.Errorf("wrong query arguments. len(end)(%d) should be 0 or 1", len(end))
	}

	// [left, right)
	left, right := start, lastIndex + 1
	if len(end) == 1 {
		right = end[0]
	}

	// 检查left/right有效性
	if left > right {
		return nil, fmt.Errorf("wrong query arguments. left(%d) should <= right(%d)", left, right)
	}
	if left > lastIndex || right > lastIndex + 1 {
		return nil, fmt.Errorf("wrong query arguments. left(%d)/right(%d) should <= lastIndex(%d)", left, right, lastIndex)
	}
	if left == right {
		return nil, nil
	}

	// 读取数据
	if len(l.entries) > 0 {		// RaftLog内有未压缩的日志记录，先在里边找，没有的就还得从l.storage找
		var ents []pb.Entry
		// 要注意检查left/right/offset三者的关系
		// offset之后的是可以直接在RaftLog.entries取到
		// 但之前的则需要从RaftLog.storage取
		if left < l.offset {
			storageEntries, err := l.storage.Entries(left, min(l.offset, right))
			if err != nil {
				return nil, err
			}
			ents = storageEntries
		}
		if right > l.offset {
			memEntries := l.entries[max(left, l.offset) - l.offset : right - l.offset]
			if len(ents) > 0 {
				ents = append(ents, memEntries...)
			} else {
				ents = append([]pb.Entry{}, memEntries...)
			}
			return ents, nil
		}
	} else {	// len(l.entries) == 0
		storageEntries, err := l.storage.Entries(left, min(l.offset, right))
		if err != nil {
			return nil, err
		}
		return storageEntries, nil
	}
	return nil, nil
}
