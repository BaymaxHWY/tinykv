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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

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
}
// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	rl := &RaftLog{
		storage:         storage,
	}
	rl.stabled, _ = storage.LastIndex()
	fIndex, _ := storage.FirstIndex()
	ents, err := storage.Entries(fIndex, rl.stabled + 1)
	if err == nil {
		rl.entries = make([]pb.Entry, len(ents))
		copy(rl.entries, ents)
	}
	return rl
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
	if len(l.entries) == 0 {
		return l.entries
	}
	offset := l.entries[0].Index
	ents := l.entries[l.stabled-offset+1:]
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return
	}
	offset := l.entries[0].Index
	ents = l.entries[l.applied-offset+1 : l.committed-offset+1]
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.lastIndex()
}

func (l *RaftLog) lastIndex() uint64 {
	index, _ := l.storage.LastIndex()
	if len(l.entries) == 0 {
		return index
	}
	return l.entries[len(l.entries) - 1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) == 0 { // 如果没有 raft 中还没有 log entry
		return l.storage.Term(i)
	}
	if i > l.lastIndex() {
		return 0, ErrUnavailable
	}
	offset := l.entries[0].Index
	if i < offset {
		return l.storage.Term(i)
	}
	return l.entries[i-offset].Term, nil
}
// 丢弃包含 i 在内往后的 log
func (l *RaftLog) discard(i uint64) {
	// 注意 log 为空的情况
	if len(l.entries) == 0 {
		return
	}
	offset := l.entries[0].Index
	l.entries = l.entries[0 : i-offset]
	if l.stabled >= i {
		l.stabled = i - 1
	}
}