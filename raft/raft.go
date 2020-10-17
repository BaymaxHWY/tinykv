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
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// my define
	randomElectionTime int

	peers []uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	rf := &Raft{
		id:               c.ID,
		Term:             0,
		RaftLog:          newLog(c.Storage),
		Prs:              map[uint64]*Progress{},
		msgs:             []pb.Message{},
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		peers:            c.peers,
	}
	for _, id := range rf.peers {
		rf.Prs[id] = &Progress{}
	}
	rf.becomeFollower(0, None)
	rf.recoverRaft()
	return rf
}

// 恢复 term、committed、vote
func (r *Raft) recoverRaft() {
	state, _, _ := r.RaftLog.storage.InitialState()
	r.Term, r.RaftLog.committed, r.Vote = state.Term, state.Commit, state.Vote
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prs := r.Prs[to]
	logTerm, _ := r.RaftLog.Term(prs.Next - 1)
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgAppend,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  logTerm,
		Index:    prs.Next - 1,
		Entries:  []*pb.Entry{},
		Commit:   r.RaftLog.committed,
		Snapshot: nil,
	}
	for _, entry := range r.RaftLog.entries {
		newEntry := &pb.Entry{
			EntryType: entry.EntryType,
			Term:      entry.Term,
			Index:     entry.Index,
			Data:      entry.Data,
		}
		if entry.Index >= prs.Next {
			msg.Entries = append(msg.Entries, newEntry)
		}
	}
	r.msgs = append(r.msgs, msg)
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if to == r.id {
		return
	}
	prs := r.Prs[to]
	logTerm, _ := r.RaftLog.Term(prs.Next - 1)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   prs.Next - 1,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// election time
	r.electionElapsed++
	if r.State == StateLeader { // heart time
		r.heartbeatElapsed++
	}
	if r.passElectionTime() {
		// 提示节点发起 campaign
		r.electionElapsed = 0 // 重置
		if r.State != StateLeader {
			r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
		}
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout && r.State == StateLeader {
		// 提示 leader 发送心跳包
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

func (r *Raft) passElectionTime() bool {
	return r.electionElapsed >= r.randomElectionTime
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	if r.Term != term { // 只有到下一个 term 才重置 vote（保证每个 term 只投一次 vote）
		r.Term = term
		r.Vote = None
	}
	rand.Seed(time.Now().UnixNano())
	r.randomElectionTime = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes = map[uint64]bool{}
	r.votes[r.id] = true
	rand.Seed(time.Now().UnixNano())
	r.randomElectionTime = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.Prs = map[uint64]*Progress{}
	newEntry := pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1}
	for _, s := range r.peers {
		if s != r.id {
			r.Prs[s] = &Progress{
				Match: 0,
				Next:  newEntry.Index,
			}
		} else {
			r.Prs[s] = &Progress{
				Match: newEntry.Index,
				Next:  newEntry.Index + 1,
			}
		}
	}
	r.RaftLog.entries = append(r.RaftLog.entries, newEntry)
	r.AppendEntry() // 马上广播发送 append rpc，让之前 leader 可以 commit 但未 commit 的 log 快速 committed
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.MsgType != pb.MessageType_MsgHup && m.MsgType != pb.MessageType_MsgBeat && m.MsgType != pb.MessageType_MsgPropose { // 这三种 local msg 不需要处理 term
		// 统一处理 request term
		if m.Term < r.Term {
			msg := pb.Message{
				From:   r.id,
				To:     m.From,
				Reject: true,
				Term:   r.Term,
			}
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				msg.MsgType = pb.MessageType_MsgRequestVoteResponse
			case pb.MessageType_MsgHeartbeat:
				msg.MsgType = pb.MessageType_MsgHeartbeatResponse
			case pb.MessageType_MsgAppend:
				msg.MsgType = pb.MessageType_MsgAppendResponse
			}
			r.msgs = append(r.msgs, msg)
			return nil
		}
		if m.Term > r.Term { // 统一处理 msg 的 term > r.term，r 转为 follower
			r.becomeFollower(m.Term, None)
		}
	}
	// 进入这里的 msg 保证 term = r.term 或者不需要判断 term
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

// follower handler msgs
func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 发起选举
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		msg := pb.Message{
			From:    r.id,
			To:      m.From,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  true,
			Term:    r.Term,
		}
		// 只有vote = null 或者已经投过 sender 并且 log up-to-date 检查通过
		canVote := r.Vote == m.From || (r.Vote == None && r.Lead == None)
		if canVote && r.checkUpToDate(m) { // 需要保证一个 term 只投一次
			msg.Reject = false
			r.Vote = m.From
			r.electionElapsed = 0 // 重置 election time
		}
		r.msgs = append(r.msgs, msg)
	case pb.MessageType_MsgHeartbeat:
		// 设置 leader 编号，重置 election time
		r.Lead = m.From
		r.electionElapsed = 0
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		r.msgs = append(r.msgs, m)
	case pb.MessageType_MsgAppend:
		// 设置 leader 编号，重置 election time
		r.Lead = m.From
		r.electionElapsed = 0
		r.handleAppendEntries(m)
	}
	return nil
}

// candidate handler msgs
func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 发起选举
		r.campaign()
	case pb.MessageType_MsgRequestVote: // 一定要响应 requestVote（如果 term 相同就直接拒绝）
		msg := pb.Message{
			From:    r.id,
			To:      m.From,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  true,
			Term:    r.Term,
		}
		r.msgs = append(r.msgs, msg)
	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject
		if !m.Reject {
			if r.isCampaignWin() == 1 {
				r.becomeLeader()
			}
			break
		}
		// Reject = true
		if m.Term > r.Term || r.isCampaignWin() == -1 {
			r.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgAppend:
		// convert to follower
		r.becomeFollower(m.Term, None)
		return r.stepFollower(m)
	}
	return nil
}

// leader handler msgs
func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for _, id := range r.peers {
			if id != r.id {
				r.sendHeartbeat(id)
			}
		}
	case pb.MessageType_MsgRequestVote: // 一定要响应 requestVote（如果 term 相同就直接拒绝）
		msg := pb.Message{
			From:    r.id,
			To:      m.From,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  true,
			Term:    r.Term,
		}
		r.msgs = append(r.msgs, msg)
	case pb.MessageType_MsgHeartbeat:
	//	r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		if m.Reject && r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgPropose:
		for i := range m.Entries {
			m.Entries[i].Term = r.Term
			m.Entries[i].Index = r.RaftLog.LastIndex() + uint64(i) + 1
		}
		r.appendEntry(m.Entries)
		r.Prs[r.id].Match, r.Prs[r.id].Next = r.RaftLog.LastIndex(), r.RaftLog.LastIndex()+1
		r.AppendEntry()
	case pb.MessageType_MsgAppend:
		r.AppendEntry()
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	}
	return nil
}

// 检查传入参数 log 和节点 r 的 log up-to-date
// return true, 传入 log up-to-date
// return false, 节点 r 的 log up-to-date
func (r *Raft) checkUpToDate(m pb.Message) bool {
	if r.RaftLog.storage == nil {
		return true
	}
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	if lastTerm > m.LogTerm || (lastTerm == m.LogTerm && lastIndex > m.Index) {
		return false
	}
	return true
}

// candidate 发起竞选
func (r *Raft) campaign() {
	// 成为 Candidate
	r.becomeCandidate()
	// 特殊判断如果当前只有一个节点
	if r.isCampaignWin() == 1 {
		r.becomeLeader()
		return
	}
	// 向所有节点发送 RequestVote
	lastIndex := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(lastIndex)
	for _, s := range r.peers {
		if vote, ok := r.votes[s]; !ok || !vote {
			r.msgs = append(r.msgs, pb.Message{
				From:    r.id,
				To:      s,
				MsgType: pb.MessageType_MsgRequestVote,
				Term:    r.Term,
				LogTerm: logTerm,
				Index:   lastIndex,
			})
		}
	}
}

// 判断是否选举成功，return 1 选举成功；return -1 遭到大部分人拒绝选举失败；return 0 选举还没完成（没有收到所有 peer 的回复）
func (r *Raft) isCampaignWin() int {
	voteCnt, rejectCnt := 0, 0
	for _, vote := range r.votes {
		if vote {
			voteCnt++
		} else {
			rejectCnt++
		}
	}
	if voteCnt > len(r.peers)/2 {
		return 1
	}
	if rejectCnt > len(r.peers)/2 {
		return -1
	}
	return 0
}

// 将 command 加入 log
func (r *Raft) appendEntry(entries []*pb.Entry) {
	for _, entry := range entries {
		newEntry := pb.Entry{
			EntryType: entry.EntryType,
			Term:      entry.Term,
			Index:     entry.Index,
			Data:      entry.Data,
		}
		r.RaftLog.entries = append(r.RaftLog.entries, newEntry)
	}
}

// send log to peers
func (r *Raft) AppendEntry() {
	if len(r.peers) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	for _, to := range r.peers {
		if to != r.id {
			r.sendAppend(to)
		}
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	prs := r.Prs[m.From]
	if m.Reject {
		// handle reject
		prs.Next = prs.Next - 1
		r.sendAppend(m.From) // 失败后更新 next 重新向该节点发送 append rpc
	} else {
		prs.Next = m.Index + 1
		prs.Match = m.Index
		if r.maybeCommit() {
			r.AppendEntry() // 提交之后广播所有节点
		}
	}
}

func (r *Raft) maybeCommit() bool {
	newCommit := r.RaftLog.committed
	for i := newCommit + 1; i <= r.RaftLog.LastIndex(); i++ {
		term, _ := r.RaftLog.Term(i)
		if term == r.Term { // 只commit当前 term 的 log
			cnt := 1
			for id, prs := range r.Prs {
				if id != r.id && prs.Match >= i {
					cnt++
				}
			}
			if cnt <= len(r.peers)/2 {
				break
			}
			newCommit = i
		}
	}

	if newCommit > r.RaftLog.committed { // 判断是否有新的 commit
		r.RaftLog.committed = newCommit
		return true
	}
	return false
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}
	if term, err := r.RaftLog.Term(m.Index); err != nil || term != m.LogTerm {
		// reject
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	// append log
	for i, entry := range m.Entries {
		if term, err := r.RaftLog.Term(entry.Index); err != nil || term != entry.Term {
			if term != entry.Term && entry.Index <= r.RaftLog.LastIndex() {
				//r.RaftLog.entries = r.RaftLog.entries[:entry.Index]
				r.RaftLog.discard(entry.Index)
			}
			r.appendEntry(m.Entries[i:])
			break
		}
	}
	// etcd 中的 index of last new entry 的 index，如果 entry == nil 的话就是 m.Index
	lastIndex := m.Index + uint64(len(m.Entries))
	// update committed
	if m.Commit > r.RaftLog.committed {
		if m.Commit > lastIndex {
			r.RaftLog.committed = lastIndex
		} else {
			r.RaftLog.committed = m.Commit
		}
	}
	msg.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	}
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
