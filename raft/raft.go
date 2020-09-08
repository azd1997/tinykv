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
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// project2aa Leader election 涉及逻辑时钟/状态切换函数/消息发送/消息处理

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
// 进度。 Leader眼中各个Follower的日志进度
type Progress struct {
	Match, Next uint64
}

// Raft
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
	// 随机选举超时 [electionTimeout, 2 * electionTimeout - 1] 范围
	randomElectionTimeout int
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	// 验证配置有效
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	// 创建RaftLog实例
	raftLog := newLog(c.Storage)

	// 获取raftLog这种Storage的状态
	hs, cs, err := raftLog.storage.InitialState()
	if err != nil {
		return nil
	}

	//

	rf := &Raft{
		id:               c.ID,
		Term:             0,	//
		Vote:             0,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),	// Leader才需要维护Prs
		State:            StateFollower,
		votes:            make(map[uint64]bool),	// Candidate和Leader才需要
		msgs:             make([]pb.Message, 0),  // 长度多少
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,	// 心跳超时/周期
		electionTimeout:  c.ElectionTick,	// 选举超时
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}

	// 初始化rf.Prs
	// 根据集群中结点ID，为每个节点初始化Process
	// 在Process中维护了Match(MatchIndex)和Next(NextIndex)两个索引值
	// 只有Leader状态下，Prs才生效
	for _, p := range c.peers {
		rf.Prs[p] = &Progress{
			Match: ,
			Next:  1,
		}
	}

	//

	// 将节点切换为Follower
	rf.becomeFollower(rf.Term, rf.Lead)

	return nil
}


/////////////////////////////// 逻辑时钟 ///////////////////////////////

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).

	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

// tickElection 选举超时 逻辑时钟，每调用一次，刻度+1
// 适用于Follower/Candidate
func (r *Raft) tickElection() {
	r.electionElapsed++		// 逻辑时间加1
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0 	// 清零
		// 发送本地消息MsgHup，而后Step()会调用becomeCandidate()将状态切换为Candidate
		// 不在此处直接调用becomeCandidate的原因：TODO
		_ = r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
		})     // 本地消息，不需要通过msgs传递
	}
}

// tickHeartbeat 心跳时钟 每调用一次，刻度加1
// 适用于Leader
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		_ = r.Step(pb.Message{
			MsgType:              pb.MessageType_MsgBeat,
			From:                 r.id,
		})   // 本地消息，不需要通过msgs传递
	}
}

/////////////////////////////// 状态切换 ///////////////////////////////

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).

	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None	// 变为Follower并且此刻没有给任何人投票
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	r.State = StateCandidate
	r.Lead = None	// 变为候选者说明网络中没有leader
	r.Term++		// 任期加1
	r.Vote = r.id	// 给自己投票
	r.votes = make(map[uint64]bool)		// votes用于收集选票
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	r.State = StateLeader
	r.Lead = r.id	// 自己就是Leader
	lastIndex := r.RaftLog.LastIndex()	// 日志最后一条的索引
	r.heartbeatElapsed = 0	// 重置心跳逻辑时钟

	// 为所有集群节点更新Prs
	// 之所以都加1是因为自己成为Leader这个事情也是要广播出去的
	for peerId := range r.Prs {
		if peerId == r.id {
			r.Prs[peerId].Next = lastIndex + 2
			r.Prs[peerId].Match = lastIndex + 1
		} else {
			r.Prs[peerId].Next = lastIndex + 1
		}
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		EntryType:            pb.EntryType_EntryNormal,
		Term:                 r.Term,
		Index:                lastIndex + 1,
		Data:                 nil,
	})
	r.broadcastAppend()

	// 如果集群只有自己，直接将日志状态变为committed
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

/////////////////////////////// 消息处理 ///////////////////////////////

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).


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

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 转变为Candidate
		r.becomeCandidate()
		// 开始竞选
		r.doElection()
	}
}

func (r *Raft) stepCandidate(m pb.Message) error {

}

func (r *Raft) stepLeader(m pb.Message) error {

}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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


func (r *Raft) doElection() {
	// 一定得是Candidate才可以竞选，否则直接返回
	if r.State != StateCandidate {
		return
	}

	// 心跳时钟清零，重新设置选举超时
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	// 如果集群中只有自己，那么直接转为Leader，并返回
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	// 向其他节点发送请求投票
	r.broadcastRequestVote()
}

/////////////////////////////// 发送消息 ///////////////////////////////

// send 发送消息，只需将消息压入Raft.msgs队列即可
func (r *Raft) send(msg pb.Message) {
	r.msgs = append(r.msgs, msg)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// 向to发送AppendEntries消息。
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	// 获取本地日志中最新一条的索引
	lastIndex := r.RaftLog.LastIndex()

	ents := make([]*pb.Entry, 0)		// 待发送的Entries

	var logTerm, idx uint64

	if lastIndex < r.Prs[to].Next {		// 发现to的日志记录比自己新
		if lastIndex + 1 != r.Prs[to].Next {	// 本地的下一条依然不是to准备接受的下一个日志
			panic("[sendAppend] assertion failure")
			return false
		}
		logTerm = r.RaftLog.LastTerm()
		idx = lastIndex
	} else {	// 把日志记录中r.Prs[to].Next及往后的都收集到entries
		entries, err := r.RaftLog.Entries(r.Prs[to].Next)
		if err != nil {
			// log
			return false
		}
		idx = r.Prs[to].Next - 1
		logTerm, err = r.RaftLog.Term(idx)
		if err != nil {
			// log
			return false
		}
		for _, v := range entries {
			tmp := v
			ents = append(ents, &tmp)
		}
	}

	// 
	entry := pb.Entry{
		EntryType:            0,
		Term:                 0,
		Index:                0,
		Data:                 nil,
	}

	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).



}

// sendRequestVote 发送请求投票消息
func (r *Raft) sendRequestVote(to uint64) {
	m := pb.Message{
		MsgType:              pb.MessageType_MsgRequestVote,
		To:                   to,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              r.RaftLog.LastTerm(),
		Index:                r.RaftLog.LastIndex(),
	}
	r.send(m)
}

// broadcastRequestVote 向集群其他节点广播请求投票消息
func (r *Raft) broadcastRequestVote() {
	for peerId := range r.Prs {
		if peerId == r.id {
			continue
		}
		r.sendRequestVote(peerId)
	}
}