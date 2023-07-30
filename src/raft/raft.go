package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// raft的状态
type raftState uint

const (
	FollowerState  raftState = 1
	CandidateState raftState = 2
	LeaderState    raftState = 3
)

// 用于标记尚未投票的状态
const NonVote int = -1

// 心跳周期
const HeartBeatPeriod = time.Millisecond * 150

// ticker睡眠时间
const SleepTime = time.Millisecond * 100

// 选举超时时间
const ElectionTimeOut = time.Millisecond * 250
const ElectionTimeInterval = 150

// 随机超时时间
func getRandTimeOut() time.Duration {
	return ElectionTimeOut + time.Duration(rand.Int()%ElectionTimeInterval)*time.Millisecond
}

// Entry结构体
type Entry struct {
	command interface{} // 提交给状态机的指令
	term    int         //leader接受该entry时的term
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     raftState
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int     // 这个server现在的term
	voteFor     int     // 把票投给谁了
	log         []Entry // logEntries

	// Volatile state on all servers:
	commitIndex int // 最大的已经commit的log entry的index
	lastApplied int // 最大的applied的log entry的index

	// Volatile state on leaders
	nextIndex  []int // 应该发送给每个server的下一条log entry的index
	matchIndex []int // 每个server的已经备份的最大的log entry的index

	timeOut time.Time //用于记录下一次选举超时时间
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LeaderState)
	return term, isleader
}

// 设置server状态，必须带锁调用
func (rf *Raft) setState(state raftState) {
	if state == FollowerState {
		rf.voteFor = NonVote
	}
	rf.state = state
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // 每次RPC返回前都需要持久化数据

	// 拒绝所有term小于自身的请求
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	// 对所有term大于自身的请求，更新自身的term，并变为follower状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.setState(FollowerState)
	}
	// 如果已经投票了, 直接返回
	if rf.voteFor != NonVote && rf.voteFor != args.CandidateId {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	// 如果candidate的log没自身的新，则拒绝投票
	// log 比较需要比较term和index
	if rf.log[len(rf.log)-1].term > args.LastLogTerm || len(rf.log)-1 > args.LastLogIndex {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	// 否则，投票给他
	rf.voteFor = args.CandidateId
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	// 重置选举超时时间
	rf.timeOut = time.Now().Add(getRandTimeOut())
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntry RPC 结构, 与论文figure2相同
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []int
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // 每次RPC返回前都要持久化数据

	// 拒绝所有term小于自身的请求
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	// 如果term大于自身，则更新自身，并变为follower状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.setState(FollowerState)
	}
	// 如果是心跳信号, 重置选举超时时间
	// 有可能是登基信号，所以同时重置自身状态放弃选举
	if len(args.Entries) == 0 {
		rf.timeOut = time.Now().Add(getRandTimeOut())
		rf.setState(FollowerState)
		return
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		// 如果不是leader且超时，则开启选举,并重置超时时间
		if rf.state != LeaderState && time.Now().After(rf.timeOut) {
			rf.timeOut = time.Now().Add(getRandTimeOut())
			rf.Election()
		}
		rf.mu.Unlock()
		// 睡一会待会再检查
		time.Sleep(SleepTime)
	}
}

// 广播心跳
func (rf *Raft) BroadcastHeartBeat() {
	for !rf.killed() {
		rf.mu.Lock()
		// 如果不是Leader了，返回
		if rf.state != LeaderState {
			rf.mu.Unlock()
			return
		}

		args := AppendEntryArgs{
			Term: rf.currentTerm,
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			reply := AppendEntryReply{}
			go func(server int) {
				// 如果发送失败了，不管他直接返回
				if !rf.sendAppendEntry(server, &args, &reply) {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 如果收到的term更大，则更新自己的term，并转换为follower
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.setState(FollowerState)
				}
			}(i)
		}
		rf.mu.Unlock()
		// 睡一会待会再发心跳。
		time.Sleep(HeartBeatPeriod)
	}
}

// 选举，带锁调用
func (rf *Raft) Election() {
	// 改变自身状态
	rf.setState(CandidateState)
	rf.currentTerm++
	rf.voteFor = rf.me
	// 用于记录票数和是否当选
	votes := 1
	done := false
	// 配置数据结构
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].term,
	}
	// 拉票
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := RequestVoteReply{}
		go func(server int) {

			// 如果RPC失败，直接返回
			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 如果已经不是candidate了， 或者是过期的返回结果, 则直接返回
			if rf.state != CandidateState || reply.Term < rf.currentTerm {
				return
			}
			// 如果返回的Term更大，则更新自身，并变为follower
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.setState(FollowerState)
				return
			}
			// 没得票直接返回
			if !reply.VoteGranted {
				return
			}
			// 记录得票
			votes++
			// 如果已经当选或票数仍然不够，则返回
			if done || votes <= len(rf.peers)/2 {
				return
			}
			// 成功当选，记录结果，并变为leader, 昭告天下
			// 单独开一个线程用于发送心跳
			done = true
			if rf.state == CandidateState {
				rf.setState(LeaderState)
				go rf.BroadcastHeartBeat()
			}
		}(i)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.dead = 0
	rf.setState(FollowerState)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.currentTerm = 0
	rf.log = make([]Entry, 1)
	rf.log[0] = Entry{nil, -1} // log下标从1开始，初始化时插入一个空entry，且将term设为-1，便于RPC时比较
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.timeOut = time.Now().Add(getRandTimeOut())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
