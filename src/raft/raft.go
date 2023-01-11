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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const FOLLOWER int = 0
const CANDIDATE int = 1
const LEADER int = 2
const HEARTSBEATS_INTERVAL int64 = 150

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//log entry
type Entry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on all servers
	currentTerm int     //服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	votedFor    int     //当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	log         []Entry //日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）
	//volatile state on all servers
	commitIndex int   //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int   //已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	state       int   //当前服务器的状态（follower,leader,candidate）
	lastTime    int64 //最后一次接收到心跳的时间
	//volatile state on leaders
	nextIndex  []int //对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex []int //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// // Your code here (2A).
	// return term, isleader
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

/******************AppendEntries RPC*******************************/

//AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int     //领导人的任期
	LeaderId     int     //领导人 ID 因此跟随者可以对客户端进行重定向
	PreLogIndex  int     //紧邻新日志条目之前的那个日志条目的索引
	PreLogTerm   int     //紧邻新日志条目之前的那个日志条目的任期
	Entries      []Entry //需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int     //领导人的已知已提交的最高的日志条目的索引
}

//AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int  //当前任期，对于领导人而言 它会更新自己的任期
	Success bool //如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//1
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastTime = time.Now().UnixMilli()

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	//2
	// args.

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderHeartBeat() {
	for rf.state == LEADER {
		args := AppendEntriesArgs{Term: rf.currentTerm,
			LeaderId:     rf.me,
			PreLogIndex:  len(rf.log) - 1,
			PreLogTerm:   rf.log[len(rf.log)-1].Term,
			Entries:      make([]Entry, 0),
			LeaderCommit: rf.commitIndex,
		}
		for peer := range rf.peers {
			go func(peer int) {
				if peer == rf.me {
					return
				}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(peer, &args, &reply)
				if ok {
					if reply.Success {
						log.Printf("Peer[%d] success transmit heartbeat to Peer[%d]", rf.me, peer)
					}
					if !reply.Success && reply.Term > rf.currentTerm {
						rf.state = FOLLOWER
						log.Printf("find Term bigger then Peer[%d],became follower", rf.me)
						return
					}
				} else {
					log.Printf("Peer[%d] can't receive leader[%d]'s heartbeat", peer, rf.me)
				}

			}(peer)
		}
		time.Sleep(time.Duration(HEARTSBEATS_INTERVAL) * time.Millisecond)
	}
}

/******************RequestVote RPC*******************************/
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期号
	CandidateID  int //请求选票的候选人的ID
	LastLogIndex int //候选人的最后日志条目的索引值
	LastLogTerm  int //候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		lastLogTerm := rf.log[len(rf.log)-1].Term
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogIndex && len(rf.log)-1 <= args.LastLogIndex) {
			rf.state = FOLLOWER
			rf.votedFor = args.CandidateID
			rf.lastTime = time.Now().UnixMilli()
			reply.VoteGranted = true
		}
	}
	rf.mu.Unlock()

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.currentTerm++

	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	cnt := 1      //接收到选票数
	finished := 1 //接收到总响应数

	args := RequestVoteArgs{Term: rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term}

	for peer := range rf.peers {
		if rf.state == CANDIDATE {
			go func(peer int) {
				if peer == rf.me {
					return
				}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer, &args, &reply)
				if ok {
					mu.Lock()
					if reply.VoteGranted {
						log.Printf("Peer[%d] voted Peer[%d]", peer, rf.me)
						cnt++
					} else {
						if reply.Term > rf.currentTerm {
							rf.state = FOLLOWER
							mu.Unlock()
							log.Printf("find Term bigger then Peer[%d],failed to become leader", rf.me)
							return
						}
					}
					finished++
					mu.Unlock()
					cond.Broadcast()
				} else {
					log.Printf("call peer[%d] failed", peer)
				}
			}(peer)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	for finished != len(rf.peers) && cnt*2 < len(rf.peers) {
		cond.Wait()
	}

	if cnt*2 >= len(rf.peers) && rf.state == CANDIDATE {
		rf.state = LEADER
		log.Printf("Peer[%d] become new leader", rf.me)
		go rf.leaderHeartBeat()
	} else {
		log.Printf("Peer[%d] failed to become leader", rf.me)
	}

}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
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
		r := rand.New(rand.NewSource(time.Now().UnixMicro()))
		electionTimeOut := r.Int63()%(600-450) + 450 //随机产生的选举超时时间 450ms<= x <=1000ms
		time.Sleep(time.Duration(electionTimeOut) * time.Millisecond)

		rf.mu.Lock()
		// log.Printf("%d %d %d", rf.me, electionTimeOut, time.Now().UnixMilli()-rf.lastTime)
		if electionTimeOut+rf.lastTime <= time.Now().UnixMilli() && rf.state != LEADER {
			log.Printf("Peer[%d] try to become leader", rf.me)
			go rf.startElection()
		}
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = FOLLOWER
	rf.lastTime = time.Now().UnixMilli()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	log.Printf("raftNode[%d] start", rf.me)
	return rf
}
