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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const (
	FOLLOWER             int           = 0
	CANDIDATE            int           = 1
	LEADER               int           = 2
	HEARTSBEATS_INTERVAL time.Duration = time.Duration(150) * time.Millisecond
	TIMEINF              time.Duration = time.Duration(100000) * time.Hour
)

func getRandTimeout() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixMicro()))
	electionTimeOut := r.Int63()%(600-450) + 450 // 随机产生的选举超时时间 450ms<= x<=600ms
	return time.Duration(electionTimeOut) * time.Millisecond
}

func (rf *Raft) switchState(state int) {
	defer rf.persist()
	// 不加这段会有bug，原因未知，猜测计时器奇怪的原因
	if state == rf.state {
		if state == FOLLOWER {
			rf.votedFor = -1
		}
		return
	}

	// 任期改变转换角色，votefor只有任期改变才初始化，保证一个任期只投一票

	rf.state = state
	switch state {
	case FOLLOWER:
		rf.votedFor = -1
		rf.heartBeatTimer.Stop()
		rf.electionTimer.Reset(getRandTimeout())
	case CANDIDATE:
		rf.heartBeatTimer.Stop()
	case LEADER:
		rf.heartBeatTimer.Reset(HEARTSBEATS_INTERVAL)
		rf.electionTimer.Stop()
		for peer := range rf.peers {
			rf.nextIndex[peer] = rf.getRealLen()
			rf.matchIndex[peer] = 0
		}
		// DPrintf("peer[%v] become leader,len:%v,currentTerm%v\n", rf.me, rf.getRealLen(), rf.currentTerm)
	}
}

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
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// log entry
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

	//单调递增的属性用max来保证

	//所有服务器上的持久性状态
	currentTerm int     //服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	votedFor    int     //当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	log         []Entry //日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）
	snapshot    Snapshot
	//所有服务器上的易失性状态
	commitIndex   int         //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied   int         //已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	state         int         //当前服务器的状态（follower,leader,candidate）
	electionTimer *time.Timer //选举超时定时器
	//领导人（服务器）上的易失性状态
	heartBeatTimer *time.Timer //发送心跳的定时器
	nextIndex      []int       //对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex     []int       //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshot)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var xxx int
	var yyy int
	var zzz []Entry
	var ppp Snapshot
	if d.Decode(&xxx) != nil ||
		d.Decode(&yyy) != nil ||
		d.Decode(&zzz) != nil ||
		d.Decode(&ppp) != nil {
		DPrintf("readPersist error!")
	} else {
		rf.currentTerm = xxx
		rf.votedFor = yyy
		rf.log = zzz
		rf.snapshot = ppp
	}
}

/******************InstallSnapshot RPC*******************************/
type InstallSnapshotArgs struct {
	Term              int    //领导人的任期号
	LeaderId          int    //领导人的 ID，以便于跟随者重定向请求
	LastIncludedIndex int    //快照中包含的最后日志条目的索引值
	LastIncludedTerm  int    //快照中包含的最后日志条目的任期号
	Data              []byte //快照的原始字节
}

type InstallSnapshotReply struct {
	Term int //当前任期号（currentTerm），便于领导人更新自己
}

type Snapshot struct {
	LastIncludedIndex int //被快照取代的最后的条目在日志中的索引
	LastIncludedTerm  int //被快照取代的最后的条目的任期号
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	rf.electionTimer.Reset(getRandTimeout())
	if rf.snapshot.LastIncludedIndex >= args.LastIncludedIndex {
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.switchState(FOLLOWER)
	}
	reply.Term = rf.currentTerm

	// 砍去快照包含的log部分
	if args.LastIncludedIndex >= rf.getRealLastIdx() {
		rf.log = []Entry{{Term: 0}}
	} else {
		idx := rf.real2Fake(args.LastIncludedIndex)
		tmplog := make([]Entry, rf.getFakeLastIdx()-idx)
		copy(tmplog, rf.log[idx+1:])
		rf.log = append([]Entry{{Term: 0}}, tmplog...)
	}
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.snapshot.LastIncludedIndex = args.LastIncludedIndex
	rf.snapshot.LastIncludedTerm = args.LastIncludedTerm
	rf.persist()

	// 协程防止程序堵塞
	go func() {
		msg := ApplyMsg{CommandValid: false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex}
		rf.applyCh <- msg
	}()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
}

func (rf *Raft) InstallSnapshotEntries(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) SendInstallSnapshotEntries(peer int) {
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{Term: rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshot.LastIncludedIndex,
		LastIncludedTerm:  rf.snapshot.LastIncludedTerm,
		Data:              rf.persister.ReadSnapshot()}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := rf.InstallSnapshotEntries(peer, &args, &reply)
	if !ok {
		// DPrintf("leader[%v] call peer[%d] failed while sending Snapshot\n", rf.me, peer)
		return
	}

	rf.mu.Lock()
	// DPrintf("leader[%v] deliver to peer[%v] ->%v\n", rf.me, peer, rf.snapshot.LastIncludedIndex)
	defer rf.mu.Unlock()

	if rf.state != LEADER || rf.currentTerm != args.Term || rf.snapshot.LastIncludedIndex != args.LastIncludedIndex {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.switchState(FOLLOWER)
		return
	}

	rf.nextIndex[peer] = max(rf.nextIndex[peer], rf.snapshot.LastIncludedIndex+1)
	rf.matchIndex[peer] = max(rf.matchIndex[peer], rf.snapshot.LastIncludedIndex)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	//you are discouraged from implementing it: instead, we suggest that you simply have it return true.
	//不需要实现
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//快照点若还未应用到状态机会导致日志丢失（这种情况应该不会发生,应用层收到了日志之后才发出snapshot命令）
	//已经快照过则无需快照
	if index > rf.commitIndex || index <= rf.snapshot.LastIncludedIndex {
		return
	}
	// DPrintf("peer[%v] Snapshot ,range->%v\n", rf.me, index)
	//index是realIdx,需转换
	idx := rf.real2Fake(index)
	rf.snapshot.LastIncludedIndex = index
	rf.snapshot.LastIncludedTerm = rf.log[idx].Term
	//砍log
	if idx > rf.getFakeLastIdx() {
		rf.log = []Entry{{Term: 0}}
	} else {
		tmplog := make([]Entry, rf.getFakeLastIdx()-idx)
		copy(tmplog, rf.log[idx+1:])
		rf.log = append([]Entry{{Term: 0}}, tmplog...)
	}
	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)

	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
}

//0作为哨兵不删一直保留，快照中只有[1,rf.snapshot.LastIncludeIndex]
func (rf *Raft) getFakeLen() int { //不含快照 含0的总长度
	return len(rf.log)
}

func (rf *Raft) getRealLen() int { //含快照 含0的总长度
	return rf.snapshot.LastIncludedIndex + rf.getFakeLen()
}

func (rf *Raft) getFakeLastIdx() int { //不含快照的下标
	return rf.getFakeLen() - 1
}

func (rf *Raft) getRealLastIdx() int { //含快照的真实下标
	return rf.getRealLen() - 1
}

func (rf *Raft) fake2Real(idx int) int { //算上快照的下标
	if idx == 0 {
		return 0
	}
	return idx + rf.snapshot.LastIncludedIndex
}

func (rf *Raft) real2Fake(idx int) int { //不算快照的下标
	if idx == 0 {
		return 0
	}
	return idx - rf.snapshot.LastIncludedIndex
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
	Term          int  //当前任期，对于领导人而言 它会更新自己的任期
	Success       bool //如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	ConflictTerm  int  //Follower中与Leader冲突的Log对应的任期号,如果Follower在对应位置没有Log，那么这里会返回 -1
	ConflictIndex int  //Follower中，对应任期号为XTerm的第一条Log条目的槽位号,XTerm为-1时返回日志总长度
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//1 如果领导人的任期小于接收者的当前任期,返回假
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.electionTimer.Reset(getRandTimeout())
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.switchState(FOLLOWER)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	/*	2
		If a follower does not have prevLogIndex in its log,
		it should return with conflictIndex = len(log) and conflictTerm = None.
	*/
	if rf.getRealLastIdx() < args.PreLogIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.getRealLen()
		return
	}
	/*	3
		If a follower does have prevLogIndex in its log, but the term does not match,
		it should return conflictTerm = log[prevLogIndex].Term,
		and then search its log for the first index whose entry has term equal to conflictTerm.
	*/
	preLogIndex := rf.real2Fake(args.PreLogIndex)
	if preLogIndex > 0 && rf.log[preLogIndex].Term != args.PreLogTerm { //找到第一个和冲突日期相同的位置
		reply.Success = false
		reply.ConflictTerm = rf.log[preLogIndex].Term
		reply.ConflictIndex = preLogIndex
		for reply.ConflictTerm == rf.log[reply.ConflictIndex-1].Term {
			reply.ConflictIndex--
		}
		return
	} //preLogIndex<=0说明已经生成快照了，则一定匹配了

	/*	4
		删除冲突的条目以及它之后的所有条目，追加日志中尚未存在的任何新条目
		-----------------------------------------------------------------
		rf.log := append(rf.log[:args.PreLogIndex+1], args.Entries...)错误
		可能因为消息延迟导致新的RPC比老的RPC先到达，
		若新的RPC日志长度变长，老的RPC会把新的RPC日志给覆盖掉，返回时会把matchIndex变小导致错误
		正确做法先找到不匹配位置再覆盖
	*/
	conflictIdx := -1
	for idx := range args.Entries {
		if preLogIndex+idx+1 <= 0 {
			continue
		}
		if rf.getFakeLastIdx() < preLogIndex+idx+1 || rf.log[preLogIndex+idx+1] != args.Entries[idx] {
			conflictIdx = idx
			break
		}
	}
	// DPrintf("id:%v\tpre:\t%v", rf.me, len(rf.log))
	if conflictIdx != -1 {
		tmplog := make([]Entry, preLogIndex+conflictIdx+1)
		copy(tmplog, rf.log[:preLogIndex+conflictIdx+1])
		rf.log = append(tmplog, args.Entries[conflictIdx:]...)
	}
	// DPrintf("id:%v\taft:\t%v", rf.me, len(rf.log))

	/*	5
		If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	*/
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getRealLastIdx())
		rf.updateCommit()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderAppendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		/*
			发送RPC前检查还是否为leader
			发送RPC后检查发送和接受RPC期间leader状态是否发生改变
		*/
		go func(peer int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			/*
				1.	args.PreLogIndex < rf.snapshot.LastIncludedIndex
				-> 	rf.nextIndex[peer] <= rf.snapshot.LastIncludedIndex
				->	log已经删除，则直接发送快照

				2.	args.PreLogIndex >= rf.snapshot.LastIncludedIndex
				->	rf.nextIndex[peer] > rf.snapshot.LastIncludedIndex
				->	log未删除,发送附加日志
			*/
			if rf.nextIndex[peer]-1 < rf.snapshot.LastIncludedIndex {
				rf.SendInstallSnapshotEntries(peer)
				return
			}

			args := AppendEntriesArgs{Term: rf.currentTerm,
				LeaderId:     rf.me,
				PreLogIndex:  rf.nextIndex[peer] - 1,
				PreLogTerm:   rf.log[rf.real2Fake(rf.nextIndex[peer]-1)].Term,
				LeaderCommit: rf.commitIndex,
			}

			if args.PreLogTerm == 0 {
				args.PreLogTerm = rf.snapshot.LastIncludedTerm
			}

			/*
				1.	rf.nextIndex[rf.me] = rf.nextIndex[peer] : 发送心跳
				2.	rf.nextIndex[rf.me] > rf.nextIndex[peer] : 发送附加条目
			*/
			args.Entries = make([]Entry, rf.nextIndex[rf.me]-rf.nextIndex[peer])
			copy(args.Entries, rf.log[rf.real2Fake(rf.nextIndex[peer]):rf.real2Fake(rf.nextIndex[rf.me])])

			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if !ok {
				// DPrintf("leaderAppendEntries : leader[%v] call peer[%d] failed", rf.me, peer)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != LEADER || rf.currentTerm != args.Term || rf.nextIndex[peer]-1 != args.PreLogIndex {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.switchState(FOLLOWER)
				// log.Printf("find Term bigger then Peer[%d],failed to become leader", rf.me)
				return
			}
			if reply.Success {
				if rf.nextIndex[peer] != rf.nextIndex[rf.me] {
					// log.Printf("Peer[%d] success copy log[%d:%d] from leader[%d]", peer, rf.nextIndex[peer], rf.nextIndex[rf.me]-1, rf.me)
				}
				// DPrintf("leader[%v]'s matchIdx[%v]: %v -> %v\n", rf.me, peer, rf.matchIndex[peer], max(rf.matchIndex[peer], args.PreLogIndex+len(args.Entries)))
				rf.nextIndex[peer] = max(rf.nextIndex[peer], args.PreLogIndex+len(args.Entries)+1)
				rf.matchIndex[peer] = max(rf.matchIndex[peer], args.PreLogIndex+len(args.Entries))
				/*
					假设存在 N 满足N > commitIndex
					使得大多数的 matchIndex[i] ≥ N 以及log[N].term ==currentTerm 成立，则令 commitIndex = N
				*/
				for N := rf.getFakeLastIdx(); N > max(0, rf.real2Fake(rf.commitIndex)); N-- {
					if rf.log[N].Term != rf.currentTerm { //leader 只能提交当前 term 的日志，不能提交旧 term 的日志
						break
					}
					cnt := 0
					for peer := range rf.peers {
						if rf.matchIndex[peer] >= rf.fake2Real(N) {
							cnt++
						}
					}
					if 2*cnt >= len(rf.peers) {
						rf.commitIndex = max(rf.commitIndex, rf.fake2Real(N))
						rf.updateCommit()
						break
					}
				}

			} else {
				rf.nextIndex[peer] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					//找到leader日志中和conflictTerm相同的最后一条日志
					for idx := rf.real2Fake(rf.nextIndex[peer]); idx > 0; idx-- {
						if rf.log[idx].Term != reply.ConflictTerm && rf.log[idx-1].Term == reply.ConflictTerm {
							rf.nextIndex[peer] = rf.fake2Real(idx)
							break
						}
						if rf.log[idx].Term < reply.ConflictTerm {
							break
						}
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) updateCommit() {
	lastApplied := max(rf.lastApplied, rf.snapshot.LastIncludedIndex)
	if rf.commitIndex > lastApplied {
		base := rf.real2Fake(lastApplied)
		entries := make([]Entry, rf.real2Fake(rf.commitIndex+1)-(base+1))
		copy(entries, rf.log[base+1:rf.real2Fake(rf.commitIndex+1)])
		// DPrintf("Peer[%d] start commit to client,range[%v,%v]\n", rf.me, lastApplied, rf.commitIndex)
		go func() {
			for idx, entry := range entries {
				msg := ApplyMsg{CommandValid: true,
					Command:       entry.Command,
					CommandIndex:  lastApplied + idx + 1,
					CommandTerm:   entry.Term,
					SnapshotValid: false,
				}
				rf.mu.Lock()
				rf.lastApplied = max(rf.lastApplied, msg.CommandIndex)
				rf.mu.Unlock()
				rf.applyCh <- msg
				// DPrintf("Peer[%d] success commit log[%d] %v to client", rf.me, lastApplied+idx+1, msg.Command)
			}
		}()

	}
	// fmt.Println(rf.me, rf.log, rf.commitIndex)

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
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.switchState(FOLLOWER)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		lastLogTerm := rf.log[rf.getFakeLastIdx()].Term
		if lastLogTerm == 0 {
			lastLogTerm = rf.snapshot.LastIncludedTerm
		}
		/*
			If votedFor is null or candidateId,
			and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		*/
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && rf.getRealLastIdx() <= args.LastLogIndex) {
			rf.switchState(FOLLOWER)
			rf.votedFor = args.CandidateID
			//reset election timer when you grant a vote to another peer.
			//this case is especially important in unreliable networks where it is likely that followers have different logs;
			rf.electionTimer.Reset(getRandTimeout())
			reply.VoteGranted = true
		}
	}
	rf.persist()

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
	rf.currentTerm++
	rf.votedFor = rf.me
	// DPrintf("Peer[%d] try to become leader,votefor:%v,currentTerm:%v\n", rf.me, rf.votedFor, rf.currentTerm)
	rf.electionTimer.Reset(getRandTimeout())
	args := RequestVoteArgs{Term: rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.getRealLastIdx(),
		LastLogTerm:  rf.log[rf.getFakeLastIdx()].Term}
	if args.LastLogTerm == 0 {
		args.LastLogTerm = rf.snapshot.LastIncludedTerm
	}
	rf.persist()

	mu := sync.Mutex{}
	cnt := 1 //接收到选票数

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			if rf.state != CANDIDATE {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)
			if !ok {
				// DPrintf("startElection : peer[%d] call peer[%d] failed", rf.me, peer)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != CANDIDATE || rf.currentTerm != args.Term {
				return
			}

			if reply.VoteGranted {
				// DPrintf("Peer[%d] voted Peer[%d]", peer, rf.me)
				mu.Lock()
				cnt++
				if cnt*2 >= len(rf.peers) && rf.state == CANDIDATE {
					rf.switchState(LEADER)
					rf.heartBeatTimer.Reset(10 * time.Millisecond)
				}
				mu.Unlock()
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.switchState(FOLLOWER)
					// DPrintf("find Term bigger then Peer[%d],failed to become leader", rf.me)
				}
			}

		}(peer)
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

	// Your code here (2B).
	rf.mu.Lock()
	index := rf.nextIndex[rf.me]
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if isLeader {
		rf.log = append(rf.log, Entry{Command: command, Term: rf.currentTerm})
		rf.nextIndex[rf.me] = rf.getRealLastIdx() + 1
		rf.matchIndex[rf.me] = rf.getRealLastIdx()
		rf.heartBeatTimer.Reset(time.Millisecond * 10)
		rf.persist()
		// DPrintf("add new log %d to leader[%d]'s log[%d],nextIndex:%v\n", command, rf.me, index, rf.nextIndex[rf.me])
	}
	rf.mu.Unlock()

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

		//rf.leaderAppendEntries()和rf.startElection()不能协程，不然会有奇怪的选举bug
		select {
		case <-rf.heartBeatTimer.C: //leader
			rf.mu.Lock()
			rf.leaderAppendEntries()
			rf.heartBeatTimer.Reset(HEARTSBEATS_INTERVAL)
			rf.mu.Unlock()

		case <-rf.electionTimer.C: //follower or candidate
			rf.mu.Lock()
			if rf.state == FOLLOWER {
				rf.switchState(CANDIDATE)
			}
			rf.startElection()
			rf.mu.Unlock()
		}

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
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		currentTerm:    0,
		votedFor:       -1,
		log:            make([]Entry, 0),
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		state:          FOLLOWER,
		electionTimer:  time.NewTimer(getRandTimeout()),
		heartBeatTimer: time.NewTimer(TIMEINF),
		applyCh:        applyCh,
		snapshot:       Snapshot{LastIncludedIndex: 0, LastIncludedTerm: 0},
	}
	rf.log = append(rf.log, Entry{Term: 0})
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
