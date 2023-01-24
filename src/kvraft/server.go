package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

// const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Tp        string //请求类型
	Key       string
	Value     string
	ClerkId   int64
	CommandId int
}

type checkType struct {
	ClerkId   int64
	CommandId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	// Your definitions here.
	kvMap      map[string]string  //持久化
	clientCond map[int]*sync.Cond //logIndex->Op
	lastSolved map[int64]int      //每个client最后已处理的commandId,持久化
	checkApply map[int]checkType  //logIndex->(clerkId,commandId) 判断该index命令是否为初始命令（即有无发生领导者变更导致覆盖日志）
}

/*
	1.	在一个Loop里，全员监听一个指标，当这个指标发生变化时，会触发操作
	2.	只有Leader能够触发这个条件，follower不主动触发，等待被同步
	3.	条件被触发后，Leader判定条件是否准确，交由Raft特定的Op Log Entry; 根据需要设置WaitChan等待结果，同时设置Timeout
	4.	Raft将Leader Op 同步给大家，ApplyEntry给上层每个Server
	5.	Servers(包括Leader)读取Raft Apply Op 进行如下操作:
			1)	Dupulication Detection
			2)	执行Write，Update这种类型的操作，Read类型操作不执行
			3)	判定Snapshot条件，满足，则SnapShot
			4)	如果有WaitChan在等待，返回结果给WaitChan
	6.	Leader通过WaitChan接受结果，执行Read类型操作，返回结果给Client. 或者Leader WaitChan超时，返回Client结果。
*/

func (kv *KVServer) solve(_Key string, _Value string, _Op string, _ClerkId int64, _CommandId int) Err {
	kv.mu.Lock()

	if v, ok := kv.lastSolved[_ClerkId]; ok && v >= _CommandId {
		//重复的任务，已经解决了直接返回
		kv.mu.Unlock()
		return OK
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(Op{Tp: _Op,
		Key:       _Key,
		Value:     _Value,
		ClerkId:   _ClerkId,
		CommandId: _CommandId,
	})
	if !isLeader {
		return ErrWrongLeader
	}
	/*
		[Applying client operations]
		record where in the Raft log the client’s operation appears when you insert it.
	*/
	kv.mu.Lock()
	if _, ok := kv.clientCond[index]; !ok {
		kv.clientCond[index] = sync.NewCond(&sync.Mutex{})
	}
	cond := kv.clientCond[index]
	kv.mu.Unlock()
	//wait response
	cond.L.Lock()
	cond.Wait()
	cond.L.Unlock()

	/*
		[Applying client operations]
		you can tell whether or not the client’s operation succeeded based on
		whether the operation that came up for that index is in fact the one you put there.
	*/
	kv.mu.Lock()
	var err Err
	if kv.checkApply[index].ClerkId != _ClerkId || kv.checkApply[index].CommandId != _CommandId {
		/*
			[Applying client operations]
			If it isn’t, a failure has happened and
			an error can be returned to the client.
		*/
		err = ErrWrongLeader
	} else {
		_, ok := kv.kvMap[_Key]
		// DPrintf("solve:KVserver[%d] response client[%v]'s command[%v]", kv.me, _ClerkId, _CommandId)
		err = OK
		if !ok {
			err = ErrNoKey
		}

	}
	delete(kv.clientCond, index)
	delete(kv.checkApply, index)
	kv.mu.Unlock()
	return err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = kv.solve(args.Key, "", "Get", args.ClerkId, args.CommandId)
	if reply.Err == OK {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value = kv.kvMap[args.Key]
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = kv.solve(args.Key, args.Value, args.Op, args.ClerkId, args.CommandId)
}

func (kv *KVServer) ApplierMonitor() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.SnapshotValid {
			//从leader接收到快照，直接读取快照数据覆盖应用层
			kv.readSnapShot(msg.Snapshot)
			DPrintf("server[%v] receive snapshot", kv.me)
		}
		if msg.CommandValid {
			cmd := msg.Command.(Op)
			/*
				[Duplicate detection]
				server keeps track of the latest sequence number it has seen for each client,
				and simply ignores any operation that it has already seen.
			*/
			if !kv.checkIsDuplicate(cmd.ClerkId, cmd.CommandId) {
				if cmd.Tp == "Append" {
					kv.kvMap[cmd.Key] += cmd.Value
					// DPrintf("kv[%v] success Append client[%v]'s cmd[%v]", kv.me, cmd.ClerkId, cmd.CommandId)
				}
				if cmd.Tp == "Put" {
					kv.kvMap[cmd.Key] = cmd.Value
					// DPrintf("kv[%v] success Put client[%v]'s cmd[%v]", kv.me, cmd.ClerkId, cmd.CommandId)
				}
				kv.lastSolved[cmd.ClerkId] = cmd.CommandId
				// DPrintf("[ApplierMonitor]\tserver[%v]update clerkId:%v\tcommandId:%v", kv.me, cmd.ClerkId, cmd.CommandId)
			}
			/*
				compare maxraftstate to persister.RaftStateSize()
				it should save a snapshot by calling Raft's Snapshot
				检测raft日志量超过限制后向raft层发出快照命令
			*/
			if kv.maxraftstate != -1 && float64(kv.persister.RaftStateSize()) > 0.9*float64(kv.maxraftstate) {
				kv.saveSnapShot(msg.CommandIndex)
			}

			if cond, ok := kv.clientCond[msg.CommandIndex]; ok { //如果存在说明命令是在该server接收的
				kv.checkApply[msg.CommandIndex] = checkType{cmd.ClerkId, cmd.CommandId}
				cond.Broadcast()
			}

		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) checkIsDuplicate(clerkId int64, commandId int) bool {
	v, ok := kv.lastSolved[clerkId]
	// DPrintf("[checkIsDuplicate]\t%v,%v,%v,%v", clerkId, commandId, v, ok)
	if !ok {
		return false
	}
	return commandId <= v
}

func (kv *KVServer) saveSnapShot(cmdIdx int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.lastSolved)
	DPrintf("%v saveSnapshot:%v", kv.me, kv.kvMap)
	data := w.Bytes()
	kv.rf.Snapshot(cmdIdx, data)
}

func (kv *KVServer) readSnapShot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var xxx map[string]string
	var yyy map[int64]int
	if d.Decode(&xxx) != nil ||
		d.Decode(&yyy) != nil {
		DPrintf("readPersist error!")
	} else {
		kv.kvMap = xxx
		DPrintf("%v readSnapshot:%v", kv.me, kv.kvMap)
		kv.lastSolved = yyy
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := &KVServer{me: me,
		maxraftstate: maxraftstate,
		persister:    persister,
		applyCh:      make(chan raft.ApplyMsg),
		kvMap:        make(map[string]string),
		clientCond:   make(map[int]*sync.Cond),
		lastSolved:   make(map[int64]int),
		checkApply:   make(map[int]checkType),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readSnapShot(kv.persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.ApplierMonitor()

	return kv
}
