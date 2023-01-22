package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

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
	ServerId  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap      map[string]string
	clientCh   map[int]chan Op //logIndex->Op
	lastSolved map[int64]int   //每个client最后已处理的commandId
}

type OptionArgs struct {
	Key       string
	Value     string
	Op        string
	ClerkId   int64
	CommandId int
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

func (kv *KVServer) solve(args *OptionArgs) Err {
	kv.mu.Lock()
	index, curTerm, isLeader := kv.rf.Start(Op{Tp: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
		ServerId:  kv.me,
	})
	if !isLeader {
		defer kv.mu.Unlock()
		return ErrWrongLeader
	}
	/*
		[Applying client operations]
		record where in the Raft log the client’s operation appears when you insert it.
	*/
	if _, ok := kv.clientCh[index]; !ok {
		kv.clientCh[index] = make(chan Op)
	}
	kv.mu.Unlock()

	msg, err := kv.waitResponse(curTerm, index)

	/*
		[Applying client operations]
		you can tell whether or not the client’s operation succeeded based on
		whether the operation that came up for that index is in fact the one you put there.
	*/

	kv.mu.Lock()
	if msg.ClerkId != args.ClerkId || msg.CommandId != args.CommandId {
		/*
			[Applying client operations]
			If it isn’t, a failure has happened and
			an error can be returned to the client.
		*/
		err = ErrWrongLeader
	} else {
		_, ok := kv.kvMap[args.Key]
		// DPrintf("Get:KVserver[%d] response client[%v]'s command[%v]", kv.me, args.ClerkId, args.CommandId)
		if !ok {
			err = ErrNoKey
		}
	}
	delete(kv.clientCh, index)
	kv.mu.Unlock()
	return err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	index, curTerm, isLeader := kv.rf.Start(Op{Tp: "Get",
		Key:       args.Key,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
		ServerId:  kv.me,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	/*
		[Applying client operations]
		record where in the Raft log the client’s operation appears when you insert it.
	*/
	if _, ok := kv.clientCh[index]; !ok {
		kv.clientCh[index] = make(chan Op)
	}
	kv.mu.Unlock()

	msg, err := kv.waitResponse(curTerm, index)
	reply.Err = err
	if err == ErrWrongLeader {
		return
	}
	/*
		[Applying client operations]
		you can tell whether or not the client’s operation succeeded based on
		whether the operation that came up for that index is in fact the one you put there.
	*/

	if msg.ClerkId != args.ClerkId || msg.CommandId != args.CommandId {
		/*
			[Applying client operations]
			If it isn’t, a failure has happened and
			an error can be returned to the client.
		*/
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	v, ok := kv.kvMap[args.Key]
	// DPrintf("Get:KVserver[%d] response client[%v]'s command[%v]", kv.me, args.ClerkId, args.CommandId)
	if !ok {
		reply.Err = ErrNoKey
		return
	}
	reply.Value = v
	kv.mu.Unlock()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	index, curTerm, isLeader := kv.rf.Start(Op{Tp: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
		ServerId:  kv.me,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	/*
		[Applying client operations]
		record where in the Raft log the client’s operation appears when you insert it.
	*/
	if _, ok := kv.clientCh[index]; !ok {
		kv.clientCh[index] = make(chan Op)
	}
	kv.mu.Unlock()
	msg, err := kv.waitResponse(curTerm, index)
	reply.Err = err
	if err == ErrWrongLeader {
		return
	}
	/*
		[Applying client operations]
		you can tell whether or not the client’s operation succeeded based on
		whether the operation that came up for that index is in fact the one you put there.
	*/
	if msg.ClerkId != args.ClerkId || msg.CommandId != args.CommandId {
		/*
			[Applying client operations]
			If it isn’t, a failure has happened and
			an error can be returned to the client.
		*/
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) waitResponse(originTerm int, logIndex int) (Op, Err) {
	timeCounter := time.NewTimer(500 * time.Millisecond)
	for {
		select {
		case res := <-kv.clientCh[logIndex]:
			return res, OK
		case <-timeCounter.C:
			nowTerm, isLeader := kv.rf.GetState()
			if nowTerm != originTerm || !isLeader {
				return Op{}, ErrWrongLeader
			}
			DPrintf("[waitResponse]server[%v]'s logIndex[%v] timeout,reset", kv.me, logIndex)
			timeCounter.Reset(500 * time.Millisecond)
		}
	}
}

func (kv *KVServer) responseWait(logIndex int, msg Op) {
	timeCounter := time.NewTimer(200 * time.Millisecond)
	select {
	case kv.clientCh[logIndex] <- msg:
		return
	case <-timeCounter.C:
		return
	}
}

func (kv *KVServer) ApplierMonitor() {
	for !kv.killed() {
		tmp := <-kv.applyCh
		if tmp.SnapshotValid {
			// kv.readPersist(tmp.Snapshot)
			continue
		}

		if tmp.CommandValid {
			kv.mu.Lock()
			cmd := tmp.Command.(Op)
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
				DPrintf("[ApplierMonitor]\tserver[%v]update clerkId:%v\tcommandId:%v", kv.me, cmd.ClerkId, cmd.CommandId)
			}
			kv.mu.Unlock()

			_, isleader := kv.rf.GetState()
			if isleader && cmd.ServerId == kv.me {
				kv.responseWait(tmp.CommandIndex, cmd)
			}

		}

	}
}

func (kv *KVServer) checkIsDuplicate(clerkId int64, commandId int) bool {
	v, ok := kv.lastSolved[clerkId]
	DPrintf("[checkIsDuplicate]\t%v,%v,%v,%v", clerkId, commandId, v, ok)
	if !ok {
		return false
	}
	return commandId <= v
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
		applyCh:      make(chan raft.ApplyMsg),
		kvMap:        make(map[string]string),
		clientCh:     make(map[int]chan Op),
		lastSolved:   make(map[int64]int),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.ApplierMonitor()

	return kv
}
