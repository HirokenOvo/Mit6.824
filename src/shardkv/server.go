package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	NShards           = 10
	Serving           = "Serving"
	Migrating         = "Migrating"
	WaitMigration     = "WaitMigration"
	GarbageCollecting = "GarbageCollecting"
)

// 服务器自身的控制流 包含updateConfig、pushMigration、pullMigration
type ControlFlow struct {
	NeConfig shardctrler.Config //updateConfig
	//Migration
	ShardMp    map[string]string
	Idx        int
	LastSolved map[int64]int
}

// 客户端发来的数据流 包含put、append、get
type DataFlow struct {
	Key       string
	Value     string
	Shard     int
	ClerkId   int64
	CommandId int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	FlowType    string
	OptionType  string
	DataFlow    DataFlow
	ControlFlow ControlFlow
	Term        int
}

type CheckType struct {
	ClerkId   int64
	CommandId int
}

type Shard struct {
	KvMap map[string]string
	/*
		Serving           拥有该分片且正提供服务				  return OK
		Migrating         拥有该分片但正在迁移,不提供读写服务		return ErrWrongGroup
		WaitMigration     即将拥有该分片						return ErrWaitMigrate
		GarbageCollecting 垃圾回收,等待清除该分片数据			  return ErrWrongGroup
	*/
	ShardStatus string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	sm           *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	nowConfig    shardctrler.Config // 持久化
	stateMachine map[int]*Shard     // 持久化
	//	DataFlow用
	clientCond map[int]*sync.Cond // logIndex->Op
	lastSolved map[int64]int      // 每个client最后已处理的commandId,持久化
	checkApply map[int]CheckType  // logIndex->(clerkId,commandId) 判断该index命令是否为初始命令（即有无发生领导者变更导致覆盖日志）
}

// DataFlow的模板启动函数
func (kv *ShardKV) templateDataFlowStart(data DataFlow, op string, term int) (Err, string) {
	kv.mu.Lock()
	if v, ok := kv.lastSolved[data.ClerkId]; ok && v >= data.CommandId {
		//	重复的任务，已经解决了直接返回
		defer kv.mu.Unlock()
		return OK, ""
	}

	if term != kv.nowConfig.Num {
		//	配置号不同，则组别错误
		defer kv.mu.Unlock()
		return ErrWrongGroup, ""
	}

	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{
		FlowType:   "DataFlow",
		OptionType: op,
		DataFlow:   data,
		Term:       term,
	})
	if !isLeader {
		return ErrWrongLeader, ""
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
	var ans string
	if kv.checkApply[index].ClerkId != data.ClerkId || kv.checkApply[index].CommandId != data.CommandId {
		/*
			[Applying client operations]
			If it isn’t, a failure has happened and
			an error can be returned to the client.
		*/
		err = ErrWrongLeader
	} else if kv.nowConfig.Num != term {
		//	配置号不同，则组别错误
		err = ErrWrongGroup
	} else {
		err = OK
		// 不在Serving模式
		if shard, ok := kv.stateMachine[data.Shard]; ok {
			switch shard.ShardStatus {
			case Migrating:
				err = ErrWrongGroup
			case WaitMigration:
				DPrintf("gid[%v]kv[%v]shard[%v]waitMigration.......................", kv.gid, kv.me, data.Shard)
				err = ErrWaitMigrate
			}
		} else {
			err = ErrWrongGroup
		}
		if op == "Get" && err == OK {
			var ok bool
			if ans, ok = kv.stateMachine[data.Shard].KvMap[data.Key]; !ok {
				err = ErrNoKey
			}
			// DPrintf("solve:KVserver[%d] response client[%v]'s command[%v]", kv.me, _ClerkId, _CommandId)
			// DPrintf("gid[%v]kv[%v]shard[%v]Get***********************", kv.gid, kv.me, data.Shard)
		}

	}
	delete(kv.clientCond, index)
	delete(kv.checkApply, index)
	kv.mu.Unlock()
	return err, ans
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// DPrintf("gid[%v]kv[%v]%v", kv.gid, kv.me, args)
	arg := DataFlow{
		Key:       args.Key,
		Value:     "",
		Shard:     args.Shard,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
	}
	reply.Err, reply.Value = kv.templateDataFlowStart(arg, "Get", args.Term)
	// DPrintf("gid[%v]kv[%v]%v\n%v", kv.gid, kv.me, reply, kv.stateMachine)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// DPrintf("gid[%v]kv[%v]%v", kv.gid, kv.me, args)
	arg := DataFlow{
		Key:       args.Key,
		Value:     args.Value,
		Shard:     args.Shard,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
	}
	reply.Err, _ = kv.templateDataFlowStart(arg, args.Op, args.Term)
	// DPrintf("gid[%v]kv[%v]%v", kv.gid, kv.me, reply)
}

func (kv *ShardKV) ApplierMonitor() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.SnapshotValid {
			// 从leader接收到快照，直接读取快照数据覆盖应用层
			kv.readSnapShot(msg.Snapshot)
			DPrintf("gid[%v]server[%v] receive snapshot", kv.gid, kv.me)
		}
		if msg.CommandValid {
			Op := msg.Command.(Op)
			switch Op.FlowType {
			case "ControlFlow":
				cmd := Op.ControlFlow
				switch Op.OptionType {
				case "updateConfig":
					if cmd.NeConfig.Num == kv.nowConfig.Num+1 {
						for i := 0; i < 10; i++ {
							//	除了config[1]是直接Serving,其他config的分片都是通过迁移达成的
							if cmd.NeConfig.Num == 1 {
								if cmd.NeConfig.Shards[i] == kv.gid {
									kv.stateMachine[i] = &Shard{KvMap: make(map[string]string), ShardStatus: Serving}
									if _, isleader := kv.rf.GetState(); isleader {
										DPrintf("gid[%v]kv[%v]shard[%v]init", kv.gid, kv.me, i)
									}
								}
							} else {
								if _, ok := kv.stateMachine[i]; !ok && cmd.NeConfig.Shards[i] == kv.gid && kv.nowConfig.Shards[i] != kv.gid {
									kv.stateMachine[i] = &Shard{KvMap: make(map[string]string), ShardStatus: WaitMigration}
								}
								if cmd.NeConfig.Shards[i] != kv.gid && kv.nowConfig.Shards[i] == kv.gid {
									kv.stateMachine[i].ShardStatus = Migrating
									if _, isleader := kv.rf.GetState(); isleader {
										DPrintf("config[%v]gid[%v]kv[%v]shard[%v]pushMigration\nmp:%v", kv.nowConfig.Num, kv.gid, kv.me, i, kv.stateMachine[i].KvMap)
									}
								}

							}
						}
						kv.nowConfig = cmd.NeConfig
						if _, isleader := kv.rf.GetState(); isleader {
							DPrintf("gid[%v]kv[%v]updateConfig->%v", kv.gid, kv.me, kv.nowConfig.Num)
							for idx, shard := range kv.stateMachine {
								DPrintf("%v:::%v", idx, shard.ShardStatus)
							}
						}

					}
				case "pullMigration":
					// if _, isleader := kv.rf.GetState(); isleader {
					// 	DPrintf("gid[%v]kv[%v]shard[%v]Op.Term%v::::nowConfig%v", kv.gid, kv.me, cmd.Idx, Op.Term, kv.nowConfig.Num)
					// }
					if Op.Term == kv.nowConfig.Num {
						// if _, ok := kv.stateMachine[cmd.Idx]; !ok {
						if _, ok := kv.stateMachine[cmd.Idx]; !ok || kv.stateMachine[cmd.Idx].ShardStatus == WaitMigration {
							kv.stateMachine[cmd.Idx] = &Shard{KvMap: cmd.ShardMp, ShardStatus: Serving}
							for k, v := range cmd.LastSolved {
								kv.lastSolved[k] = max(kv.lastSolved[k], v)
							}
							if _, isleader := kv.rf.GetState(); isleader {
								DPrintf("config[%v]gid[%v]kv[%v]shard[%v]success pull", kv.nowConfig.Num, kv.gid, kv.me, cmd.Idx)
								for idx, shard := range kv.stateMachine {
									DPrintf("%v:::%v", idx, shard.ShardStatus)
								}
							}
						}
					}
				case "garbageCollect":
					if Op.Term == kv.nowConfig.Num {
						delete(kv.stateMachine, cmd.Idx)
						if _, isleader := kv.rf.GetState(); isleader {
							DPrintf("config[%v]gid[%v]kv[%v]shard[%v]success delete", kv.nowConfig.Num, kv.gid, kv.me, cmd.Idx)
							for idx, shard := range kv.stateMachine {
								DPrintf("%v:::%v", idx, shard.ShardStatus)
							}
						}
					}
				}

			case "DataFlow":
				cmd := Op.DataFlow
				/*
					[Duplicate detection]
					server keeps track of the latest sequence number it has seen for each client,
					and simply ignores any operation that it has already seen.
				*/
				if !kv.checkIsDuplicate(cmd.ClerkId, cmd.CommandId) && Op.Term == kv.nowConfig.Num {
					f := true
					switch Op.OptionType {
					case "Append":
						if shard, ok := kv.stateMachine[cmd.Shard]; ok && shard.ShardStatus == Serving {
							shard.KvMap[cmd.Key] += cmd.Value
							// DPrintf("gid[%v]kv[%v] success Append client[%v]'s cmd[%v],shard[%v],%v+=%v,now:%v", kv.gid, kv.me, cmd.ClerkId, cmd.CommandId, cmd.Shard, cmd.Key, cmd.Value, shard.KvMap[cmd.Key])
						} else {
							f = false
						}
					case "Put":
						if shard, ok := kv.stateMachine[cmd.Shard]; ok && shard.ShardStatus == Serving {
							shard.KvMap[cmd.Key] = cmd.Value
							// DPrintf("gid[%v]kv[%v] success Put client[%v]'s cmd[%v],shard[%v],%v=%v,now:%v", kv.gid, kv.me, cmd.ClerkId, cmd.CommandId, cmd.Shard, cmd.Key, cmd.Value, shard.KvMap[cmd.Key])
						} else {
							f = false
						}
					case "Get":
						if shard, ok := kv.stateMachine[cmd.Shard]; ok && shard.ShardStatus == Serving {
						} else {
							f = false
						}
					}
					if f {
						kv.lastSolved[cmd.ClerkId] = cmd.CommandId
					}
				}
				if cond, ok := kv.clientCond[msg.CommandIndex]; ok { // 如果存在说明命令是在该server接收的
					kv.checkApply[msg.CommandIndex] = CheckType{cmd.ClerkId, cmd.CommandId}
					cond.Broadcast()
				}
			}
			/*
				compare maxraftstate to persister.RaftStateSize()
				it should save a snapshot by calling Raft's Snapshot
				检测raft日志量超过限制后向raft层发出快照命令
			*/
			if kv.maxraftstate != -1 && float64(kv.persister.RaftStateSize()) > 0.9*float64(kv.maxraftstate) {
				kv.saveSnapShot(msg.CommandIndex)
			}
		}
		kv.mu.Unlock()
	}
}

// 检查请求是否重复
func (kv *ShardKV) checkIsDuplicate(clerkId int64, commandId int) bool {
	v, ok := kv.lastSolved[clerkId]
	if !ok {
		return false
	}
	return commandId <= v
}

func (kv *ShardKV) saveSnapShot(cmdIdx int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastSolved)
	e.Encode(kv.nowConfig)
	data := w.Bytes()
	kv.rf.Snapshot(cmdIdx, data)
}

func (kv *ShardKV) readSnapShot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var xxx map[int]*Shard
	var yyy map[int64]int
	var zzz shardctrler.Config
	if d.Decode(&xxx) != nil ||
		d.Decode(&yyy) != nil ||
		d.Decode(&zzz) != nil {
		// DPrintf("readPersist error!")
	} else {
		kv.stateMachine = xxx
		kv.lastSolved = yyy
		kv.nowConfig = zzz
	}
}

//	分片回收
func (kv *ShardKV) garbageCollect() {
	for !kv.killed() {
		_, isleader := kv.rf.GetState()
		if !isleader {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		for sid, shard := range kv.stateMachine {
			if shard.ShardStatus == GarbageCollecting {
				kv.rf.Start(Op{
					FlowType:    "ControlFlow",
					OptionType:  "garbageCollect",
					ControlFlow: ControlFlow{Idx: sid},
					Term:        kv.nowConfig.Num,
				})
			}
		}
		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

}

//	接收分片
func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.stateMachine[args.Idx]; ok && kv.stateMachine[args.Idx].ShardStatus == Serving {
		reply.Err = OK
		return
	}
	kv.rf.Start(Op{
		FlowType:    "ControlFlow",
		OptionType:  "pullMigration",
		ControlFlow: ControlFlow{ShardMp: args.Shard, Idx: args.Idx, LastSolved: args.LastSolved},
		Term:        args.Term,
	})
}

// 协程检查migrating状态分片并上传
func (kv *ShardKV) pushMigration() {
	for !kv.killed() {
		_, isleader := kv.rf.GetState()
		if !isleader {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		for sid, shard := range kv.stateMachine {
			if shard.ShardStatus == Migrating {
				tmp := make(map[string]string)
				for k, v := range shard.KvMap {
					tmp[k] = v
				}
				gid := kv.nowConfig.Shards[sid]
				if servers, ok := kv.nowConfig.Groups[gid]; ok {
					for si := 0; si < len(servers); si++ {
						go func(si int, sid int, tmp map[string]string, term int) {
							srv := kv.make_end(servers[si])
							// DPrintf("---------sid%v::gid%v", sid, gid)
							// DPrintf("---------kv.nowConfig num%v::g%v", kv.nowConfig.Num, kv.nowConfig.Groups)
							kv.mu.Lock()
							lastsolved := make(map[int64]int, 0)
							for k, v := range kv.lastSolved {
								lastsolved[k] = v
							}
							args := MigrateArgs{
								Idx:        sid,
								Shard:      tmp,
								Term:       term,
								LastSolved: lastsolved,
							}
							kv.mu.Unlock()
							reply := MigrateReply{}
							ok := srv.Call("ShardKV.Migrate", &args, &reply)
							if !ok {
								return
							}
							if reply.Err == OK {
								kv.mu.Lock()
								defer kv.mu.Unlock()
								if _, ok := kv.stateMachine[sid]; ok {
									kv.stateMachine[sid].ShardStatus = GarbageCollecting
								}
							}
						}(si, sid, tmp, kv.nowConfig.Num)
					}
				}
			}
		}

		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

// 协程检查是否可更新配置
func (kv *ShardKV) checkConfig() {
	for !kv.killed() {
		canUpdate := true
		_, isleader := kv.rf.GetState()
		if !isleader {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		for i := 0; i < 10; i++ {
			if kv.nowConfig.Shards[i] == kv.gid { //目标分片由该复制组管理,但不存在或不正在服务
				if shard, ok := kv.stateMachine[i]; !ok || shard.ShardStatus != Serving {
					// DPrintf("gid[%v]kv[%v]config[%v]  !ok || shard.ShardStatus != Serving\n%v\n%v:%v", kv.gid, kv.me, kv.nowConfig.Num, kv.stateMachine, i, kv.stateMachine[i].ShardStatus)
					canUpdate = false
					break
				}
			} else { // 目标分片不由该复制组管理，但仍存在
				if _, ok := kv.stateMachine[i]; ok {
					canUpdate = false
					// DPrintf("gid[%v]kv[%v]config[%v]ok \n%v\n%v:%v ", kv.gid, kv.me, kv.nowConfig.Num, kv.stateMachine, i, kv.stateMachine[i].ShardStatus)
					break
				}
			}
		}

		if canUpdate {
			neConfig := kv.sm.Query(kv.nowConfig.Num + 1)
			if neConfig.Num == kv.nowConfig.Num+1 {
				DPrintf("\nconfig[%v]:%v------------\nconfig[%v]:%v------------", kv.nowConfig.Num, kv.nowConfig.Gid2Shards, neConfig.Num, neConfig.Gid2Shards)
				kv.rf.Start(Op{
					FlowType:    "ControlFlow",
					OptionType:  "updateConfig",
					ControlFlow: ControlFlow{NeConfig: neConfig},
				})
			}
		}

		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ControlFlow{})
	labgob.Register(DataFlow{})
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})
	labgob.Register(Shard{})
	kv := &ShardKV{
		me:           me,
		applyCh:      make(chan raft.ApplyMsg),
		make_end:     make_end,
		gid:          gid,
		sm:           shardctrler.MakeClerk(ctrlers),
		maxraftstate: maxraftstate,
		persister:    persister,

		stateMachine: make(map[int]*Shard),
		clientCond:   make(map[int]*sync.Cond),
		lastSolved:   make(map[int64]int),
		checkApply:   make(map[int]CheckType),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.nowConfig = kv.sm.Query(0)
	kv.readSnapShot(kv.persister.ReadSnapshot())
	DPrintf("gid[%v]restart...........", kv.gid)

	// Your initialization code here.
	go kv.checkConfig()
	go kv.ApplierMonitor()
	go kv.pushMigration()
	go kv.garbageCollect()

	return kv
}
