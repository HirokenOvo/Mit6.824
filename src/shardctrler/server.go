package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type checkType struct {
	ClerkId   int64
	CommandId int
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.
	configs    []Config           // indexed by config num
	clientCond map[int]*sync.Cond //logIndex->Op
	lastSolved map[int64]int      //每个client最后已处理的commandId,持久化
	checkApply map[int]checkType  //logIndex->(clerkId,commandId) 判断该index命令是否为初始命令（即有无发生领导者变更导致覆盖日志）

}

type Op struct {
	// Your data here.
	ClerkId   int64
	CommandId int
	Tp        string
	JoinCmd   JoinArgs
	LeaveCmd  LeaveArgs
	MoveCmd   MoveArgs
	QueryCmd  QueryArgs
}

func (sc *ShardCtrler) solve(_Tp string, _CommandId int, _ClerkId int64, _JoinCmd JoinArgs, _LeaveCmd LeaveArgs, _MoveCmd MoveArgs, _QueryCmd QueryArgs) bool {
	sc.mu.Lock()

	if v, ok := sc.lastSolved[_ClerkId]; ok && v >= _CommandId {
		//重复的任务，已经解决了直接返回
		sc.mu.Unlock()
		return false
	}
	sc.mu.Unlock()
	_Op := Op{Tp: _Tp,
		ClerkId:   _ClerkId,
		CommandId: _CommandId,
		JoinCmd:   _JoinCmd,
		LeaveCmd:  _LeaveCmd,
		MoveCmd:   _MoveCmd,
		QueryCmd:  _QueryCmd,
	}
	index, _, isLeader := sc.rf.Start(_Op)
	if !isLeader {
		return true
	}
	/*
		[Applying client operations]
		record where in the Raft log the client’s operation appears when you insert it.
	*/
	sc.mu.Lock()
	if _, ok := sc.clientCond[index]; !ok {
		sc.clientCond[index] = sync.NewCond(&sync.Mutex{})
	}
	cond := sc.clientCond[index]
	sc.mu.Unlock()
	//wait response
	cond.L.Lock()
	cond.Wait()
	cond.L.Unlock()

	/*
		[Applying client operations]
		you can tell whether or not the client’s operation succeeded based on
		whether the operation that came up for that index is in fact the one you put there.
	*/
	sc.mu.Lock()
	WrongLeader := false
	if sc.checkApply[index].ClerkId != _ClerkId || sc.checkApply[index].CommandId != _CommandId {
		/*
			[Applying client operations]
			If it isn’t, a failure has happened and
			an error can be returned to the client.
		*/
		WrongLeader = true
	}
	delete(sc.clientCond, index)
	delete(sc.checkApply, index)
	sc.mu.Unlock()
	return WrongLeader
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	arg := JoinArgs{
		Servers:   args.Servers,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	reply.WrongLeader = sc.solve("Join", args.CommandId, args.ClientId, arg, LeaveArgs{}, MoveArgs{}, QueryArgs{})

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	arg := LeaveArgs{
		GIDs:      args.GIDs,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	reply.WrongLeader = sc.solve("Leave", args.CommandId, args.ClientId, JoinArgs{}, arg, MoveArgs{}, QueryArgs{})

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	arg := MoveArgs{
		Shard:     args.Shard,
		GID:       args.GID,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	reply.WrongLeader = sc.solve("Move", args.CommandId, args.ClientId, JoinArgs{}, LeaveArgs{}, arg, QueryArgs{})
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	arg := QueryArgs{
		Num:       args.Num,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	reply.WrongLeader = sc.solve("Query", args.CommandId, args.ClientId, JoinArgs{}, LeaveArgs{}, MoveArgs{}, arg)
	sc.mu.Lock()
	if !reply.WrongLeader {
		reply.Config = sc.GetConfig(args.Num)
		// raft.DPrintf("Query%v:%v\n", arg.Num, reply.Config.Gid2Shards)
		// for i, s := range sc.configs {
		// 	raft.DPrintf("%v:%v", i, s.Gid2Shards)
		// }
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) GetConfig(idx int) Config {
	if idx == -1 || idx >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[idx]
}

func getShards(tar int, origin [NShards]int) []int {
	ans := make([]int, 0)
	for i := 0; i < NShards; i++ {
		if origin[i] == tar {
			ans = append(ans, i)
		}
	}
	return ans
}

func (sc *ShardCtrler) ReBanlance(lastConfig *Config) {
	nGid := len(lastConfig.Groups)
	if nGid == 0 {
		return
	}
	avg := NShards / nGid
	res := NShards % nGid
	//Go的map遍历顺序不确定,将已有的gid排序后确定遍历顺序使得所有副本的分片一致
	idxGid := make([]int, 0)
	for gid := range lastConfig.Groups {
		idxGid = append(idxGid, gid)
	}

	sort.Ints(idxGid)

	for _, gid := range idxGid {
		tar := avg
		shards := getShards(gid, lastConfig.Shards)
		if res > 0 {
			tar++
		}
		if len(shards) > tar {
			if res > 0 {
				res--
			}
			for len(shards) > tar {
				lastConfig.Shards[shards[len(shards)-1]] = 0
				shards = shards[:len(shards)-1]
			}
			lastConfig.Gid2Shards[gid] = shards
		}
	}

	bucket := make([]int, 0)
	for i := 0; i < 10; i++ {
		if lastConfig.Shards[i] == 0 {
			bucket = append(bucket, i)
		}
	}

	for _, gid := range idxGid {
		shards := getShards(gid, lastConfig.Shards)
		for len(shards) < avg && len(bucket) > 0 {
			shards = append(shards, bucket[len(bucket)-1])
			lastConfig.Shards[bucket[len(bucket)-1]] = gid
			bucket = bucket[:len(bucket)-1]
		}
		lastConfig.Gid2Shards[gid] = shards
	}
}

// 深复制旧配置到新配置中
func (sc *ShardCtrler) Copy(now *Config, pre *Config) {
	now.Groups = make(map[int][]string)
	now.Gid2Shards = make(map[int][]int)

	for i := 0; i < 10; i++ {
		now.Shards[i] = pre.Shards[i]
	}

	for gid, server := range pre.Groups {
		now.Groups[gid] = server
	}

	for gid, shards := range pre.Gid2Shards {
		now.Gid2Shards[gid] = shards
	}

	now.Num = len(sc.configs)
}

func (sc *ShardCtrler) ApplierMonitor() {
	for !sc.killed() {
		msg := <-sc.applyCh
		sc.mu.Lock()
		if msg.CommandValid {
			cmd := msg.Command.(Op)

			if !sc.CheckIsDuplicate(cmd.ClerkId, cmd.CommandId) {
				cfg := Config{}
				lastConfig := sc.GetConfig(-1)
				sc.Copy(&cfg, &lastConfig)
				// Join(servers) -- add a set of groups (gid -> server-list mapping).
				// 添加新组
				if cmd.Tp == "Join" {
					cmd := cmd.JoinCmd

					for gid, server := range cmd.Servers {
						cfg.Groups[gid] = server
						cfg.Gid2Shards[gid] = make([]int, 0)
					}

					sc.ReBanlance(&cfg)
					// raft.DPrintf("newconfig[%v]:%v", cfg.Num, cfg.Gid2Shards)
					sc.configs = append(sc.configs, cfg)
					// for i, s := range sc.configs {
					// 	raft.DPrintf("%v:%v", i, s.Gid2Shards)
					// }
				}
				// Leave(gids) -- delete a set of groups.
				// 删除指定组,组内的分片定义为未分配,rebanlance重新分配
				if cmd.Tp == "Leave" {
					cmd := cmd.LeaveCmd

					for _, leaveGid := range cmd.GIDs {
						for _, shard := range cfg.Gid2Shards[leaveGid] {
							cfg.Shards[shard] = 0
						}
						delete(cfg.Gid2Shards, leaveGid)
						delete(cfg.Groups, leaveGid)
					}

					sc.ReBanlance(&cfg)
					// raft.DPrintf("newconfig[%v]:%v", cfg.Num, cfg.Gid2Shards)
					sc.configs = append(sc.configs, cfg)
					// for i, s := range sc.configs {
					// 	raft.DPrintf("%v:%v\t%v", i, s.Gid2Shards, s.Shards)
					// }
				}
				// Move(shard, gid) -- hand off one shard from current owner to gid.
				if cmd.Tp == "Move" {
					cmd := cmd.MoveCmd
					pre := lastConfig.Shards[cmd.Shard]
					for idx, shard := range cfg.Gid2Shards[pre] {
						if shard == cmd.Shard {
							cfg.Gid2Shards[pre] = append(cfg.Gid2Shards[pre][:idx], cfg.Gid2Shards[pre][idx+1:]...)
							break
						}
					}
					cfg.Gid2Shards[cmd.GID] = append(cfg.Gid2Shards[cmd.GID], cmd.Shard)
					cfg.Shards[cmd.Shard] = cmd.GID
					// raft.DPrintf("newconfig[%v]:%v", cfg.Num, cfg.Gid2Shards)
					sc.configs = append(sc.configs, cfg)
				}
				// Query(num) -> fetch Config # num, or latest config if num==-1.
				if cmd.Tp == "Query" {
					//查询操作不需要修改
				}
				sc.lastSolved[cmd.ClerkId] = cmd.CommandId
			}

			if cond, ok := sc.clientCond[msg.CommandIndex]; ok { //如果存在说明命令是在该server接收的
				sc.checkApply[msg.CommandIndex] = checkType{cmd.ClerkId, cmd.CommandId}
				cond.Broadcast()
			}

		}

		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) CheckIsDuplicate(clerkId int64, commandId int) bool {
	v, ok := sc.lastSolved[clerkId]
	if !ok {
		return false
	}
	return commandId <= v
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := &ShardCtrler{
		me:         me,
		configs:    make([]Config, 1),
		applyCh:    make(chan raft.ApplyMsg),
		clientCond: make(map[int]*sync.Cond),
		lastSolved: make(map[int64]int),
		checkApply: make(map[int]checkType),
	}
	sc.configs[0] = Config{
		Num:        0,
		Shards:     [NShards]int{},
		Groups:     make(map[int][]string),
		Gid2Shards: make(map[int][]int),
	}
	labgob.Register(Op{})
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	go sc.ApplierMonitor()
	return sc
}
