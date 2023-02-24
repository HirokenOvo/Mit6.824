package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWaitMigrate = "ErrWaitMigrate"
	// Debug          = true
	Debug = false
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Shard     int
	CommandId int
	ClerkId   int64
	Term      int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Shard     int
	CommandId int
	ClerkId   int64
	Term      int
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateArgs struct {
	Idx        int
	Shard      map[string]string
	Term       int
	LastSolved map[int64]int
}

type MigrateReply struct {
	Err Err
}

func max(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}
