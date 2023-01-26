package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clerkId    int64
	commandId  int
	leaderHint int
}

type node struct {
	ok    bool
	reply interface{}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// Your code here.
	ck := &Clerk{
		servers:    servers,
		clerkId:    nrand(),
		leaderHint: 0,
		commandId:  0,
	}
	return ck
}

// Query(num) -> fetch Config # num, or latest config if num==-1.
func (ck *Clerk) Query(num int) Config {
	// Your code here.
	args := &QueryArgs{
		Num:       num,
		CommandId: ck.commandId,
		ClientId:  ck.clerkId,
	}
	ck.commandId++
	resultChan := make(chan node)

	for {
		// try each known server.
		ck.leaderHint = (ck.leaderHint + 1) % len(ck.servers)
		tar := ck.leaderHint
		go func() {
			reply := QueryReply{}
			ok := ck.servers[tar].Call("ShardCtrler.Query", args, &reply)
			resultChan <- node{ok, reply}
		}()

		select {
		case receive := <-resultChan:
			ok, reply := receive.ok, receive.reply.(QueryReply)
			if !ok || reply.WrongLeader {
				continue
			}
			return reply.Config
		case <-time.After(time.Millisecond * 500):
			continue
		}
	}
}

// Join(servers) -- add a set of groups (gid -> server-list mapping).
func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	args := &JoinArgs{
		Servers:   servers,
		CommandId: ck.commandId,
		ClientId:  ck.clerkId,
	}
	ck.commandId++
	resultChan := make(chan node)

	for {
		// try each known server.
		ck.leaderHint = (ck.leaderHint + 1) % len(ck.servers)
		tar := ck.leaderHint
		go func() {
			reply := JoinReply{}
			ok := ck.servers[tar].Call("ShardCtrler.Join", args, &reply)
			resultChan <- node{ok, reply}
		}()

		select {
		case receive := <-resultChan:
			ok, reply := receive.ok, receive.reply.(JoinReply)
			if !ok || reply.WrongLeader {
				continue
			}
			return
		case <-time.After(time.Millisecond * 500):
			continue
		}
	}
}

// Leave(gids) -- delete a set of groups.
func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	args := &LeaveArgs{
		GIDs:      gids,
		CommandId: ck.commandId,
		ClientId:  ck.clerkId,
	}
	ck.commandId++
	resultChan := make(chan node)

	for {
		// try each known server.
		ck.leaderHint = (ck.leaderHint + 1) % len(ck.servers)
		tar := ck.leaderHint
		go func() {
			reply := LeaveReply{}
			ok := ck.servers[tar].Call("ShardCtrler.Leave", args, &reply)
			resultChan <- node{ok, reply}
		}()

		select {
		case receive := <-resultChan:
			ok, reply := receive.ok, receive.reply.(LeaveReply)
			if !ok || reply.WrongLeader {
				continue
			}
			return
		case <-time.After(time.Millisecond * 500):
			continue
		}
	}
}

// Move(shard, gid) -- hand off one shard from current owner to gid.
func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	args := &MoveArgs{
		Shard:     shard,
		GID:       gid,
		CommandId: ck.commandId,
		ClientId:  ck.clerkId,
	}
	ck.commandId++
	resultChan := make(chan node)

	for {
		// try each known server.
		ck.leaderHint = (ck.leaderHint + 1) % len(ck.servers)
		tar := ck.leaderHint
		go func() {
			reply := MoveReply{}
			ok := ck.servers[tar].Call("ShardCtrler.Move", args, &reply)
			resultChan <- node{ok, reply}
		}()

		select {
		case receive := <-resultChan:
			ok, reply := receive.ok, receive.reply.(MoveReply)
			if !ok || reply.WrongLeader {
				continue
			}
			return
		case <-time.After(time.Millisecond * 500):
			continue
		}
	}

}
