package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId    int64 //该客户端的唯一标识符
	commandId  int   //该客户端的命令唯一标识符
	leaderHint int   //记录访问过的leader
	mu         sync.Mutex
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
	// You'll have to add code here.
	ck := &Clerk{servers: servers,
		clerkId:    nrand(),
		commandId:  0,
		leaderHint: 0, //初始0,不对再重试
	}
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	args := GetArgs{Key: key,
		ClerkId:   ck.clerkId,
		CommandId: ck.commandId,
	}
	/*
		[Duplicate detection]
		give each client a unique identifier,
		and then have them tag each request
		with a monotonically increasing sequence number.
	*/
	ck.commandId++
	ck.mu.Unlock()
	resultChan := make(chan node)
	for {
		/*
			[Duplicate detection]
			If a client re-sends a request,
			it re-uses the same sequence number.
		*/
		ck.mu.Lock()
		ck.leaderHint = (ck.leaderHint + 1) % len(ck.servers)
		tar := ck.leaderHint
		ck.mu.Unlock()
		// DPrintf("Get:client[%d] start call kvserver[%v],commandId:%v", ck.clerkId, ck.leaderHint, args.CommandId)
		go func() {
			reply := GetReply{}
			ok := ck.servers[tar].Call("KVServer.Get", &args, &reply)
			resultChan <- node{ok, reply}

		}()
		// 利用channel设置超时,500ms未收到响应重试
		select {
		case receive := <-resultChan:
			ok, reply := receive.ok, receive.reply.(GetReply)
			if !ok || reply.Err == ErrWrongLeader {
				continue
			}
			DPrintf("Get:client[%d] find Leader:%v,commandId%v,receive response", ck.clerkId, ck.leaderHint, args.CommandId)
			if reply.Err == ErrNoKey {
				reply.Value = ""
			}
			return reply.Value
		case <-time.After(time.Millisecond * 500):
			continue
		}

	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	args := PutAppendArgs{Key: key,
		Value:     value,
		Op:        op,
		ClerkId:   ck.clerkId,
		CommandId: ck.commandId,
	}
	ck.commandId++
	ck.mu.Unlock()
	resultChan := make(chan node)
	for {
		ck.mu.Lock()
		ck.leaderHint = (ck.leaderHint + 1) % len(ck.servers)
		tar := ck.leaderHint
		ck.mu.Unlock()
		// DPrintf("PutAppend:client[%d] start call kvserver[%v],commandId:%v", ck.clerkId, ck.leaderHint, args.CommandId)
		go func() {
			reply := PutAppendReply{}
			ok := ck.servers[tar].Call("KVServer.PutAppend", &args, &reply)
			resultChan <- node{ok, reply}
		}()
		select {
		case receive := <-resultChan:
			ok, reply := receive.ok, receive.reply.(PutAppendReply)
			if !ok || reply.Err == ErrWrongLeader {
				continue
			}
			DPrintf("PutAppend:client[%d] find Leader:%v,commandId%v,\ncmd:%v->%v", ck.clerkId, ck.leaderHint, args.CommandId, args.Key, args.Value)
			return

		case <-time.After(time.Millisecond * 500):
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
