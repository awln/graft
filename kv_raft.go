package graft

import (
	"net"
	"sync"
)

type kvRaft struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	rf         *Raft

	store map[string]string
	seq   int
}

type GetArgs struct {
	key      string
	seq      int
	clientId int
}

type GetReply struct {
	success    bool
	value      string
	leaderHint int32
}

func (kv *kvRaft) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.rf.mu.Lock()

	reply.success = false
	reply.leaderHint = kv.rf.impl.leaderIndex

	if kv.rf.impl.leaderIndex != kv.rf.me {
		return
	}

	if args.seq <= kv.seq {
		reply.success = true
		reply.value = kv.store[args.key]
		return
	}

	kv.rf.impl.log = append(kv.rf.impl.log, LogEntry{GET, args.key, "", kv.rf.impl.currentTerm, kv.rf.impl.lastLogIndex() + 1, args.clientId})

	kv.rf.mu.Unlock()
	if kv.rf.sendAppendEntriesToPeers() {
		reply.success = true
		reply.value = kv.store[args.key]
		kv.seq++
	}
}

type PutArgs struct {
	key      string
	value    string
	seq      int
	clientId int
}

type PutReply struct {
	success    bool
	leaderHint int32
}

func (kv *kvRaft) Put(args *PutArgs, reply *PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.rf.mu.Lock()

	reply.success = false
	reply.leaderHint = kv.rf.impl.leaderIndex

	if kv.rf.impl.leaderIndex != kv.rf.me {
		return
	}

	if args.seq <= kv.seq {
		reply.success = true
		return
	}

	kv.rf.impl.log = append(kv.rf.impl.log, LogEntry{PUT, args.key, args.value, kv.rf.impl.currentTerm, kv.rf.impl.lastLogIndex() + 1, args.clientId})
	kv.store[args.key] = args.value

	kv.rf.mu.Unlock()
	if kv.rf.sendAppendEntriesToPeers() {
		reply.success = true
		kv.seq++
	}
}
