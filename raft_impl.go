package graft

import (
	"fmt"
	"sync/atomic"
	"time"
)

type RaftImpl struct {
	state State

	currentTerm int32
	votedFor    int32
	log         []LogEntry

	commitIndex int32
	lastApplied int32

	nextIndex  []int32
	matchIndex []int32

	heartbeatTimer time.Timer
}

type LogEntry struct {
	Operation Operation
	Key       string
	Value     string
	Term      int32
}

type Operation int32

const (
	GET Operation = iota
	PUT
	DELETE
	NOOP
)

type State int32

const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

type AppendEntriesArgs struct {
	Term              int32
	LeaderId          int32
	PrevLogIndex      int32
	PrevLogTerm       int32
	Entries           []LogEntry
	LeaderCommitIndex int32
}

type AppendEntriesReply struct {
	CurrentTerm int32
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	// append entries implementation here

	return nil
}

type RequestVoteArgs struct {
	Term           int32
	CandidateIndex int32
	LastLogIndex   int32
	LastLogTerm    int32
}

type RequestVoteReply struct {
	CurrentTerm int32
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.impl.currentTerm {
		reply.CurrentTerm = rf.impl.currentTerm
		reply.VoteGranted = false
	} else if rf.impl.votedFor == -1 || rf.impl.votedFor == args.CandidateIndex {
		if args.LastLogIndex > int32(len(rf.impl.log)) {
			reply.CurrentTerm = rf.impl.currentTerm
			reply.VoteGranted = true
		} else if args.LastLogTerm >= rf.impl.log[args.LastLogIndex].Term {
			reply.CurrentTerm = rf.impl.currentTerm
			reply.VoteGranted = true
		} else {
			reply.CurrentTerm = rf.impl.currentTerm
			reply.VoteGranted = false
		}
	}
	fmt.Println("Replying to RequestVote:", reply)
	return nil
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("Starting Election: ", rf.me)
	atomic.AddInt32(&rf.impl.currentTerm, 1)
	rf.impl.votedFor = rf.me
	rf.impl.state = CANDIDATE
	votes := 0
	for id, peer := range rf.peers {
		if id == int(rf.me) {
			votes++
			continue
		}
		reply := RequestVoteReply{}
		fmt.Println("Request vote to:", peer)
		success := Call(peer, "Raft.RequestVote", &RequestVoteArgs{
			rf.impl.currentTerm,
			rf.me,
			int32(len(rf.impl.log) - 1),
			rf.impl.log[len(rf.impl.log)-1].Term},
			&reply)
		if !success {
			fmt.Println("Failed to Call RequestVote")
			return
		}
		if reply.CurrentTerm >= rf.impl.currentTerm {
			rf.impl.currentTerm = reply.CurrentTerm
			rf.impl.state = FOLLOWER
			fmt.Println("Converting to follower, returning")
			return
		}
		if reply.VoteGranted {
			votes++
		}
	}
	if votes > len(rf.peers)/2 {
		rf.impl.state = LEADER
		for _, peer := range rf.peers {
			reply := AppendEntriesReply{}
			// broadcast results until success
			fmt.Println("Broadcasting election results to:", peer)
			for !Call(peer, "Raft.AppendEntries", &AppendEntriesArgs{
				rf.impl.currentTerm,
				rf.me,
				int32(len(rf.impl.log)),
				rf.impl.log[len(rf.impl.log)-1].Term,
				[]LogEntry{},
				rf.impl.commitIndex},
				&reply) {
			}
		}
	}
	fmt.Println("Election success. Current Leader is:", rf.me)
	rf.resetTimer()
}
