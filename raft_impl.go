package graft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type RaftImpl struct {
	state    State
	leaderId string

	currentTerm int32
	votedFor    string
	log         []LogEntry

	commitIndex int32
	lastApplied int32

	nextIndex  []int32
	matchIndex []int32

	electionCond    *sync.Cond
	electionTimeout time.Duration
	lastPingTime    time.Time
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
	PRECANDIDATE
	CANDIDATE
)

type AppendEntriesArgs struct {
	Term              int32
	LeaderId          string
	PrevLogIndex      int32
	PrevLogTerm       int32
	Entries           []LogEntry
	LeaderCommitIndex int32
}

type AppendEntriesReply struct {
	CurrentTerm int32
	Success     bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer fmt.Println("Replying to AppendEntries: ", reply)

	fmt.Println("AppendEntries received ", rf.me)

	reply.Success = false
	reply.CurrentTerm = rf.impl.currentTerm

	if args.Term < rf.impl.currentTerm {
		fmt.Println(fmt.Sprintf("Rejecting AppendEntries. current term: %d, requested term: %d", rf.impl.currentTerm, args.Term))
		return nil
	}

	rf.impl.lastPingTime = time.Now()
	rf.impl.leaderId = args.LeaderId

	if args.Term > rf.impl.currentTerm {
		rf.impl.convertToFollower(args.LeaderId, args.Term)
	}

	// if the existing log at PrevLogIndex does not match the client's term
	// first check if out of bounds
	// if rf.impl.lastLogIndex()+1 <= args.PrevLogIndex {
	// 	return nil
	// }
	if rf.impl.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		fmt.Println("Log terms do not match, returning from AppendEntries")
		return nil
	}

	// TODO: Add log compaction check

	var index int32 = 0

	for ; index < int32(len(args.Entries)) && args.PrevLogIndex+index+1 < int32(len(rf.impl.log)); index++ {
		// found conflicting entry, overwrite from here
		logNextIndex := args.PrevLogIndex + index + 1
		if rf.impl.log[logNextIndex].Term != args.Entries[index].Term {
			overwriteLen := min(len(rf.impl.log[logNextIndex:]), len(args.Entries))
			rf.impl.log = append(rf.impl.log, args.Entries[:overwriteLen]...)
			index += int32(overwriteLen)
			break
		}
	}

	// append remaining entries
	rf.impl.log = append(rf.impl.log, args.Entries[index:]...)

	if args.LeaderCommitIndex > rf.impl.commitIndex {
		rf.impl.commitIndex = min(args.LeaderCommitIndex, int32(len(rf.impl.log)-1))
	}

	reply.Success = true

	return nil
}

type RequestVoteArgs struct {
	Term         int32
	CandidateId  string
	LastLogIndex int32
	LastLogTerm  int32
	PreVote      bool
}

type RequestVoteReply struct {
	CurrentTerm int32
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer fmt.Println("Replying to RequestVote:", reply)

	reply.CurrentTerm = rf.impl.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.impl.currentTerm {
		return nil
	}

	if !args.PreVote && args.Term > rf.impl.currentTerm {
		rf.impl.convertToFollower(args.CandidateId, args.Term)
		reply.CurrentTerm = rf.impl.currentTerm
	}

	// already voted
	if !args.PreVote && rf.impl.votedFor != "" && rf.impl.votedFor != args.CandidateId {
		return nil
	}

	// If candidate last log term is less than the current term, or if the candidate last log term is equal but
	// the log length is less than the current log, the candidate has an out-of-date view when compared
	// with the current Raft instance.
	if args.LastLogTerm < rf.impl.lastLogTerm() {
		return nil
	} else if args.LastLogTerm == rf.impl.lastLogTerm() && args.LastLogIndex < rf.impl.lastLogIndex() {
		return nil
	}

	// vote success
	reply.VoteGranted = true

	if !args.PreVote {
		rf.impl.votedFor = args.CandidateId
		rf.impl.lastPingTime = time.Now()
	}
	return nil
}

func (rf *Raft) heartbeatLoop() {
	for {
		time.Sleep(rf.impl.electionTimeout)
		rf.mu.Lock()
		if rf.impl.state == FOLLOWER {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		rf.sendAppendEntriesToPeers()
	}
}

func (rf *Raft) sendAppendEntriesToPeers() {
	numReplies := 1
	for idx, peer := range rf.peers {
		if idx != int(rf.me) {
			go rf.sendAppendEntriesToPeer(peer, &numReplies)
		}
	}
}

func (rf *Raft) sendAppendEntriesToPeer(peer string, numResponses *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := AppendEntriesArgs{
		rf.impl.currentTerm,
		rf.peers[rf.me],
		0,                   // TODO: send view of latest logs for the peer
		rf.impl.log[0].Term, // send view of latest logs for the peer
		[]LogEntry{},
		rf.impl.commitIndex}
	reply := AppendEntriesReply{}

	rf.mu.Unlock()
	success := Call(peer, "Raft.AppendEntries", &args, &reply)
	rf.mu.Lock()

	if !success || rf.impl.state != LEADER {
		return
	}
}

func (rf *Raft) sendRequestVotes() {
	fmt.Println(fmt.Sprintf("Requesting Votes for term:{%d}, idx:{%d}, state:{%d}", rf.impl.currentTerm, rf.me, rf.impl.state))
	preVote := rf.impl.state == PRECANDIDATE
	votes := 1
	for idx, peer := range rf.peers {
		if idx != int(rf.me) {
			go rf.sendRequestVote(peer, &votes, preVote)
		}
	}
}

func (rf *Raft) sendRequestVote(address string, votes *int, prevote bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	meAddress := rf.peers[rf.me]
	args := RequestVoteArgs{
		rf.impl.currentTerm,
		meAddress,
		rf.impl.lastLogIndex(),
		rf.impl.lastLogTerm(),
		true}
	reply := RequestVoteReply{}
	fmt.Println("Request vote to:", address)

	rf.mu.Unlock()
	success := Call(address, "Raft.RequestVote", &args, &reply)
	rf.mu.Lock()

	if !success {
		fmt.Println("Failed to Call RequestVote")
		return
	}
	if reply.CurrentTerm >= rf.impl.currentTerm {
		rf.impl.convertToFollower(address, reply.CurrentTerm)
		return
	}
	if reply.VoteGranted {
		*votes++
	}
	if *votes > len(rf.peers)/2 {
		if rf.impl.state == PRECANDIDATE {
			rf.impl.state = CANDIDATE
			rf.impl.electionCond.Signal()
			fmt.Println("PreElection success. Candidate is:", rf.me)
		} else if !prevote && rf.impl.state == CANDIDATE {
			rf.convertToLeader()
			fmt.Println(fmt.Sprintf("Election success. Current Leader is: %d, term: %d", rf.me, rf.impl.currentTerm))
		}
	} else {
		fmt.Println(fmt.Sprintf("Election failure, did not receive enough votes. Received %d votes out of %d peers", *votes, len(rf.peers)))
	}
}

func (rf *RaftImpl) convertToFollower(leaderId string, term int32) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	fmt.Println("Converting to follower, returning")
}

func (rf *RaftImpl) lastLogIndex() int32 {
	return int32(len(rf.log) - 1)
}

func (rf *RaftImpl) lastLogTerm() int32 {
	return rf.log[int32(len(rf.log)-1)].Term
}

// Force instance to become leader
func (rf *Raft) convertToLeader() {
	rf.impl.state = LEADER
	rf.impl.log = append(rf.impl.log, LogEntry{NOOP, "", "", 0})
	rf.sendAppendEntriesToPeers()
}

func (rf *Raft) electionTimer() {
	for {
		minTimeout := rf.impl.electionTimeout
		maxTimeout := 2 * rf.impl.electionTimeout
		electionTimeoutDuration := rf.impl.electionTimeout + time.Duration(rand.Int63n(int64(maxTimeout-minTimeout)))
		fmt.Println(fmt.Sprintf("election timeout duration: %s, peer: %d", electionTimeoutDuration, rf.me))
		time.Sleep(electionTimeoutDuration)
		rf.impl.electionCond.Signal()
	}
}

func (rf *Raft) electionLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		rf.impl.electionCond.Wait()
		if rf.impl.state == LEADER || time.Since(rf.impl.lastPingTime) < rf.impl.electionTimeout {
			fmt.Println(fmt.Sprintf("Time since last ping: %s, election timeout: %s, peer: %d", time.Since(rf.impl.lastPingTime), rf.impl.electionTimeout, rf.me))
			continue
		}
		if rf.impl.state == FOLLOWER {
			rf.impl.state = PRECANDIDATE
			rf.impl.currentTerm++
		} else if rf.impl.state == PRECANDIDATE {
			rf.impl.state = CANDIDATE
		}
		rf.sendRequestVotes()
	}
}
