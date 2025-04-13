package graft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

type RaftImpl struct {
	state       State
	leaderIndex int32

	currentTerm int32
	votedFor    int32
	log         []LogEntry

	clientSessions map[int]*ClientSession
	clientIndex    int

	commitIndex int32
	lastApplied int32

	nextIndex       []int32 // volatile state: only for leader to use, reset on leader election
	matchIndex      []int32 // volatile state: only for leader to use, reset on leader election
	leaderCommitted bool

	electionCond            *sync.Cond
	commitCond              *sync.Cond
	readCond                *sync.Cond
	electionTimeout         time.Duration
	electionTimeoutDuration time.Duration
	heartbeatTimeout        time.Duration
	lastPingTime            time.Time
}

type ClientSession struct {
	seq           int
	seqToLogIndex map[int]int
	store         map[string]string
}

type LogEntry struct {
	Operation Operation
	Key       string
	Value     string
	Term      int32
	Index     int32 // used for linearizable semantics, serializable view from client's perspective
	ClientId  int
	SeqId     int
}

type Operation int32

const (
	NOOP Operation = iota
	GET
	PUT
	APPEND
	DELETE
	REGISTER
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
	LeaderIndex       int32
	PrevLogIndex      int32
	PrevLogTerm       int32
	Entries           []LogEntry
	LeaderCommitIndex int32
}

type AppendEntriesReply struct {
	CurrentTerm int32
	Success     bool
	LeaderIndex int32
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Println("AppendEntries received:", args, "raft idx: ", rf.me)

	reply.Success = false
	reply.CurrentTerm = rf.impl.currentTerm

	if args.Term < rf.impl.currentTerm {
		log.Println(fmt.Sprintf("Rejecting AppendEntries. current term: %d, requested term: %d", rf.impl.currentTerm, args.Term))
		return nil
	}

	rf.impl.lastPingTime = time.Now()
	rf.impl.leaderIndex = args.LeaderIndex

	// if requester is the leader
	if args.Term > rf.impl.currentTerm || rf.impl.state == PRECANDIDATE || rf.impl.state == CANDIDATE {
		rf.impl.convertToFollower(args.LeaderIndex, args.Term)
		reply.CurrentTerm = rf.impl.currentTerm
	}

	// if the existing log at PrevLogIndex does not match the client's term
	// first check if out of bounds
	if rf.impl.lastLogIndex() < args.PrevLogIndex {
		return nil
	}
	if rf.impl.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		log.Println("Log terms do not match, Conflicting entry:", rf.impl.log[args.PrevLogIndex], "Requested entries:",
			args.Entries, "returning from AppendEntries")
		return nil
	}

	// TODO: Add log compaction check

	var index int32 = 0
	for ; index < int32(len(args.Entries)); index++ {
		if rf.impl.lastLogIndex() < args.Entries[index].Index {
			break
		}
		// found conflicting entry, overwrite from here
		if rf.impl.log[args.Entries[index].Index].Term != args.Entries[index].Term {
			overwriteLen := copy(rf.impl.log[args.Entries[index].Index:], args.Entries[index:])
			index += int32(overwriteLen)
			break
		}
	}

	// append remaining entries
	rf.impl.log = append(rf.impl.log, args.Entries[index:]...)

	if args.LeaderCommitIndex > rf.impl.commitIndex {
		rf.impl.commitIndex = min(args.LeaderCommitIndex, rf.impl.lastLogIndex())
		rf.impl.commitCond.Broadcast()
	}

	reply.Success = true

	log.Println("Replying to AppendEntries, reply:", reply, "log:", rf.impl.log, "peer:", rf.me)

	return nil
}

func (rf *Raft) applyLog(index int32) {
	switch rf.impl.log[index].Operation {
	case DELETE:
		delete(rf.impl.clientSessions[rf.impl.log[index].ClientId].store, rf.impl.log[index].Key)
		rf.impl.clientSessions[rf.impl.log[index].ClientId].seq = rf.impl.log[index].SeqId
	case PUT:
		rf.impl.clientSessions[rf.impl.log[index].ClientId].store[rf.impl.log[index].Key] = rf.impl.log[index].Value
		rf.impl.clientSessions[rf.impl.log[index].ClientId].seq = rf.impl.log[index].SeqId
	case APPEND:
		rf.impl.clientSessions[rf.impl.log[index].ClientId].store[rf.impl.log[index].Key] = rf.impl.clientSessions[rf.impl.log[index].ClientId].store[rf.impl.log[index].Key] + rf.impl.log[index].Value
		rf.impl.clientSessions[rf.impl.log[index].ClientId].seq = rf.impl.log[index].SeqId
	case REGISTER:
		rf.impl.clientSessions[rf.impl.log[index].ClientId] = &ClientSession{-1, make(map[int]int), make(map[string]string)}
	}
}

type RequestVoteArgs struct {
	Term           int32
	CandidateIndex int32
	LastLogIndex   int32
	LastLogTerm    int32
	Prepare        bool
}

type RequestVoteReply struct {
	CurrentTerm int32
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer log.Println("Replying to RequestVote:", reply)

	reply.CurrentTerm = rf.impl.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.impl.currentTerm {
		return nil
	}

	if !args.Prepare && args.Term > rf.impl.currentTerm {
		rf.impl.convertToFollower(args.CandidateIndex, args.Term)
		reply.CurrentTerm = rf.impl.currentTerm
	}

	// already voted
	if !args.Prepare && rf.impl.votedFor != -1 && rf.impl.votedFor != args.CandidateIndex {
		return nil
	}

	// If candidate last log term is less than the current term, or if the candidate last log term is equal but
	// the log length is less than the current log, the candidate has an out-of-date view when compared
	// with the current Raft instance.
	if args.LastLogTerm < rf.impl.lastLogTerm() {
		return nil
	} else if args.LastLogTerm == rf.impl.lastLogTerm() && args.LastLogIndex < rf.impl.lastLogIndex() {
		log.Println("Rejecting RequestVote, peer:", rf.me, "Candidate Log is out of date, Candidate last log index:", args.LastLogIndex,
			"Current last log index:", rf.impl.lastLogIndex())
		return nil
	}

	// vote success
	reply.VoteGranted = true

	if !args.Prepare {
		rf.impl.votedFor = args.CandidateIndex
		rf.impl.lastPingTime = time.Now()
	}
	return nil
}

// Send empty appends to peers to update
// if less than majority of clusters responded
// and election timer passes, step down to a follower.
// Meant to be ran in a separate go routine.
func (rf *Raft) heartbeatLoop() {
	for !rf.isdead() {
		time.Sleep(rf.impl.electionTimeout)
		rf.mu.Lock()
		if rf.impl.state == FOLLOWER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.sendAppendEntriesToPeers()
	}
}

func (rf *Raft) sendAppendEntriesToPeers() {
	log.Println("Leader idx:", rf.me, "Leader log state:", rf.impl.log)
	numSuccess := 1
	for idx, peer := range rf.peers {
		if idx != int(rf.me) {
			go rf.sendAppendEntriesToPeer(idx, peer, &numSuccess)
		}
	}
}

func (rf *Raft) sendAppendEntriesToPeer(idx int, peer string, numSuccess *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.isdead() {
		if rf.impl.state != LEADER {
			return
		}

		args := AppendEntriesArgs{
			rf.impl.currentTerm,
			rf.me,
			rf.impl.nextIndex[idx],                   // TODO: send view of latest logs for the peer
			rf.impl.log[rf.impl.nextIndex[idx]].Term, // send view of latest logs for the peer
			rf.impl.log[rf.impl.nextIndex[idx]+1:],
			rf.impl.commitIndex}
		reply := AppendEntriesReply{}

		rf.mu.Unlock()
		success := Call(peer, "Raft.AppendEntries", &args, &reply)
		rf.mu.Lock()

		if !success || rf.impl.state != LEADER {
			return
		}

		if !reply.Success {
			if reply.CurrentTerm > rf.impl.currentTerm {
				rf.impl.convertToFollower(reply.LeaderIndex, reply.CurrentTerm)
				return
			}
			rf.impl.nextIndex[idx]--
		} else {
			rf.impl.nextIndex[idx] = rf.impl.lastLogIndex()
			rf.impl.matchIndex[idx] = rf.impl.lastLogIndex()
			log.Println("Current nextIndexes:", rf.impl.nextIndex, "Leader Last log index:", rf.impl.lastLogIndex(),
				"Current Commit:", rf.impl.commitIndex)
			for potentialCommit := rf.impl.lastLogIndex(); potentialCommit > rf.impl.commitIndex; potentialCommit-- {
				count := 0
				for _, nextIndex := range rf.impl.nextIndex {
					if nextIndex >= potentialCommit {
						count++
					}
				}
				if count > len(rf.peers)/2 {
					log.Println("Current commit index:", rf.impl.commitIndex, "new commit index is:", potentialCommit)
					rf.impl.commitIndex = max(rf.impl.commitIndex, potentialCommit)
					rf.impl.commitCond.Broadcast()
					break
				}
			}
			*numSuccess++
			log.Println("Num Successes:", *numSuccess)
			if *numSuccess > len(rf.peers)/2 {
				rf.impl.lastPingTime = time.Now()
				rf.impl.leaderCommitted = true
				log.Println("Leader idx:", rf.me, "Broadcasting commit condition, commit index:", rf.impl.commitIndex)
				rf.impl.commitCond.Broadcast()
			}
			return
		}
	}
}

func (rf *Raft) sendRequestVotes() {
	log.Println(fmt.Sprintf("Requesting Votes for term:{%d}, idx:{%d}, state:{%d}", rf.impl.currentTerm, rf.me, rf.impl.state))
	prepare := rf.impl.state == PRECANDIDATE
	votes := 1
	for idx, peer := range rf.peers {
		if idx != int(rf.me) {
			go rf.sendRequestVote(peer, idx, &votes, prepare)
		}
	}
}

func (rf *Raft) sendRequestVote(address string, idx int, votes *int, prepare bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := RequestVoteArgs{
		rf.impl.currentTerm,
		rf.me,
		rf.impl.lastLogIndex(),
		rf.impl.lastLogTerm(),
		true}
	reply := RequestVoteReply{}
	log.Println("Request vote to:", address)

	rf.mu.Unlock()
	success := Call(address, "Raft.RequestVote", &args, &reply)
	rf.mu.Lock()

	if !success {
		log.Println("Failed to Call RequestVote")
		return
	}
	if reply.CurrentTerm > rf.impl.currentTerm {
		rf.impl.convertToFollower(int32(idx), reply.CurrentTerm)
		return
	}
	if reply.VoteGranted {
		*votes++
	}
	if *votes > len(rf.peers)/2 {
		if rf.impl.state == PRECANDIDATE {
			rf.impl.state = CANDIDATE
			rf.impl.electionCond.Broadcast()
			log.Println("PreElection success. Candidate is:", rf.me)
		} else if !prepare && rf.impl.state == CANDIDATE {
			rf.convertToLeader()
			log.Println(fmt.Sprintf("Election success. Current Leader is: %d, term: %d", rf.me, rf.impl.currentTerm))
		}
	} else {
		log.Println(fmt.Sprintf("Election failure, did not receive enough votes. Received %d votes out of %d peers", *votes, len(rf.peers)))
	}
}

func (rf *RaftImpl) convertToFollower(leaderIndex int32, term int32) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.leaderIndex = leaderIndex
	rf.leaderCommitted = false
	rf.commitCond.Broadcast()
	rf.electionCond.Broadcast()
	rf.readCond.Broadcast()
	log.Println("Converting to follower")
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
	rf.impl.leaderIndex = rf.me
	rf.impl.log = append(rf.impl.log, LogEntry{NOOP, "NOOP", "NOOP", rf.impl.currentTerm, rf.impl.lastLogIndex() + 1, 0, 0})
	rf.impl.matchIndex = make([]int32, len(rf.peers))
	for i := range rf.impl.matchIndex {
		rf.impl.matchIndex[i] = 0
	}
	rf.impl.nextIndex = make([]int32, len(rf.peers))
	for i := range rf.impl.nextIndex {
		rf.impl.nextIndex[i] = rf.impl.lastLogIndex()
	}
	rf.impl.leaderCommitted = false
	rf.sendAppendEntriesToPeers()
	go rf.heartbeatLoop()
}

func (rf *Raft) electionTimer() {
	for !rf.isdead() {
		minTimeout := rf.impl.electionTimeout
		maxTimeout := 2 * rf.impl.electionTimeout
		rf.impl.electionTimeoutDuration = rf.impl.electionTimeout + time.Duration(rand.Int63n(int64(maxTimeout-minTimeout)))
		log.Println(fmt.Sprintf("election timeout duration: %s, peer: %d", rf.impl.electionTimeoutDuration, rf.me))
		time.Sleep(rf.impl.electionTimeoutDuration)
		rf.impl.electionCond.Broadcast()
	}
}

func (rf *Raft) electionLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.isdead() {
		rf.impl.electionCond.Wait()
		if (rf.impl.state == LEADER && time.Since(rf.impl.lastPingTime) < rf.impl.electionTimeoutDuration) || time.Since(rf.impl.lastPingTime) < rf.impl.electionTimeoutDuration {
			continue
		}
		log.Println(fmt.Sprintf("Time since last ping: %s, election timeout: %s, peer: %d", time.Since(rf.impl.lastPingTime), rf.impl.electionTimeout, rf.me))
		if rf.impl.state == LEADER {
			rf.impl.state = FOLLOWER
			rf.impl.leaderIndex = -1
			rf.impl.votedFor = -1
		} else if rf.impl.state == FOLLOWER {
			rf.impl.state = PRECANDIDATE
			rf.impl.leaderIndex = -1
			rf.impl.votedFor = -1
		} else if rf.impl.state == CANDIDATE {
			rf.impl.currentTerm++
			rf.impl.votedFor = rf.me
		}
		rf.sendRequestVotes()
	}
}

type GetArgs struct {
	Key      string
	Seq      int
	ClientId int
}

type GetReply struct {
	Success    bool
	Value      string
	LeaderHint int32
}

func (rf *Raft) Get(args *GetArgs, reply *GetReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.LeaderHint = rf.impl.leaderIndex

	readIndex := rf.impl.commitIndex

	if rf.impl.leaderIndex != rf.me {
		log.Println("Called Get on peer:", rf.me, "leaderIndex:", rf.impl.leaderIndex)
		return nil
	}

	log.Println("Log after calling Get on leader:", rf.impl.log, "waiting for readIndex to be applied:", readIndex)

	for !rf.isdead() && rf.impl.state == LEADER && !rf.impl.leaderCommitted && readIndex > rf.impl.lastApplied {
		rf.impl.commitCond.Wait()
	}

	if rf.impl.state != LEADER {
		reply.LeaderHint = rf.impl.leaderIndex
		return nil
	}

	reply.Value = rf.impl.clientSessions[args.ClientId].store[args.Key]
	reply.Success = true
	return nil
}

type PutArgs struct {
	Key      string
	Value    string
	Seq      int
	ClientId int
}

type PutReply struct {
	Success    bool
	LeaderHint int32
}

func (rf *Raft) Put(args *PutArgs, reply *PutReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.LeaderHint = rf.impl.leaderIndex

	if rf.impl.leaderIndex != rf.me {
		return nil
	}

	if val, ok := rf.impl.clientSessions[args.ClientId]; ok && args.Seq <= val.seq {
		reply.Success = true
		return nil
	} else if !ok {
		return nil
	}

	rf.impl.log = append(rf.impl.log, LogEntry{PUT, args.Key, args.Value, rf.impl.currentTerm, rf.impl.lastLogIndex() + 1, args.ClientId, args.Seq})
	rf.impl.nextIndex[rf.me] = rf.impl.lastLogIndex()
	logIndex := rf.impl.lastLogIndex()
	log.Println("Log after calling PUT on leader:", rf.impl.log, "waiting for logIndex to be committed:", logIndex)

	for !rf.isdead() && rf.impl.state == LEADER && rf.impl.commitIndex < logIndex {
		rf.impl.commitCond.Wait()
	}

	if rf.impl.commitIndex >= logIndex {
		reply.Success = true
		return nil
	}

	if rf.impl.state != LEADER {
		reply.Success = false
		reply.LeaderHint = rf.impl.leaderIndex
		return nil
	}

	reply.Success = true
	return nil
}

type AppendArgs struct {
	Key      string
	Value    string
	Seq      int
	ClientId int
}

type AppendReply struct {
	Success    bool
	LeaderHint int32
}

func (rf *Raft) Append(args *AppendArgs, reply *AppendReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.LeaderHint = rf.impl.leaderIndex

	if rf.impl.leaderIndex != rf.me {
		return nil
	}

	if val, ok := rf.impl.clientSessions[args.ClientId]; ok && args.Seq <= val.seq {
		reply.Success = true
		return nil
	} else if !ok {
		return nil
	}

	rf.impl.log = append(rf.impl.log, LogEntry{APPEND, args.Key, args.Value, rf.impl.currentTerm, rf.impl.lastLogIndex() + 1, args.ClientId, args.Seq})
	rf.impl.nextIndex[rf.me] = rf.impl.lastLogIndex()
	logIndex := rf.impl.lastLogIndex()
	log.Println("Log after calling APPEND on leader:", rf.impl.log, "waiting for logIndex to be committed:", logIndex)

	for !rf.isdead() && rf.impl.state == LEADER && rf.impl.commitIndex < logIndex {
		rf.impl.commitCond.Wait()
	}

	if rf.impl.commitIndex >= logIndex {
		reply.Success = true
		return nil
	}

	if rf.impl.state != LEADER {
		reply.Success = false
		reply.LeaderHint = rf.impl.leaderIndex
		return nil
	}

	return nil
}

func (rf *Raft) commitLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.isdead() {
		rf.impl.commitCond.Wait()
		for index := rf.impl.lastApplied; index <= rf.impl.commitIndex; index++ {
			rf.applyLog(index)
		}
		rf.impl.lastApplied = rf.impl.commitIndex
		log.Println("Current state after applying logs: peer idx:", rf.me)
		for clientId, session := range rf.impl.clientSessions {
			log.Println("peer idx:", rf.me, "ClientId:", clientId, "session state:", *session)
		}
	}
}

type RegisterClientArgs struct {
}

type RegisterClientReply struct {
	Success    bool
	LeaderHint int32
	ClientId   int
}

func (rf *Raft) RegisterClient(args *RegisterClientArgs, reply *RegisterClientReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.LeaderHint = rf.impl.leaderIndex

	if rf.impl.leaderIndex != rf.me {
		return nil
	}

	reply.ClientId = rf.impl.clientIndex
	rf.impl.log = append(rf.impl.log, LogEntry{REGISTER, "REGISTER", "", rf.impl.currentTerm, rf.impl.lastLogIndex() + 1, rf.impl.clientIndex, 0})
	rf.impl.clientIndex++

	rf.impl.nextIndex[rf.me] = rf.impl.lastLogIndex()
	logIndex := rf.impl.lastLogIndex()

	log.Println("Log after calling REGISTER on leader:", rf.impl.log, "waiting for logIndex to be committed:", logIndex)

	for !rf.isdead() && rf.impl.state == LEADER && rf.impl.commitIndex < logIndex {
		rf.impl.commitCond.Wait()
	}

	if rf.impl.state != LEADER {
		reply.Success = false
		reply.LeaderHint = rf.impl.leaderIndex
		return nil
	}

	reply.Success = true

	log.Println("Returning from RegisterClient, with reply: ", reply)

	return nil
}
