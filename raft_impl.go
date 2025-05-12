package graft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// TODO: log compaction, log persistence,

type RaftImpl struct {
	state State

	log                   []LogEntry
	currentTerm           int32
	commitIndex           int32
	lastApplied           int32
	lastAppliedPeerChange int32
	lastIncluded          int32

	snapshotService SnapshotService
	previousPeers   map[string]struct{}

	clientSessions map[int]*ClientSession
	clientIndex    int

	leaderAddress   string
	votedFor        string
	peers           map[string]struct{}
	nextIndex       map[string]int32 // volatile state: only for leader to use, reset on leader election
	matchIndex      map[string]int32 // volatile state: only for leader to use, reset on leader election
	leaderCommitted bool
	leaderEntry     int32

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
	REGISTERCLIENT
	REGISTERSERVER
	DEREGISTERSERVER
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
	LeaderAddress     string
	PrevLogIndex      int32
	PrevLogTerm       int32
	Entries           []LogEntry
	LeaderCommitIndex int32
}

type AppendEntriesReply struct {
	CurrentTerm   int32
	Success       bool
	LeaderAddress string
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log.Println("AppendEntries received:", args, "raft idx: ", rf.me)

	reply.Success = false
	reply.CurrentTerm = rf.impl.currentTerm

	if args.Term < rf.impl.currentTerm {
		// log.Println(fmt.Sprintf("Rejecting AppendEntries. current term: %d, requested term: %d", rf.impl.currentTerm, args.Term))
		return nil
	}

	rf.impl.lastPingTime = time.Now()
	rf.impl.leaderAddress = args.LeaderAddress

	// if requester is the leader
	if args.Term > rf.impl.currentTerm || rf.impl.state == PRECANDIDATE || rf.impl.state == CANDIDATE {
		rf.impl.convertToFollower(args.LeaderAddress, args.Term)
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

	// log.Println("Replying to AppendEntries, reply:", reply, "log:", rf.impl.log, "peer:", rf.me)

	return nil
}

func (rf *Raft) applyLog(index int32) {
	if val, ok := rf.impl.clientSessions[rf.impl.log[index].ClientId]; ok && rf.impl.log[index].SeqId <= val.seq {
		return
	}
	// log.Println("raft peer:", rf.me, "applying log:", rf.impl.log[index])
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
	case REGISTERCLIENT:
		if _, ok := rf.impl.clientSessions[rf.impl.log[index].ClientId]; ok {
			// log.Println("Already applied index", index, "returning")
			return
		}
		rf.impl.clientIndex = max(rf.impl.clientIndex, rf.impl.log[index].ClientId+1)
		rf.impl.clientSessions[rf.impl.log[index].ClientId] = &ClientSession{-1, make(map[int]int), make(map[string]string)}
	case REGISTERSERVER:
		rf.impl.peers[rf.impl.log[index].Value] = struct{}{}
		rf.impl.nextIndex[rf.impl.log[index].Value] = rf.impl.lastLogIndex()
		rf.impl.matchIndex[rf.impl.log[index].Value] = 0
		rf.impl.lastAppliedPeerChange = index
		if rf.impl.log[index].Value == rf.me && rf.impl.commitIndex >= index {
			// committed part of quorum, can participate in elections
			go rf.electionTimer()
			go rf.electionLoop()
		}
	}
}

type RequestVoteArgs struct {
	Term             int32
	CandidateAddress string
	LastLogIndex     int32
	LastLogTerm      int32
	Prepare          bool
}

type RequestVoteReply struct {
	CurrentTerm int32
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.CurrentTerm = rf.impl.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.impl.currentTerm {
		return nil
	}

	// already voted
	if !args.Prepare && rf.impl.votedFor != "" && rf.impl.votedFor != args.CandidateAddress {
		return nil
	}

	// If candidate last log term is less than the current term, or if the candidate last log term is equal but
	// the log length is less than the current log, the candidate has an out-of-date view when compared
	// with the current Raft instance.
	if args.LastLogTerm < rf.impl.lastLogTerm() {
		return nil
	} else if args.LastLogTerm == rf.impl.lastLogTerm() && args.LastLogIndex < rf.impl.lastLogIndex() {
		// log.Println("Rejecting RequestVote, peer:", rf.me, "Candidate Log is out of date, Candidate last log index:", args.LastLogIndex,
		// 	"Current last log index:", rf.impl.lastLogIndex())
		return nil
	}

	// vote success
	reply.VoteGranted = true

	if !args.Prepare {
		rf.impl.votedFor = args.CandidateAddress
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
		rf.sendAppendEntriesToPeers()
		time.Sleep(rf.impl.electionTimeout)
		rf.mu.Lock()
		if rf.impl.state == FOLLOWER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntriesToPeers() {
	// log.Println("Leader idx:", rf.me, "Leader log state:", rf.impl.log)
	numSuccess := 1
	for peer := range rf.impl.peers {
		if peer != rf.me {
			go rf.sendAppendEntriesToPeer(peer, &numSuccess)
		}
	}
	rf.handleSingleNodeCluster()
}

func (rf *Raft) handleSingleNodeCluster() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.impl.peers) == 1 {
		rf.impl.commitIndex = rf.impl.lastLogIndex()
		rf.impl.leaderCommitted = true
		rf.impl.commitCond.Broadcast()
	}
}

func (rf *Raft) sendAppendEntriesToPeer(peer string, numSuccess *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.isdead() {
		if rf.impl.state != LEADER {
			return
		}

		lastIndexSent := rf.impl.lastLogIndex()

		args := AppendEntriesArgs{
			rf.impl.currentTerm,
			rf.me,
			rf.impl.nextIndex[peer], // TODO: send view of latest logs for the peer
			rf.impl.log[rf.impl.nextIndex[peer]].Term, // send view of latest logs for the peer
			rf.impl.log[rf.impl.nextIndex[peer]+1:],
			rf.impl.commitIndex}
		reply := AppendEntriesReply{}

		// log.Println("sending AppendEntries", args)

		rf.mu.Unlock()
		success := Call(peer, "Raft.AppendEntries", &args, &reply)
		rf.mu.Lock()

		if !success || rf.impl.state != LEADER {
			return
		}

		if !reply.Success {
			if reply.CurrentTerm > rf.impl.currentTerm {
				rf.impl.convertToFollower(reply.LeaderAddress, reply.CurrentTerm)
				return
			}
			rf.impl.nextIndex[peer]--
		} else {
			rf.impl.nextIndex[peer] = max(rf.impl.nextIndex[peer], lastIndexSent)
			rf.impl.matchIndex[peer] = max(rf.impl.nextIndex[peer], lastIndexSent)
			// log.Println("Current nextIndexes:", rf.impl.nextIndex, "Leader Last log index:", rf.impl.lastLogIndex(),
			// 	"Current Commit:", rf.impl.commitIndex)
			for potentialCommit := rf.impl.lastLogIndex(); potentialCommit > rf.impl.commitIndex; potentialCommit-- {
				count := 0
				for _, nextIndex := range rf.impl.nextIndex {
					if nextIndex >= potentialCommit {
						count++
					}
				}
				if count > len(rf.impl.peers)/2 {
					// log.Println("Current commit index:", rf.impl.commitIndex, "new commit index is:", potentialCommit)
					rf.impl.commitIndex = max(rf.impl.commitIndex, potentialCommit)
					rf.impl.commitCond.Broadcast()
					break
				}
			}
			*numSuccess++
			// log.Println("Num Successes:", *numSuccess)
			if *numSuccess > len(rf.impl.peers)/2 {
				rf.impl.lastPingTime = time.Now()
				rf.impl.leaderCommitted = true
				// log.Println("Leader idx:", rf.me, "Broadcasting commit condition, commit index:", rf.impl.commitIndex)
				rf.impl.commitCond.Broadcast()
			}
			return
		}
	}
}

func (rf *Raft) sendRequestVotes() {
	// log.Println(fmt.Sprintf("Requesting Votes for term:{%d}, idx:{%s}, state:{%d}", rf.impl.currentTerm, rf.me, rf.impl.state))
	prepare := rf.impl.state == PRECANDIDATE
	votes := 1
	for peer := range rf.impl.peers {
		if peer != rf.me {
			go rf.sendRequestVote(peer, &votes, prepare)
		}
	}
}

func (rf *Raft) sendRequestVote(peer string, votes *int, prepare bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := RequestVoteArgs{
		rf.impl.currentTerm,
		rf.me,
		rf.impl.lastLogIndex(),
		rf.impl.lastLogTerm(),
		prepare}
	reply := RequestVoteReply{}
	// log.Println("Request vote to:", peer)

	rf.mu.Unlock()
	success := Call(peer, "Raft.RequestVote", &args, &reply)
	rf.mu.Lock()

	if !success {
		// log.Println("Failed to Call RequestVote")
		return
	}
	if reply.CurrentTerm > rf.impl.currentTerm {
		rf.impl.convertToFollower(peer, reply.CurrentTerm)
		return
	}
	if reply.VoteGranted {
		*votes++
	}
	if *votes > len(rf.impl.peers)/2 {
		if rf.impl.state == PRECANDIDATE {
			rf.impl.state = CANDIDATE
			rf.impl.currentTerm++
			rf.impl.votedFor = rf.me
			rf.impl.electionCond.Broadcast()
			// log.Println("PreElection success. Candidate is:", rf.me)
		} else if !prepare && rf.impl.state == CANDIDATE {
			rf.convertToLeader()
			log.Println(fmt.Sprintf("Election success. Current Leader is: %s, term: %d", rf.me, rf.impl.currentTerm))
		}
	} else {
		// log.Println(fmt.Sprintf("Election failure, did not receive enough votes. Received %d votes out of %d peers", *votes, len(rf.impl.peers)))
	}
}

func (rf *RaftImpl) convertToFollower(leaderAddress string, term int32) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.leaderAddress = leaderAddress
	rf.leaderCommitted = false
	rf.votedFor = ""
	rf.commitCond.Broadcast()
	rf.electionCond.Broadcast()
	rf.readCond.Broadcast()
	// log.Println("Converting to follower")
}

func (rf *RaftImpl) lastLogIndex() int32 {
	return rf.log[int32(len(rf.log)-1)].Index
}

func (rf *RaftImpl) lastLogTerm() int32 {
	return rf.log[int32(len(rf.log)-1)].Term
}

// Force instance to become leader
func (rf *Raft) convertToLeader() {
	rf.impl.state = LEADER
	rf.impl.leaderAddress = rf.me
	rf.impl.log = append(rf.impl.log, LogEntry{NOOP, "NOOP", "NOOP", rf.impl.currentTerm, rf.impl.lastLogIndex() + 1, 0, 0})
	rf.impl.leaderEntry = rf.impl.lastLogIndex()
	// log.Println("Leader idx:", rf.me, "Leader log state:", rf.impl.log)
	// for clientId, session := range rf.impl.clientSessions {
	// 	log.Println("peer idx:", rf.me, "ClientId:", clientId, "session state:", *session)
	// }
	rf.impl.matchIndex = make(map[string]int32, len(rf.impl.peers))
	for peer := range rf.impl.matchIndex {
		rf.impl.matchIndex[peer] = 0
	}
	rf.impl.nextIndex = make(map[string]int32, len(rf.impl.peers))
	for peer := range rf.impl.nextIndex {
		rf.impl.nextIndex[peer] = rf.impl.lastLogIndex()
	}
	rf.impl.leaderCommitted = false
	go rf.heartbeatLoop()
}

func (rf *Raft) electionTimer() {
	for !rf.isdead() {
		minTimeout := rf.impl.electionTimeout
		maxTimeout := 2 * rf.impl.electionTimeout
		rf.impl.electionTimeoutDuration = rf.impl.electionTimeout + time.Duration(rand.Int63n(int64(maxTimeout-minTimeout)))
		// log.Println(fmt.Sprintf("election timeout duration: %s, peer: %s", rf.impl.electionTimeoutDuration, rf.me))
		time.Sleep(rf.impl.electionTimeoutDuration)
		rf.impl.electionCond.Broadcast()
	}
}

func (rf *Raft) electionLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.isdead() {
		rf.impl.electionCond.Wait()
		if rf.impl.state == LEADER {
			continue
		} else if rf.impl.state != LEADER && time.Since(rf.impl.lastPingTime) < rf.impl.electionTimeoutDuration {
			continue
		}
		// log.Println(fmt.Sprintf("Time since last ping: %s, election timeout: %s, peer: %s", time.Since(rf.impl.lastPingTime), rf.impl.electionTimeout, rf.me))
		if rf.impl.state == FOLLOWER {
			rf.impl.state = PRECANDIDATE
			rf.impl.leaderAddress = ""
			rf.impl.votedFor = ""
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
	LeaderHint string
}

func (rf *Raft) Get(args *GetArgs, reply *GetReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.LeaderHint = rf.impl.leaderAddress

	if rf.impl.state != LEADER {
		// log.Println("Called Get on peer:", rf.me, "leaderAddress:", rf.impl.leaderAddress)
		return nil
	}

	log.Println("Log after calling Get on leader:", rf.impl.log, "waiting for readIndex to be applied:", rf.impl.leaderEntry)

	for !rf.isdead() && rf.impl.state == LEADER && (!rf.impl.leaderCommitted || rf.impl.lastApplied < rf.impl.leaderEntry) {
		rf.impl.commitCond.Wait()
	}

	if rf.impl.state != LEADER {
		reply.LeaderHint = rf.impl.leaderAddress
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
	LeaderHint string
}

func (rf *Raft) Put(args *PutArgs, reply *PutReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.LeaderHint = rf.impl.leaderAddress

	if rf.impl.state != LEADER {
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
	// log.Println("Log after calling PUT on leader:", rf.impl.log, "waiting for logIndex to be committed:", logIndex)

	for !rf.isdead() && rf.impl.state == LEADER && rf.impl.lastApplied < logIndex {
		rf.impl.commitCond.Wait()
	}

	if rf.impl.state != LEADER {
		reply.Success = false
		reply.LeaderHint = rf.impl.leaderAddress
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
	LeaderHint string
}

func (rf *Raft) Append(args *AppendArgs, reply *AppendReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.LeaderHint = rf.impl.leaderAddress

	if rf.impl.state != LEADER {
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
	// log.Println("Log after calling APPEND on leader:", rf.impl.log, "waiting for logIndex to be committed:", logIndex)

	for !rf.isdead() && rf.impl.state == LEADER && rf.impl.lastApplied < logIndex {
		rf.impl.commitCond.Wait()
	}

	if rf.impl.state != LEADER {
		reply.Success = false
		reply.LeaderHint = rf.impl.leaderAddress
		return nil
	}

	reply.Success = true

	return nil
}

func (rf *Raft) commitLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.isdead() {
		rf.impl.commitCond.Wait()
		for index := rf.impl.lastApplied; index <= rf.impl.commitIndex; index++ {
			rf.applyLog(index) // adjusted index
		}
		rf.impl.lastApplied = rf.impl.commitIndex

		rf.sendSnapshotToStorage()

		rf.impl.commitCond.Broadcast()
		// log.Println("Current state after applying logs: peer idx:", rf.me)
		// for clientId, session := range rf.impl.clientSessions {
		// 	log.Println("peer idx:", rf.me, "ClientId:", clientId, "session state:", *session)
		// }
	}
}

func (rf *Raft) sendSnapshotToStorage() {
	// create snapshot from start of log to commit index
	appliedIndex := rf.impl.lastApplied - rf.impl.log[0].Index
	appliedLog := rf.impl.log[appliedIndex]
	snapshot := &Snapshot{appliedLog.Index, appliedLog.Term, rf.impl.log[:appliedIndex]}
	// call snapshot service using this snapshot
	rf.impl.snapshotService.writeSnapshotToDisk(snapshot)
	// retain commit index - 1's log entry, and retain the last log configuration in the snapshotted log
	// rf.impl.log = rf.impl.log[appliedIndex:]
	rf.impl.lastIncluded = appliedIndex
}

type RegisterClientArgs struct {
}

type RegisterClientReply struct {
	Success    bool
	LeaderHint string
	ClientId   int
}

func (rf *Raft) RegisterClient(args *RegisterClientArgs, reply *RegisterClientReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.LeaderHint = rf.impl.leaderAddress

	if rf.impl.leaderAddress != rf.me {
		return nil
	}

	for !rf.isdead() && rf.impl.state == LEADER && (!rf.impl.leaderCommitted || rf.impl.lastApplied < rf.impl.leaderEntry) {
		rf.impl.commitCond.Wait()
	}

	reply.ClientId = rf.impl.clientIndex

	rf.impl.log = append(rf.impl.log, LogEntry{REGISTERCLIENT, "REGISTERCLIENT", "", rf.impl.currentTerm, rf.impl.lastLogIndex() + 1, rf.impl.clientIndex, 0})
	rf.impl.clientIndex++

	rf.impl.nextIndex[rf.me] = rf.impl.lastLogIndex()
	logIndex := rf.impl.lastLogIndex()

	// log.Println("Log after calling REGISTERCLIENT on leader:", rf.impl.log, "waiting for logIndex to be committed:", logIndex)

	for !rf.isdead() && rf.impl.state == LEADER && rf.impl.lastApplied < logIndex {
		rf.impl.commitCond.Wait()
	}

	if rf.impl.state != LEADER {
		reply.Success = false
		reply.ClientId = 0
		reply.LeaderHint = rf.impl.leaderAddress
		return nil
	}

	reply.Success = true

	// log.Println("Returning from RegisterClient, with reply:", reply)

	return nil
}

type RegisterServerArgs struct {
	Peer     string
	ClientId int
}

type RegisterServerReply struct {
	Success    bool
	LeaderHint string
}

func (rf *Raft) RegisterServer(args *RegisterServerArgs, reply *RegisterServerReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.LeaderHint = rf.impl.leaderAddress

	if rf.impl.state != LEADER {
		return nil
	}

	// log.Println("Catching up new peer:", args.Peer, "sending append entries")
	if args.Peer != rf.me {
		// catch up new server
		rf.impl.nextIndex[args.Peer] = rf.impl.lastLogIndex()
		rf.impl.matchIndex[args.Peer] = 0
		prevSuccess := 1
		for lastSendAppliedEntries := time.Now(); rf.impl.state == LEADER && (prevSuccess == 0 || (prevSuccess == 1 && time.Since(lastSendAppliedEntries) > rf.impl.electionTimeout)); lastSendAppliedEntries = time.Now() {
			rf.mu.Unlock()
			success := 0
			rf.sendAppendEntriesToPeer(args.Peer, &success)
			rf.mu.Lock()
			prevSuccess = success
		}
	}

	if rf.impl.state != LEADER {
		reply.Success = false
		reply.LeaderHint = rf.impl.leaderAddress
		return nil
	}

	// wait for previous configuration to be completed. TODO: add timeout configuration
	for !rf.isdead() && rf.impl.state == LEADER && rf.impl.commitIndex < rf.impl.lastAppliedPeerChange {
		rf.impl.commitCond.Wait()
	}

	if rf.impl.state != LEADER {
		reply.Success = false
		reply.LeaderHint = rf.impl.leaderAddress
		return nil
	}

	rf.impl.log = append(rf.impl.log, LogEntry{REGISTERSERVER, "REGISTERSERVER", args.Peer, rf.impl.currentTerm, rf.impl.lastLogIndex() + 1, rf.impl.clientIndex, args.ClientId})
	// need to apply register server to configuration before getting committed
	rf.impl.peers[args.Peer] = struct{}{}

	rf.impl.nextIndex[rf.me] = rf.impl.lastLogIndex()
	logIndex := rf.impl.lastLogIndex()

	// log.Println("Log after calling REGISTERSERVER on leader:", rf.impl.log, "waiting for logIndex to be committed:", logIndex)

	for !rf.isdead() && rf.impl.state == LEADER && rf.impl.lastApplied < logIndex {
		rf.impl.commitCond.Wait()
	}

	if rf.impl.state != LEADER {
		reply.Success = false
		reply.LeaderHint = rf.impl.leaderAddress
		return nil
	}

	reply.Success = true

	// log.Println("Returning from RegisterServer, with reply: ", reply)

	return nil
}
