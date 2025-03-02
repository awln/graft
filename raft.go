package graft

//
// raft library, to be included in an application.
// Multiple applications will run, each including
// a raft peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, etc.).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// rf = raft.Make(peers []string, me string)
// rf.Start(seq int, v interface{}) -- start agreement on new instance
// rf.Status(seq int) (Fate, v interface{}) -- get info about an instance
// rf.Done(seq int) -- ok to forget all instances <= seq
// rf.Max() int -- highest instance seq known, or -1
// rf.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// rf.Status() return values, indicating
// whether an agreement has been decided,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Raft struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int32 // index into peers[]
	impl       RaftImpl
}

type Error struct {
}

// tell the peer to shut itself down.
// for testing.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	if rf.l != nil {
		rf.l.Close()
	}
}

// has this peer been asked to shut down?
func (rf *Raft) isdead() bool {
	return atomic.LoadInt32(&rf.dead) != 0
}

func (rf *Raft) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&rf.unreliable, 1)
	} else {
		atomic.StoreInt32(&rf.unreliable, 0)
	}
}

func (rf *Raft) isunreliable() bool {
	return atomic.LoadInt32(&rf.unreliable) != 0
}

func (rf *Raft) initImpl() {
	// TODO: write initialization here
	rf.impl.currentTerm = 0
	rf.impl.electionTimeout = time.Millisecond * 150
	rf.impl.state = FOLLOWER
	rf.impl.electionCond = sync.NewCond(&rf.mu)
	rf.impl.log = append(rf.impl.log, LogEntry{NOOP, "", "", 0})

	go rf.electionTimer()
	go rf.electionLoop()
}

// the application wants to create a raft peer.
// the ports of all the raft peers (including this one)
// are in peers[]. this server's port is peers[me].
func Make(peers []string, me int32, rpcs *rpc.Server) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	rf.initImpl()

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(rf)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(rf)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		rf.l = l

		// create a thread to accept RPC connections
		go func() {
			for rf.isdead() == false {
				conn, err := rf.l.Accept()
				if err == nil && rf.isdead() == false {
					if rf.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if rf.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&rf.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&rf.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && rf.isdead() == false {
					fmt.Printf("Raft(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return rf
}
