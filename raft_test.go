package graft

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nraft = 3
	var rfServers []*Raft = make([]*Raft, nraft)
	var rfPorts []string = make([]string, nraft)

	defer cleanup(rfServers)

	for i := 0; i < nraft; i++ {
		rfPorts[i] = port("basic", i)
	}
	for i := 0; i < nraft; i++ {
		rfServers[i] = Make(rfPorts, int32(i), nil)
	}
	rfServers[0].convertToLeader()

	time.Sleep(2 * time.Second)

	fmt.Printf("Test: Single proposer ...\n")
}

func TestAppendEntries(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nraft = 3
	var rfServers []*Raft = make([]*Raft, nraft)
	var rfPorts []string = make([]string, nraft)

	defer cleanup(rfServers)

	for i := 0; i < nraft; i++ {
		rfPorts[i] = port("AppendEntries", i)
	}
	for i := 0; i < nraft; i++ {
		rfServers[i] = Make(rfPorts, int32(i), nil)
	}
	rfServers[0].convertToLeader()

	time.Sleep(2 * time.Second)

	srv := rfPorts[0]
	putReply := PutReply{}
	for !Call(srv, "Raft.Put", &PutArgs{"key1", "value1", 0, 0}, &putReply) || !putReply.Success {
		if !putReply.Success {
			srv = rfPorts[putReply.LeaderHint]
		}
	}

	time.Sleep(2 * time.Second)

	fmt.Println("Calling Get on Raft cluster for 'key1'")

	getReply := GetReply{}
	for i := 0; i < nraft; i++ {
		for !Call(srv, "Raft.Get", &GetArgs{"key1", 1, 0}, &getReply) || !getReply.Success {
			if !getReply.Success {
				srv = rfPorts[getReply.LeaderHint]
			} else {
				if getReply.Value != "value1" {
					t.Errorf("Servers should store 'value1' for the key: 'key1'")
				}
			}
		}
	}

	fmt.Printf("Test: Single AppendEntries ...\n")
}

func cleanup(rfServers []*Raft) {
	for i := 0; i < len(rfServers); i++ {
		if rfServers[i] != nil {
			rfServers[i].Kill()
		}
	}
}
