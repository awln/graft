package graft

import (
	"fmt"
	"math/rand"
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
	for {
		callSuccess := Call(srv, "Raft.Get", &GetArgs{"key1", 1, 0}, &getReply)
		if callSuccess {
			if !getReply.Success {
				srv = rfPorts[getReply.LeaderHint]
			} else {
				if getReply.Value != "value1" {
					t.Errorf("Servers should store 'value1' for the key: 'key1'")
				} else {
					fmt.Println("Leader currently stores value1 for key1")
					break
				}
			}
		}
	}

	cleanup(rfServers)

	fmt.Printf("Test: Single AppendEntries ...\n")
}

func TestAppendEntriesUnreliable(t *testing.T) {
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
		rfServers[i].setunreliable(true)
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
	for {
		callSuccess := Call(srv, "Raft.Get", &GetArgs{"key1", 1, 0}, &getReply)
		if callSuccess {
			if !getReply.Success {
				srv = rfPorts[getReply.LeaderHint]
			} else {
				if getReply.Value != "value1" {
					t.Errorf("Servers should store 'value1' for the key: 'key1'")
				} else {
					fmt.Println("Leader currently stores value1 for key1")
					break
				}
			}
		}
	}

	cleanup(rfServers)

	fmt.Printf("Test: Single AppendEntries Unreliable ...\n")
}

func TestAppendEntriesLongUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nraft = 5
	var rfServers []*Raft = make([]*Raft, nraft)
	var rfPorts []string = make([]string, nraft)

	defer cleanup(rfServers)

	for i := 0; i < nraft; i++ {
		rfPorts[i] = port("AppendEntries", i)
	}
	for i := 0; i < nraft; i++ {
		rfServers[i] = Make(rfPorts, int32(i), nil)
		rfServers[i].setunreliable(true)
	}
	rfServers[0].convertToLeader()

	time.Sleep(2 * time.Second)

	srv := rfPorts[0]
	putReply := PutReply{}
	for !Call(srv, "Raft.Put", &PutArgs{"key1", "value1", 0, 0}, &putReply) || !putReply.Success {
		if !putReply.Success && putReply.LeaderHint != -1 {
			srv = rfPorts[putReply.LeaderHint]
		} else if !putReply.Success {
			srv = rfPorts[rand.Int63()%nraft]
		}
	}

	for !Call(srv, "Raft.Put", &PutArgs{"key2", "value1", 1, 0}, &putReply) || !putReply.Success {
		if !putReply.Success && putReply.LeaderHint != -1 {
			srv = rfPorts[putReply.LeaderHint]
		} else if !putReply.Success {
			srv = rfPorts[rand.Int63()%nraft]
		}
	}

	time.Sleep(2 * time.Second)

	fmt.Println("Calling Get on Raft cluster for 'key1'")

	getReply := GetReply{}
	for {
		callSuccess := Call(srv, "Raft.Get", &GetArgs{"key1", 2, 0}, &getReply)
		if callSuccess {
			if !getReply.Success && getReply.LeaderHint != -1 {
				srv = rfPorts[getReply.LeaderHint]
			} else if !getReply.Success {
				srv = rfPorts[rand.Int63()%nraft]
			} else {
				if getReply.Value != "value1" {
					t.Errorf("Servers should store 'value1' for the key: 'key1'")
				} else {
					fmt.Println("Leader currently stores value1 for key1")
					break
				}
			}
		}
	}

	srv = rfPorts[0]
	putReply = PutReply{}
	for !Call(srv, "Raft.Put", &PutArgs{"key1", "value2", 3, 0}, &putReply) || !putReply.Success {
		if !putReply.Success && putReply.LeaderHint != -1 {
			srv = rfPorts[putReply.LeaderHint]
		} else if !putReply.Success {
			srv = rfPorts[rand.Int63()%nraft]
		}
	}

	for !Call(srv, "Raft.Put", &PutArgs{"key2", "value2", 4, 0}, &putReply) || !putReply.Success {
		if !putReply.Success && putReply.LeaderHint != -1 {
			srv = rfPorts[putReply.LeaderHint]
		} else if !putReply.Success {
			srv = rfPorts[rand.Int63()%nraft]
		}
	}

	for {
		callSuccess := Call(srv, "Raft.Get", &GetArgs{"key1", 5, 0}, &getReply)
		if callSuccess {
			if !getReply.Success && getReply.LeaderHint != -1 {
				srv = rfPorts[getReply.LeaderHint]
			} else if !getReply.Success {
				srv = rfPorts[rand.Int63()%nraft]
			} else {
				if getReply.Value != "value2" {
					t.Errorf("Servers should store 'value2' for the key: 'key1'")
				} else {
					fmt.Println("Leader currently stores value2 for key1")
					break
				}
			}
		}
	}

	for {
		callSuccess := Call(srv, "Raft.Get", &GetArgs{"key2", 6, 0}, &getReply)
		if callSuccess {
			if !getReply.Success && getReply.LeaderHint != -1 {
				srv = rfPorts[getReply.LeaderHint]
			} else if !getReply.Success {
				srv = rfPorts[rand.Int63()%nraft]
			} else {
				if getReply.Value != "value2" {
					t.Errorf("Servers should store 'value2' for the key: 'key2'")
				} else {
					fmt.Println("Leader currently stores value2 for key2")
					break
				}
			}
		}
	}

	cleanup(rfServers)

	fmt.Printf("Test: Single AppendEntries Unreliable ...\n")
}

func cleanup(rfServers []*Raft) {
	for i := 0; i < len(rfServers); i++ {
		if rfServers[i] != nil {
			rfServers[i].Kill()
		}
	}
}
