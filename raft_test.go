package graft

import (
	"fmt"
	"runtime"
	"testing"
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
		rfServers[i] = Make(rfPorts, i, nil)
	}

	fmt.Printf("Test: Single proposer ...\n")
}

func cleanup(rfServers []*Raft) {
	for i := 0; i < len(rfServers); i++ {
		if rfServers[i] != nil {
			rfServers[i].Kill()
		}
	}
}
