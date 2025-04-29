package graft

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

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
		rfServers[i] = Make(rfPorts, rfPorts[i], nil)
	}
	rfServers[0].convertToLeader()
	client := NewClient(rfServers, rfPorts, nraft)

	for i := 0; i < nraft; i++ {
		client.RegisterServer(rfPorts[i])
	}

	client.Put("key1", "value1")

	time.Sleep(2 * time.Second)

	fmt.Println("Calling Get on Raft cluster for 'key1'")

	if val := client.Get("key1"); val != "value1" {
		t.Errorf("Servers should store 'value1' for the key: 'key1', observed value: %s", val)
	} else {
		fmt.Println("Leader currently stores value1 for key1")
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
		rfServers[i] = Make(rfPorts, rfPorts[i], nil)
		rfServers[i].setunreliable(true)
	}
	rfServers[0].convertToLeader()
	client := NewClient(rfServers, rfPorts, nraft)

	for i := 0; i < nraft; i++ {
		client.RegisterServer(rfPorts[i])
	}

	client.Put("key1", "value1")

	time.Sleep(2 * time.Second)

	fmt.Println("Calling Get on Raft cluster for 'key1'")

	if val := client.Get("key1"); val != "value1" {
		t.Errorf("Servers should store 'value1' for the key: 'key1', observed value: %s", val)
	} else {
		fmt.Println("Leader currently stores value1 for key1")
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
		rfServers[i] = Make(rfPorts, rfPorts[i], nil)
		rfServers[i].setunreliable(true)
	}
	rfServers[0].convertToLeader()
	client := NewClient(rfServers, rfPorts, nraft)

	for i := 0; i < nraft; i++ {
		client.RegisterServer(rfPorts[i])
	}

	time.Sleep(2 * time.Second)

	client.Put("key1", "value1")
	client.Put("key2", "value1")

	time.Sleep(2 * time.Second)

	fmt.Println("Calling Get on Raft cluster for 'key1'")

	if val := client.Get("key1"); val != "value1" {
		t.Errorf("Servers should store 'value1' for the key: 'key1', observed value: %s", val)
	} else {
		fmt.Println("Leader currently stores value1 for key1")
	}

	if val := client.Get("key2"); val != "value1" {
		t.Errorf("Servers should store 'value1' for the key: 'key2', observed value: %s", val)
	} else {
		fmt.Println("Leader currently stores value1 for key1")
	}

	client.Put("key1", "value2")
	client.Put("key2", "value2")

	if val := client.Get("key1"); val != "value2" {
		t.Errorf("Servers should store 'value2' for the key: 'key1', observed value: %s", val)
	} else {
		fmt.Println("Leader currently stores value2 for key1")
	}

	if val := client.Get("key2"); val != "value2" {
		t.Errorf("Servers should store 'value2' for the key: 'key2', observed value: %s", val)
	} else {
		fmt.Println("Leader currently stores value2 for key1")
	}

	cleanup(rfServers)

	fmt.Printf("Test: Long AppendEntries Unreliable Passed...\n")
}

func TestAppendEntriesTimeoutUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nraft = 5
	var rfServers []*Raft = make([]*Raft, nraft)
	var rfPorts []string = make([]string, nraft)

	defer cleanup(rfServers)

	for i := 0; i < nraft; i++ {
		rfPorts[i] = port("AppendEntries", i)
	}
	for i := 0; i < nraft; i++ {
		rfServers[i] = Make(rfPorts, rfPorts[i], nil)
		rfServers[i].impl.electionTimeout = time.Millisecond * 60
		rfServers[i].setunreliable(true)
	}
	rfServers[0].convertToLeader()
	client := NewClient(rfServers, rfPorts, nraft)

	for i := 0; i < nraft; i++ {
		client.RegisterServer(rfPorts[i])
	}

	time.Sleep(2 * time.Second)

	client.Put("key1", "value1")
	client.Put("key2", "value1")

	time.Sleep(2 * time.Second)

	fmt.Println("Calling Get on Raft cluster for 'key1'")

	if val := client.Get("key1"); val != "value1" {
		t.Errorf("Servers should store 'value1' for the key: 'key1', observed value: %s", val)
	} else {
		fmt.Println("Leader currently stores value1 for key1")
	}

	if val := client.Get("key2"); val != "value1" {
		t.Errorf("Servers should store 'value1' for the key: 'key2', observed value: %s", val)
	} else {
		fmt.Println("Leader currently stores value1 for key1")
	}

	client.Put("key1", "value2")
	client.Put("key2", "value2")

	if val := client.Get("key1"); val != "value2" {
		t.Errorf("Servers should store 'value2' for the key: 'key1', observed value: %s", val)
	} else {
		fmt.Println("Leader currently stores value2 for key1")
	}

	if val := client.Get("key2"); val != "value2" {
		t.Errorf("Servers should store 'value2' for the key: 'key2', observed value: %s", val)
	} else {
		fmt.Println("Leader currently stores value2 for key1")
	}

	cleanup(rfServers)

	fmt.Printf("Test: Short Timeout AppendEntries Unreliable Passed...\n")
}

func TestConcurrentLongUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nservers = 5
	var rfServers []*Raft = make([]*Raft, nservers)
	var rfPorts []string = make([]string, nservers)
	defer cleanup(rfServers)

	for i := 0; i < nservers; i++ {
		rfPorts[i] = port("un", i)
	}
	for i := 0; i < nservers; i++ {
		rfServers[i] = Make(rfPorts, rfPorts[i], nil)
		rfServers[i].setunreliable(true)
	}
	rfServers[0].convertToLeader()
	client := NewClient(rfServers, rfPorts, nservers)

	for i := 0; i < nservers; i++ {
		client.RegisterServer(rfPorts[i])
	}

	fmt.Printf("Test: Basic put/get, unreliable ...\n")

	client.Put("a", "aa")

	check(t, client, "a", "aa")

	client.Put("a", "aaa")

	check(t, client, "a", "aaa")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent appends, unreliable ...\n")

	for iters := 0; iters < 6; iters++ {
		const ncli = 5
		var ca [ncli]chan bool
		for cli := 0; cli < ncli; cli++ {
			ca[cli] = make(chan bool)
			go func(me int) {
				ok := false
				defer func() { ca[me] <- ok }()
				key := strconv.Itoa(me)
				client := NewClient(rfServers, rfPorts, nservers)
				vv := client.Get(key)
				client.Append(key, "0")
				vv = NextValue(vv, "0")
				client.Append(key, "1")
				vv = NextValue(vv, "1")
				client.Append(key, "2")
				vv = NextValue(vv, "2")
				time.Sleep(100 * time.Millisecond)
				v := client.Get(key)
				if v != vv {
					t.Errorf("wrong value for client %d, key %s, got %s, expected %s", client.clientId, key, v, vv)
				}
				ok = true
			}(cli)
		}
		for cli := 0; cli < ncli; cli++ {
			x := <-ca[cli]
			if x == false {
				t.Errorf("failure")
			}
		}
	}

	// cleanup(rfServers)

	fmt.Printf("  ... Passed\n")
}

// predict effect of Append(k, val) if old value is prev.
func NextValue(prev string, val string) string {
	return prev + val
}

func checkAppends(t *testing.T, v string, counts []int) {
	nclients := len(counts)
	for i := 0; i < nclients; i++ {
		lastoff := -1
		for j := 0; j < counts[i]; j++ {
			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
			off := strings.Index(v, wanted)
			if off < 0 {
				t.Fatalf("missing element in Append result")
			}
			off1 := strings.LastIndex(v, wanted)
			if off1 != off {
				t.Fatalf("duplicate element in Append result")
			}
			if off <= lastoff {
				t.Fatalf("wrong order for element in Append result")
			}
			lastoff = off
		}
	}
}

func check(t *testing.T, client *Client, key string, value string) {
	if client.Get(key) != value {
		t.Errorf("Servers should store '%s' for the key: '%s'", value, key)
	} else {
		fmt.Printf("Leader currently stores %s for %s\n", value, key)
	}
}

func cleanup(rfServers []*Raft) {
	for i := 0; i < len(rfServers); i++ {
		if rfServers[i] != nil {
			rfServers[i].Kill()
		}
	}
}
