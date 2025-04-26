package graft

import (
	"fmt"
	"math/rand"
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

	fmt.Printf("Test: Sequence of puts, unreliable ...\n")

	for iters := 0; iters < 6; iters++ {
		const ncli = 5
		var ca [ncli]chan bool
		for cli := 0; cli < ncli; cli++ {
			ca[cli] = make(chan bool)
			go func(me int) {
				ok := false
				defer func() { ca[me] <- ok }()
				key := strconv.Itoa(me)
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
					t.Fatalf("wrong value, key %s, got %s, expected %s", key, v, vv)
				}
				ok = true
			}(cli)
		}
		for cli := 0; cli < ncli; cli++ {
			x := <-ca[cli]
			if x == false {
				t.Fatalf("failure")
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent clients, unreliable ...\n")

	var clients []*Client = make([]*Client, nservers)

	for idx := range nservers {
		clients[idx] = NewClient(rfServers, rfPorts, nservers)
	}

	for iters := 0; iters < 20; iters++ {
		const ncli = 15
		var ca [ncli]chan bool
		for cli := 0; cli < ncli; cli++ {
			ca[cli] = make(chan bool)
			go func(me int) {
				defer func() { ca[me] <- true }()
				myck := clients[rand.Intn(nservers)]
				if (rand.Int() % 1000) < 500 {
					myck.Put("b", strconv.Itoa(rand.Int()))
				} else {
					myck.Get("b")
				}
			}(cli)
		}
		for cli := 0; cli < ncli; cli++ {
			<-ca[cli]
		}

		var va [nservers]string
		for i := 0; i < nservers; i++ {
			va[i] = clients[rand.Intn(nservers)].Get("b")
			if va[i] != va[0] {
				t.Fatalf("mismatch; 0 got %v, %v got %v", va[0], i, va[i])
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent Append to same key, unreliable ...\n")

	client.Put("k", "")

	ff := func(me int, ch chan int) {
		ret := -1
		defer func() { ch <- ret }()
		myck := clients[rand.Intn(nservers)]
		n := 0
		for n < 5 {
			myck.Append("k", "x "+strconv.Itoa(me)+" "+strconv.Itoa(n)+" y")
			n++
		}
		ret = n
	}

	ncli := 5
	cha := []chan int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan int))
		go ff(i, cha[i])
	}

	counts := []int{}
	for i := 0; i < ncli; i++ {
		n := <-cha[i]
		if n < 0 {
			t.Fatal("client failed")
		}
		counts = append(counts, n)
	}

	vx := client.Get("k")
	checkAppends(t, vx, counts)

	{
		for i := 0; i < nservers; i++ {
			vi := clients[rand.Intn(nservers)].Get("k")
			if vi != vx {
				t.Fatalf("mismatch; 0 got %v, %v got %v", vx, i, vi)
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	time.Sleep(1 * time.Second)
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
	if client.Get(key) != "value2" {
		t.Errorf("Servers should store 'value2' for the key: 'key2'")
	} else {
		fmt.Println("Leader currently stores value2 for key2")
	}
}

func cleanup(rfServers []*Raft) {
	for i := 0; i < len(rfServers); i++ {
		if rfServers[i] != nil {
			rfServers[i].Kill()
		}
	}
}
