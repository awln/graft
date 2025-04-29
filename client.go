package graft

import (
	"math/rand"
	"sync"
	"time"
)

type Client struct {
	mu              *sync.Mutex
	rfServers       []*Raft
	rfPorts         []string
	nraft           int64
	seq             int
	lastSeqFinished int
	srv             string
	clientId        int
}

func NewClient(rfServers []*Raft, rfPorts []string, nraft int) *Client {
	c := &Client{&sync.Mutex{}, rfServers, rfPorts, int64(nraft), 0, 0, "", -1}
	c.mu.Lock()
	defer c.mu.Unlock()
	registerReply := &RegisterClientReply{}
	if c.srv == "" {
		c.srv = c.rfPorts[rand.Int63()%c.nraft]
	}
	for {
		callSuccess := Call(c.srv, "Raft.RegisterClient", &RegisterClientArgs{}, registerReply)
		if callSuccess {
			if !registerReply.Success {
				c.srv = registerReply.LeaderHint
			} else {
				c.clientId = registerReply.ClientId
				return c
			}
		} else {
			c.srv = c.rfPorts[rand.Int63()%c.nraft]
		}
	}
}

func (c *Client) Put(key string, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	putReply := PutReply{}
	if c.srv == "" {
		c.srv = c.rfPorts[rand.Int63()%c.nraft]
	}
	for {
		callSuccess := Call(c.srv, "Raft.Put", &PutArgs{key, value, c.seq, c.clientId}, &putReply)
		if callSuccess {
			if !putReply.Success {
				c.srv = putReply.LeaderHint
			} else {
				c.seq++
				return
			}
		} else {
			time.Sleep(10 * time.Millisecond)
			c.srv = c.rfPorts[rand.Int63()%c.nraft]
		}
	}
}

func (c *Client) Append(key string, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	appendReply := AppendReply{}
	if c.srv == "" {
		c.srv = c.rfPorts[rand.Int63()%c.nraft]
	}
	for {
		callSuccess := Call(c.srv, "Raft.Append", &AppendArgs{key, value, c.seq, c.clientId}, &appendReply)
		if callSuccess {
			if !appendReply.Success {
				c.srv = appendReply.LeaderHint
			} else {
				c.seq++
				return
			}
		} else {
			time.Sleep(10 * time.Millisecond)
			c.srv = c.rfPorts[rand.Int63()%c.nraft]
		}
	}
}

func (c *Client) Get(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	getReply := GetReply{}
	if c.srv == "" {
		c.srv = c.rfPorts[rand.Int63()%c.nraft]
	}
	for {
		callSuccess := Call(c.srv, "Raft.Get", &GetArgs{key, c.seq, c.clientId}, &getReply)
		if callSuccess {
			if !getReply.Success {
				c.srv = getReply.LeaderHint
			} else {
				c.seq++
				return getReply.Value
			}
		} else {
			time.Sleep(10 * time.Millisecond)
			c.srv = c.rfPorts[rand.Int63()%c.nraft]
		}
	}
}

func (c *Client) RegisterServer(peer string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	regsisterServerReply := &RegisterServerReply{}
	if c.srv == "" {
		c.srv = c.rfPorts[rand.Int63()%c.nraft]
	}
	for {
		callSuccess := Call(c.srv, "Raft.RegisterServer", &RegisterServerArgs{peer, c.clientId}, &regsisterServerReply)
		if callSuccess {
			if !regsisterServerReply.Success {
				c.srv = regsisterServerReply.LeaderHint
			} else {
				c.seq++
				return
			}
		} else {
			time.Sleep(10 * time.Millisecond)
			c.srv = c.rfPorts[rand.Int63()%c.nraft]
		}
	}
}
