package graft

import (
	"log"
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
	registerReply := &RegisterClientReply{}
	if c.srv == "" {
		c.srv = c.rfPorts[rand.Int63()%c.nraft]
	}
	for {
		callSuccess := Call(c.srv, "Raft.RegisterClient", &RegisterClientArgs{}, registerReply)
		if callSuccess {
			if !registerReply.Success && registerReply.LeaderHint != -1 {
				c.srv = c.rfPorts[registerReply.LeaderHint]
			} else if !registerReply.Success {
				time.Sleep(10 * time.Millisecond)
				c.srv = c.rfPorts[rand.Int63()%c.nraft]
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
			if !putReply.Success && putReply.LeaderHint != -1 {
				c.srv = c.rfPorts[putReply.LeaderHint]
			} else if !putReply.Success {
				time.Sleep(10 * time.Millisecond)
				c.srv = c.rfPorts[rand.Int63()%c.nraft]
				log.Println("Calling Put on random peer", c.srv)
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
			if !appendReply.Success && appendReply.LeaderHint != -1 {
				c.srv = c.rfPorts[appendReply.LeaderHint]
			} else if !appendReply.Success {
				time.Sleep(10 * time.Millisecond)
				c.srv = c.rfPorts[rand.Int63()%c.nraft]
				log.Println("Calling Append on random peer", c.srv)
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
			if !getReply.Success && getReply.LeaderHint != -1 {
				c.srv = c.rfPorts[getReply.LeaderHint]
			} else if !getReply.Success {
				time.Sleep(10 * time.Millisecond)
				c.srv = c.rfPorts[rand.Int63()%c.nraft]
				// log.Println("Calling Get on random peer", c.srv)
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
