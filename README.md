# Graft
Raft implementation in Golang. The boilerplate code, including test suites, RPC server initialization, is based off MIT 6.824 2015 Paxos. 

Implementation and features are described in
Raft dissertation: https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf. Paper contains recommended threading model and alternatives for RPC implementations.


## Features
* Leader Election is separated into a Prepare-Accept approach to prevent partitioned node from forcing up-to-date leaders to retrigger an election (section 9.6). Additonal Raft thread: https://groups.google.com/g/raft-dev/c/JEtBYaPpHXo
* Read queries are optimized to wait for the leader's no-op entry to be applied instead of all log indices.
* Log compaction and snapshotting is based off an in-memory data structure Raft Finite State Machine. Currently using WAL (write ahead log) for log storage and BoltDB as the metadata engine for persistence. Log entries are committed to persisted storage before returning control to the client.
* Supports AppendEntries, RequestVote, RegisterClient, RegisterServer, and PUT/GET/APPEND RPCs.

## Future Optimizations and Features
* 

# Raft Abstract
Raft is a consensus algorithm for managing a replicated
log. Raft separates the key elements of consensus, such as
leader election, log replication, and safety, and it enforces
a stronger degree of coherency to reduce the number of
states that must be considered. Similar to Multi-Paxos, Raft
relies on the pigeonhole principle and overlapping majorities to guarantee safety and 
visibility of changes across nodes. Leader elections are rejected if candidate logs are not up to date.
AppendEntries can only be committed if they are replicated across a majority of nodes.
