# Graft
Raft implementation in Golang. The boilerplate code, including test suites, RPC server initialization, is based off MIT 6.824 2015 Paxos. 

Implementation and features are described in
Raft dissertation: https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf. Paper contains recommended threading model and alternatives for RPC implementations.

Leader Election is separated into a Prepare-Accept approach to prevent partitioned node from forcing up-to-date leaders to step down, violating the Leader Completeness property.

# Raft Abstract
Raft is a consensus algorithm for managing a replicated
log. Raft separates the key elements of consensus, such as
leader election, log replication, and safety, and it enforces
a stronger degree of coherency to reduce the number of
states that must be considered. Similar to Multi-Paxos, Raft
relies on the pigeonhole principle and overlapping majorities to guarantee safety and 
visibility of changes across nodes. Leader elections are rejected if candidate logs are not up to date.
AppendEntries can only be committed if they are replicated across a majority of nodes.
