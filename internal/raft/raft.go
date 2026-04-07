package raft

import (
	"sync"
	"time"

	"github.com/user/luminadb/internal/transport"
	"github.com/user/luminadb/proto"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Node represents a single server in the Raft cluster
type Node struct {
	mu sync.Mutex

	id    string
	peers []string
	state State

	// Persisted state
	currentTerm uint64
	votedFor    string
	log         []*proto.LogEntry

	// Volatile state
	commitIndex uint64
	lastApplied uint64

	// Leader specific volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Timers
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker

	// Channels for communication
	applyCh chan *proto.LogEntry
	client  *transport.RaftClient
}

func NewNode(id string, peers []string, applyCh chan *proto.LogEntry) *Node {
	return &Node{
		id:          id,
		peers:       peers,
		state:       Follower,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*proto.LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		applyCh:     applyCh,
		client:      transport.NewRaftClient(),
	}
}

// Start initializes the node's timers and starts the main loop
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()

	go n.run()
}

func (n *Node) run() {
	for {
		n.mu.Lock()
		state := n.state
		n.mu.Unlock()

		switch state {
		case Follower:
			select {
			case <-n.electionTimer.C:
				n.startElection()
			}
		case Candidate:
			select {
			case <-n.electionTimer.C:
				n.startElection()
			}
			// Handling votes will be done in separate goroutines
		case Leader:
			// Leaders send heartbeats
			time.Sleep(50 * time.Millisecond)
			n.sendHeartbeats()
		}
	}
}

func (n *Node) resetElectionTimer() {
	// Random timeout between 150ms and 300ms
	timeout := time.Duration(150+ (time.Now().UnixNano() % 150)) * time.Millisecond
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(timeout)
	} else {
		if !n.electionTimer.Stop() {
			select {
			case <-n.electionTimer.C:
			default:
			}
		}
		n.electionTimer.Reset(timeout)
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.resetElectionTimer()
	term := n.currentTerm
	lastLogIndex := uint64(len(n.log))
	var lastLogTerm uint64
	if lastLogIndex > 0 {
		lastLogTerm = n.log[lastLogIndex-1].Term
	}
	n.mu.Unlock()

	votes := 1
	for _, peer := range n.peers {
		go func(peerAddr string) {
			req := &proto.VoteRequest{
				Term:         term,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			resp, err := n.client.RequestVote(peerAddr, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.state = Follower
				n.votedFor = ""
				return
			}

			if n.state == Candidate && resp.VoteGranted && term == n.currentTerm {
				votes++
				if votes > (len(n.peers)+1)/2 {
					n.state = Leader
					n.resetHeartbeatTimer() // Initialize leader state
				}
			}
		}(peer)
	}
}

func (n *Node) resetHeartbeatTimer() {
	for _, peer := range n.peers {
		n.nextIndex[peer] = uint64(len(n.log)) + 1
		n.matchIndex[peer] = 0
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	leaderId := n.id
	commitIndex := n.commitIndex
	peers := n.peers
	n.mu.Unlock()

	for _, peer := range peers {
		go func(peerAddr string) {
			n.mu.Lock()
			prevLogIndex := n.nextIndex[peerAddr] - 1
			var prevLogTerm uint64
			if prevLogIndex > 0 && prevLogIndex <= uint64(len(n.log)) {
				prevLogTerm = n.log[prevLogIndex-1].Term
			}
			// Simplified: send no entries for heartbeat
			req := &proto.AppendEntriesRequest{
				Term:         term,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: commitIndex,
				Entries:      nil,
			}
			n.mu.Unlock()

			resp, err := n.client.AppendEntries(peerAddr, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.state = Follower
				n.votedFor = ""
				return
			}
		}(peer)
	}
}

// Handler for RequestVote RPC
func (n *Node) HandleRequestVote(req *proto.VoteRequest) *proto.VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &proto.VoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = ""
	}

	canVote := n.votedFor == "" || n.votedFor == req.CandidateId
	
	// Check if candidate's log is at least as up-to-date
	lastIndex := uint64(len(n.log))
	var lastTerm uint64
	if lastIndex > 0 {
		lastTerm = n.log[lastIndex-1].Term
	}

	isUpToDate := req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)

	if canVote && isUpToDate {
		n.votedFor = req.CandidateId
		n.resetElectionTimer()
		resp.VoteGranted = true
	}

	resp.Term = n.currentTerm
	return resp
}

// Handler for AppendEntries RPC
func (n *Node) HandleAppendEntries(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &proto.AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	if req.Term < n.currentTerm {
		return resp
	}

	n.resetElectionTimer()

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = ""
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if req.PrevLogIndex > 0 {
		if uint64(len(n.log)) < req.PrevLogIndex {
			return resp
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			return resp
		}
	}

	// 3 & 4. Append entries
	for i, entry := range req.Entries {
		index := req.PrevLogIndex + uint64(i) + 1
		if uint64(len(n.log)) >= index {
			if n.log[index-1].Term != entry.Term {
				n.log = n.log[:index-1]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	// 5. Update commitIndex
	if req.LeaderCommit > n.commitIndex {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNewIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
		n.applyCommitted()
	}

	resp.Success = true
	resp.Term = n.currentTerm
	return resp
}

func (n *Node) Propose(key string, value []byte, opType proto.LogEntry_CommandType) bool {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return false
	}

	entry := &proto.LogEntry{
		Index: uint64(len(n.log)) + 1,
		Term:  n.currentTerm,
		Type:  opType,
		Key:   key,
		Value: value,
	}
	n.log = append(n.log, entry)
	term := n.currentTerm
	index := entry.Index
	n.mu.Unlock()

	// Wait for entry to be committed (simplified: polling)
	for {
		time.Sleep(10 * time.Millisecond)
		n.mu.Lock()
		if n.currentTerm != term || n.state != Leader {
			n.mu.Unlock()
			return false
		}
		if n.commitIndex >= index {
			n.mu.Unlock()
			return true
		}
		n.mu.Unlock()
	}
}

func (n *Node) GetLeader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	// In a real implementation, we'd track who the leader is from AppendEntries
	return "unknown"
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		n.applyCh <- entry
	}
}
