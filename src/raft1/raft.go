package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int
	votedFor          *int // this can be nil if no candidate has been voted for in the current term
	currentRole       Role
	log               []LogEntry
	lastAppendEntries time.Time

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders to be re-init after election
	nextIndex  []int
	matchIndex []int

	applyChannel chan raftapi.ApplyMsg

	lastIncludedIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func hasMajority(arr []int, n int) bool {
	count := 0
	for _, value := range arr {
		if value >= n {
			count++
		}
	}
	return count > len(arr)/2
}

func (rf *Raft) logLength() int {
	return (len(rf.log) + rf.lastIncludedIndex)
}

func (rf *Raft) lastLogIndex() int {
	return (len(rf.log) + rf.lastIncludedIndex) - 1
}

func (rf *Raft) termAt(index int) int {
	return rf.log[index-rf.lastIncludedIndex].Term
}

func (rf *Raft) entryAt(index int) LogEntry {
	return rf.log[index-rf.lastIncludedIndex]
}

func (rf *Raft) commandAt(index int) interface{} {
	return rf.log[index-rf.lastIncludedIndex].Command
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentRole == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	if rf.votedFor != nil {
		e.Encode(*rf.votedFor)
	}
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
	DPrintf("Server %d persisted state for term %d, votedFor %v, log length %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil {
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = &votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XLen    int
	XIndex  int
	XTerm   int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentRole = Follower
		rf.votedFor = nil
		rf.persist()
	}

	if rf.votedFor == nil || *rf.votedFor == args.CandidateId {

		voterLastIndex := rf.lastLogIndex()
		voterLastTerm := rf.termAt(voterLastIndex)

		if args.LastLogTerm < voterLastTerm {
			return
		}

		// Here candidate probably has a lastlogterm equal to or greater than this server.
		// If the logs end with the same term, candidate must have a longer log.
		if args.LastLogTerm == voterLastTerm && args.LastLogIndex < voterLastIndex {
			return
		}

		// Here we know that
		// 1. The Candidate's LastLogTerm is greater than or equal to this server's LastLogTerm.
		// 2. If the LastLogTerm is equal, then the Candidate's log is at least as long as this server's log.
		// 3. Can safely grant the vote
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.votedFor = &args.CandidateId
		rf.persist()
		rf.lastAppendEntries = time.Now()
		DPrintf("Server %d granting vote to %d for term %d", rf.me, args.CandidateId, args.Term)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if len(args.Entries) > 0 {
		DPrintf("Server %d received AppendEntries from server %d for term %d. Log size %d", rf.me, args.LeaderId, args.Term, len(args.Entries))
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XLen = rf.logLength()

	if args.Term < rf.currentTerm {
		DPrintf("Server %d rejecting AppendEntries from server %d for term %d cus term too old", rf.me, args.LeaderId, args.Term)
		rf.mu.Unlock()
		return
	}

	rf.lastAppendEntries = time.Now()

	if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.votedFor = nil

		}
		rf.currentTerm = args.Term
		rf.currentRole = Follower
		reply.Term = args.Term
	}

	// Reply false if log doesnt contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= rf.logLength() {
		// does not contain an entry at prevLogIndex
		rf.mu.Unlock()
		DPrintf("Server %d rejecting AppendEntries from server %d for term %d cus log too short", rf.me, args.LeaderId, args.Term)
		return
	}

	// This handler will only update succcess to true if it
	// 1. prevLogIndex contains an entry and its term matches prevLogTerm
	// 2. if existing entry conflicts, delete the existing entry and all that follow it
	// 3. append any new entries not already in the log
	// 4. Commit.
	if rf.termAt(args.PrevLogIndex) == args.PrevLogTerm {
		reply.Success = true

		for i := range args.Entries {
			index := args.PrevLogIndex + 1 + i // actual raft index
			if index >= rf.logLength() {
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
			if rf.termAt(index) != args.Entries[i].Term {
				rf.log = append(rf.log[:(index-rf.lastIncludedIndex)], args.Entries[i:]...)
				rf.persist()
				break
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			var commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
			rf.commitIndex = commitIndex
		}
		rf.mu.Unlock()

	} else {
		// Conflicting terms at PrevLogIndex for this follower
		// So we find the first entry with the same term in the logs
		curr := args.PrevLogIndex
		for curr > rf.lastIncludedIndex && rf.termAt(curr) == rf.termAt(args.PrevLogIndex) {
			curr -= 1
		}
		reply.XTerm = rf.termAt(args.PrevLogIndex)
		reply.XIndex = curr + 1
		rf.mu.Unlock()
	}

}

func (rf *Raft) applyEntries() {
	for {
		rf.mu.Lock()
		var lastApplied = rf.lastApplied
		if lastApplied == rf.commitIndex {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		var nextIndex = lastApplied + 1
		command := rf.commandAt(nextIndex)
		rf.mu.Unlock()
		rf.applyChannel <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      command,

			CommandIndex: nextIndex,
		}
		rf.mu.Lock()
		rf.lastApplied = nextIndex
		rf.mu.Unlock()
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	// Your code here (3B).
	//
	term := rf.currentTerm
	if rf.currentRole != Leader {
		rf.mu.Unlock()
		return -1, term, false
	}

	// Leader has received a command, needs to append to local log
	// respond after entry applied to state machine
	// then send appendEntries to all followers
	state := append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.log = state
	rf.persist()
	index := rf.lastLogIndex()
	rf.matchIndex[rf.me] = index

	DPrintf("Server %d received command %v for term %d, appended to log at index %d", rf.me, command, term, index)

	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		if index >= rf.nextIndex[i] {
			// Send AppendEntries to this follower starting at nextIndex
			next := rf.nextIndex[i]
			var Term, leaderId, commitIndex int
			leaderId = rf.me
			commitIndex = rf.commitIndex
			Term = rf.currentTerm
			rf.mu.Unlock()
			go rf.sendLogEntries(Term, leaderId, next, commitIndex, i)
		} else {
			// Sorry about this abomination
			rf.mu.Unlock()

		}

	}
	// TODO: Deal with this unlock placement
	return index, term, true
}

func (rf *Raft) sendLogEntries(term int, leaderId int, nextIndex int, commitIndex int, server int) {
	curr := nextIndex
	for {
		rf.mu.Lock()
		if rf.currentRole != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		var reply AppendEntriesReply
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     leaderId,
			PrevLogIndex: curr - 1,
			PrevLogTerm:  rf.termAt(curr - 1),
			Entries:      rf.log[curr-rf.lastIncludedIndex:],
			LeaderCommit: commitIndex,
		}
		rf.mu.Unlock()
		DPrintf("Server %d sending AppendEntries to server %d", rf.me, server)
		ok := rf.sendAppendEntries(server, args, &reply)
		if ok {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentRole = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = nil
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				rf.nextIndex[server] = curr + len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1

				for N := rf.lastLogIndex(); N > rf.commitIndex; N-- {
					if rf.termAt(N) == rf.currentTerm && hasMajority(rf.matchIndex, N) {
						rf.commitIndex = N
					}
				}
				rf.mu.Unlock()
				return
			}

			// Log mismatch. Decrement nextIndex and try again
			// Rejection
			if reply.XLen < curr {
				curr = reply.XLen
			} else {
				term_search := rf.lastLogIndex()
				for term_search > rf.lastIncludedIndex && rf.termAt(term_search) > reply.XTerm {
					term_search -= 1
				}

				// Case 2. Leader has XTerm
				if rf.termAt(term_search) == reply.XTerm {
					curr = term_search + 1
				} else {
					curr = reply.XIndex
				}
			}
			rf.mu.Unlock()
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}

}

func (rf *Raft) PeriodicElectionTimer() {
	// Check if a leader election should be started.
	electionTimeout := 500*time.Millisecond +
		time.Duration(rand.Intn(400))*time.Millisecond
	rf.mu.Lock()
	shouldStart := time.Since(rf.lastAppendEntries) >= electionTimeout && rf.currentRole != Leader
	// start a new election
	if shouldStart {
		DPrintf("Server %d starting election for term %d", rf.me, rf.currentTerm+1)
		rf.currentTerm++
		term := rf.currentTerm
		rf.currentRole = Candidate
		rf.votedFor = &rf.me
		rf.persist()
		lastLogIndex := rf.lastLogIndex()
		req := &RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  rf.termAt(lastLogIndex),
		}
		voteCount := 1
		rf.lastAppendEntries = time.Now()
		rf.mu.Unlock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int, term int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, req, reply)
				if !ok {
					return
				}
				rf.mu.Lock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.currentRole = Follower
					rf.votedFor = nil
					rf.persist()
					rf.mu.Unlock()
					return
				}

				// Also gotta ignore replies from an election that already concluded
				if rf.currentRole != Candidate || rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted {
					voteCount++
					if voteCount > len(rf.peers)/2 {
						rf.currentRole = Leader
						for i := range rf.peers {
							rf.nextIndex[i] = rf.logLength()
							rf.matchIndex[i] = 0
						}
						rf.mu.Unlock()
						rf.SendHeartBeats()
						return
					}
				}
				rf.mu.Unlock()

			}(i, term)
		}
		return
	}
	rf.mu.Unlock()

}

func (rf *Raft) SendHeartBeats() {
	rf.mu.Lock()
	if rf.currentRole != Leader {
		rf.mu.Unlock()
		return
	}
	// log the current leader
	commitIndex := rf.commitIndex
	lastLogIndex := rf.lastLogIndex()

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: lastLogIndex,
		PrevLogTerm:  rf.termAt(lastLogIndex),
		Entries:      nil,
		LeaderCommit: commitIndex,
	}
	rf.mu.Unlock()
	// DPrintf("Server %d sending heartbeat to all peers for term %d", rf.me, rf.currentTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(server, args, &reply)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.currentRole = Follower
					rf.votedFor = nil
					rf.persist()
				}
				rf.mu.Unlock()
				return
			}
		}(i)
	}
}

func (rf *Raft) ticker(pauseFor time.Duration, fn func()) {
	for true {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		fn()
		// ms := 50 + (rand.Int63() % 300)
		time.Sleep(pauseFor * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.lastAppendEntries = time.Now()
	rf.log = []LogEntry{{Term: 0, Command: nil}}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyChannel = applyCh
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	state := persister.ReadRaftState()
	rf.readPersist(state)
	// rf.commitIndex = 
	// snapshot := persister.ReadSnapshot()
	go rf.applyEntries()

	// start ticker goroutine to start elections
	go rf.ticker(50, rf.PeriodicElectionTimer)

	go rf.ticker(100, rf.SendHeartBeats)

	return rf
}
