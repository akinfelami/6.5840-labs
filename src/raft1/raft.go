package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
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
	commitIndex       int
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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

	if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
		var voterLastIndex, voterLastTerm int
		if len(rf.log) > 0 {
			voterLastIndex = len(rf.log) - 1
			voterLastTerm = rf.log[len(rf.log)-1].Term
		}
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
		DPrintf("Server %d granting vote to %d for term %d", rf.me, args.CandidateId, args.Term)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil
	}

	rf.currentRole = Follower
	rf.lastAppendEntries = time.Now()
	reply.Term = rf.currentTerm

	// Reply false if log doesnt contain an entry at prevLogIndex
	// whose term matches prevLogTerm

	// TODO: This handler will only update succcess to true if it
	// 1. prevLogIndex contains an entry and its term matches prevLogTerm
	// 2. if existing entry conflicts, delete the existing entry and all that follow it
	// 3. append any new entries not already in the log
	// 4. Commit.

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	term := rf.currentTerm
	rf.currentRole = Candidate
	rf.votedFor = &rf.me
	var lastLogIndex, lastLogTerm int
	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	req := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	voteCount := 1
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
					commitIndex := rf.commitIndex
					var lastLogIndex, lastLogTerm int
					if len(rf.log) > 0 {
						lastLogIndex = len(rf.log) - 1
						lastLogTerm = rf.log[len(rf.log)-1].Term
					}
					rf.mu.Unlock()
					args := &AppendEntriesArgs{
						Term:         term,
						LeaderId:     rf.me,
						PrevLogIndex: lastLogIndex,
						PrevLogTerm:  lastLogTerm,
						Entries:      nil,
						LeaderCommit: commitIndex,
					}

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
								}
								rf.mu.Unlock()
								return
							}
						}(i)
					}
					return

				}
			}
			rf.mu.Unlock()

		}(i, term)
	}
}

func (rf *Raft) ticker(fn func()) {
	for true {
		// pause for a random amount of time between 50 xand 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) ElectionHeartBeats() {
	// Check if a leader election should be started.
	electionTimeout := 500*time.Millisecond +
		time.Duration(rand.Intn(400))*time.Millisecond
	rf.mu.Lock()
	shouldStart := time.Since(rf.lastAppendEntries) >= electionTimeout && rf.currentRole != Leader
	// start a new election
	rf.mu.Unlock()
	if shouldStart {
		DPrintf("Server %d starting election for term %d", rf.me, rf.currentTerm+1)
		rf.startElection()
	}
}

type HeartBeat int

const (
	Empty HeartBeat = iota
	Regular
)

func (rf *Raft) SendHeartbeats(BeatType HeartBeat) {
	rf.mu.Lock()
	commitIndex := rf.commitIndex
	var lastLogIndex, lastLogTerm int
	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	var entries []LogEntry
	if BeatType == Regular {
		entries = append([]LogEntry{}, rf.log...)
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: lastLogIndex,
		PrevLogTerm:  lastLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	rf.mu.Unlock()
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
				}
				rf.mu.Unlock()
				return
			}
		}(i)
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
	rf.log = []LogEntry{}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker(rf.ElectionHeartBeats)

	// TODO: As aside leader has to send out periodic appendEntries rpcs
	go rf.SendHeartbeats(Regular)

	return rf
}
