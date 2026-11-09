package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int64
	Req interface{}
}

type OpResponse struct {
	response chan any
	term     int
	opId     int64
	LeaderId int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	counter    int64
	opResponse map[int]OpResponse
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		opResponse:   make(map[int]OpResponse),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.ApplyChannelReader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	rsm.mu.Lock()
	op := Op{Me: rsm.me, Id: rsm.counter, Req: req}
	rsm.counter++
	indexToBeCommittedAt, currentTerm, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	ch := make(chan any)

	rsm.opResponse[indexToBeCommittedAt] = OpResponse{
		response: ch,
		term:     currentTerm,
		opId:     op.Id,
		LeaderId: op.Me}

	rsm.mu.Unlock()

	for {
		select {
		case res, ok := <-ch:
			if !ok {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, res
		case <-time.After(20 * time.Millisecond):
			curr_term, _ := rsm.rf.GetState()
			if curr_term != currentTerm {
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}

func (rsm *RSM) ApplyChannelReader() {
	for msg := range rsm.applyCh {
		cmdResp := msg.Command.(Op)
		res := rsm.sm.DoOp(cmdResp.Req)
		rsm.mu.Lock()
		respChan, ok := rsm.opResponse[msg.CommandIndex]
		rsm.mu.Unlock()
		if !ok {
			continue
		}

		if cmdResp.Id != respChan.opId || cmdResp.Me != respChan.LeaderId {
			// This was previously a leader
			// Need to close all channels where Submit is waiting
			rsm.mu.Lock()
			for idx, ch := range rsm.opResponse {
				curr_term, _ := rsm.rf.GetState()
				if curr_term != ch.term {
					close(ch.response)
					delete(rsm.opResponse, idx)
				}
			}
			rsm.mu.Unlock()

		} else {
			respChan.response <- res
			rsm.mu.Lock()
			delete(rsm.opResponse, msg.CommandIndex)
			rsm.mu.Unlock()
		}
	}
}
