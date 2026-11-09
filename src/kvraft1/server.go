package kvraft

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KvValue struct {
	Value   string
	Version rpc.Tversion
}
type KVServer struct {
	me   int
	rsm  *rsm.RSM
	mu   sync.Mutex
	Data map[string]KvValue

	// Your definitions here.
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}
		kv.mu.Lock()
		defer kv.mu.Unlock()
		value, ok := kv.Data[args.Key]
		if !ok {
			reply.Err = rpc.ErrNoKey
			return reply
		}
		reply.Value = value.Value
		reply.Version = value.Version
		reply.Err = rpc.OK
		return reply
	case rpc.PutArgs:
		reply := rpc.PutReply{}
		kv.mu.Lock()
		defer kv.mu.Unlock()
		value, ok := kv.Data[args.Key]
		if !ok {
			// Key no exist, we gotta check version number
			if args.Version > 0 {
				reply.Err = rpc.ErrNoKey
				return reply
			} else {
				// Install the value with version 1
				kv.Data[args.Key] = KvValue{Value: args.Value, Version: 1}
				reply.Err = rpc.OK
				return reply
			}
		}
		// Here we know key exits gotta compare version numbers
		if args.Version != value.Version {
			reply.Err = rpc.ErrVersion
			return reply
		}

		// Finally version numbers match
		kv.Data[args.Key] = KvValue{Value: args.Value, Version: value.Version + 1}
		reply.Err = rpc.OK
		return reply
	default:
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Data)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var mapData map[string]KvValue
	if d.Decode(&mapData) != nil {
		return
	}
	kv.Data = mapData
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)

}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, Data: make(map[string]KvValue)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
