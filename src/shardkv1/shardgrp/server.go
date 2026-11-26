package shardgrp

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type ShardState int

const (
	StateActive ShardState = iota
	StateFrozen
)

type KvValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid
	mu  sync.Mutex

	Data      map[string]KvValue
	ShardNums [shardcfg.NShards]shardcfg.Tnum
	State     [shardcfg.NShards]ShardState
}

func (kv *KVServer) DoGetOp(args rpc.GetArgs) rpc.GetReply {
	reply := rpc.GetReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.State[shardcfg.Key2Shard(args.Key)] == StateFrozen {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	value, ok := kv.Data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return reply
	}
	reply.Value = value.Value
	reply.Version = value.Version
	reply.Err = rpc.OK
	return reply

}

func (kv *KVServer) DoPutOp(args rpc.PutArgs) rpc.PutReply {
	reply := rpc.PutReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.State[shardcfg.Key2Shard(args.Key)] == StateFrozen {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
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

}

func (kv *KVServer) DoFreezeShardOp(args shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	reply := shardrpc.FreezeShardReply{}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardNum := kv.ShardNums[args.Shard]
	if args.Num < shardNum {
		reply.Err = rpc.ErrVersion
		reply.Num = shardNum
		return reply
	}
	data := make(map[string]KvValue)
	for k, v := range kv.Data {
		if shardcfg.Key2Shard(k) == args.Shard {
			data[k] = v
		}
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(data)
	reply.State = w.Bytes()

	kv.ShardNums[args.Shard] = args.Num
	kv.State[args.Shard] = StateFrozen
	reply.Err = rpc.OK
	reply.Num = args.Num
	return reply
}

func (kv *KVServer) DoInstallShardOp(args shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	reply := shardrpc.InstallShardReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardNum := kv.ShardNums[args.Shard]
	if args.Num < shardNum {
		reply.Err = rpc.ErrVersion
		return reply
	}

	if args.Num == shardNum {
		reply.Err = rpc.OK

		return reply
	}
	var shardMap map[string]KvValue
	r := bytes.NewBuffer(args.State)
	d := labgob.NewDecoder(r)
	if d.Decode(&shardMap) != nil {
		return reply
	}

	for k, v := range shardMap {
		kv.Data[k] = v
	}
	reply.Err = rpc.OK
	kv.State[args.Shard] = StateActive
	kv.ShardNums[args.Shard] = args.Num
	return reply
}

func (kv *KVServer) DoDeleteShardOp(args shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	reply := shardrpc.DeleteShardReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardNum := kv.ShardNums[args.Shard]
	if args.Num < shardNum {
		reply.Err = rpc.ErrVersion
		return reply
	}

	for k, _ := range kv.Data {
		if shardcfg.Key2Shard(k) == args.Shard {
			delete(kv.Data, k)
		}
	}

	reply.Err = rpc.OK
	kv.ShardNums[args.Shard] = args.Num
	kv.State[args.Shard] = StateFrozen
	return reply
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.DoGetOp(args)
	case rpc.PutArgs:
		return kv.DoPutOp(args)
	case shardrpc.FreezeShardArgs:
		return kv.DoFreezeShardOp(args)
	case shardrpc.InstallShardArgs:
		return kv.DoInstallShardOp(args)
	case shardrpc.DeleteShardArgs:
		return kv.DoDeleteShardOp(args)
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
	e.Encode(kv.ShardNums)
	e.Encode(kv.State)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var mapData map[string]KvValue
	var shardNums [shardcfg.NShards]shardcfg.Tnum
	var state [shardcfg.NShards]ShardState
	if d.Decode(&mapData) != nil || d.Decode(&shardNums) != nil || d.Decode(&state) != nil {
		return
	}
	kv.Data = mapData
	kv.ShardNums = shardNums
	kv.State = state
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.DeleteShardReply)
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me, Data: make(map[string]KvValue)}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
