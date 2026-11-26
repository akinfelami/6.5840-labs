package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	arg := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	if !ck.Call(ck.servers[ck.Leader()], "KVServer.Get", &arg, &reply) || reply.Err == rpc.ErrWrongLeader {
		ck.leader = (ck.leader + 1) % len(ck.servers)
		if reply.Err == "" {
			reply.Err = rpc.ErrWrongLeader
		}
	}

	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	arg := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	if !ck.Call(ck.servers[ck.Leader()], "KVServer.Put", &arg, &reply) || reply.Err == rpc.ErrWrongLeader {
		ck.leader = (ck.leader + 1) % len(ck.servers)
		if reply.Err == "" {
			reply.Err = rpc.ErrWrongLeader
		}
	}
	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	arg := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	reply := shardrpc.FreezeShardReply{}
	for !ck.Call(ck.servers[ck.Leader()], "KVServer.FreezeShard", &arg, &reply) || reply.Err == rpc.ErrWrongLeader {
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(20 * time.Millisecond)
		reply = shardrpc.FreezeShardReply{}
	}
	return reply.State, reply.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	arg := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	reply := shardrpc.InstallShardReply{}
	for !ck.Call(ck.servers[ck.Leader()], "KVServer.InstallShard", &arg, &reply) || reply.Err == rpc.ErrWrongLeader {
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(20 * time.Millisecond)
		reply = shardrpc.InstallShardReply{}
	}
	return reply.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	arg := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	reply := shardrpc.DeleteShardReply{}
	for !ck.Call(ck.servers[ck.Leader()], "KVServer.DeleteShard", &arg, &reply) || reply.Err == rpc.ErrWrongLeader {
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(20 * time.Millisecond)
		reply = shardrpc.DeleteShardReply{}
	}
	return reply.Err
}
