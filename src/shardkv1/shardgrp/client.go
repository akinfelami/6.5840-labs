package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
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

	for !ck.Call(ck.servers[ck.Leader()], "KVServer.Get", &arg, &reply) || reply.Err == rpc.ErrWrongLeader {
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(20 * time.Millisecond)
		reply = rpc.GetReply{}
	}

	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	arg := rpc.PutArgs{Key: key, Value: value, Version: version}
	retried := false
	for {
		reply := rpc.PutReply{}
		ok := ck.Call(ck.servers[ck.Leader()], "KVServer.Put", &arg, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			if reply.Err == rpc.ErrVersion && retried {
				return rpc.ErrMaybe
			}
			return reply.Err
		}
		retried = true
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(20 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
