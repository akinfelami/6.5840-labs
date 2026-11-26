package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {

		cfg := ck.sck.Query()
		gid := cfg.Shards[shardcfg.Key2Shard(key)]
		servers := cfg.Groups[gid]
		clerk, ok := ck.rcks[gid]
		if !ok {
			clerk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.rcks[gid] = clerk
		} else {
			clerk = ck.rcks[gid]
		}

		val, version, err := clerk.Get(key)
		if err != rpc.ErrWrongGroup && err != rpc.ErrWrongLeader {
			return val, version, err
		}
		time.Sleep(20 * time.Millisecond)

	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	retried := false
	for {
		cfg := ck.sck.Query()
		gid := cfg.Shards[shardcfg.Key2Shard(key)]
		servers := cfg.Groups[gid]
		clerk, ok := ck.rcks[gid]
		if !ok {
			clerk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.rcks[gid] = clerk
		} else {
			clerk = ck.rcks[gid]
		}

		err := clerk.Put(key, value, version)
		if err == rpc.ErrVersion && retried {
			return rpc.ErrMaybe
		}

		if err != rpc.ErrWrongGroup && err != rpc.ErrWrongLeader {
			return err
		}
		retried = true
		time.Sleep(20 * time.Millisecond)
	}
}
