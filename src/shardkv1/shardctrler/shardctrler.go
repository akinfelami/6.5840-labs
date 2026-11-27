package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	curr, version, err := sck.Get("init")
	if err != rpc.OK {
		return
	}
	next, _, next_err := sck.Get("next")
	if next_err != rpc.OK {
		return
	}

	nextConfig := shardcfg.FromString(next)
	oldConfig := shardcfg.FromString(curr)
	if nextConfig.Num > oldConfig.Num {
		sck.moveConfig(oldConfig, version, nextConfig)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	cfg_string := cfg.String()
	// Assuming this cannot fail ---
	sck.Put("init", cfg_string, 0)
	sck.Put("next", cfg_string, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	cfg, version, err := sck.Get("init")
	if err != rpc.OK {
		return
	}
	nextCfg, nextVersion, err := sck.Get("next")
	if err != rpc.OK {
		return
	}
	next := shardcfg.FromString(nextCfg)
	if new.Num == next.Num+1 {
		// only case to take action
		// Store next config
		err = sck.Put("next", new.String(), nextVersion)
		if err == rpc.ErrVersion {
			// Already has this version
			return
		}
		if err == rpc.ErrMaybe {
			nextCfg, nextVersion, err = sck.Get("next")
			next = shardcfg.FromString(nextCfg)
			if next.Num != new.Num {
				// That did not work
				return
			}
		}
		oldCfg := shardcfg.FromString(cfg)
		sck.moveConfig(oldCfg, version, new)
		return
	}

}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Also assuming this did not fail --
	cfg, _, _ := sck.Get("init")

	return shardcfg.FromString(cfg)
}

func (sck *ShardCtrler) moveConfig(oldCfg *shardcfg.ShardConfig, currVersion rpc.Tversion, new *shardcfg.ShardConfig) {
	for i := range shardcfg.NShards {

		oldGid := oldCfg.Shards[i]
		newGid := new.Shards[i]
		if oldGid != newGid {
			oldClerk := shardgrp.MakeClerk(sck.clnt, oldCfg.Groups[oldGid])
			newClerk := shardgrp.MakeClerk(sck.clnt, new.Groups[newGid])

			var state []byte
			var err rpc.Err

			for {
				state, err = oldClerk.FreezeShard(shardcfg.Tshid(i), new.Num)
				if err == rpc.OK {
					break
				}
				if err == rpc.ErrWrongLeader {
					curr, _, err := sck.Get("init")
					if err != rpc.OK {
						return
					}
					cfg := shardcfg.FromString(curr)
					if cfg.Num < new.Num {
						time.Sleep(20 * time.Millisecond)
						continue
					}

				}
				return
			}

			sck.doWithRetry(func() rpc.Err { return newClerk.InstallShard(shardcfg.Tshid(i), state, new.Num) }, new.Num)
			sck.doWithRetry(func() rpc.Err { return oldClerk.DeleteShard(shardcfg.Tshid(i), new.Num) }, new.Num)
		}

	}
	err := sck.Put("init", new.String(), currVersion)
	if err != rpc.OK {
		return
	}
}

func (sck *ShardCtrler) doWithRetry(op func() rpc.Err, newNum shardcfg.Tnum) {
	for {
		err := op()
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrWrongLeader {
			curr, _, err := sck.Get("init")
			if err != rpc.OK {
				return
			}
			cfg := shardcfg.FromString(curr)
			if cfg.Num < newNum {
				time.Sleep(20 * time.Millisecond)
				continue
			}

		}
		return
	}
}
