package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Value struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]Value
}

func MakeKVServer() *KVServer {
	kv := &KVServer{data: make(map[string]Value)}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = value.value
	reply.Version = value.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// if server version matches, increment the version number of the key
	// if no match we say no version.
	// if version number is larger than 0 and key no exist
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.data[args.Key]
	if !ok {
		// Key no exist, we gotta check version number
		if args.Version > 0 {
			reply.Err = rpc.ErrNoKey
			return
		} else {
			// Install the value with version 1
			kv.data[args.Key] = Value{value: args.Value, version: 1}
			reply.Err = rpc.OK
			return
		}
	}
	// Here we know key exits gotta compare version numbers
	if args.Version != value.version {
		reply.Err = rpc.ErrVersion
		return
	}

	// Finally version numbers match
	kv.data[args.Key] = Value{value: args.Value, version: value.version + 1}
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
