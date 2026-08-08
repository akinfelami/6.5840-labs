package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockname string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname, clientID: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		// get lock key from kvstore
		value, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			// if no key, try to acquire lock by putting our clientID
			err := lk.ck.Put(lk.lockname, lk.clientID, 0)
			if err == rpc.OK {
				return
			} else {
				continue
			}
		}

		// key exists, check if we already hold the lock
		if value == lk.clientID {
			// we already hold the lock, return
			return
		}

		if value != "" {
			// lock is held by someone else
			continue
		}

		// lock is free, try to acquire it by putting our clientID
		err = lk.ck.Put(lk.lockname, lk.clientID, version)
		if err == rpc.OK {
			return
		} else {
			continue
		}

	}
}

func (lk *Lock) Release() {
	value, version, err := lk.ck.Get(lk.lockname)

	if err == rpc.ErrNoKey {
		// no key can't release
		return
	}

	// Has key compare value with clientID
	if value != lk.clientID {
		// we don't hold the lock, can't release
		// also covers the case when lock is free (value == "")
		return
	}

	// At this point we have the lock, try to release it by putting an empty string
	err = lk.ck.Put(lk.lockname, "", version)
	if err != rpc.OK {
		// failed to release lock, maybe someone else acquired it
		return
	}
}
