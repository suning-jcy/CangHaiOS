package storage

import (
	"hash/crc32"
	"sync"
)

type PathLock struct {
	hashNum int
	locks   []sync.Mutex
}

func NewPathLock(num int) *PathLock {
	if num <= 0 {
		return nil
	}
	return &PathLock{hashNum: num, locks: make([]sync.Mutex, num)}
}
func (pLock *PathLock) Lock(path string) {
	hash := crc32.ChecksumIEEE([]byte(path))
	pos := hash % uint32(pLock.hashNum)
	pLock.locks[pos].Lock()
}
func (pLock *PathLock) Unlock(path string) {
	hash := crc32.ChecksumIEEE([]byte(path))
	pos := hash % uint32(pLock.hashNum)
	pLock.locks[pos].Unlock()
}
func (pLock *PathLock) LockAll() {
	for k := 0; k < pLock.hashNum; k++ {
		pLock.locks[k].Lock()
	}
}
func (pLock *PathLock) UnLockAll() {
	for k := 0; k < pLock.hashNum; k++ {
		pLock.locks[k].Unlock()
	}
}
