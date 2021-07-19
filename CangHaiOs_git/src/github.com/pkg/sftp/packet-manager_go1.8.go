// +build go1.8

package sftp

import (
	"sort"
	"sync"
)

type responsePackets []responsePacket

func (r responsePackets) Sort() {
	sort.Slice(r, func(i, j int) bool {
		return r[i].uid () < r[j].uid()
	})
}

type requestPacketIDs []uint64

func (r requestPacketIDs) Sort() {
	sort.Slice(r, func(i, j int) bool {
		return r[i] < r[j]
	})
}

var uni_seq *UnSeqId

type UnSeqId struct {
	id   uint64
	lock sync.Mutex
}

func init() {
	uni_seq = NewSeqId()
}
func NewSeqId() *UnSeqId {
	return &UnSeqId{id: uint64(8888)}
}
func (us *UnSeqId) NextId() uint64 {
	us.lock.Lock()
	defer us.lock.Unlock()
	id := us.id
	us.id++
	return id
}
func NextId() uint64 {
	return uni_seq.NextId()
}
