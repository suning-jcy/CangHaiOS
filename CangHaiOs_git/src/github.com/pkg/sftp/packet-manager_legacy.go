// +build !go1.8

package sftp

import "sort"
import "sync"

// for sorting/ordering outgoing
type responsePackets []responsePacket

func (r responsePackets) Len() int           { return len(r) }
func (r responsePackets) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r responsePackets) Less(i, j int) bool { return r[i].uid() < r[j].uid() }
func (r responsePackets) Sort()              { sort.Sort(r) }

// for sorting/ordering incoming
type requestPacketIDs []uint64

func (r requestPacketIDs) Len() int           { return len(r) }
func (r requestPacketIDs) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r requestPacketIDs) Less(i, j int) bool { return r[i] < r[j] }
func (r requestPacketIDs) Sort()              { sort.Sort(r) }

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
