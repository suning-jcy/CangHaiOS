package storage

import (
	"code.google.com/p/weed-fs/go/operation"
)

type PartitionInfo struct {
	Id               PartitionId
	ReplicaPlacement *ReplicaPlacement
	Collection       string
	AccessUrl        string
}

func NewPartitionInfo(m *operation.PartitionInformationMessage) (vi PartitionInfo, err error) {
	vi = PartitionInfo{
		Id:               PartitionId(*m.Id),
		Collection:       *m.Collection,
	}
	if m.AccessUrl != nil {
		vi.AccessUrl = *m.AccessUrl
	}
	rp, e := NewReplicaPlacementFromByte(byte(*m.ReplicaPlacement))
	if e != nil {
		return vi, e
	}
	vi.ReplicaPlacement = rp
	return vi, nil
}
/*
func (pinfo PartitionInfo) String() string {
   return pinfo.Id.String() + "_" + pinfo.ReplicaPlacement.String()

}
*/
