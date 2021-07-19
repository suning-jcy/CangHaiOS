package storage

import (
	"code.google.com/p/weed-fs/go/operation"
)

type VolumeInfo struct {
	Id               VolumeId
	SerialNum        string
	Size             uint64
	MaxFileKey       uint64
	ReplicaPlacement *ReplicaPlacement
	Collection       string
	Version          Version
	FileCount        int
	DeleteCount      int
	DeletedByteCount uint64
	ReadOnly         bool
	CanWrite         bool
	Mnt              string
}

func NewVolumeInfo(m *operation.VolInfoMessage,path string) (vi VolumeInfo, err error) {
	vi = VolumeInfo{
		Id:               VolumeId(*m.Id),
		SerialNum:		  *m.Sn,
		Size:             *m.Size,
		MaxFileKey:       *m.MaxFileKey,
		Collection:       *m.Collection,
		FileCount:        int(*m.FileCount),
		DeleteCount:      int(*m.DeleteCount),
		DeletedByteCount: *m.DeletedByteCount,
		ReadOnly:         *m.ReadOnly,
		CanWrite:         *m.CanWrite,
		Version:          Version(*m.Version),
	}
	rp, e := NewReplicaPlacementFromByte(byte(*m.ReplicaPlacement))
	if e != nil {
		return vi, e
	}
	vi.ReplicaPlacement = rp
	return vi, nil
}

type VolumeInfo2 struct{
	VolumeInfo
	Mode byte
}