package bucket

import (
	"strconv"
)

type PartitionId uint32

func NewPartitionId(id string) (PartitionId, error) {
	filerPartitionId, err := strconv.ParseUint(id, 10, 64)
	return PartitionId(filerPartitionId), err
}
func (id *PartitionId) String() string {
	return strconv.FormatUint(uint64(*id), 10)
}
func (id *PartitionId) Next() PartitionId {
	return PartitionId(uint32(*id) + 1)
}
