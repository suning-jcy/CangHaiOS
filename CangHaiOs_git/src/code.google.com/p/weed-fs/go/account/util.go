package account

import (
	"errors"

	"code.google.com/p/weed-fs/go/storage"
)

func NewReplicaPlacementFromString(id string) (*storage.ReplicaPlacement, error) {
	return storage.NewReplicaPlacementFromString(id)
}

func BADPARIDERR(id string) error {
	return errors.New("BadParID:" + id)
}
func BADREPTYPEERR(rp string) error {
	return errors.New("BadRepType:" + rp)
}
