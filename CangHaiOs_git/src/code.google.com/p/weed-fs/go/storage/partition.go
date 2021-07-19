package storage

import (
	"errors"
	"strconv"

	"code.google.com/p/weed-fs/go/filemap"
)

type FileEntry struct {
	Name string `json:"name,omitempty"` //file name without path
	Id   string `json:"fid,omitempty"`
}

var PartitionMovedErr = errors.New("partition have moved permanently")

type FilerPartitionResult struct {
	Fid      string `json:"fid,omitempty"`
	ExpireAt string `json:"ExpireAt,omitempty"`
	Length   int64  `json:"Length"`
	MTime    string `json:"MTime,omitempty"`
	Tag      string `json:"Tag,omitempty"`
	Error    string `json:"error,omitempty"`
	Mtype    string `json:"mtype,omitempty"`
	Disposition string `json:"disposition,omitempty"`
	Etag     string `json:"etag,omitempty"`
}

type FilerPartitionTaggingResult struct {
	Tagging string `json:"tagging,omitempty"`
	Error   string `json:"error,omitempty"`
}

type FilerPartitionResults struct {
	Fids  []string `json:"fids,omitempty"`
	Error string   `json:"error,omitempty"`
}
type FilerPartitionResults1 struct {
	Filepaths []string `json:"filepaths,omitempty"`
	Error     string   `json:"error,omitempty"`
}
type FilerPartitionCountResult struct {
	Count string `json:"count,omitempty"`
	Error string `json:"error,omitempty"`
}
type PartitionId uint32
type Partition interface {
	CreateFile(filepath string, fid string, expireat string, length int64, mtime,disposition,mimeType,etag string) (oldfid string, olen int64, oldMTime string, err error)
	//CreateFile3  expireat second
	CreateFile3(filepath string, fid string, expireat string, length int64, mtime,disposition,mimeType,etag string) (oldfid string, olen int64, oldMTime string, err error)
	DeleteFile(filepath string) (oldfid string, length int64, oldMTime string, err error)
	//FindFile(filepath string) (expireat string, fid string, length int64, MTime,Mtype,Disposition,Etag string, err error)
	FindFile(filepath string, freshAccessTime bool, updateCycle int) (expireat string, fid string, length int64, MTime,Mtype,Disposition,Etag string, err error)
	SetFileExpire(filepath string, lifeCycle int, mtime string) (oldMTime string, err error)
	SetFileTagByPath(filepath, tag string) (err error)
	SetFileTagByFid(fid, tag string) (err error)
	//SetFileTagging:set tagging(fid)
	SetFileTagging(filepath, tagging string) (oldtagging string, err error)
	GetFileTagging(filepath string) (tagging string, err error)
	GetFileTag(fid string) (tag string, err error)
	List(account string, bucket string, marker string, prefix string, max int) (objList *filemap.ObjectList, err error)
	GetFileCount(account string, bucket string) (countSum int64, sizeSum int64, err error)
	ListFilesFromPar(lastFileName string, dir string, limit int) (objList *filemap.ObjectListHissync, err error)
	DeleteExpired(startlimit string, limitcnt, prefix string) (*filemap.ObjectList, error)
	Collection() (collection string)
	Rp() (rp *ReplicaPlacement)
	Close()
	Destroy() error
	//重命名使用
	CreateFile2(filePath string, fid string, expireat string, length int64, mtime string) (err error)
	UpdateFid(filePath string, oldFid string, newFid string) (err error)
	//重命名使用
	DeleteFile2(filePath, fid string) (err error)
}
type PartitionRebalance interface {
	prepareParRebal(dstAddr string) (err error)
	transferPartition(dstAddr string, inf string) (err error)
	commitPartition(dstAddr string) (err error)
	ParRebalance(dstAddr string) error
	ParRebalanceExt(dstAddr string) (err error)
	ParRebalanceCommit(dstAddr string) (err error)
	RebalanceRollBack()
}
type PartitionRepair interface {
	prepareParRepair(dstAddr string) (err error)
	transferPartition(dstAddr string, inf string) (err error)
	applyExtraUpdate(dstAddr string) (err error)
	commitParRepair(dstAddr string) (err error)
	ParRepRepair(dstAddr string) error
	parRepairResetStat() error
	parRepairSetRepStat() error
}
type PartitionRecover interface {
	batchUpdateFilemaps(data []byte) (err error)
}

func NewPartitionId(vid string) (PartitionId, error) {
	filerPartitionId, err := strconv.ParseUint(vid, 10, 64)
	return PartitionId(filerPartitionId), err
}
func (vid *PartitionId) String() string {
	return strconv.FormatUint(uint64(*vid), 10)
}
func (vid *PartitionId) Next() PartitionId {
	return PartitionId(uint32(*vid) + 1)
}
