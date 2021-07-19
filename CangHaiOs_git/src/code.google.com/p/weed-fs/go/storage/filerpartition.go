package storage

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"

	"code.google.com/p/weed-fs/go/filemap"
	"code.google.com/p/weed-fs/go/glog"
	weedutil "code.google.com/p/weed-fs/go/util"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

/*
type FileEntry struct {
	Name string `json:"name,omitempty"` //file name without path
	Id   string `json:"fid,omitempty"`
}

type FilerPartitionResult struct {
	Fid    string `json:"fid,omitempty"`
	Error  string `json:"error,omitempty"`
}
type PartitionId uint32

func NewPartitionId(vid string) (PartitionId, error) {
	filerPartitionId, err := strconv.ParseUint(vid, 10, 64)
	return PartitionId(filerPartitionId), err
}
func (vid *PartitionId) String() string {
	return strconv.FormatUint(uint64(*vid), 10)
}
func (vid *PartitionId) Next() PartitionId {
	return FilerPartitionId(uint32(*vid) + 1)
}
*/
type FilerPartition struct {
	Id          PartitionId
	dir         string
	collection  string
	db          *leveldb.DB
	rp          *ReplicaPlacement
	rebalstatus int
	accessLock  sync.Mutex
}

func NewFilerPartition(dir string, id PartitionId, collection string, replacement *ReplicaPlacement) (sfp *FilerPartition, err error) {
	sfp = &FilerPartition{Id: id, collection: collection, rp: replacement}
	if collection != "" {
		sfp.dir = dir + "/par_" + collection + "_" + id.String() + "_" + replacement.String()
	} else {
		sfp.dir = dir + "/par_" + id.String() + "_" + replacement.String()
	}
	if err = os.MkdirAll(sfp.dir, 0700); err != nil {
		return
	}

	if sfp.db, err = leveldb.OpenFile(sfp.dir, nil); err != nil {
		return
	}
	return
}

func (sfp *FilerPartition) CreateFile(filepath string, fid string, expireat string, length int64, mtime,disposition,mimeType,etag string) (oldfid string, olen int64, oldMTime string, err error) {
	glog.V(4).Infoln("fileName", filepath, "fid", fid)
	sfp.accessLock.Lock()
	defer sfp.accessLock.Unlock()
	_, oldfid, olen, oldMTime,_,_,_, _ = sfp.FindFile(filepath,false,0)
	err = sfp.db.Put([]byte(filepath), []byte(expireat+"_"+fid+"_"+fmt.Sprint(length)), nil)
	return
}
func (sfp *FilerPartition) DeleteFile(filepath string) (fid string, length int64, oldMTime string, err error) {
	sfp.accessLock.Lock()
	defer sfp.accessLock.Unlock()
	if _, fid, length, _,_,_,_, err = sfp.FindFile(filepath,false,0); err != nil {
		return
	}
	err = sfp.db.Delete([]byte(filepath), nil)
	return fid, length, "", err
}

func (sfp *FilerPartition) FindFile(filepath string, freshAccessTime bool, updateCycle int) (expireat string, fid string, length int64, MTime,Mtype,Disposition,Etag string, err error) {
	data, e := sfp.db.Get([]byte(filepath), nil)
	if e != nil {
		return "", "", 0, "","","","", e
	}
	str := string(data)
	parts := strings.Split(str, "_")
	if len(parts) != 3 {
		return "", "", 0, "","","","", fmt.Errorf("Bad data!")
	}
	return "", string(data), weedutil.ParseInt64(parts[2], 0), "", "","","",nil
}

func (sfp *FilerPartition) SetFileExpire(filepath string, lifeCycle int, mtime string) (oldMTime string, err error) {
	//To do
	return "", err
}
func (sfp *FilerPartition) ListFiles(dir string, lastFileName string, limit int, reg string) (files []FileEntry) {
	glog.V(4).Infoln("directory", dir, "lastFileName", lastFileName, "limit", limit)
	iter := sfp.db.NewIterator(&util.Range{Start: []byte(dir + lastFileName)}, nil)
	limitCounter := 0
	for iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, []byte(dir)) {
			break
		}
		fileName := string(key[len(dir):])
		if fileName == lastFileName {
			continue
		}
		limitCounter++
		if limit > 0 {
			if limitCounter > limit {
				break
			}
		}
		files = append(files, FileEntry{Name: fileName, Id: string(iter.Value())})
	}
	iter.Release()
	return
}
func (sfp *FilerPartition) CreateFile3(filepath string, fid string, expireat string, length int64, mtime,disposition,mimeType,etag string) (oldfid string, olen int64, oldMTime string, err error) {
	return "", 0, "", fmt.Errorf("NotImplemented!")
}
func (sfp *FilerPartition) List(account string, bucket string, marker string, prefix string, max int) (*filemap.ObjectList, error) {
	return nil, fmt.Errorf("NotImplemented!")
}
func (sfp *FilerPartition) ListFilesFromPar(lastFileName string, dir string, limit int) (*filemap.ObjectListHissync, error) {
	return nil, fmt.Errorf("NotImplemented!")
}
func (sfp *FilerPartition) DeleteExpired(startlimit string, limitcnt, prefix string) (*filemap.ObjectList, error) {
	return nil, fmt.Errorf("NotImplemented!")
}
func (sfp *FilerPartition) NeedToReplicate() bool {
	return sfp.rp.GetCopyCount() > 1
}

func (sfp *FilerPartition) Close() {
	sfp.db.Close()
}

func (sfp *FilerPartition) Collection() (collection string) {
	return sfp.collection
}

func (sfp *FilerPartition) Rp() (rp *ReplicaPlacement) {
	return sfp.rp
}

func (p *FilerPartition) Rebalstatus() (rebalstatus int) {
	return p.rebalstatus
}

//hujf 150413
func (p *FilerPartition) Destroy() (err error) {
	glog.V(0).Infoln("Drop leveldb")
	if err = p.db.Close(); err != nil {
		return
	}
	err = os.RemoveAll(p.dir)
	return
}

func (p *FilerPartition) BatchDeleteFile(filePath string) (oldfids []string, err error) {
	return
}
func (p *FilerPartition) BatchFindFile(filePath string) (oldfids []string, err error) {
	return
}

func (p *FilerPartition) DirFileCount(dir, reg string) (count int, err error) {
	return
}
func (p *FilerPartition) CreateFile2(filePath string, fid string, expireat string, length int64, mtime string) (err error) {
	return
}
func (p *FilerPartition) DeleteFile2(filePath, fid string) (err error) {
	return
}

func (p *FilerPartition) GetFileTag(fid string) (tag string, err error) {
	return
}

func (p *FilerPartition) SetFileTagByPath(filepath, tag string) (err error) {
	return
}

func (p *FilerPartition) SetFileTagByFid(fid, tag string) (err error) {
	return
}

func (p *FilerPartition) SetFileTagging(filepath, tag string) (oldtagging string, err error) {
	return
}

func (p *FilerPartition) GetFileTagging(filePath string) (tagging string, err error) {
	return
}

func (p *FilerPartition) DelFileTagging(filePath string) (err error) {
	return
}

func (p *FilerPartition) GetFileCount(account string, bucket string) (countSum int64, sizeSum int64, err error) {
	return
}

func (p *FilerPartition)UpdateFid(filePath string, oldFid string, newFid string) (err error) {
	return
}
