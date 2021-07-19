package filemap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"net/url"
	"strconv"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"context"
)

type FilerBackup struct {
	master *string
	cache  *operation.ParidCache
}

func NewFilerBackup(master *string) (filer *FilerBackup, err error) {
	filer = &FilerBackup{
		master: master,
	}
	filer.cache = operation.NewParidCache()
	return filer, nil
}
func (filer *FilerBackup) CreateFile(filePath string, flen int64, fid, expireat, collection string, mtime string, ctx *context.Context) (oldfid string, olen int64, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", 0, err
	}
	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	values.Add("fileid", fid)
	values.Add("expireat", expireat)
	values.Add("length", fmt.Sprint(flen))
	values.Add("mtime", mtime)
	direct := false
	lookup, lookupError := filer.cache.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, direct, ctx)
	if lookupError != nil {
		return "", 0, lookupError
	}

	urls := make([]string, 1)
	count := len(lookup.Locations)
	firstPos := produceSeqByName(filePath, count)
	for i := 0; i < count; i++ {
		urls[0] = lookup.Locations[(firstPos+i)%count].Url
		if urls[0] == "" {
			continue
		}
		glog.V(4).Infoln("create file:", filePath, " firstPos:", firstPos, " count:", count, " i:", i, " pos", pos)
		_, oldfid, olen, _, err = postRequests3(urls, ACTION_CREATE, values, ctx)
		if err != nil {
			glog.V(0).Info("create file err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", filePath, " fid:", fid, " collection:", collection, " partition:", pos)
			continue
		}
		return
	}
	return
}
func (filer *FilerBackup) FindFile2(filePath string, collection string, needtag bool, ctx *context.Context) (url1, expireat string, fid string, olen int64, oldMTime, tag string, err1 error, url2, expireat2 string, fid2 string, olen2 int64, oldMTime2, tag2 string, err2, err error) {
	return "", "", "", 0, "", "", nil, "", "", "", 0, "", "", nil, err
}
func (filer *FilerBackup) FindFile(filePath string, collection string, ctx *context.Context) (expireat string, fid string, olen int64, lmtime string, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", "", 0, "", err
	}
	lookup, lookupError := filer.cache.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		return "", "", 0, "", lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", "", 0, "", fmt.Errorf("can not find partition", pos)
	}

	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)

	randpos := rand.Intn(len(lookup.Locations))
	expireat, fid, olen, lmtime, _, err = postRequest(lookup.Locations[randpos].Url+"/find", values, ctx)
	if err != nil {
		glog.V(0).Info("find filemap failed at:", lookup.Locations[randpos].Url, filePath, err.Error(), "retry again!")
		expireat, fid, olen, lmtime, _, err = postRequest(lookup.Locations[(randpos+1)%len(lookup.Locations)].Url+"/find", values, ctx)
	}
	return
}
func (filer *FilerBackup) SetFileExpire(filePath string, collection string, lifeCycle, mtime string, ctx *context.Context) (err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return err
	}
	lookup, lookupError := filer.cache.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		return lookupError
	}
	if len(lookup.Locations) == 0 {
		return fmt.Errorf("can not find partition", pos)
	}
	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	values.Add("lifecycle", lifeCycle)
	values.Add("mtime", mtime)

	urls := make([]string, 1)
	count := len(lookup.Locations)
	firstPos := produceSeqByName(filePath, count)
	for i := 0; i < count; i++ {
		urls[0] = lookup.Locations[(firstPos+i)%count].Url
		if urls[0] == "" {
			continue
		}
		glog.V(4).Infoln("set expire file:", filePath, " firstPos:", firstPos, " count:", count, " i:", i, " pos", pos)
		_, _, _, _, err = postRequests3(urls, "/set/expire", values, ctx)
		if err != nil {
			glog.V(0).Info("set expire err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", filePath, " collection:", collection, " partition:", pos)
			continue
		}
		return
	}
	if err != nil && err.Error() == NotFound.Error() {
		return NotFound
	}
	return
}
func (filer *FilerBackup) DeleteFile(filePath string, collection string, ctx *context.Context) (oldfid string, olen int64, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", 0, err
	}
	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	direct := false
	lookup, lookupError := filer.cache.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, direct, ctx)
	if lookupError != nil {

		return "", 0, lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", 0, fmt.Errorf("can not find partition", pos)
	}

	urls := make([]string, 1)
	count := len(lookup.Locations)
	firstPos := produceSeqByName(filePath, count)
	for i := 0; i < count; i++ {
		urls[0] = lookup.Locations[(firstPos+i)%count].Url
		if urls[0] == "" {
			continue
		}
		glog.V(4).Infoln("delete file:", filePath, " firstPos:", firstPos, " count:", count, " i:", i, " pos", pos)
		_, oldfid, olen, _, err = postRequests3(urls, ACTION_DELETE, values, ctx)
		if err != nil {
			glog.V(0).Info("delete file err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", filePath, " collection:", collection, " partition:", pos)
			continue
		}
		return
	}
	if err != nil && err.Error() == "NotFound" {
		return "", 0, NotFound
	}
	return
}
func (filer *FilerBackup) ParmAdjust(parmtype string, value string) (err error) {
	return
}
func (filer *FilerBackup) SetMaster(master *string) (err error) {
	filer.master = master
	return
}
func (filer *FilerBackup) HashDBPos(filePath string, collection string, ctx *context.Context) (uint32, error) {
	filerPartitionNum, _, err := operation.GetCollectionPartCountRp(*filer.master, collection, ctx)
	if err != nil {
		return 0, err
	}
	if filerPartitionNum == 0 {
		return 0, errors.New("Bad filer partition count!")
	}
	hash := crc32.ChecksumIEEE([]byte(filePath))
	pos := hash % uint32(filerPartitionNum)
	glog.V(4).Infoln("Get filepath ", filePath, "loc", pos)
	return pos, nil
}
func (filer *FilerBackup) Rename(fromfilePath, tofilePath string, mtime, collection string, ctx *context.Context) (oldfid, oldMTime, oldexpireat string, oldlen int64, err error) {
	return "", "", "", 0, nil
}
