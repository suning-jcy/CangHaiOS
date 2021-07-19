package filemap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"net/url"
	"strconv"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
	"context"
	"sync"
)

var (
	FILER_PARTITION_NUM = 128
)
var NotFound = errors.New("Not Find")
var NotFit = errors.New("Not Fit")
var PartitionDiffErr = errors.New("PartitionDifferentError")
var (
	ACTION_CREATE = "/create"
	ACTION_DELETE = "/delete"
	ACTION_FIND   = "/find"
)

type PartitionId int32

type FilerPartitionResult struct {
	Fid      string `json:"fid,omitempty"`
	ExpireAt string `json:"ExpireAt,omitempty"`
	Length   int64  `json:"Length,omitempty"`
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

type FilerEmbedded struct {
	master     *string
	collection string
	pslb       *util.RingSelector
}

var CreateFileMapFailed = errors.New("create filemap failed")
var DelFileMapFailed = errors.New("delete filemap failed")

func NewFilerEmbedded(master *string) (filer *FilerEmbedded, err error) {
	filer = &FilerEmbedded{}
	filer.master = master
	if public.FilerLoadBalance {
		filer.pslb = util.NewRingSelector("psLoadBalancer", filer.selectPS)
	}
	return
}
func (filer *FilerEmbedded) CreateFile(filePath string, flen int64, mtime, fid, expireat, tag, collection,disposition,mimeType,etag string, ctx *context.Context) (oldfid string, olen int64, oldMTime string, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", 0, "", err
	}
	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	values.Add("fileid", fid)
	values.Add("expireat", expireat)
	values.Add("length", fmt.Sprint(flen))
	values.Add("mtime", mtime)
	values.Add("tag", tag)
	values.Add("disposition", disposition)
	values.Add("mimeType", mimeType)
	values.Add("etag", etag)
	direct := false
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, direct, ctx)
	if lookupError != nil {
		return "", 0, "", lookupError
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
		_, oldfid, olen, oldMTime, err = postRequests3(urls, ACTION_CREATE, values, ctx)
		if err != nil {
			glog.V(0).Info("create file err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", filePath, " fid:", fid, " collection:", collection, " partition:", pos)
			continue
		}
		if oldfid == fid && olen == flen {
			oldfid = ""
			olen = 0
		}
		return
	}
	return "", 0, "", errors.New("No availabe partition url")
}

//expireat  second
func (filer *FilerEmbedded) CreateFile2(filePath string, flen int64, mtime, fid, expireat, tag, collection string, ctx *context.Context) (oldfid string, olen int64, oldMTime string, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", 0, "", err
	}
	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	values.Add("fileid", fid)
	values.Add("expireatSsecond", expireat)
	values.Add("length", fmt.Sprint(flen))
	values.Add("mtime", mtime)
	values.Add("tag", tag)
	direct := false
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, direct, ctx)
	if lookupError != nil {
		return "", 0, "", lookupError
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
		_, oldfid, olen, oldMTime, err = postRequests3(urls, ACTION_CREATE, values, ctx)
		if err != nil {
			glog.V(0).Info("create file err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", filePath, " fid:", fid, " collection:", collection, " partition:", pos)
			continue
		}
		if oldfid == fid && olen == flen {
			oldfid = ""
			olen = 0
		}
		return
	}
	return
}

//获取旧信息，旧的lmtime需要返回
//创建新条目，
//删除旧条目，需要比对path和fid
//（操作期间要是有修改生命周期这样的动作会被冲掉）
func (filer *FilerEmbedded) Rename(fromfilePath, tofilePath string, mtime, collection string, ctx *context.Context) (oldfid, oldMTime, oldexpireat string, oldlen, oldlen2 int64, err error) {
	var topos uint32

	topos, err = filer.HashDBPos(tofilePath, collection, ctx)
	if err != nil {
		return "", "", "", 0, 0, err
	}

	lastexpireat, lastfid, lastlen, lastmtime, lasttag,_,_, _, err := filer.FindFile(fromfilePath, collection, true, false, 0, ctx)
	if err != nil {
		return "", "", "", 0, 0, err
	}
	//创建新条目期间错误就返回
	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(topos)))
	values.Add("collection", collection)
	values.Add("filename", tofilePath)
	values.Add("fileid", lastfid)
	//values.Add("expireat", lastexpireat)
	values.Add("length", fmt.Sprint(lastlen))
	values.Add("mtime", mtime)
	values.Add("tag", lasttag)
	//
	values.Add("type", "rename")
	values.Add("expireatSsecond", lastexpireat)
	values.Add("force", "true")
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(topos)), "", collection, false, ctx)
	if lookupError != nil {
		return "", "", "", 0, 0, lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", "", "", 0, 0, fmt.Errorf("can not find partition", topos)
	}
	//进行重命名操作
	urls := make([]string, 1)
	count := len(lookup.Locations)
	firstPos := produceSeqByName(tofilePath, count)
	for i := 0; i < count; i++ {
		urls[0] = lookup.Locations[(firstPos+i)%count].Url
		if urls[0] == "" {
			continue
		}
		glog.V(4).Infoln("create file:", tofilePath, " firstPos:", firstPos, " count:", count, " i:", i, " pos", topos)
		_, oldfid, oldlen, oldMTime, err = postRequests3(urls, ACTION_CREATE, values, ctx)
		if err != nil {
			glog.V(0).Info("create file err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", tofilePath, " fid:", lastfid, " collection:", collection, " partition:", topos)
			continue
		}
		break
	}

	if err != nil {
		return "", "", "", 0, 0, err
	}
	//删除旧条目，删除期间错误就删除前面的新条目

	err = filer.deletewithfid(fromfilePath, lastfid, collection, ctx)
	if err != nil {
		//try again
		err = filer.deletewithfid(fromfilePath, lastfid, collection, ctx)
	}
	//旧的删不掉，上面新建的也删除了
	if err != nil {
		filer.deletewithfid(tofilePath, lastfid, collection, ctx)
	}
	return oldfid, lastmtime, lastexpireat, lastlen, oldlen, err
}

func (filer *FilerEmbedded) FindFile(filePath string, collection string, needtag bool,needFreshA bool, updateCycle int, ctx *context.Context) (expireat string, fid string, olen int64, oldMTime, tag,mtype,disposition,etag string, err error) {
	if public.FilerLoadBalance {
		return filer.FindFileFromLBPS(filePath, collection, needtag,needFreshA, updateCycle, ctx)
	}
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", "", 0, "", "","","","", err
	}
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		return "", "", 0, "", "", "","","",lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", "", 0, "", "","","","", fmt.Errorf("can not find partition", pos)
	}

	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	if needtag {
		values.Add("needtag", "true")
	}
	var urls string
	if needFreshA {
		values.Add(public.NEEDFRESHACCESSTIME, "true")
	}
	values.Add(public.UPDATECYCLE, strconv.Itoa(updateCycle))
	count := len(lookup.Locations)
	firstPos := produceSeqByName(filePath, count)
	for i := 0; i < count; i++ {
		urls = lookup.Locations[(firstPos+i)%count].Url
		if urls == "" {
			continue
		}
		glog.V(4).Infoln("find file:", filePath, " firstPos:", firstPos, " count:", count, " i:", i, " pos", pos)
		expireat, fid, olen, oldMTime,tag,mtype,disposition,etag, err = GetRequest(urls+"/find", values, ctx)
		if err != nil {
			glog.V(1).Infoln("find file err:", err, " i:", i, " count:", count, " url:", urls, " filename:", filePath, " fid:", fid, " collection:", collection, " partition:", pos)
			continue
		}
		return
	}
	return
}

//zqx detectFile
type FilePartitionResult struct {
	Url      string `json:"url,omitempty"`
	Expireat string `json:"expireat,omitempty"`
	Fid      string `json:"fid,omitempty"`
	Olen     int64  `json:"olen,omitempty"`
	OldMTime string `json:"old_m_time,omitempty"`
	Tag      string `json:"tag,omitempty"`
	Err      error  `json:"err,omitempty"`
}

func (filer *FilerEmbedded) FindFile2(filePath string, collection string, needtag bool, ctx *context.Context) (url1, expireat string, fid string, olen int64, oldMTime, tag string, err1 error, url2, expireat2 string, fid2 string, olen2 int64, oldMTime2, tag2 string, err2, err error) {
	if public.FilerLoadBalance {
		return filer.FindFileFromLBPS2(filePath, collection, needtag, ctx)
	}
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", "", "", 0, "", "", nil, "", "", "", 0, "", "", nil, err
	}
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		return "", "", "", 0, "", "", nil, "", "", "", 0, "", "", nil, lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", "", "", 0, "", "", nil, "", "", "", 0, "", "", nil, fmt.Errorf("can not find partition", pos)
	}

	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	if needtag {
		values.Add("needtag", "true")
	}

	var urls string
	count := len(lookup.Locations)
	firstPos := produceSeqByName(filePath, count)
	var res []FilePartitionResult
	for i := 0; i < count; i++ {
		urls = lookup.Locations[(firstPos+i)%count].Url
		if urls == "" {
			continue
		}
		glog.V(4).Infoln("find file:", filePath, " firstPos:", firstPos, " count:", count, " i:", i, " pos", pos)
		expireat, fid, olen, oldMTime, tag, err := postRequest(urls+"/find", values, ctx)
		if err != nil {
			glog.V(1).Info("find file err:", err, " i:", i, " count:", count, " url:", urls, " filename:", filePath, " fid:", fid, " collection:", collection, " partition:", pos)
		}
		res = append(res, FilePartitionResult{urls, expireat, fid, olen, oldMTime, tag, err})
	}
	if len(res) != 2 {
		return "", "", "", 0, "", "", nil, "", "", "", 0, "", "", nil, errors.New("Short of partition info. ")
	} else {
		samePartition := res[0].Expireat == res[1].Expireat && res[0].Fid == res[1].Fid && res[0].Olen == res[1].Olen && res[0].OldMTime == res[1].OldMTime && res[0].Tag == res[1].Tag && res[0].Err == res[1].Err
		if !samePartition {
			return res[0].Url, res[0].Expireat, res[0].Fid, res[0].Olen, res[0].OldMTime, res[0].Tag, res[0].Err, res[1].Url, res[1].Expireat, res[1].Fid, res[1].Olen, res[1].OldMTime, res[1].Tag, res[1].Err, PartitionDiffErr
		} else {
			return res[0].Url, res[0].Expireat, res[0].Fid, res[0].Olen, res[0].OldMTime, res[0].Tag, res[0].Err, res[1].Url, res[1].Expireat, res[1].Fid, res[1].Olen, res[1].OldMTime, res[1].Tag, res[1].Err, nil
		}
	}
	return "", "", "", 0, "", "", nil, "", "", "", 0, "", "", nil, nil
}

//jjj filer去partition访问时，不能是完全随机，保证对同一个文件的操作都在一个partition上完成
//根据文件名来优先选择去哪个partition，失败则再去另一个
func produceSeqByName(filename string, copynum int) int {
	var sum = 0
	if copynum < 2 {
		return 0
	}
	for _, s := range filename {
		sum += int(s)
	}
	return sum % copynum
}

func (filer *FilerEmbedded) FindFileFromLBPS(filePath string, collection string, needtag bool,needFreshA bool, updateCycle int, ctx *context.Context) (expireat string, fid string, olen int64, oldMTime, tag,mtype,disposition,etag string, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		glog.V(0).Info("find file:", filePath, "HashDBPos err:", err)
		return "", "", 0, "", "","","","", err
	}
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		glog.V(0).Info("file file:", filePath, "PartitionLookup err:", lookupError)
		return "", "", 0, "", "","","","", lookupError
	}
	if len(lookup.Locations) == 0 {
		glog.V(0).Info("file file:", filePath, "PartitionLookup err: no location")
		return "", "", 0, "", "","","","", fmt.Errorf("can not find partition", pos)
	}

	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	if needtag {
		values.Add("needtag", "true")
	}
	if needFreshA {
		values.Add(public.NEEDFRESHACCESSTIME, "true")
	}
	values.Add(public.UPDATECYCLE, strconv.Itoa(updateCycle))
	count := len(lookup.Locations)
	//randpos := rand.Intn(count)
	firstPos := produceSeqByName(filePath, count)
	for i := 0; i < count; i++ {

		url := lookup.Locations[(firstPos+i)%count].Url
		if url == "" {
			url = lookup.Locations[(firstPos+i)%count].PublicUrl
		}

		/*urlLb := filer.loadBalancePS(url)
		if urlLb != url {
			values.Add("loadbalance", "true")
			url = urlLb
		}*/
		filer.updatePsLoadReqNum(url, 1)
		expireat, fid, olen, oldMTime, tag,mtype,disposition,etag,err = GetRequest(url+"/find", values, ctx)
		filer.updatePsLoadReqNum(url, -1)

		if err == nil {
			filer.updatePsLoadErrNum(url, -1)
			break
		}
		glog.V(0).Infoln("find file:", filePath, "from ", url, "i", i, "err:", err)
		if err != NotFound {
			filer.updatePsLoadErrNum(url, 1)
		} else {
			filer.updatePsLoadErrNum(url, -1)
		}
	}

	return
}
func (filer *FilerEmbedded) FindFileFromLBPS2(filePath string, collection string, needtag bool, ctx *context.Context) (url1, expireat string, fid string, olen int64, oldMTime, tag string, err1 error, url2, expireat2 string, fid2 string, olen2 int64, oldMTime2, tag2 string, err2, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		glog.V(0).Info("find file:", filePath, "HashDBPos err:", err)
		return "", "", "", 0, "", "", nil, "", "", "", 0, "", "", nil, err
	}
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		glog.V(0).Info("file file:", filePath, "PartitionLookup err:", lookupError)
		return "", "", "", 0, "", "", nil, "", "", "", 0, "", "", nil, lookupError
	}
	if len(lookup.Locations) == 0 {
		glog.V(0).Info("file file:", filePath, "PartitionLookup err: no location")
		return "", "", "", 0, "", "", nil, "", "", "", 0, "", "", nil, fmt.Errorf("can not find partition", pos)
	}

	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	if needtag {
		values.Add("needtag", "true")
	}
	count := len(lookup.Locations)
	firstPos := produceSeqByName(filePath, count)
	var res []FilePartitionResult
	for i := 0; i < count; i++ {

		url := lookup.Locations[(firstPos+i)%count].Url
		if url == "" {
			url = lookup.Locations[(firstPos+i)%count].PublicUrl
		}

		filer.updatePsLoadReqNum(url, 1)
		expireat, fid, olen, oldMTime, tag, err := postRequest(url+"/find", values, ctx)
		filer.updatePsLoadReqNum(url, -1)

		if err == nil {
			filer.updatePsLoadErrNum(url, -1)
			res = append(res, FilePartitionResult{url, expireat, fid, olen, oldMTime, tag, err})
		}
		glog.V(0).Info("find file:", filePath, "from ", url, "i", i, "err:", err)
		if err != NotFound {
			filer.updatePsLoadErrNum(url, 1)
		} else {
			filer.updatePsLoadErrNum(url, -1)
		}
	}

	if len(res) != 2 {
		return "", "", "", 0, "", "", nil, "", "", "", 0, "", "", nil, errors.New("Short of partition info. ")
	} else {
		samePartition := res[0].Expireat == res[1].Expireat && res[0].Fid == res[1].Fid && res[0].Olen == res[1].Olen && res[0].OldMTime == res[1].OldMTime && res[0].Tag == res[1].Tag && res[0].Err == res[1].Err
		if !samePartition {
			return res[0].Url, res[0].Expireat, res[0].Fid, res[0].Olen, res[0].OldMTime, res[0].Tag, res[0].Err, res[1].Url, res[1].Expireat, res[1].Fid, res[1].Olen, res[1].OldMTime, res[1].Tag, res[1].Err, PartitionDiffErr
		} else {
			return res[0].Url, res[0].Expireat, res[0].Fid, res[0].Olen, res[0].OldMTime, res[0].Tag, res[0].Err, res[1].Url, res[1].Expireat, res[1].Fid, res[1].Olen, res[1].OldMTime, res[1].Tag, res[1].Err, nil
		}
	}
	return "", "", "", 0, "", "", nil, "", "", "", 0, "", "", nil, nil
}

func (filer *FilerEmbedded) FindFileWithTag(filePath string, collection string, ctx *context.Context) (expireat string, fid string, olen int64, oldMTime, tag string, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", "", 0, "", "", err
	}
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		return "", "", 0, "", "", lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", "", 0, "", "", fmt.Errorf("can not find partition", pos)
	}

	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	values.Add("needtag", "true")
	//	randpos := rand.Intn(len(lookup.Locations))
	//	expireat, fid, olen, err = postRequest(lookup.Locations[randpos].Url+"/find", values)
	//	if err != nil {
	//		glog.V(2).Info("find filemap failed at:", lookup.Locations[randpos].Url, filePath, err.Error(), "retry again!")
	//		expireat, fid, olen, err = postRequest(lookup.Locations[(randpos+1)%len(lookup.Locations)].Url+"/find", values)
	//	}

	var urls string
	count := len(lookup.Locations)
	firstPos := produceSeqByName(filePath, count)
	for i := 0; i < count; i++ {
		urls = lookup.Locations[(firstPos+i)%count].Url
		if urls == "" {
			continue
		}
		glog.V(4).Infoln("find file:", filePath, " firstPos:", firstPos, " count:", count, " i:", i, " pos", pos)
		expireat, fid, olen, oldMTime, tag, err = postRequest(urls+"/find", values, ctx)
		if err != nil {
			glog.V(0).Info("find file err:", err, " i:", i, " count:", count, " url:", urls, " filename:", filePath, " collection:", collection, " partition:", pos)
			continue
		}
		return
	}
	return
}

func (filer *FilerEmbedded) SetFileExpire(filePath string, collection string, lifeCycle, mtime string, ctx *context.Context) (oldMTime string, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", err
	}
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		return "", lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", fmt.Errorf("can not find partition", pos)
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
		glog.V(4).Infoln("set file expire :", filePath, " firstPos:", firstPos, " count:", count, " i:", i, " pos", pos)
		_, _, _, oldMTime, err = postRequests3(urls, "/set/expire", values, ctx)
		if err != nil {
			glog.V(0).Info("set file expire err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", filePath, " collection:", collection, " partition:", pos)
			continue
		}
		return
	}
	if err != nil && err.Error() == NotFound.Error() {
		return "", NotFound
	}
	return
}

func (filer *FilerEmbedded) SetFileTag(filePath string, collection string, tag string, ctx *context.Context) (err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return err
	}
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
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
	values.Add("tag", tag)

	urls := make([]string, 1)
	count := len(lookup.Locations)
	firstPos := produceSeqByName(filePath, count)
	for i := 0; i < count; i++ {
		urls[0] = lookup.Locations[(firstPos+i)%count].Url
		if urls[0] == "" {
			continue
		}
		glog.V(4).Infoln("set file tag:", filePath, " firstPos:", firstPos, " count:", count, " i:", i, " pos", pos)
		_, _, _, _, err = postRequests3(urls, "/set/tag", values, ctx)
		if err != nil {
			glog.V(0).Info("set file tag err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", filePath, " collection:", collection, " partition:", pos)
			continue
		}
		return
	}
	if err != nil && err.Error() == NotFound.Error() {
		return NotFound
	}
	return
}

//zqx put tagging
func (filer *FilerEmbedded) SetFileTagging(filePath string, collection string, tagging string, ctx *context.Context) (oldtagging string, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", err
	}
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		return "", lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", fmt.Errorf("can not find partition", pos)
	}
	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	values.Add("tagging", tagging)

	urls := make([]string, 1)
	count := len(lookup.Locations)
	firstPos := produceSeqByName(filePath, count)
	for i := 0; i < count; i++ {
		urls[0] = lookup.Locations[(firstPos+i)%count].Url
		if urls[0] == "" {
			continue
		}
		glog.V(4).Infoln("set file tag:", filePath, " firstPos:", firstPos, " count:", count, " i:", i, " pos", pos)
		_, _, _, _, err = postRequests3(urls, "/set/tagging", values, ctx)
		if err != nil {
			glog.V(0).Info("set file tagging err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", filePath, " collection:", collection, " partition:", pos)
			continue
		}
		return
	}
	if err != nil && err.Error() == NotFound.Error() {
		return "", NotFound
	}
	return
}

func (filer *FilerEmbedded) GetFileTagging(filePath string, collection string, ctx *context.Context) (tagging string, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", err
	}
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		return "", lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", fmt.Errorf("can not find partition", pos)
	}

	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)

	var urls string
	count := len(lookup.Locations)
	firstPos := produceSeqByName(filePath, count)
	for i := 0; i < count; i++ {
		urls = lookup.Locations[(firstPos+i)%count].Url
		if urls == "" {
			continue
		}
		glog.V(4).Infoln("find file:", filePath, " firstPos:", firstPos, " count:", count, " i:", i, " pos", pos)
		tagging, err = GetTaggingRequest(urls+"/get/tagging", values, ctx)
		if err != nil {
			glog.V(1).Infoln("find file err:", err, " i:", i, " count:", count, " url:", urls, " filename:", filePath, " collection:", collection, " partition:", pos)
			continue
		}
		return
	}

	return
}

func (filer *FilerEmbedded) DelFileTagging(filePath string, collection string, ctx *context.Context) (err error) {
	return
}

func (filer *FilerEmbedded) DeleteFile(filePath, collection string, ctx *context.Context) (oldfid string, olen int64, oldMTime string, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", 0, "", err
	}
	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		return "", 0, "", lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", 0, "", fmt.Errorf("can not find partition", pos)
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
		_, oldfid, olen, oldMTime, err = postRequests3(urls, ACTION_DELETE, values, ctx)
		if err != nil {
			glog.V(0).Info("delete file err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", filePath, " collection:", collection, " partition:", pos)
			if err.Error() == "NotFound" {
				return "", 0, "", NotFound
			} else {
				continue
			}
		}
		return
	}
	return
}
func (filer *FilerEmbedded) DeleteFile2(filePath, collection string, ctx *context.Context) (oldfid string, olen int64, oldMTime,oldTag string, err error) {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return "", 0, "","", err
	}
	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	values.Add("needtag", "true")
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		return "", 0, "", "",lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", 0, "", "",fmt.Errorf("can not find partition", pos)
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
		_, oldfid, olen, oldMTime,oldTag, err = postRequests2(urls, ACTION_DELETE, values, ctx)
		if err != nil {
			glog.V(0).Info("delete file err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", filePath, " collection:", collection, " partition:", pos)
			if err.Error() == "NotFound" {
				return "", 0, "","", NotFound
			} else {
				continue
			}
		}
		return
	}
	return
}

func (filer *FilerEmbedded) ParmAdjust(parmtype string, value string) (err error) {
	return
}
func (filer *FilerEmbedded) HashDBPos(filePath string, collection string, ctx *context.Context) (uint32, error) {
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

//用于重命名的删除，判定条件有个fid
func (filer *FilerEmbedded) deletewithfid(filePath, fid, collection string, ctx *context.Context) error {
	pos, err := filer.HashDBPos(filePath, collection, ctx)
	if err != nil {
		return err
	}
	values := make(url.Values)
	values.Add("partition", strconv.Itoa(int(pos)))
	values.Add("collection", collection)
	values.Add("filename", filePath)
	values.Add("fileid", fid)
	//
	values.Add("type", "rename")
	lookup, lookupError := operation.PartitionLookup(*filer.master, strconv.Itoa(int(pos)), "", collection, false, ctx)
	if lookupError != nil {
		return lookupError
	}
	if len(lookup.Locations) == 0 {
		return fmt.Errorf("can not find partition", pos)
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
		_, _, _, _, err = postRequests3(urls, ACTION_DELETE, values, ctx)
		if err != nil {
			glog.V(0).Info("delete file err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", filePath, " fid:", fid, " collection:", collection, " partition:", pos)
			if err.Error() == "NotFound" {
				//删除过程中的不可预见的错误，是不希望出现的
				return err
			}
			continue
		}
		return nil
	}
	return err
}

const (
	MAX_REQNUM_PER_PS = 10
	MAX_ERRNUM_PER_PS = 10
)

type PsLoadStat struct {
	reqDoing int
	errNum   int
	sync.RWMutex
}

func (filer *FilerEmbedded) selectPS(stat interface{}) bool {
	psls, ok := stat.(*PsLoadStat)
	if !ok {
		return false
	}
	psls.RLock()
	defer psls.RUnlock()
	if psls.reqDoing < MAX_REQNUM_PER_PS && psls.errNum < MAX_ERRNUM_PER_PS {
		return true
	}
	return false
}
func (filer *FilerEmbedded) updatePsLoadReqNum(loc string, reqNum int) {
	psls, ok := filer.pslb.Get(loc).(*PsLoadStat)
	if !ok || psls == nil {
		if reqNum < 0 {
			return
		}
		psls = &PsLoadStat{}
		psls = filer.pslb.Set(loc, psls).(*PsLoadStat)
	}
	psls.Lock()
	psls.reqDoing += reqNum
	psls.Unlock()

}
func (filer *FilerEmbedded) updatePsLoadErrNum(loc string, errNum int) {
	psls, ok := filer.pslb.Get(loc).(*PsLoadStat)
	if ok && psls != nil {
		needDel := false
		psls.Lock()
		if errNum < 0 {
			psls.errNum = 0
			psls.Unlock()
			return
		}
		psls.errNum += errNum
		if psls.errNum >= MAX_ERRNUM_PER_PS {
			needDel = true
		}
		psls.Unlock()
		if needDel {
			filer.pslb.Remove(loc)
		}
	}

}
func (filer *FilerEmbedded) loadBalancePS(loc string) string {
	dstLoc := filer.pslb.SelectFromCurrent(loc)
	/*
		if loc != dstLoc {
			glog.V(0).Infoln("loc:", loc, "dstLoc:", dstLoc)
		}
	*/
	return dstLoc

}
func (filer *FilerEmbedded) GetRingSelector() *util.RingSelector {

	return filer.pslb

}
