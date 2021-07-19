package filemap

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"net/url"
	"path"
	"strconv"
	"time"

	"context"
	"io/ioutil"
	"net/http"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
)

type FileId string //file id on weedfs

type FileEntry struct {
	Name string `json:"name,omitempty"` //file name without path
	Id   FileId `json:"fid,omitempty"`
}

type Filer interface {
	CreateFile(filePath string, length int64, mtime, fid, expire, tag, collection,disposition,mimeType,etag string, ctx *context.Context) (oldFid string, olen int64, oldMTime string, err error)
	//CreateFile2  expire second
	CreateFile2(filePath string, length int64, mtime, fid, expire, tag, collection string, ctx *context.Context) (oldFid string, olen int64, oldMTime string, err error)
	//FindFile(filePath string, collection string, needtag bool, ctx *context.Context) (expireat string, fid string, olen int64, oldMTime, tag, mtype, disposition, etag string, err error)
	FindFile(filePath string, collection string, needtag bool,needFreshAcsTime bool, updateCycle int, ctx *context.Context) (expireat string, fid string, olen int64, oldMTime, tag, mtype, disposition, etag string, err error)
	FindFile2(filePath string, collection string, needtag bool, ctx *context.Context) (url1, expireat string, fid string, olen int64, oldMTime, tag string, err1 error, url2, expireat2 string, fid2 string, olen2 int64, oldMTime2, tag2 string, err2, err error)
	SetFileExpire(filePath string, collection string, lifeCycle, mtime string, ctx *context.Context) (oldMTime string, err error)
	SetFileTag(filePath string, collection string, tag string, ctx *context.Context) (err error)
	SetFileTagging(filePath string, collection string, tag string, ctx *context.Context) (oldtagging string, err error)
	GetFileTagging(filePath string, collection string, ctx *context.Context) (tagging string, err error)
	DelFileTagging(filePath string, collection string, ctx *context.Context) (err error)
	//删除，要是主partition活着（正常通信）但是数据库中不存在。返回的size为空。
	DeleteFile(filePath string, collection string, ctx *context.Context) (fid string, olen int64, oldMTime string, err error)
	DeleteFile2(filePath string, collection string, ctx *context.Context) (fid string, olen int64, oldMTime,tag string, err error)

	ParmAdjust(parmtype string, value string) (err error)
	//oldfid, 被覆盖的文件的fid    oldlen2 是被覆盖的文件的程度
	//oldMTime oldexpireat oldlen 被重命名的文件信息
	Rename(fromfilePath, tofilePath string, mtime, collection string, ctx *context.Context) (oldfid, oldMTime, oldexpireat string, oldlen, oldlen2 int64, err error)
}

//Some tool functions
func postRequest(url string, values url.Values, ctx *context.Context) (expireat string, oldfid string, olen int64, oldMTime, tag string, err error) {
	var ret FilerPartitionResult
	if url == "" {
		err = CreateFileMapFailed
	}
	headers := make(map[string]string)
	if ctx != nil {
		reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
		headers[public.SDOSS_REQUEST_ID] = reqid
	}

	resp, err := util.MakeHttpRequest("http://"+url, "POST", values, headers, false)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	jsonBlob, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	glog.V(2).Info("assign result :", string(jsonBlob))
	if err == nil {
		err = json.Unmarshal(jsonBlob, &ret)
	}
	if err == nil {
		if ret.Error == "" {
			oldfid = ret.Fid
			olen = ret.Length
			expireat = ret.ExpireAt
			oldMTime = ret.MTime
			tag = ret.Tag
		} else {
			err = fmt.Errorf(ret.Error)
		}
	}
	if err != nil && err.Error() == NotFound.Error() {
		err = NotFound
	}
	return
}

//Some tool functions
func GetRequest(url string, values url.Values, ctx *context.Context) (expireat string, oldfid string, olen int64, oldMTime, tag,mtype,disposition,etag string, err error) {
	var ret FilerPartitionResult
	if url == "" {
		err = CreateFileMapFailed
	}
	headers := make(map[string]string)
	if ctx != nil {
		reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
		headers[public.SDOSS_REQUEST_ID] = reqid
	}

	resp, err := util.MakeHttpRequestQuickTimeout("http://"+url, "GET", values, headers)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	jsonBlob, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	glog.V(2).Info("assign result :", string(jsonBlob))
	if err == nil {
		err = json.Unmarshal(jsonBlob, &ret)
	}
	if err == nil {
		if ret.Error == "" {
			oldfid = ret.Fid
			olen = ret.Length
			expireat = ret.ExpireAt
			oldMTime = ret.MTime
			tag = ret.Tag
			mtype = ret.Mtype
			disposition = ret.Disposition
			etag = ret.Etag
		} else {
			err = fmt.Errorf(ret.Error)
		}
	}
	if err != nil && err.Error() == NotFound.Error() {
		err = NotFound
	}
	return
}

func GetTaggingRequest(url string, values url.Values, ctx *context.Context) (tagging string, err error) {
	var ret FilerPartitionTaggingResult
	if url == "" {
		err = CreateFileMapFailed
	}
	headers := make(map[string]string)
	if ctx != nil {
		reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
		headers[public.SDOSS_REQUEST_ID] = reqid
	}

	resp, err := util.MakeHttpRequestQuickTimeout("http://"+url, "GET", values, headers)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	jsonBlob, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	glog.V(2).Info("assign result :", string(jsonBlob))
	if err == nil {
		err = json.Unmarshal(jsonBlob, &ret)
	}
	if err == nil {
		if ret.Error == "" {
			tagging = ret.Tagging
		} else {
			err = fmt.Errorf(ret.Error)
		}
	}
	if err != nil && err.Error() == NotFound.Error() {
		err = NotFound
	}
	return
}

func postRequests3(nodes []string, path string, values url.Values, ctx *context.Context) (expireat string, oldfid string, olen int64, oldMTime string, err error) {
	if len(nodes) < 1 {
		err = errors.New("No Node Specified!")
		return
	}
	trytimes := 0
	for i, node := range nodes {
		var ret FilerPartitionResult
		url := "http://" + node + path
		if url == "" {
			err = CreateFileMapFailed
			return
		}

		headers := make(map[string]string)
		if ctx != nil {
			reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
			headers[public.SDOSS_REQUEST_ID] = reqid
		}

		var jsonBlob []byte
		resp, errPost := util.MakeHttpRequest(url, "POST", values, headers, true)
		if errPost == nil {
			defer resp.Body.Close()
			jsonBlob, errPost = ioutil.ReadAll(resp.Body)
		}
		if errPost != nil {
			err = errPost
			glog.V(4).Infoln("Network failed when request to", node, err.Error())
			if i < len(nodes)-1 && path == ACTION_CREATE {
				time.Sleep(time.Duration(100+rand.Intn(5)) * time.Millisecond)
			}
			continue
		}
		glog.V(2).Info(values, "assign result :", string(jsonBlob))

		trytimes++
		err = json.Unmarshal(jsonBlob, &ret)
		if err != nil {
			glog.V(2).Infoln("Bad json data", err.Error())
			if i < len(nodes)-1 && path == ACTION_CREATE {
				time.Sleep(time.Duration(100+rand.Intn(5)) * time.Millisecond)
			}
			continue
		}
		if ret.Error == "" {
			err = nil
			oldfid = ret.Fid
			olen = ret.Length
			expireat = ret.ExpireAt
			oldMTime = ret.MTime
			if path == ACTION_DELETE && trytimes > 1 {
				olen = 0
			}
			glog.V(4).Infoln("write file:", values.Get("filename"), " to partition:", node, " success. new_filelength:", values.Get("length"))
			break
		} else {
			err = fmt.Errorf(ret.Error)
			if i < len(nodes)-1 && path == ACTION_CREATE {
				time.Sleep(time.Duration(100+rand.Intn(5)) * time.Millisecond)
			}
			continue
		}
	}
	return
}
func postRequests2(nodes []string, path string, values url.Values, ctx *context.Context) (expireat string, oldfid string, olen int64, oldMTime,oldTag string, err error) {
	if len(nodes) < 1 {
		err = errors.New("No Node Specified!")
		return
	}
	trytimes := 0
	for i, node := range nodes {
		var ret FilerPartitionResult
		url := "http://" + node + path
		if url == "" {
			err = CreateFileMapFailed
			return
		}

		headers := make(map[string]string)
		if ctx != nil {
			reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
			headers[public.SDOSS_REQUEST_ID] = reqid
		}

		var jsonBlob []byte
		resp, errPost := util.MakeHttpRequest(url, "POST", values, headers, true)
		if errPost == nil {
			defer resp.Body.Close()
			jsonBlob, errPost = ioutil.ReadAll(resp.Body)
		}
		if errPost != nil {
			err = errPost
			glog.V(4).Infoln("Network failed when request to", node, err.Error())
			if i < len(nodes)-1 && path == ACTION_CREATE {
				time.Sleep(time.Duration(100+rand.Intn(5)) * time.Millisecond)
			}
			continue
		}
		glog.V(2).Info(values, "assign result :", string(jsonBlob))

		trytimes++
		err = json.Unmarshal(jsonBlob, &ret)
		if err != nil {
			glog.V(2).Infoln("Bad json data", err.Error())
			if i < len(nodes)-1 && path == ACTION_CREATE {
				time.Sleep(time.Duration(100+rand.Intn(5)) * time.Millisecond)
			}
			continue
		}
		if ret.Error == "" {
			err = nil
			oldfid = ret.Fid
			olen = ret.Length
			expireat = ret.ExpireAt
			oldMTime = ret.MTime
			oldTag = ret.Tag
			if path == ACTION_DELETE && trytimes > 1 {
				olen = 0
			}
			glog.V(4).Infoln("write file:", values.Get("filename"), " to partition:", node, " success. new_filelength:", values.Get("length"))
			break
		} else {
			err = fmt.Errorf(ret.Error)
			if i < len(nodes)-1 && path == ACTION_CREATE {
				time.Sleep(time.Duration(100+rand.Intn(5)) * time.Millisecond)
			}
			continue
		}
	}
	return
}

func FindFile(fPath, ms, col string,needTag bool, ctx *context.Context) (exp string, fid string, size int64, oldMTime,tag,mtype,disposition,etag string, err error) {
	var (
		lookupLen int
		pTotal    int
	)
	if pTotal, _, err = operation.GetCollectionPartCountRp(ms, col, ctx); err != nil {
		return
	} else if pTotal <= 0 {
		err = errors.New("failed to get partition num!")
		return
	}
	fPath = path.Join("/", fPath)
	pNum := crc32.ChecksumIEEE([]byte(fPath)) % uint32(pTotal)
	pNumStr := strconv.Itoa(int(pNum))
	lookup, err := operation.PartitionLookup(ms, pNumStr, "", col, false, ctx)
	if err != nil {
		return
	}
	if lookupLen = len(lookup.Locations); lookupLen <= 0 {
		err = fmt.Errorf("can not find partition", pNum)
		return
	}
	values := make(url.Values)
	values.Add("partition", pNumStr)
	values.Add("collection", col)
	values.Add("filename", fPath)
	if needTag{
		values.Add("needtag", "true")
	}

	for i, j := 0, int(pNum); i < lookupLen; i++ {
		url := "http://" + lookup.Locations[(i+j)%lookupLen].Url + "/find"
		headers := make(map[string]string)
		if ctx != nil {
			reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
			headers[public.SDOSS_REQUEST_ID] = reqid
		}

		var jsonBlob []byte
		var resp *http.Response
		resp, err = util.MakeHttpRequest(url, "GET", values, headers, true)
		if err == nil {
			defer resp.Body.Close()
			jsonBlob, err = ioutil.ReadAll(resp.Body)
		}
		if err == nil {
			var (
				ret FilerPartitionResult
			)
			if err = json.Unmarshal(jsonBlob, &ret); err == nil {
				if ret.Error == "" {
					fid = ret.Fid
					size = ret.Length
					exp = ret.ExpireAt
					oldMTime = ret.MTime
					tag = ret.Tag
					mtype = ret.Mtype
					disposition = ret.Disposition
					etag = ret.Etag
					break
				} else {
					err = fmt.Errorf(ret.Error)
				}
			}
		}
	}
	return
}

func UpdateFileId(fPath, oldFid,newFid, col,ms string, ctx *context.Context) (err error) {
	var (
		lookupLen int
		pTotal    int
	)
	if pTotal, _, err = operation.GetCollectionPartCountRp(ms, col, ctx); err != nil {
		return
	} else if pTotal <= 0 {
		err = errors.New("failed to get partition num!")
		return
	}
	fPath = path.Join("/", fPath)
	pNum := crc32.ChecksumIEEE([]byte(fPath)) % uint32(pTotal)
	pNumStr := strconv.Itoa(int(pNum))
	lookup, err := operation.PartitionLookup(ms, pNumStr, "", col, false, ctx)
	if err != nil {
		return
	}
	if lookupLen = len(lookup.Locations); lookupLen <= 0 {
		err = fmt.Errorf("can not find partition", pNum)
		return
	}
	values := make(url.Values)
	values.Add("partition", pNumStr)
	values.Add("collection", col)
	values.Add("oldFid", oldFid)
	values.Add("newFid", newFid)
	values.Add("filename", fPath)
	for i, j := 0, int(pNum); i < lookupLen; i++ {
		url := "http://" + lookup.Locations[(i+j)%lookupLen].Url + "/updateFid"
		headers := make(map[string]string)
		if ctx != nil {
			reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
			headers[public.SDOSS_REQUEST_ID] = reqid
		}

		var jsonBlob []byte
		var resp *http.Response
		resp, err = util.MakeHttpRequest(url, "POST", values, headers, true)
		if err == nil {
			defer resp.Body.Close()
			jsonBlob, err = ioutil.ReadAll(resp.Body)
		}
		if err == nil {
			var (
				ret FilerPartitionResult
			)
			if err = json.Unmarshal(jsonBlob, &ret); err == nil {
				if ret.Error == "" {
					break
				} else {
					err = fmt.Errorf(ret.Error)
				}
			}
		}
	}
	return
}

func CreatECFile(fPath, newFid, col,ms,mtime,expireat,mtype,disposition,etag string,obsize int64, ctx *context.Context) (oldfid string,olen int64,oldMTime string,err error) {
	var (
		lookupLen int
		pTotal    int
	)
	if pTotal, _, err = operation.GetCollectionPartCountRp(ms, col, ctx); err != nil {
		return
	} else if pTotal <= 0 {
		err = errors.New("failed to get partition num!")
		return
	}
	fPath = path.Join("/", fPath)
	pNum := crc32.ChecksumIEEE([]byte(fPath)) % uint32(pTotal)
	pNumStr := strconv.Itoa(int(pNum))
	lookup, err := operation.PartitionLookup(ms, pNumStr, "", col, false, ctx)
	if err != nil {
		return
	}
	if lookupLen = len(lookup.Locations); lookupLen <= 0 {
		err = fmt.Errorf("can not find partition", pNum)
		return
	}
	values := make(url.Values)
	values.Add("partition", pNumStr)
	values.Add("collection", col)
	values.Add("length", fmt.Sprintf("%d",obsize))
	values.Add("mtime", mtime)
	values.Add("filename", fPath)
	values.Add("fileid", newFid)
	values.Add("expireat", expireat)
	values.Add("disposition", disposition)
	values.Add("mimeType", mtype)
	values.Add("etag", etag)

	urls := make([]string, 1)
	count := len(lookup.Locations)
	firstPos := produceSeqByName(fPath, count)
	for i := 0; i < count; i++ {
		urls[0] = lookup.Locations[(firstPos+i)%count].Url
		if urls[0] == "" {
			continue
		}
		glog.V(4).Infoln("create file:", fPath, " firstPos:", firstPos, " count:", count, " i:", i, " pos", pNumStr)
		_, oldfid, olen, oldMTime, err = postRequests3(urls, ACTION_CREATE, values, ctx)
		if err != nil {
			glog.V(0).Info("create file err:", err, " i:", i, " count:", count, " url:", urls[0], " filename:", fPath, " fid:", newFid, " collection:", col, " partition:", pNumStr)
			continue
		}
		if oldfid == newFid && olen == obsize {
			oldfid = ""
			olen = 0
		}
		return
	}
	return "", 0, "", errors.New("No availabe partition url")
}

