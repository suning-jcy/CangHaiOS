package main

import (
	"errors"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/weed-fs/go/auth"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/util"

	"net/url"
	"code.google.com/p/weed-fs/go/public"
	"fmt"
)

//path is fs-path
//like "/"  or "/mnt/data/weedfs-redis/src"
func (sf *SdossDriver) path2name(path string) (isDir bool, fileName, parentKey, myKey string, err error) {

	parts := strings.Split(path, "/")
	isDir = false
	if strings.HasSuffix(path, "/") {
		isDir = true
	}
	if len(parts) == 2 {
		if parts[1] == "" {
			myKey = "/"
			parentKey = ""
			fileName = "/"
			isDir = true
		} else {
			myKey = parts[1]
			parentKey = "/"
			fileName = parts[1]
			isDir = false
		}

	} else {
		if isDir {
			fileName = parts[len(parts)-2] + "/"
		} else {
			fileName = parts[len(parts)-1]
		}
		myKey = path[1:]
		parentKey = myKey[:len(myKey)-len(fileName)]
		if parentKey == "" {
			parentKey = "/"
		}
	}
	err = nil
	return

}

//object: object_key to filer (+"/" if dir)
//mathod,PUT/DELETE/GET
//parameters,request param
func (sf *SdossDriver) urlRequestOLD(object, method string, data []byte, parameters map[string]string) (res []byte, err error) {
	urlpath := "/" + sf.Account + "/" + sf.Bucket + "/" + object

	if parameters != nil && len(parameters) > 0 {
		urlpath += "?"
		for k, v := range parameters {
			urlpath = urlpath + k + "=" + v + "&"
		}
		urlpath = urlpath[:len(urlpath)-1]
	}
	glog.V(4).Infoln("getHeaders  bucket：" + sf.Bucket)

	header := sf.getHeaders(sf.Bucket, object, method)

	var bucketRspStatus *http.Response
	var errrsp2 error

	//glog.V(4).Infoln("urlRequest  sf.Filer：" + sf.Filer + " urlpath:" + urlpath + " method " + method)
	glog.V(0).Infoln("urlRequest url:", urlpath)
	tempfiler := sf.FilerSelecter.GetFiler(false)
	if tempfiler == "" {
		err = errors.New("no available filer server")
		return nil, err
	}
	bucketRspStatus, errrsp2 = util.MakeRequest_timeout(tempfiler, urlpath, method, data, header)
	if errrsp2 != nil {
		tempfiler = sf.FilerSelecter.GetFiler(true)
		if tempfiler == "" {
			err = errors.New("no available filer server")
			return nil, err
		}
		//try again
		bucketRspStatus, errrsp2 = util.MakeRequest_timeout(tempfiler, urlpath, method, data, header)
		if errrsp2 != nil {
			return nil, errrsp2
		}
	}

	//only create dir
	if method == "PUT" {
		if bucketRspStatus.StatusCode != http.StatusCreated && bucketRspStatus.StatusCode != http.StatusOK {
			err = errors.New("error get from filer with StatusCode " + strconv.Itoa(bucketRspStatus.StatusCode))
			return
		}
		return nil, nil
	}
	if method == "DELETE" {
		if bucketRspStatus.StatusCode != http.StatusOK && bucketRspStatus.StatusCode != http.StatusNoContent && bucketRspStatus.StatusCode != http.StatusAccepted {
			err = errors.New("error get from filer with StatusCode " + strconv.Itoa(bucketRspStatus.StatusCode))
			return
		}
		return nil, nil
	}
	if bucketRspStatus.StatusCode != http.StatusOK {
		err = errors.New("error get from filer with StatusCode " + strconv.Itoa(bucketRspStatus.StatusCode))
		return
	}

	res, _ = ioutil.ReadAll(bucketRspStatus.Body)
	bucketRspStatus.Body.Close()
	return
}

//upload file to filer
//todo 使用ftp一样的流式上传方式
func (sf *SdossDriver) upload(object string, reader io.Reader) (uint32, error) {

	urlpath := "/" + sf.Account + "/" + sf.Bucket + "/"
	if object != "" && object != "/" {
		urlpath += object
	}
	glog.V(4).Infoln("upload", urlpath)
	//urlpath = url.QueryEscape(urlpath)
	//urlpath = strings.Replace(urlpath, "%2F", "/", -1)
	//urlpath = strings.Replace(urlpath, "%25", "%", -1)

	MimeType := ""
	ext := strings.ToLower(path.Ext(object))
	IsGzipped := ext == ".gz"
	if ext != "" {
		MimeType = mime.TypeByExtension(ext)
	}
	if object == "/" {
		object = ""
	}
	header := getHeaders(sf.AccessKeyId, sf.AccessKeysecret, sf.Bucket, object, "POST")
	var res *operation.UploadResult
	var err error
	tempfiler := sf.FilerSelecter.GetFiler(false)
	if tempfiler == "" {
		err = errors.New("no available filer server")
		return 0, err
	}
	res, err = operation.Upload3("http://"+tempfiler+urlpath, urlpath, reader, IsGzipped, MimeType, header)
	if err != nil {
		tempfiler = sf.FilerSelecter.GetFiler(true)
		if tempfiler == "" {
			err = errors.New("no available  filer server")
			return 0, err
		}
		res, err = operation.Upload3("http://"+tempfiler+urlpath, urlpath, reader, IsGzipped, MimeType, header)
		if err != nil {
			glog.V(0).Infoln("failed to upload file!", err.Error())
			return 0, err
		}
	}

	if err != nil {
		return 0, err
	} else {
		return res.Size, nil
	}

}

//todo 鉴权要调整在url中
func (sf *SdossDriver) getHeaders(bucket, object, method string) map[string]string {
	date := util.ParseUnix(strconv.FormatInt(time.Now().Unix(), 10))
	Signature := auth.GetSignature(bucket, object, method, date, sf.AccessKeysecret, "")
	header := make(map[string]string)
	header["Authorization"] = " SDOSS " + sf.AccessKeyId + ":" + Signature
	header["Date"] = date
	return header
}

//包装了重试，请求鉴权放到头内
func (sf *SdossDriver) urlRequest(object, method string, parameters map[string]string, heads map[string]string) (data []byte, err error) {
	tempfiler := sf.FilerSelecter.GetFiler(false)
	if tempfiler == "" {
		err = errors.New("no available filer server")
		return
	}
	data, err = urlRequest(tempfiler, sf.Account, sf.Bucket, object, method, parameters, heads, sf.AccessKeyId, sf.AccessKeysecret)
	if err != nil {
		glog.V(4).Infoln("urlRequest:", err)
		if err == ErrBadAuth {
			err = FUSEErrBadSerAcc
			return
		} else if err == ErrNotExist {
			return
		}
		tempfiler = sf.FilerSelecter.GetFiler(true)
		if tempfiler == "" {
			err = errors.New("no available filer server")
			return
		}
		data, err = urlRequest(tempfiler, sf.Account, sf.Bucket, object, method, parameters, heads, sf.AccessKeyId, sf.AccessKeysecret)
		if err != nil {
			glog.V(0).Infoln(err.Error())
			return
		}
	}
	return data, err
}

//其他请求 列目录直接使用
func urlRequest(server string, account, bucket, object, method string, parameters map[string]string, heads map[string]string, AccessID, AccessSecret string) (res []byte, err error) {

	urlpath := "/" + account + "/" + bucket
	tempkey1:=url.QueryEscape(object)
	if object != "" && tempkey1 != "/" {
		if strings.HasPrefix(object, "/") {
			urlpath +=  tempkey1
		} else {
			urlpath += "/" + tempkey1
		}

	} else {
		object = ""
	}

	if parameters != nil && len(parameters) > 0 {
		urlpath += "?"
		for k, v := range parameters {
			if k == "dir" { //dir作为参数时，会带'/'，此时不能转义
				names := strings.Split(v,"/")
				var vv string
				for _,v1 := range names{
					if len(v1) != 0{
						vv += url.QueryEscape(v1) + "/"
					}
				}
				if len(vv) == 0{
					vv += "/"
				}
				urlpath = urlpath + k + "=" + vv + "&"
			}else {//如果参数中携带特殊字符，比如空格，&等，要转义
				urlpath = urlpath + k + "=" + url.QueryEscape(v) + "&"
			}
		}
		urlpath = urlpath[:len(urlpath)-1]
	}

	realpath, unescapeerr := url.QueryUnescape(tempkey1)
	if unescapeerr != nil {
		glog.V(0).Infoln(unescapeerr)
		err = ErrRequest
		return
	}
	authheader := getHeaders(AccessID, AccessSecret, bucket, realpath, method)
	if heads == nil {
		heads = make(map[string]string)
	}
	for k, v := range authheader {
		heads[k] = v
	}
	glog.V(4).Infoln("make request:",method, server, urlpath, heads," object:",object)
	var resp *http.Response
	if method == "DELETE" {
		resp, err = util.MakeRequest(server, urlpath, method, nil, heads)
	} else {
		resp, err = util.MakeRequest_timeout(server, urlpath, method, nil, heads)
	}
	if err != nil {
		glog.V(0).Infoln(" error :", err.Error())
		return nil, err
	}
	data, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	if resp.StatusCode == 403 {
		return nil, ErrBadAuth
	}
	if resp.StatusCode == 404 {
		return nil, ErrNotExist
	}

	if resp.StatusCode < 200 || resp.StatusCode > 300 {
		glog.V(0).Infoln(" resp.StatusCode :", resp.StatusCode)
		return nil, errors.New("Error with StatusCode :" + fmt.Sprint(resp.StatusCode))
	}

	return data, nil
}

func getHeaders(AccessKeyId, AccessKeysecret, bucket, object, method string) map[string]string {
	date := util.ParseUnix(strconv.FormatInt(time.Now().Unix(), 10))
	return getHeadersWithDate(AccessKeyId, AccessKeysecret, bucket, object, method, date)
}

func getHeadersWithDate(AccessKeyId, AccessKeysecret, bucket, object, method, date string) map[string]string {
	Signature := auth.GetSignature(bucket, object, method, date, AccessKeysecret, "")
	header := make(map[string]string)
	header["Authorization"] = " SDOSS " + AccessKeyId + ":" + Signature
	header["Date"] = date
	header["User-Agent"] = public.FUSE_User_Agent
	return header
}
