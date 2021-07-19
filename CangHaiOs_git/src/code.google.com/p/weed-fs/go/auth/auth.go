package auth

import (
	"net/http"
	"time"

	Bucket "code.google.com/p/weed-fs/go/bucket"
	"code.google.com/p/weed-fs/go/cache"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
	"code.google.com/p/weed-fs/go/public"
	"context"
	"strings"
	"strconv"
)

type AddAccessUserResult struct {
	Error string `json:"error,omitempty"`
}
type Auth interface {
	AuthValidCluster(r *http.Request, server, clusternum string, ctx *context.Context) (bool, error)
	AuthValid(r *http.Request, server string, ctx *context.Context) (bool, error)
	AuthType() string
}

func AuthRefererCluster(server *string, r *http.Request, clusternum string, ctx *context.Context) bool {
	reqid := ""
	if ctx != nil {
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}
	glog.V(1).Info(reqid, " start Auth check with Cluster")
	if r.Header.Get(AUTHORIZATION) != "" {
		glog.V(2).Info(reqid, " AUTHORIZATION is null")
		return true
	}
	account, bucket, object := util.ParseServiceUrlForCluster(r, clusternum)

	if object == "" {
		glog.V(2).Info(reqid, " object is null")
		return true
	}
	var buk *Bucket.Bucket
	var err error
	for i := time.Duration(0); i < 3; i++ {
		glog.V(3).Info(reqid, " get bucket ,turns:", i)
		if buk, err = cache.LookupBucket(account, bucket, *server, false, ctx); err != nil {
			glog.V(2).Info(reqid, " get bucket error:", err)
			//404的，不要重试了
			if (strings.Contains(err.Error(), "NOT-FOUND-ERROR")) {
				return false
			}
			if i < 2 {
				time.Sleep((i + 1) * 5 * time.Second)
			}
		} else {
			glog.V(2).Info(reqid, " get bucket ok")
			return buk.AuthReferer(r)
		}
	}
	glog.V(0).Infoln(reqid, "auth:failed to auth referer for can not get bucket:", account, bucket)
	return false
}
func AuthReferer(server *string, r *http.Request, ctx *context.Context) bool {
	reqid := ""
	if ctx != nil {
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}
	glog.V(1).Info(reqid, " start Auth check without Cluster")
	if r.Header.Get(AUTHORIZATION) != "" {
		glog.V(2).Info(reqid, " AUTHORIZATION is null")
		return true
	}
	account, bucket, object := util.ParseServiceUrl(r)
	if object == "" {
		glog.V(2).Info(reqid, " object is null")
		return true
	}
	var buk *Bucket.Bucket
	var err error
	for i := time.Duration(0); i < 3; i++ {
		glog.V(3).Info(reqid, " get bucket ,turns:", i)
		if buk, err = cache.LookupBucket(account, bucket, *server, false, ctx); err != nil {
			glog.V(2).Info(reqid, " get bucket error:", err)
			//404的，不要重试了
			if (strings.Contains(err.Error(), "NOT-FOUND-ERROR")) {
				return false
			}
			if i < 2 {
				time.Sleep((i + 1) * 5 * time.Second)
			}
		} else {
			return buk.AuthReferer(r)
		}
	}
	glog.V(0).Infoln(reqid, " auth:failed to auth referer for can not get bucket:", account, bucket, err)
	return false
}

//获取鉴权头，根据现在时间来
func GetHeaders(AccessKeyId, AccessKeysecret, bucket, object, method string) map[string]string {
	date := util.ParseUnix(strconv.FormatInt(time.Now().Unix(), 10))
	return getHeadersWithDate(AccessKeyId, AccessKeysecret, bucket, object, method, date)
}
func getHeadersWithDate(AccessKeyId, AccessKeysecret, bucket, object, method, date string) map[string]string {
	Signature := GetSignature(bucket, object, method, date, AccessKeysecret, "")
	header := make(map[string]string)
	header["Authorization"] = " SDOSS " + AccessKeyId + ":" + Signature
	header["Date"] = date
	return header
}
