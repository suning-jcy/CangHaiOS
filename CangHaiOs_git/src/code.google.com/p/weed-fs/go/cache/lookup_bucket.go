package cache

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"context"

	"code.google.com/p/weed-fs/go/bucket"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
)

type RateLimit struct {
	Lock     sync.Mutex
	LastTime []int64
}
type BucketCacheType struct {
	buk       *bucket.Bucket
	cors      *CORSReq
	merge     *MergeReq
	rateLimit *RateLimit
	expire    time.Time
}
type BucketCache struct {
	m map[string]BucketCacheType //key:/account/bucket
	sync.RWMutex
}

func NewBucketCache() *BucketCache {
	return &BucketCache{m: make(map[string]BucketCacheType)}
}

var bukcetCache = &BucketCache{m: make(map[string]BucketCacheType)}

func (bc *BucketCache) Get(account, bucket string) (*bucket.Bucket, error) {
	return bc.get(account, bucket)
}
func (bc *BucketCache) get(account string, bucket string) (*bucket.Bucket, error) {
	bc.RLock()
	defer bc.RUnlock()
	if cpn, ok := bc.m[account+"/"+bucket]; ok {
		if cpn.expire.After(time.Now()) {
			return cpn.buk, nil
		} else {
			return cpn.buk, errors.New("Expired")
		}
	}
	return nil, errors.New("Not Found")
}
func (bc *BucketCache) Set(buk *bucket.Bucket, expTime time.Duration) {
	bc.set(buk, expTime)
	return
}
func (bc *BucketCache) set(buk *bucket.Bucket, expTime time.Duration) {
	bukCache := BucketCacheType{
		buk:       buk,
		rateLimit: &RateLimit{LastTime: make([]int64, 3)},
		expire:    time.Now().Add(expTime),
	}
	corsReq := &CORSReq{}
	if buk.CORS != "" {
		cors := ParseCORS([]byte(buk.CORS))
		if cors != nil {
			corsReq = cors.ToCORSReq()
		}
	}
	bukCache.cors = corsReq
	//var mergeReq *MergeReq
	mergeReq := &MergeReq{}
	if buk.Reserver2 != "" {
		mergeReq = ParseMergeReq([]byte(buk.Reserver2))
	}
	bukCache.merge = mergeReq
	bc.Lock()
	defer bc.Unlock()
	bc.m[buk.Name] = bukCache
}
func (bc *BucketCache) deleteCache(acc, buk string) {
	bc.Lock()
	defer bc.Unlock()
	delete(bc.m, acc+"/"+buk)
}

func (bc *BucketCache) getAllRateLimit(account string, bucket string) (*bucket.Bucket, *bucket.BucketThresholds) {
	bc.RLock()
	defer bc.RUnlock()
	if cpn, ok := bc.m[account+"/"+bucket]; ok {
		if cpn.expire.Before(time.Now()) {
			return nil, nil
		} else {
			return cpn.buk, cpn.buk.Thresholds
		}
	} else {
		return nil, nil
	}
}
func (bc *BucketCache) getRateLimit(account string, bucket string) (*bucket.Bucket, *RateLimit) {
	bc.RLock()
	defer bc.RUnlock()
	if cpn, ok := bc.m[account+"/"+bucket]; ok {
		if cpn.expire.Before(time.Now()) {
			return nil, nil
		} else {
			return cpn.buk, cpn.rateLimit
		}
	} else {
		return nil, nil
	}
}

func (bc *BucketCache) getCORS(account string, bucket string) (*bucket.Bucket, *CORSReq) {
	bc.RLock()
	defer bc.RUnlock()
	if cpn, ok := bc.m[account+"/"+bucket]; ok {
		if cpn.expire.Before(time.Now()) {
			return nil, nil
		} else {
			return cpn.buk, cpn.cors
		}
	} else {
		return nil, nil
	}
}

func (bc *BucketCache) getmerge(account string, bucket string) (*bucket.Bucket, *MergeReq) {
	bc.RLock()
	defer bc.RUnlock()
	if cpn, ok := bc.m[account+"/"+bucket]; ok {
		if cpn.expire.Before(time.Now()) {
			return nil, nil
		} else {
			return cpn.buk, cpn.merge
		}
	} else {
		return nil, nil
	}
}
func (bc *BucketCache) GetCORSString(account string, bucket string) string {
	bc.RLock()
	defer bc.RUnlock()
	if cpn, ok := bc.m[account+"/"+bucket]; ok {
		if cpn.expire.Before(time.Now()) {
			return ""
		} else {
			if cpn.cors == nil {
				return ""
			}
			cors := cpn.cors.ToCORS()
			if cors == nil {
				return ""
			}
			data, _ := json.Marshal(*cors)
			return string(data)
		}
	} else {
		return ""
	}
}

//注意！！！NOT-FOUND-ERROR返回码有使用判定，变更前一致调整
func LookupBucket(acc string, buk string, host string, direct bool, ctx *context.Context) (bukPtr *bucket.Bucket, errRes error) {
	reqid := ""
	if ctx != nil {
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}
	buc, err := bukcetCache.get(acc, buk)
	if direct == true || err != nil {
		lookup, pos, err := operation.PartitionLookup2(host, acc, public.BUCKET, false, ctx)
		if err != nil {
			glog.V(0).Infoln("get bucket pos err", acc, buk, err)
			bukPtr = nil
			errRes = err
			//return nil, err
		} else {
			header := make(map[string]string)
			header[public.SDOSS_REQUEST_ID] = reqid

			path := "/get?account=" + acc + "&bucket=" + buk + "&parId=" + pos
			rspStatus, data, err := util.MakeRequestsQuickTimeout(lookup.List(), path, "GET", nil, header)
			if err != nil {
				if rspStatus == http.StatusNotFound {
					glog.V(0).Infoln("can not get bucket", acc, buk, rspStatus, err)
					bukPtr = nil
					errRes = errors.New("NOT-FOUND-ERROR,do not get bucket:" + buk + " infomation")
					//return nil, errors.New("NOT-FOUND-ERROR,do not get bucket:" + buk + " infomation")
				} else {
					glog.V(0).Infoln("can not get bucket", acc, buk, rspStatus, err)
					bukPtr = nil
					errRes = err
					//return nil, err
				}

			} else {
				if rspStatus != http.StatusOK {
					if rspStatus == http.StatusNotFound {
						bukPtr = nil
						errRes = errors.New("NOT-FOUND-ERROR,do not get bucket:" + buk + " infomation")
						//return nil, errors.New("NOT-FOUND-ERROR,do not get bucket:" + buk + " infomation")
					} else {
						bukPtr = nil
						errRes = errors.New("ServiceUnavilable,fail to get bucket:" + buk + " information")
						//return nil, errors.New("ServiceUnavilable,fail to get bucket:" + buk + " information")
					}
				} else {
					bukPtr = &bucket.Bucket{}
					err = json.Unmarshal(data, &bukPtr)
					if err != nil {
						glog.V(0).Infoln("Unmarshal data error while get bucket", acc, buk, err)
						err = errors.New("fail to resolve bucket:" + buk + " information")
						bukPtr = nil
						errRes = err
						//return nil, err
					} else {
						bukPtr.ParseRefererConfig()
						bukcetCache.set(bukPtr, 5*time.Minute)
						errRes = nil
						//return bukPtr, nil
					}
				}
			}
		}
	} else {
		bukPtr = buc
	}
	if bukPtr != nil {
		return bukPtr, nil
	} else {
		//如果缓存过期，跟bs要都没有，就用过期的缓存，并更新过期时间为30s后
		bukPtr = buc
		if bukPtr != nil {
			bukcetCache.set(bukPtr, 30*time.Second)
			return bukPtr, nil
		}
		return nil, errors.New("NOT-FOUND-ERROR,do not get bucket:" + buk + " infomation")
	}
}
func DeleteBucketCache(acc, buk string) {
	bukcetCache.deleteCache(acc, buk)
}
func LookupBucketCollection(account, bucket, host string, ctx *context.Context) (collection string) {
	buk, err := LookupBucket(account, bucket, host, false, ctx)
	if err != nil {
		glog.V(4).Infoln("Failed to get collection bound:", err.Error())
		return ""
	}
	return buk.Collection
}
func LookupBucketLocation(account, bucket, host string, ctx *context.Context) (location string, err error) {
	buk, err := LookupBucket(account, bucket, host, false, ctx)
	if err != nil {
		glog.V(4).Infoln("Failed to get location bound:", err.Error())
		return "", err
	}
	return buk.Location, nil
}
func LookupBucketLogPrefix(account, bucket, host string, ctx *context.Context) (logprefix string, err error) {
	buk, err := LookupBucket(account, bucket, host, false, ctx)
	if err != nil {
		glog.V(4).Infoln("Failed to get bucket degrade lifecycle:", err.Error())
		return "", err
	}
	return buk.LogPrefix, nil
}
func LookupBucketSysDef(account string, bucket string, host string, ctx *context.Context) (error, string) {
	buk, err := LookupBucket(account, bucket, host, false, ctx)
	if err != nil {
		return err, ""
	}
	return nil, buk.SysDefine
}
func LookupBucketMaxAge(account, bucket, host string, ctx *context.Context) (sec int, err error) {
	buk, err := LookupBucket(account, bucket, host, false, ctx)
	if err == nil {
		return buk.MaxAgeDay, nil
	}
	return
}
func LookupBucketMaxFileSize(account, bucket, host string, ctx *context.Context) (int64, error) {
	buk, err := LookupBucket(account, bucket, host, false, ctx)
	if err != nil {
		return 0, err
	}
	return int64(buk.MaxFileSize) * 1024 * 1024, nil
}
func IsBucketFull(account, bucket, host string, ctx *context.Context) (bool, error) {
	buk, err := LookupBucket(account, bucket, host, false, ctx)
	if err != nil {
		return false, err
	}
	if buk.Thresholds == nil || buk.Thresholds.MaxSpace == 0 {
		return false, nil
	}
	return buk.ByteUsed >= int64(buk.Thresholds.MaxSpace)*1024*1024*1024, nil
}
func IsBucketSupportEmpty(account, buc, host string, ctx *context.Context) (bool, error) {
	buk, err := LookupBucket(account, buc, host, false, ctx)
	if err != nil {
		return false, err
	}
	if buk.Reserver4&(1<<bucket.SUPPORTED_EMPTY_LOCATION) > 0 {
		return true, nil
	}
	return false, nil
}
func IsBucketSupportFileHeadCheck(account, buc, host string, ctx *context.Context) (bool, error) {
	buk, err := LookupBucket(account, buc, host, false, ctx)
	if err != nil {
		return false, err
	}
	if buk.Reserver4&(1<<bucket.SUPPORTED_FILE_HEAD_CHECK_LOCATION) > 0 {
		return true, nil
	}
	return false, nil
}
func IsBucketSupportDegrade(account, buc, host string, ctx *context.Context) (bool, error) {
	buk, err := LookupBucket(account, buc, host, false, ctx)
	if err != nil {
		return false, err
	}
	if buk.Reserver4&(1<<bucket.SUPPORTED_BUCKET_DEGRADE_LOCATION) > 0 {
		return true, nil
	}
	return false, nil
}
func LookupBucketRateLimit(account, bucket string, opType int) (int, *RateLimit) {
	buk, rateLimit := bukcetCache.getRateLimit(account, bucket)
	if buk == nil || rateLimit == nil {

		return 0, nil
	}
	maxRate := 0
	switch opType {
	case public.UPLOAD:
		maxRate = buk.MaxUpTps
	case public.DOWNLOAD:
		maxRate = buk.MaxDonwTps
	}
	return maxRate, rateLimit
}

/*
	maxtps int
	mintps int
	//单位字节
	maxrate int64
	minrate int64
*/

func LookupBucketAllLimit(account, bucket string, opType int, host string, ctx *context.Context) (string, string, string, string) {
	buk, err := LookupBucket(account, bucket, host, false, ctx)
	if err != nil {
		glog.V(4).Infoln("Failed to get collection bound:", err.Error())
		if opType == public.LISTBUCKET {
			return "1", "1", "unlimited", "unlimited"
		} else {
			return "unlimited", "unlimited", "unlimited", "unlimited"
		}
	}
	rateLimit := buk.Thresholds
	if rateLimit == nil {
		if opType == public.LISTBUCKET {
			return "1", "1", "unlimited", "unlimited"
		} else {
			return "unlimited", "unlimited", "unlimited", "unlimited"
		}
	}
	switch opType {
	case public.UPLOAD:
		return rateLimit.MaxUpTps, rateLimit.MinUpTps, rateLimit.MaxUpRate, rateLimit.MinUpRate
	case public.DOWNLOAD:
		return rateLimit.MaxDownTps, rateLimit.MinDownTps, rateLimit.MaxDownRate, rateLimit.MinDownRate
	case public.DELETE:
		return rateLimit.MaxDelTps, rateLimit.MinDelTps, rateLimit.MaxDelRate, rateLimit.MinDelRate
	case public.PICDEAL:
		return rateLimit.MaxPicDealTps, rateLimit.MinPicDealTps, rateLimit.MaxPicDealRate, rateLimit.MinPicDealRate
	case public.LISTBUCKET:
		return rateLimit.MaxListTps, rateLimit.MinListTps, rateLimit.MaxListRate, rateLimit.MinListRate
	}
	if opType == public.LISTBUCKET {
		return "1", "1", "unlimited", "unlimited"
	} else {
		return "unlimited", "unlimited", "unlimited", "unlimited"
	}
}

func LookupBucketCORS(account, bucket, master string, ctx *context.Context) (error, *CORSReq) {
	if _, err := LookupBucket(account, bucket, master, false, ctx); err != nil {
		return err, nil
	}
	buk, cors := bukcetCache.getCORS(account, bucket)
	if buk == nil || cors == nil {
		return errors.New("NotSet"), nil
	}
	return nil, cors
}
func LookupBucketmerge(account, bucket, master string, ctx *context.Context) (error, *MergeReq) {
	if _, err := LookupBucket(account, bucket, master, false, ctx); err != nil {
		return err, nil
	}
	buk, merge := bukcetCache.getmerge(account, bucket)
	if buk == nil || merge == nil {
		return errors.New("NotSet"), nil
	}
	return nil, merge
}

func LookupBucketLifeCycle(account, bucket, host string, ctx *context.Context) (lifeCycle string, err error) {
	buk, err := LookupBucket(account, bucket, host, false, ctx)
	if err != nil {
		glog.V(4).Infoln("Failed to get bucket degrade lifecycle:", err.Error())
		return "", err
	}
	return buk.LifeCycle, nil
}
type CORS struct {
	Rules []Rule `json:"R,omitempty"`
}

func (cr *CORS) ToCORSReq() *CORSReq {
	cors := CORSReq{}
	for _, rq := range cr.Rules {
		cors.Rules = append(cors.Rules, rq.ToRuleReq())
	}
	return &cors
}

type CORSReq struct {
	Rules []RuleReq `json:"Rule,omitempty"`
}
type MergeReq struct {
	Enable   int             `json:"E"`
	Maxsize  int             `json:"S,omitempty"`
	MimeSlic []string        `json:"M,omitempty"`
	Mime     map[string]bool `json:"-"`
}

func (cr *CORSReq) ToCORS() *CORS {
	cors := CORS{}
	for _, rq := range cr.Rules {
		cors.Rules = append(cors.Rules, rq.ToRule())
	}
	return &cors
}

type Rule struct {
	SrcUrl       string `json:"S,omitempty"`
	Method       string `json:"M,omitempty"`
	AllowHeader  string `json:"A,omitempty"`
	ExposeHeader string `json:"E,omitempty"`
	MaxAge       string `json:"Ma,omitempty"`
}
type RuleReq struct {
	SrcUrl       string `json:"SrcUrl,omitempty"`
	Method       string `json:"Method,omitempty"`
	AllowHeader  string `json:"AllowHeader,omitempty"`
	ExposeHeader string `json:"ExposeHeader,omitempty"`
	MaxAge       string `json:"Max-Age,omitempty"`
}

func (rq *RuleReq) ToRule() Rule {
	r := Rule{SrcUrl: rq.SrcUrl,
		Method:       rq.Method,
		AllowHeader:  rq.AllowHeader,
		ExposeHeader: rq.ExposeHeader,
		MaxAge:       rq.MaxAge,
	}
	r.Method = strings.Replace(r.Method, "PUT", "PU", -1)
	r.Method = strings.Replace(r.Method, "HEAD", "H", -1)
	r.Method = strings.Replace(r.Method, "POST", "PO", -1)
	r.Method = strings.Replace(r.Method, "DELETE", "D", -1)
	r.Method = strings.Replace(r.Method, "GET", "G", -1)
	return r
}

func (rq *Rule) ToRuleReq() RuleReq {
	r := RuleReq{SrcUrl: rq.SrcUrl,
		Method:       rq.Method,
		AllowHeader:  rq.AllowHeader,
		ExposeHeader: rq.ExposeHeader,
		MaxAge:       rq.MaxAge,
	}
	r.Method = strings.Replace(r.Method, "PU", "PUT", -1)
	r.Method = strings.Replace(r.Method, "D", "DELETE", -1)
	r.Method = strings.Replace(r.Method, "H", "HEAD", -1)
	r.Method = strings.Replace(r.Method, "PO", "POST", -1)
	r.Method = strings.Replace(r.Method, "G", "GET", -1)
	return r
}
func ParseCORS(data []byte) *CORS {
	c := &CORS{}
	if err := json.Unmarshal(data, c); err != nil {
		return nil
	}
	return c
}
func ParseMergeReq(data []byte) *MergeReq {
	Mr := &MergeReq{}
	if len(data) > public.MAX_MERGECONF_LENGTH {
		glog.V(0).Info(" merge conf length is longer than maxsize :", string(data))
		return nil
	}
	if err := json.Unmarshal(data, Mr); err != nil {
		return nil
	}
	if Mr.Enable > 1 || Mr.Enable < 0 {
		glog.V(0).Info(" merge conf enable is err :", string(data))
		return nil
	}
	if Mr.Maxsize > public.MAX_MERGE_LENGTH || Mr.Maxsize < 0 {
		glog.V(0).Info(" merge conf MAXSIZE is ERR :", string(data))
		return nil
	}
	Mr.Mime = make(map[string]bool)
	for _, v := range Mr.MimeSlic {
		Mr.Mime[v] = true
	}
	return Mr
}
func ParseCORSReq(data []byte) *CORSReq {
	c := &CORSReq{}
	if err := json.Unmarshal(data, c); err != nil {
		return nil
	}
	return c
}
func (core *CORSReq) GetRule(origin, method string) *RuleReq {
	for _, rule := range core.Rules {
		regExp := ""
		if !strings.HasSuffix(rule.SrcUrl, "*") {
			regExp = rule.SrcUrl + "$"
		}
		if !strings.HasPrefix(regExp, "*") {
			regExp = "^" + regExp
		}
		regExp = strings.Replace(regExp, "*", "\\w*", 1)

		reg := regexp.MustCompile(regExp)
		if reg.MatchString(origin) {
			if strings.Contains(rule.Method, method) {
				ret := &RuleReq{}
				ret.SrcUrl = rule.SrcUrl
				ret.Method = rule.Method
				ret.ExposeHeader = rule.ExposeHeader
				if ret.SrcUrl != "*" {
					ret.SrcUrl = origin
				}
				return ret
			}
		}
	}
	return nil
}

func (core *CORSReq) OptionsGetRule(origin string) *RuleReq {
	for _, rule := range core.Rules {
		regExp := ""
		if !strings.HasSuffix(rule.SrcUrl, "*") {
			regExp = rule.SrcUrl + "$"
		}
		if !strings.HasPrefix(regExp, "*") {
			regExp = "^" + regExp
		}
		regExp = strings.Replace(regExp, "*", "\\w*", 1)

		reg := regexp.MustCompile(regExp)
		if reg.MatchString(origin) {
			ret := &RuleReq{}
			ret.SrcUrl = rule.SrcUrl
			ret.Method = rule.Method
			ret.ExposeHeader = rule.ExposeHeader
			if ret.SrcUrl != "*" {
				ret.SrcUrl = origin
			}
			return ret
		}
	}
	return nil
}

func IsBucketECSurpport(account, bucket, host string, ctx *context.Context) (bool, error) {
	buk, err := LookupBucket(account, bucket, host, false, ctx)
	if err != nil {
		return false, err
	}
	if buk.Location == public.STORE_BUCKET_EC {
		return true, nil
	}
	return false, nil
}
