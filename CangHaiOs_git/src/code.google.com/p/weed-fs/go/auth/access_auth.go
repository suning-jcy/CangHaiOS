package auth

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"time"

	"context"

	"code.google.com/p/weed-fs/go/cache"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
)

type AccessAuth struct {
	prefix string
	suffix string
	master string
}
type AccountAuthInfo struct {
	AccessKeyID     string
	AccessKeySecret string
	ExpireDate      string
	Err             string
}

var (
	PRIVATE_HEADER_PREIFIX = "x-sdoss-"
)

const (
	HEADER_SIGNATURE = iota
	URL_SIGNATURE
)
const (
	DATE          = "Date"
	EXPIRES       = "Expires"
	KEY_ID        = "SDOSSAccessKeyId"
	SIGNATURE     = "Signature"
	AUTHORIZATION = "Authorization"
)

//authchecktype
//现在只有0,1
const (
	SIGNATURE_Unique        = iota
	SIGNATURE_WithoutUnique = iota
)

var (
	Auth_Check_Type = SIGNATURE_Unique
)

var (
	ErrAuthBadPar = errors.New("Not proper auth parameter")
	ErrAuthNoSign = errors.New("Authorization expected")
	ErrAuthNoUser = errors.New("No accesskey user specified")
)

func NewAccessAuth(prefix string, suffix string) (*AccessAuth, error) {
	aa := &AccessAuth{}
	aa.prefix = prefix
	aa.suffix = suffix
	return aa, nil
}

//AuthValidCluster
func (aa *AccessAuth) AuthValidCluster(r *http.Request, server, clusternum string, ctx *context.Context) (authorized bool, err error) {
	reqid := ""
	if ctx != nil {
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}

	glog.V(1).Info(reqid, " start AuthValid with Cluster")
	method := r.Method
	account, bucket, object := util.ParseServiceUrlForCluster(r, clusternum)
	if bucket != "" && aa.authBucket(account, bucket, object, r.Method, server, ctx) {
		return true, nil
	}
	signType, id, signature, err := aa.getRequestAuthorization(r)
	if err != nil {
		glog.V(0).Infoln("get auth parameter error:", err)
		return false, err
	}

	if object != "" && (method == "GET" || method == "HEAD") && r.FormValue("token") != "" {
		glog.V(0).Infoln("get auth parameter error:", ErrAuthBadPar)
		return false, ErrAuthBadPar
	}
	if bucket == "" && (method == "POST" || method == "PUT" || method == "DELETE" || method == "HEAD" || (method == "GET" && account == "") || (method == "GET" && account != "" && r.FormValue("type") != "")) {
		sysId, sysSignature, err := aa.genSysAccessSignatureCluster(r, server, signType, clusternum)
		if err != nil {
			glog.V(0).Infoln("gen access signature error:", err)
			return false, err
		}
		if sysId != id || signature != sysSignature {
			glog.V(0).Infoln("Not proper access id or signature!")
			return false, errors.New("Not proper access id or signature!")
		}
		return true, nil
	}
	keySec := cache.LookupAccountUserKey(account, id, server, false, ctx)
	if keySec == "" {
		glog.V(0).Infoln("look up account user key error", ErrAuthNoUser)
		return false, ErrAuthNoUser
	}
	signatureLocal := aa.genAccountSignatureCluster(r, keySec, signType, clusternum)
	if signatureLocal == "" {
		glog.V(0).Infoln("Bad local signature!")
		return false, errors.New("Bad local signature!")
	}
	glog.V(4).Infoln("Server signature:", signatureLocal, "Remote signature:", signature, keySec, id)
	if signatureLocal != signature {
		glog.V(0).Infoln("Not proper access signature!")
		return false, errors.New("Not proper access signature!")
	}
	return true, nil
}
func (aa *AccessAuth) AuthValid(r *http.Request, server string, ctx *context.Context) (authorized bool, err error) {
	account, bucket, object := util.ParseServiceUrl(r)
	if bucket != "" && aa.authBucket(account, bucket, object, r.Method, server, ctx) {
		return true, nil
	}
	signType, id, signature, err := aa.getRequestAuthorization(r)
	if err != nil {
		glog.V(0).Infoln("get auth parameter error:", err)
		return false, err
	}
	method := r.Method
	if object != "" && (method == "GET" || method == "HEAD") && r.FormValue("token") != "" {
		glog.V(0).Infoln("get auth parameter error:", ErrAuthBadPar)
		return false, ErrAuthBadPar
	}
	if bucket == "" && (method == "POST" || method == "PUT" || method == "DELETE" || method == "HEAD" || (method == "GET" && account == "") || (method == "GET" && account != "" && r.FormValue("type") != "")) {
		sysId, sysSignature, err := aa.genSysAccessSignature(r, server, signType)
		if err != nil {
			glog.V(0).Infoln("gen access signature error:", err)
			return false, err
		}
		if sysId != id || signature != sysSignature {
			glog.V(0).Infoln("Not proper access id or signature!")
			return false, errors.New("Not proper access id or signature!")
		}
		return true, nil
	}
	keySec := cache.LookupAccountUserKey(account, id, server, false, ctx)
	if keySec == "" {
		glog.V(0).Infoln("look up account user key error", ErrAuthNoUser)
		return false, ErrAuthNoUser
	}
	signatureLocal := aa.genAccountSignature(r, keySec, signType)
	if signatureLocal == "" {
		glog.V(0).Infoln("Bad local signature!")
		return false, errors.New("Bad local signature!")
	}
	glog.V(4).Infoln("Server signature:", signatureLocal, "Remote signature:", signature, keySec, id)
	if signatureLocal != signature {
		glog.V(0).Infoln("Not proper access signature!")
		return false, errors.New("Not proper access signature!")
	}
	return true, nil
}
func (aa *AccessAuth) AuthType() string {
	return "AccessAuth"
}
func (aa *AccessAuth) genAccountSignatureCluster(r *http.Request, accessKey string, signatureType int, clusternum string) string {
	_, buk, obj := util.ParseServiceUrlForCluster(r, clusternum)
	privateHeaderMap := make(map[string]string)
	var keySlic []string
	for k, v := range r.Header {
		k = strings.TrimSpace(strings.ToLower(k))
		glog.V(4).Infoln(k, strings.Index(k, PRIVATE_HEADER_PREIFIX), PRIVATE_HEADER_PREIFIX)
		if strings.Index(k, PRIVATE_HEADER_PREIFIX) == 0 {
			for _, v := range v {
				privateHeaderMap[k] = privateHeaderMap[k] + ":" + strings.TrimSpace(strings.ToLower(v))
			}
			keySlic = append(keySlic, k)

		}
	}
	sort.Sort(sort.StringSlice(keySlic))
	privateStr := ""
	if keySlic != nil {
		for _, key := range keySlic {
			if key == public.SDOSS_REQUEST_ID {
				continue
			}
			privateStr = privateStr + key + privateHeaderMap[key] + "\n"
		}
	}
	date := ""
	if signatureType == HEADER_SIGNATURE {
		date = r.Header.Get(DATE)
	} else if signatureType == URL_SIGNATURE {
		date = r.FormValue(EXPIRES)
	} else {
		return ""
	}
	return GetSignature(buk, obj, r.Method, date, accessKey, privateStr)
}
func (aa *AccessAuth) genAccountSignature(r *http.Request, accessKey string, signatureType int) string {
	_, buk, obj := util.ParseServiceUrl(r)

	privateHeaderMap := make(map[string]string)
	var keySlic []string
	for k, v := range r.Header {
		k = strings.TrimSpace(strings.ToLower(k))
		glog.V(4).Infoln(k, strings.Index(k, PRIVATE_HEADER_PREIFIX), PRIVATE_HEADER_PREIFIX)
		if strings.Index(k, PRIVATE_HEADER_PREIFIX) == 0 {
			for _, v := range v {
				privateHeaderMap[k] = privateHeaderMap[k] + ":" + strings.TrimSpace(strings.ToLower(v))
			}
			keySlic = append(keySlic, k)

		}
	}
	sort.Sort(sort.StringSlice(keySlic))
	privateStr := ""
	if keySlic != nil {
		for _, key := range keySlic {
			if key == public.SDOSS_REQUEST_ID {
				continue
			}
			privateStr = privateStr + key + privateHeaderMap[key] + "\n"
		}
	}
	date := ""
	if signatureType == HEADER_SIGNATURE {
		date = r.Header.Get(DATE)
	} else if signatureType == URL_SIGNATURE {
		date = r.FormValue(EXPIRES)
	} else {
		return ""
	}
	return GetSignature(buk, obj, r.Method, date, accessKey, privateStr)
}

func GetSignature(bucket, object, method, date, accessKey, privateStr string) string {
	glog.V(4).Infoln("GetSignature", bucket, object, method, date, accessKey, privateStr)

	hm := hmac.New(sha1.New, []byte(accessKey))

	canResStr := ""
	if object != "" {
		canResStr = "/" + bucket + "/" + object
	} else if bucket != "" {
		canResStr = "/" + bucket + "/"
	} else {
		canResStr = "/"
	}

	signStr := method + "\n" + "" + "\n" + "" + "\n" + date + "\n" + privateStr + canResStr
	hm.Write([]byte(signStr))
	return base64.StdEncoding.EncodeToString(hm.Sum(nil))
}
func (aa *AccessAuth) genSysAccessSignature(r *http.Request, server string, signatureType int) (id string, signature string, err error) {
	path := "/sys/getAdmin"
	resp, err := util.MakeRequest_timeout(server, path, "GET", nil, nil)
	if err != nil {
		return "", "", fmt.Errorf("Get sys admin auth info:", err.Error())
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("Get json blob data:", err.Error())
	}
	authInfo := &AccountAuthInfo{}
	err = json.Unmarshal(data, authInfo)
	if err != nil {
		return "", "", fmt.Errorf("Parse json data:", err.Error())
	}
	if authInfo.Err != "" {
		return "", "", fmt.Errorf("Get sys admin auth info:", authInfo.Err)
	}
	id = authInfo.AccessKeyID
	signature = aa.genAccountSignature(r, authInfo.AccessKeySecret, signatureType)
	return
}
func (aa *AccessAuth) genSysAccessSignatureCluster(r *http.Request, server string, signatureType int, clusternum string) (id string, signature string, err error) {
	path := "/sys/getAdmin"
	resp, err := util.MakeRequest_timeout(server, path, "GET", nil, nil)
	if err != nil {
		return "", "", fmt.Errorf("Get sys admin auth info:", err.Error())
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("Get json blob data:", err.Error())
	}
	authInfo := &AccountAuthInfo{}
	err = json.Unmarshal(data, authInfo)
	if err != nil {
		return "", "", fmt.Errorf("Parse json data:", err.Error())
	}
	if authInfo.Err != "" {
		return "", "", fmt.Errorf("Get sys admin auth info:", authInfo.Err)
	}
	id = authInfo.AccessKeyID
	signature = aa.genAccountSignatureCluster(r, authInfo.AccessKeySecret, signatureType, clusternum)
	return
}
func (aa *AccessAuth) getRequestAuthorization(r *http.Request) (signType int, id string, signature string, err error) {
	headerSign := r.Header.Get(AUTHORIZATION)
	urlSign := r.FormValue(SIGNATURE)
	if headerSign == "" && urlSign == "" {
		err = ErrAuthNoSign
		return
	}
	if headerSign != "" && urlSign != "" {
		if Auth_Check_Type == SIGNATURE_Unique {
			err = ErrAuthBadPar
			return
		} else {
			headerSign = ""
		}
	}

	if headerSign != "" {
		dateStr := r.Header.Get(DATE)
		if date, errParse := time.Parse(http.TimeFormat, dateStr); err != nil {
			err = errParse
			return
		} else {
			nowUTC := time.Now().UTC()
			if nowUTC.After(date) {
				if nowUTC.Sub(date) > 15*time.Minute {
					err = errors.New("Date out of range!")
					return
				}
			} else if nowUTC.Before(date) {
				if date.Sub(nowUTC) > 15*time.Minute {
					err = errors.New("Date out of range!")
					return
				}
			}
		}

		if strings.Index(headerSign, public.SDOSS_AUTH_PREFIX) != 0 {
			err = fmt.Errorf("SDOSS Authorization  expeted but not found in authorization!")
			return
		}
		idSignature := headerSign[len(public.SDOSS_AUTH_PREFIX):]
		parts := strings.SplitN(idSignature, ":", 2)
		if len(parts) != 2 {
			err = fmt.Errorf("Unexpected type of authorization!")
			return
		}
		idReq := parts[0]
		idReq = strings.TrimSpace(idReq)
		signatureReq := parts[1]
		signatureReq = strings.TrimSpace(signatureReq)
		return HEADER_SIGNATURE, idReq, signatureReq, nil
	} else {
		id = r.FormValue(KEY_ID)
		expire := r.FormValue(EXPIRES)
		if id == "" || expire == "" {
			err = fmt.Errorf("SDOSS Authorization  expeted but not found in authorization!")
			return
		}
		exSec := util.ParseInt64(expire, 0)
		exDate := time.Unix(exSec, 0)
		if exDate.Before(time.Now()) {
			err = fmt.Errorf("SDOSS Authorization expired!")
			return
		}
		return URL_SIGNATURE, id, urlSign, nil
	}
	err = fmt.Errorf("Not supported signature type!")
	return
}
func (aa *AccessAuth) authBucket(account, bucket, object, method, server string, ctx *context.Context) bool {
	if account != "" && bucket == "" {
		return true
	}
	buk, err := cache.LookupBucket(account, bucket, server, false, ctx)
	if err != nil {
		glog.V(4).Infoln("Failed to auth:", err.Error())
		return false
	}
	if object != "" {
		if buk.AccessType == public.BUCKET_PUBLIC_RO {
			if method == "HEAD" || method == "GET" {
				return true
			}
		} else if buk.AccessType == public.BUCKET_PUBLIC_WR {
			return true
		}
	}
	return false
}

//生成随机的用户接入码和密文
func GenAccessKeyIDSecret(account string) (keyID string, keySecret string, err error) {
	keyID = randKeyID()
	rs := rand.New(rand.NewSource(time.Now().UnixNano()))
	randPos := rs.Intn(16) + 1
	date := fmt.Sprint(time.Now().UnixNano() / int64(randPos))
	source := account + date
	sh2 := sha1.New()
	bts := []byte(source)
	wl, err := sh2.Write(bts)
	if wl != len(bts) || err != nil {
		return "", "", errors.New("FAILEDTOSHA1!")
	}
	keySecret = base64.URLEncoding.EncodeToString(sh2.Sum(nil))
	return
}

//16位随机字符串用于key ID
func randKeyID() string {
	kinds, result := [][]int{[]int{10, 48}, []int{26, 65}}, make([]byte, 16)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 16; i++ {
		ikind := rand.Intn(2)
		scope, base := kinds[ikind][0], kinds[ikind][1]
		result[i] = uint8(base + rand.Intn(scope))
	}
	return string(result)
}
