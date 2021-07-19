package util

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"code.google.com/p/weed-fs/go/glog"
)

var (
	client              *http.Client
	Transport           *http.Transport
	client_timeout      *http.Client
	client_QuickTimeout *http.Client
	Transport_timeout   *http.Transport
	client_Recycle      *http.Client
)
var NotFound = errors.New("Not Find")

func init() {
	Transport = &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		MaxIdleConnsPerHost: 16,
	}
	client = &http.Client{Transport: Transport}
	Transport_timeout = &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 30 * time.Second,
		MaxIdleConnsPerHost: 16,
		//hujf
		ResponseHeaderTimeout: time.Second * 10,
	}
	client_timeout = &http.Client{Transport: Transport_timeout, Timeout: 10 * time.Second}
	client_QuickTimeout = &http.Client{Transport: Transport_timeout, Timeout: 5 * time.Second}
	client_Recycle = &http.Client{Transport: Transport}
}

//hujf
func PostBytes_timeout(url string, body []byte) ([]byte, error) {
	r, err := client_timeout.Post(url, "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		r, err = client_timeout.Post(url, "application/octet-stream", bytes.NewReader(body))
	}
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func PostBytes(url string, body []byte) ([]byte, error) {
	r, err := client.Post(url, "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		r, err = client.Post(url, "application/octet-stream", bytes.NewReader(body))
	}
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func Post(url string, values url.Values) ([]byte, error) {

	r, err := client.PostForm(url, values)
	if err != nil {
		r, err = client.PostForm(url, values)
	}
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return b, nil
}
func PostOnce(addr string, inf string, values url.Values) ([]byte, error) {
	url := "http://" + addr + inf
	r, err := client.PostForm(url, values)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return b, nil
}
func Post_timeout(url string, values url.Values) ([]byte, error) {
	r, err := client_timeout.PostForm(url, values)
	if err != nil {
		r, err = client_timeout.PostForm(url, values)
	}
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return b, nil
}
func Get(url string) ([]byte, error) {
	r, err := client.Get(url)
	if err != nil {
		r, err = client.Get(url)
	}
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if r.StatusCode != 200 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}
func GetQuickTimeout(url string) ([]byte, error) {
	r, err := client_QuickTimeout.Get(url)
	if err != nil {
		r, err = client_QuickTimeout.Get(url)
	}
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if r.StatusCode != 200 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}
func Delete(url string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	resp, e := client.Do(req)
	if e != nil {
		resp, e = client.Do(req)
	}

	if e != nil {
		return e
	}
	defer resp.Body.Close()
	if _, err := ioutil.ReadAll(resp.Body); err != nil {
		return err
	}
	return nil
}

func DownloadUrl(fileUrl string) (filename string, content []byte, e error) {
	response, err := client.Get(fileUrl)
	if err != nil {
		response, err = client.Get(fileUrl)
	}
	if err != nil {
		return "", nil, err
	}
	defer response.Body.Close()
	contentDisposition := response.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		if strings.HasPrefix(contentDisposition[0], "filename=") {
			filename = contentDisposition[0][len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}
	content, e = ioutil.ReadAll(response.Body)
	return
}

func Do(req *http.Request) (resp *http.Response, err error) {
	resp, err = client.Do(req)
	if err != nil {
		fmt.Println("do err", err.Error())
		resp, err = client.Do(req)
	}
	return
}

func DownloadFile(fileUrl string, headers map[string]string) (filename string, content []byte, mimeType string, e error) {
	method := "GET"
	req, err := http.NewRequest(method, fileUrl, nil)
	if err != nil {
		return "", nil, "", err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	response, err := client.Do(req)
	if err != nil {
		response, err = client.Do(req)
	}
	if err != nil {
		return "", nil, "", err
	}
	defer response.Body.Close()
	contentDisposition := response.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		if strings.HasPrefix(contentDisposition[0], "filename=") {
			filename = contentDisposition[0][len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}
	contentType := response.Header["Content-Type"]
	if len(contentType) > 0 {
		mimeType = contentType[0]
	}
	content, e = ioutil.ReadAll(response.Body)
	if len(content) <= 0 {
		e = errors.New("failed to download file:" + fileUrl)
	}
	return
}

func DownloadFilewithlastmodify(fileUrl string) (filename string, content []byte, mimeType string, lastmodify int64, e error) {
	response, err := client.Get(fileUrl)
	if err != nil {
		response, err = client.Get(fileUrl)
	}
	if err != nil {
		return "", nil, "", 0, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusServiceUnavailable {
		return "", nil, "", 0, NotFound
	}
	contentDisposition := response.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		if strings.HasPrefix(contentDisposition[0], "filename=") {
			filename = contentDisposition[0][len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}
	contentType := response.Header["Content-Type"]
	lasttime := response.Header["Last-Modified"]
	if len(lasttime) > 0 {
		time, terr := time.Parse(http.TimeFormat, lasttime[0])
		if terr != nil {
			lastmodify = 0
		} else {
			lastmodify = time.Unix()
		}
	}

	if len(contentType) > 0 {
		mimeType = contentType[0]
	}
	content, e = ioutil.ReadAll(response.Body)

	return
}

func MakeRequest_timeout(node string, path string, method string, data []byte, header map[string]string) (resp *http.Response, err error) {
	url := "http://" + node + path
	req, err := http.NewRequest(method, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	for key, value := range header {
		req.Header.Set(key, value)
	}
	return client_timeout.Do(req)
}

func MakeRequest_QuickTimeout(node string, path string, method string, data []byte, header map[string]string) (resp *http.Response, err error) {
	url := "http://" + node + path
	req, err := http.NewRequest(method, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	for key, value := range header {
		req.Header.Set(key, value)
	}
	return client_QuickTimeout.Do(req)
}

func MakeRequest(node string, path string, method string, data []byte, header map[string]string) (resp *http.Response, err error) {
	url := "http://" + node + path
	req, err := http.NewRequest(method, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	for key, value := range header {
		req.Header.Set(key, value)
	}
	return client.Do(req)
}
func MakeRequests(srvs []string, url, method string, reqDat []byte, h map[string]string) (ret int, data []byte, err error) {
	defer func(err *error, ret *int) {
		glog.V(4).Infoln("MakeRequests:", srvs, url, method, *ret, *err)
	}(&err, &ret)

	var repCnt = len(srvs)
	if repCnt <= 0 {
		err = errors.New("ServiceUnavilable")
		return
	}
	isRead := method == "GET" || method == "HEAD"
	if isRead {
		srvs = RandStringArray(srvs)
	}
	var rsp *http.Response
	for i, srv := range srvs {
		if rsp, err = MakeRequest_timeout(srv, url, method, reqDat, h); err == nil {
			defer rsp.Body.Close()
			ret = rsp.StatusCode
			if isRead && ret == http.StatusOK {
				if data, err = ioutil.ReadAll(rsp.Body); err == nil {
					return ret, data, nil
				}
			}
			if !isRead {
				if ret != http.StatusNotFound && ret != http.StatusInternalServerError {
					return ret, nil, nil
				}
			}
		}
		if i < repCnt-1 && !isRead {
			time.Sleep(time.Duration(100+rand.Intn(5)) * time.Millisecond)
		}
	}
	return ret, nil, err
}

func MakeRequests2(srvs []string, url, method string, reqDat []byte, h map[string]string) (ret int, data []byte, err error) {
	defer func(err *error, ret *int) {
		glog.V(4).Infoln("MakeRequests:", srvs, url, method, *ret, *err)
	}(&err, &ret)

	var repCnt = len(srvs)
	if repCnt <= 0 {
		err = errors.New("ServiceUnavilable")
		return
	}
	isRead := method == "GET" || method == "HEAD"
	if isRead {
		srvs = RandStringArray(srvs)
	}
	var rsp *http.Response
	for i, srv := range srvs {
		if rsp, err = MakeRequest(srv, url, method, reqDat, h); err == nil {
			defer rsp.Body.Close()
			ret = rsp.StatusCode
			if isRead && ret == http.StatusOK {
				if data, err = ioutil.ReadAll(rsp.Body); err == nil {
					return ret, data, nil
				}
			}
			if !isRead {
				if ret != http.StatusNotFound && ret != http.StatusInternalServerError {
					return ret, nil, nil
				}
			}
		}
		if i < repCnt-1 && !isRead {
			time.Sleep(time.Duration(100+rand.Intn(5)) * time.Millisecond)
		}
	}
	return ret, nil, err
}

//发往Partition的GET/HEAD请求，不需要很大的超时时间
func MakeRequestsQuickTimeout(srvs []string, url, method string, reqDat []byte, h map[string]string) (ret int, data []byte, err error) {
	defer func(err *error, ret *int) {
		glog.V(4).Infoln("MakeRequests:", srvs, url, method, *ret, *err)
	}(&err, &ret)

	var repCnt = len(srvs)
	if repCnt <= 0 {
		err = errors.New("ServiceUnavilable")
		return
	}
	isRead := method == "GET" || method == "HEAD"
	if isRead {
		srvs = RandStringArray(srvs)
	}
	var rsp *http.Response
	for i, srv := range srvs {
		if rsp, err = MakeRequest_QuickTimeout(srv, url, method, reqDat, h); err == nil {
			defer rsp.Body.Close()
			ret = rsp.StatusCode
			if isRead && ret == http.StatusOK {
				if data, err = ioutil.ReadAll(rsp.Body); err == nil {
					return ret, data, nil
				}
			}
			if !isRead {
				if ret != http.StatusNotFound && ret != http.StatusInternalServerError {
					return ret, nil, nil
				}
			}
		}
		if i < repCnt-1 && !isRead {
			time.Sleep(time.Duration(100+rand.Intn(5)) * time.Millisecond)
		}
	}
	return ret, nil, err
}
func MakeRequestWithTimeout(node string, path string, method string, data []byte, header map[string]string, timeout int64) (resp *http.Response, err error) {
	url := "http://" + node + path
	req, err := http.NewRequest(method, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	for key, value := range header {
		req.Header.Set(key, value)
	}
	if timeout == int64(0) {
		return client.Do(req)
	} else {
		if client_Recycle.Timeout != time.Duration(timeout)*time.Second {
			client_Recycle.Timeout = time.Duration(timeout) * time.Second
		}
		glog.V(4).Infoln("makereq timeout:", client_Recycle.Timeout)
		return client_Recycle.Do(req)
	}
}

func MakeRequestsWithTimeout(srvs []string, url, method string, reqDat []byte, h map[string]string, timeout int64) (ret int, data []byte, err error) {
	defer func(err *error, ret *int) {
		glog.V(4).Infoln("MakeRequests:", srvs, url, method, *ret, *err)
	}(&err, &ret)

	var repCnt = len(srvs)
	if repCnt <= 0 {
		err = errors.New("ServiceUnavilable")
		return
	}
	isRead := method == "GET" || method == "HEAD"
	if isRead {
		srvs = RandStringArray(srvs)
	}
	var rsp *http.Response
	for i, srv := range srvs {
		if rsp, err = MakeRequestWithTimeout(srv, url, method, reqDat, h, timeout); err == nil {
			defer rsp.Body.Close()
			ret = rsp.StatusCode
			if isRead && ret == http.StatusOK {
				if data, err = ioutil.ReadAll(rsp.Body); err == nil {
					return ret, data, nil
				}
			}
			if !isRead {
				if ret != http.StatusNotFound && ret != http.StatusInternalServerError {
					return ret, nil, nil
				}
			}
		}
		if i < repCnt-1 && !isRead {
			time.Sleep(time.Duration(100+rand.Intn(5)) * time.Millisecond)
		}
	}
	return ret, nil, err
}

func WriteHttpLine(w http.ResponseWriter, line string) (written int64, err error) {
	if written, err = io.Copy(w, strings.NewReader(line)); err != nil {
		return
	} else {
		return io.Copy(w, strings.NewReader("\n"))
	}
}

func WriteHttpLines(w http.ResponseWriter, lines []string) (err error) {
	for _, line := range lines {
		if _, err = WriteHttpLine(w, line); err != nil {
			return err
		}
	}
	return nil
}

func GetIpV4() string {
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, add := range addrs {
			strSplt := strings.SplitN(add.String(), "/", 2)
			sip := strSplt[0]
			if sip != "127.0.0.1" && sip != "0.0.0.0" && len(sip) >= 7 && len(sip) <= 15 {
				return sip
			}
		}
	}
	return "127.0.0.1"
}

type ReqHeader map[string][]string

func NewReqHeader() *ReqHeader {
	header := make(map[string][]string)
	reqHeader := ReqHeader(header)
	return &reqHeader
}
func (rh *ReqHeader) Set(key, value string) {
	(*rh)[key] = []string{value}
}
func (rh *ReqHeader) Delete(key string) {
	delete(*rh, key)
}
func (rh *ReqHeader) Map() map[string][]string {
	return (map[string][]string)(*rh)
}
func CompareStringInt64(str1, str2 string) bool {
	c1 := ParseInt64(str1, 0)
	c2 := ParseInt64(str2, 0)
	return c1 > c2
}
func HashPos(path string, parNum int) int {
	hash := crc32.ChecksumIEEE([]byte(path))
	pos := hash % uint32(parNum)
	return int(pos)
}
func ReverseProxy(w http.ResponseWriter, r *http.Request, server string) {
	targetUrl, err := url.Parse("http://" + server)
	if err != nil {
		glog.V(0).Infoln("reverse porxy: ", err)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(targetUrl)
	proxy.Transport = Transport_timeout
	proxy.ServeHTTP(w, r)
	return
}

/*
usrstr  url，带有http://的
method  请求方法
values  请求参数
data    上传的数据
header  头
*/
func MakeHttpRequest(usrstr, method string, values url.Values, header map[string]string, timeout bool) (resp *http.Response, err error) {

	if values.Encode() != "" {
		usrstr += "?" + values.Encode()
	}

	req, err := http.NewRequest(method, usrstr, nil)
	glog.V(4).Infoln(req, err)
	if err != nil {
		return nil, err
	}
	//	req.Form=values
	for key, value := range header {
		req.Header.Set(key, value)
	}
	if timeout {
		return client_timeout.Do(req)
	} else {
		return client.Do(req)
	}

}

func MakeHttpRequestQuickTimeout(usrstr, method string, values url.Values, header map[string]string) (resp *http.Response, err error) {
	if values.Encode() != "" {
		usrstr += "?" + values.Encode()
	}

	req, err := http.NewRequest(method, usrstr, nil)
	glog.V(4).Infoln(req, err)
	if err != nil {
		return nil, err
	}
	//	req.Form=values
	for key, value := range header {
		req.Header.Set(key, value)
	}
	return client_QuickTimeout.Do(req)
}

/*
把req转发到targetUrl，并返回给rw
一般的请求
*/
func SingleHostReverseProxy(targetServer string, rw http.ResponseWriter, oldreq *http.Request) error {
	/*
		把请求发到targetUrl
	*/
	tryLocal := oldreq.FormValue("tryLocal")
	req, err := http.NewRequest(oldreq.Method, targetServer+oldreq.RequestURI, oldreq.Body)
	glog.V(4).Infoln(err)
	if err != nil {
		if tryLocal == "" {
			rw.WriteHeader(http.StatusServiceUnavailable)
		}
		glog.V(0).Infoln(err)
		return err
	}
	h2 := make(http.Header, len(oldreq.Header))
	for k, vv := range oldreq.Header {
		vv2 := make([]string, len(vv))
		copy(vv2, vv)
		h2[k] = vv2
	}
	req.Header = h2
	resp, err := client_timeout.Do(req)
	if err != nil {
		if tryLocal == "" {
			rw.WriteHeader(http.StatusServiceUnavailable)
		}
		glog.V(0).Infoln(err)
		return err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil && tryLocal != "" {
		glog.V(0).Infoln(err)
		return err
	}
	/*
		把结果写入rw
	*/

	for k, v := range resp.Header {
		rw.Header()[k] = v
	}

	rw.WriteHeader(resp.StatusCode)
	rw.Write(data)
	reqid := req.Header.Get("x-sdoss-request-id")
	glog.V(2).Infoln("proxying to leader :", reqid, targetServer+req.RequestURI, " rescode:", resp.StatusCode, string(data))
	return err
}

func GetWithTimeOut(url string) (resp *http.Response, err error) {
	return client_timeout.Get(url)
}
