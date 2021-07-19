package operation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"path/filepath"
	"strings"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/public"
	"net/url"
	"path"
	"code.google.com/p/weed-fs/go/util"
)

var BUFFER_SIZE = int64(32 * 1024)

type UploadResult struct {
	Name  string `json:"name,omitempty"`
	Size  uint32 `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
	Etag  string `json:"etag,omitempty"`
}

var (
	client *http.Client
)

func init() {
	client = &http.Client{Transport: &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		MaxIdleConnsPerHost: 16,
	}}
}

var fileNameEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

//上传一个reader
func Upload(uploadUrl string, filename string, reader io.Reader, isGzipped bool, mtype, reqid string) (*UploadResult, error) {

	return upload_content(uploadUrl, func(w io.Writer) (err error) {
		_, err = io.Copy(w, reader)
		return
	}, filename, isGzipped, mtype, reqid)
}

//上传一个byte数组
func Upload1(uploadUrl string, filename string, data []byte, isGzipped bool, mtype, reqid string) (*UploadResult, error) {
	/*
			uploadRet, err := upload_content1(uploadUrl, data, filename, isGzipped, mtype)
		return uploadRet, err
	*/
	return Upload(uploadUrl, filename, bytes.NewReader(data), isGzipped, mtype, reqid)
}

func upload_content(uploadUrl string, fillBufferFunction func(w io.Writer) error, filename string, isGzipped bool, mtype, reqid string) (*UploadResult, error) {
	body_buf := bytes.NewBufferString("")
	body_writer := multipart.NewWriter(body_buf)
	h := make(textproto.MIMEHeader)
	filename, _ = url.QueryUnescape(filename)
	//_, filename = path.Split(filename)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, fileNameEscaper.Replace(filename)))
	if mtype == "" {
		mtype = mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	}
	if mtype != "" {
		h.Set("Content-Type", mtype)
	}
	if isGzipped {
		h.Set("Content-Encoding", "gzip")
	}
	file_writer, cp_err := body_writer.CreatePart(h)
	if cp_err != nil {
		glog.V(0).Infoln("error creating form file", cp_err.Error())
		return nil, cp_err
	}
	if err := fillBufferFunction(file_writer); err != nil {
		glog.V(0).Infoln("error copying data", err)
		return nil, err
	}
	content_type := body_writer.FormDataContentType()
	if err := body_writer.Close(); err != nil {
		glog.V(0).Infoln("error closing body", err)
		return nil, err
	}

	req, err := http.NewRequest("POST", uploadUrl, body_buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", content_type)
	req.Header.Set(public.SDOSS_REQUEST_ID, reqid)
	resp, post_err := client.Do(req)
	if post_err != nil {
		resp, post_err = client.Do(req)
	}
	if post_err != nil {
		glog.V(0).Infoln("failing to upload to", uploadUrl, post_err.Error())
		return nil, post_err
	}
	defer resp.Body.Close()
	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		glog.V(0).Infoln("failing to read response", uploadUrl, ra_err.Error())
		return nil, ra_err
	}
	var ret UploadResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.V(0).Infoln("failing to read upload resonse", uploadUrl, string(resp_body))
		return nil, unmarshal_err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}

func Upload3(uploadUrl string, filename string, reader io.Reader, isGzipped bool, mtype string, header map[string]string) (*UploadResult, error) {
	fillBufferFunction := func(w io.Writer) (err error) {
		if reader == nil {
			err = errors.New("reader is nil")
			return
		}
		_, err = io.Copy(w, reader)
		return
	}

	body_buf := bytes.NewBufferString("")
	body_writer := multipart.NewWriter(body_buf)
	h := make(textproto.MIMEHeader)
	filename, _ = url.QueryUnescape(filename)
	_, filename = path.Split(filename)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, fileNameEscaper.Replace(filename)))
	if mtype == "" {
		mtype = mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	}
	if mtype != "" {
		h.Set("Content-Type", mtype)
	}
	if isGzipped {
		h.Set("Content-Encoding", "gzip")
	}
	file_writer, cp_err := body_writer.CreatePart(h)
	if cp_err != nil || file_writer == nil {
		glog.V(0).Infoln("error creating form file", cp_err.Error())
		return nil, cp_err
	}
	if err := fillBufferFunction(file_writer); err != nil {
		glog.V(0).Infoln("error copying data", err)
		return nil, err
	}
	content_type := body_writer.FormDataContentType()
	if err := body_writer.Close(); err != nil {
		glog.V(0).Infoln("error closing body", err)
		return nil, err
	}

	req, err := http.NewRequest("POST", uploadUrl, body_buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", content_type)

	for key, value := range header {
		req.Header.Set(key, value)
	}

	resp, post_err := client.Do(req)
	if post_err != nil {
		resp, post_err = client.Do(req)
	}
	if post_err != nil {
		glog.V(0).Infoln("failing to upload to", uploadUrl, post_err.Error())
		return nil, post_err
	}
	defer resp.Body.Close()
	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		glog.V(0).Infoln("failing to read response", uploadUrl, ra_err.Error())
		return nil, ra_err
	}

	if resp.StatusCode == 403 {
		return nil, errors.New("Error Authorization")
	}

	var ret UploadResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.V(0).Infoln("failing to read upload resonse", uploadUrl, string(resp_body))
		return nil, unmarshal_err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil

}

func Upload5(uploadUrl string, filename string, data []byte, isGzipped bool, mtype string, header map[string]string) (*UploadResult, int, error) {
	req, err := http.NewRequest("POST", uploadUrl, bytes.NewReader(data))
	if err != nil {
		return nil, 0, err
	}
	for key, value := range header {
		req.Header.Set(key, value)
	}

	resp, post_err := client.Do(req)
	if post_err != nil {
		resp, post_err = client.Do(req)
	}
	if post_err != nil {
		glog.V(0).Infoln("failing to upload to", uploadUrl, post_err.Error())
		return nil, 0, post_err
	}
	defer resp.Body.Close()
	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		glog.V(0).Infoln("failing to read response", uploadUrl, ra_err.Error())
		return nil, 0, ra_err
	}

	//if resp.StatusCode == 403 {
	//	return nil, errors.New("Error Authorization")
	//}
	if len(resp_body) <= 0 {
		return nil, resp.StatusCode, nil
	}
	var ret UploadResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.V(0).Infoln("failing to read upload resonse", uploadUrl, string(resp_body))
		return nil, resp.StatusCode, unmarshal_err
	}

	return &ret, resp.StatusCode, nil

}

func Upload4(uploadUrl string, filename string, data []byte, isGzipped bool, mtype string, header map[string]string, needbody bool) (*UploadResult, int, error) {
	body_buf := bytes.NewBufferString("")
	body_writer := multipart.NewWriter(body_buf)
	h := make(textproto.MIMEHeader)
	filename, _ = url.QueryUnescape(filename)
	_, filename = path.Split(filename)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, fileNameEscaper.Replace(filename)))
	if mtype == "" {
		mtype = mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	}
	if mtype != "" {
		h.Set("Content-Type", mtype)
	}
	if isGzipped {
		h.Set("Content-Encoding", "gzip")
	}
	file_writer, cp_err := body_writer.CreatePart(h)
	if cp_err != nil {
		glog.V(0).Infoln("error creating form file", cp_err.Error())
		return nil, 0, cp_err
	}
	if _, err := io.Copy(file_writer, bytes.NewReader(data)); err != nil {
		glog.V(0).Infoln("error copying data", err)
		return nil, 0, err
	}
	content_type := body_writer.FormDataContentType()
	if err := body_writer.Close(); err != nil {
		glog.V(0).Infoln("error closing body", err)
		return nil, 0, err
	}
	glog.V(4).Infoln("uploadurl: ", uploadUrl," filename:",filename," lens:",len(data)," len:",body_buf.Len())
	req, err := http.NewRequest("POST", uploadUrl, body_buf)
	if err != nil {
		return nil, 0, err
	}
	for key, value := range header {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", content_type)

	resp, post_err := client.Do(req)
	if post_err != nil {
		resp, post_err = client.Do(req)
	}
	if post_err != nil {
		glog.V(0).Infoln("failing to upload to", uploadUrl, post_err.Error())
		return nil, 0, post_err
	}
	if needbody {
		defer resp.Body.Close()
		resp_body, ra_err := ioutil.ReadAll(resp.Body)
		if ra_err != nil {
			glog.V(0).Infoln("failing to read response", uploadUrl, ra_err.Error())
			return nil, 0, ra_err
		}

		//if resp.StatusCode == 403 {
		//	return nil, errors.New("Error Authorization")
		//}
		if len(resp_body) == 0 {
			return &UploadResult{}, resp.StatusCode, nil
		}

		var ret UploadResult
		unmarshal_err := json.Unmarshal(resp_body, &ret)
		if unmarshal_err != nil {
			glog.V(0).Infoln("failing to read upload resonse", uploadUrl, string(resp_body))
			return nil, resp.StatusCode, unmarshal_err
		}

		return &ret, resp.StatusCode, nil
	} else {
		defer resp.Body.Close()
		ioutil.ReadAll(resp.Body)
		return nil, resp.StatusCode, nil
	}

}
func Upload6(uploadUrl string, filename string, data []byte, isGzipped bool, mtype string, header map[string]string) (*UploadResult, int, error) {
	body_buf := bytes.NewBufferString("")
	body_writer := multipart.NewWriter(body_buf)
	h := make(textproto.MIMEHeader)
//	filename, _ = url.QueryUnescape(filename)
	_, filename = path.Split(filename)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, fileNameEscaper.Replace(filename)))
	if mtype == "" {
		mtype = mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	}
	if mtype != "" {
		h.Set("Content-Type", mtype)
	}
	if isGzipped {
		h.Set("Content-Encoding", "gzip")
	}
	file_writer, cp_err := body_writer.CreatePart(h)
	if cp_err != nil {
		glog.V(0).Infoln("error creating form file", cp_err.Error())
		return nil, 0, cp_err
	}
	if _, err := io.Copy(file_writer, bytes.NewReader(data)); err != nil {
		glog.V(0).Infoln("error copying data", err)
		return nil, 0, err
	}
	content_type := body_writer.FormDataContentType()
	if err := body_writer.Close(); err != nil {
		glog.V(0).Infoln("error closing body", err)
		return nil, 0, err
	}
	req, err := http.NewRequest("POST", uploadUrl, body_buf)
	if err != nil {
		return nil, 0, err
	}
	for key, value := range header {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", content_type)

	if req.Header.Get("Content-Disposition") == "" && len(data) == 0 {
		req.Header.Set("Content-Disposition", fmt.Sprintf(`filename="%s"`,fileNameEscaper.Replace(filename)))
	}
	resp, post_err := client.Do(req)
	if post_err != nil {
		resp, post_err = client.Do(req)
	}
	if post_err != nil {
		glog.V(0).Infoln("failing to upload to", uploadUrl, post_err.Error())
		return nil, 0, post_err
	}
	defer resp.Body.Close()
	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		glog.V(0).Infoln("failing to read response", uploadUrl, ra_err.Error())
		return nil, 0, ra_err
	}

	//if resp.StatusCode == 403 {
	//	return nil, errors.New("Error Authorization")
	//}
	if len(resp_body) <= 0 {
		return nil, resp.StatusCode, nil
	}
	var ret UploadResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.V(0).Infoln("failing to read upload resonse", uploadUrl, string(resp_body))
		return nil, resp.StatusCode, unmarshal_err
	}

	return &ret, resp.StatusCode, nil

}

/*
流式上传，会自动分块
只从reader中读取
64M以内正常上传，超过自动分片
header中是秘钥
onretry 在重试之前调用
12-7 采用url带鉴权
*/
func UploadByFlow(
	server *string,
	uploadUrl string,
	filename string,
	reader io.Reader,
	appfile *util.Appendfile,
	isGzipped bool,
	mtype string,
	get_sign func(method string) map[string]string,
	headers map[string]string, //包含agent和reqid
	onretry func(),
) (size int64, err error) {
	var r0 io.ReadCloser
	var blocksize int64
	var filetag string
	var uploadid string
	var idx int
	var multthreshold int64
	if headers == nil {
		headers = make(map[string]string)
	}

	if filetag != "" {
		headers["tag"] = filetag
	}

	if appfile != nil {
		r0 = appfile.Lastblock
		filetag = appfile.Tag
		glog.V(4).Infoln("appfile != nil  ", filename)
		//分块文件？？
		if appfile.Ftype == "blockedFile" {
			blocksize = appfile.Blocksize
			multthreshold = appfile.Blocksize
			uploadid = appfile.Uploadid
			idx = appfile.Fixidx //默认从
		} else {
			blocksize = int64(4 * 1024 * 1024)
			multthreshold = int64(64 * 1024 * 1024)
		}
	} else {
		r0 = nil
		blocksize = int64(4 * 1024 * 1024)
		multthreshold = int64(64 * 1024 * 1024)
	}
	if r0 != nil {
		defer r0.Close()
	}
	flowreader, err := util.NewTempflow(filename, reader, isGzipped, mtype, blocksize, multthreshold, r0)
	/*这儿出错无法重试*/
	if err != nil {
		return 0, err
	}
	glog.V(4).Infoln("UploadByFlow  ", uploadUrl, flowreader.IsOver(), uploadid)

	//需要分块的
	if !flowreader.IsOver() || uploadid != "" {
		if uploadid == "" {
			//取id
			urlsign := get_sign("POST")
			getidurl := "http://" + *server + uploadUrl + "?type=uploads"
			for k, v := range urlsign {
				getidurl += "&" + k + "=" + v
			}
			resp, err := flowreader.MakeRequest(getidurl, "POST", nil, headers)
			if err != nil {
				if onretry != nil {
					onretry()
				}
				/*重试*/
				resp, err = flowreader.MakeRequest(getidurl, "POST", nil, headers)
				if err != nil {
					glog.V(0).Infoln("upload error ", err)
					return 0, err
				}
			}
			iddata, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode == 403 {
				return 0, errors.New("Error Authorization")
			}
			if resp.StatusCode == 404 {
				return 0, errors.New("Error Not Exist")
			}
			if resp.StatusCode < 200 || resp.StatusCode > 300 {
				return 0, errors.New("Error with code:" + fmt.Sprint(resp.StatusCode))
			}
			var tempmap map[string]interface{}
			err = json.Unmarshal(iddata, &tempmap)
			if err != nil {
				glog.V(0).Infoln(err.Error())
				return 0, errors.New("Error :" + err.Error())
			}
			if _, ok := tempmap["upload-id"]; !ok {
				glog.V(0).Infoln("error while get upload-id")
				return 0, errors.New(" error while get upload-id")
			}
			uploadid = tempmap["upload-id"].(string)
		}
	} else {
		flowreader.SetOnce(true)
		//直接上传的
		urlsign := get_sign("POST")
		uploadurl := "http://" + *server + uploadUrl + "?pretty=yes"
		for k, v := range urlsign {
			uploadurl += "&" + k + "=" + v
		}
		glog.V(4).Infoln("UploadByFlow ", uploadUrl, " once")
		err = flowreader.Upload(uploadurl, headers)
		if err != nil {
			/*重试*/
			flowreader.Reset()
			if onretry != nil {
				onretry()
			}
			err = flowreader.Upload(uploadurl, headers)
			if err != nil {
				return 0, err
			} else {
				return flowreader.GetOffset(), nil
			}
		}
		return flowreader.GetOffset(), nil
	}

	for {
		glog.V(4).Infoln("UploadByFlow ", "http://" + *server+uploadUrl, "the ", idx, " time")
		urlsign := get_sign("POST")
		uploadurl := "http://" + *server + uploadUrl + "?upload-id=" + uploadid + "&part-number=" + fmt.Sprint(idx)
		for k, v := range urlsign {
			uploadurl += "&" + k + "=" + v
		}
		err = flowreader.Upload(uploadurl, headers)
		if err != nil {
			if onretry != nil {
				onretry()
			}
			glog.V(4).Infoln("ReUploadByFlow ", "http://" + *server+uploadUrl, "the ", idx, " time")
			flowreader.Reset()
			err = flowreader.Upload(uploadurl, headers)
			if err != nil {
				glog.V(0).Infoln(err)
				goto Complete
			}
		}
		idx++
		flowreader.CreateBoundary()
		if flowreader.IsOver() {
			break
		}
	}

Complete:
	if idx == 0 {
		return 0, errors.New("Error Unknow:get bad data from client")
	}

	//不管结果如何，都进行提交动作
	//结束
	urlsign := get_sign("POST")
	//提交
	commiturl := "http://" + *server + uploadUrl + "?upload-id=" + uploadid
	for k, v := range urlsign {
		commiturl += "&" + k + "=" + v
	}

	headers["Content-Type"] = "application/json"

	parts := AuditInfoSt{}
	parts.ObjectParts = make([]AuditPartSt, idx)
	for i := 0; i < idx; i++ {
		parts.ObjectParts[i] = AuditPartSt{
			PartNumber: i,
		}
	}
	by, _ := json.Marshal(parts)
	resp, err := flowreader.MakeRequest(commiturl, "POST", by, headers)
	if err != nil {
		if onretry != nil {
			onretry()
		}
		resp, err = flowreader.MakeRequest(commiturl, "POST", by, headers)
		if err != nil {
			glog.V(0).Infoln("Complete error ", err)
			return 0, err
		}
	}
	tempdata, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	glog.V(4).Infoln("UploadByFlow ", "http://" + *server+uploadUrl, "commit")
	if resp.StatusCode == 403 {
		return 0, errors.New("Complete Error Authorization")
	}
	if resp.StatusCode == 404 {
		return 0, errors.New("Complete Error Not Exist")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 300 {
		return 0, errors.New("Complete Error  with code from remote:" + fmt.Sprint(resp.StatusCode) + string(tempdata))
	}
	return flowreader.GetOffset(), nil
}

func MakeRequest(uploadUrl string, method string, data []byte, header map[string]string) (resp *http.Response, err error) {
	req, err := http.NewRequest(method, uploadUrl, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	for key, value := range header {
		req.Header.Set(key, value)
	}
	return client.Do(req)
}


func GetMtype(filename,content_Type string)string  {
	var ext string
	dotIndex := strings.LastIndex(filename, ".")
	if dotIndex > 0 {
		ext = filename[dotIndex:]
	}
	mtype := ""
	if ext != "" {
		mtype = mime.TypeByExtension(ext)
	}
	if content_Type != "application/octet-stream" && content_Type != ""{
		mtype = content_Type
	}
	return mtype
}