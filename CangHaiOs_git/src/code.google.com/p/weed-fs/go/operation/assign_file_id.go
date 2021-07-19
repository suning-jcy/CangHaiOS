package operation

import (
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"code.google.com/p/weed-fs/go/public"
	"io/ioutil"
)

type AssignResult struct {
	Fid       string `json:"fid,omitempty"`
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
	Count     int    `json:"count,omitempty"`
	Error     string `json:"error,omitempty"`
	SerialNum string `json:"serialNum"`
}

func Assign(server string, count int, replication string, collection string, reqid string) (*AssignResult, error) {
	values := make(url.Values)
	values.Add("count", strconv.Itoa(count))
	if replication != "" {
		values.Add("replication", replication)
	}
	if collection != "" {
		values.Add("collection", collection)
	}

	headers := make(map[string]string)
	headers[public.SDOSS_REQUEST_ID] = reqid

	resp, err := util.MakeHttpRequest("http://"+server+"/dir/assign", "POST", values,   headers, true)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	jsonBlob, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var ret AssignResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Count <= 0 {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}

func AssignEC(server string, count int, replication string, collection string, reqid string) ([]AssignResult, error) {
	values := make(url.Values)
	values.Add("count", strconv.Itoa(count))
	if replication != "" {
		values.Add("replication", replication)
	}
	if collection != "" {
		values.Add("collection", collection)
	}
	values.Add("ec", "true")
	headers := make(map[string]string)
	headers[public.SDOSS_REQUEST_ID] = reqid

	resp, err := util.MakeHttpRequest("http://"+server+"/dir/assign", "POST", values,   headers, true)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	jsonBlob, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		var ret AssignResult
		err = json.Unmarshal(jsonBlob, &ret)
		if err != nil {
			return nil,err
		}
		return nil,errors.New(ret.Error)
	}
	var ret []AssignResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

