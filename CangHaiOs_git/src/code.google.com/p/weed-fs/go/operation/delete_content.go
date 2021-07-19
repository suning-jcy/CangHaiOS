package operation

import (
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync"
	"context"
	"code.google.com/p/weed-fs/go/public"
	"io/ioutil"
)

type DeleteResult struct {
	Fid   string `json:"fid"`
	Size  int    `json:"size"`
	Error string `json:"error,omitempty"`
}

func DeleteFile(master, id, collection string,ctx *context.Context) error {
	fileUrl, err := LookupFileId(master, id, collection,ctx)
	if err != nil {
		return err
	}
	headers:=make(map[string]string)
	if ctx!=nil{
		reqid,_:=((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
		headers[public.SDOSS_REQUEST_ID]=reqid
	}
	resp,err:= util.MakeHttpRequest(fileUrl,"DELETE",nil,headers,false)
	if err!=nil{
		return  err
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return  err
	}
	return  nil
}

func DeleteECFile(url, fid, serialNum string,ctx *context.Context) error {
	headers:=make(map[string]string)
	if ctx!=nil{
		reqid,_:=((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
		headers[public.SDOSS_REQUEST_ID]=reqid
	}
	fileUrl:="http://" + url + "/" + fid+"?serialNum="+serialNum
	resp,err:= util.MakeHttpRequest(fileUrl,"DELETE",nil,headers,false)
	if err!=nil{
		return  err
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return  err
	}
	return  nil
}

func ParseFileId(fid string) (vid string, key_cookie string, err error) {
	commaIndex := strings.Index(fid, ",")
	if commaIndex <= 0 {
		return "", "", errors.New("Wrong fid format.")
	}
	return fid[:commaIndex], fid[commaIndex+1:], nil
}

type DeleteFilesResult struct {
	Errors  []string
	Results []DeleteResult
}

func DeleteFiles(master string, fileIds []string) (*DeleteFilesResult, error) {
	vid_to_fileIds := make(map[string][]string)
	ret := &DeleteFilesResult{}
	var vids []string
	for _, fileId := range fileIds {
		vid, _, err := ParseFileId(fileId)
		if err != nil {
			ret.Results = append(ret.Results, DeleteResult{Fid: vid, Error: err.Error()})
			continue
		}
		if _, ok := vid_to_fileIds[vid]; !ok {
			vid_to_fileIds[vid] = make([]string, 0)
			vids = append(vids, vid)
		}
		vid_to_fileIds[vid] = append(vid_to_fileIds[vid], fileId)
	}

	lookupResults, err := LookupVolumeIds(master, vids,nil)
	if err != nil {
		return ret, err
	}

	server_to_fileIds := make(map[string][]string)
	for vid, result := range lookupResults {
		if result.Error != "" {
			ret.Errors = append(ret.Errors, result.Error)
			continue
		}
		for _, location := range result.Locations {
			if _, ok := server_to_fileIds[location.PublicUrl]; !ok {
				server_to_fileIds[location.PublicUrl] = make([]string, 0)
			}
			server_to_fileIds[location.PublicUrl] = append(
				server_to_fileIds[location.PublicUrl], vid_to_fileIds[vid]...)
		}
	}

	var wg sync.WaitGroup
	for server, fidList := range server_to_fileIds {
		wg.Add(1)
		go func(server string, fidList []string) {
			defer wg.Done()
			values := make(url.Values)
			for _, fid := range fidList {
				values.Add("fid", fid)
			}
			jsonBlob, err := util.Post("http://"+server+"/delete", values)
			if err != nil {
				ret.Errors = append(ret.Errors, err.Error()+" "+string(jsonBlob))
				return
			}
			var result []DeleteResult
			err = json.Unmarshal(jsonBlob, &result)
			if err != nil {
				ret.Errors = append(ret.Errors, err.Error()+" "+string(jsonBlob))
				return
			}
			ret.Results = append(ret.Results, result...)
		}(server, fidList)
	}
	wg.Wait()

	return ret, nil
}
