package operation

import (
	"errors"
	"strings"
	"time"

	"context"

	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
)

func DownloadInCollection(fid, collection, ms string, ctx *context.Context) (filename string, content []byte, mType string, fsize int64, err error) {
	parts := strings.SplitN(fid, ",", 2)
	if len(parts) != 2 {
		err = errors.New("Bad Fid " + fid)
		return
	}
	lookup, err := Lookup(ms, parts[0], collection, ctx)
	if err != nil {
		return
	}
	var (
		list  = lookup.List()
		start = time.Now().Unix() % int64(len(list))
	)

	headers := make(map[string]string)
	if ctx != nil {
		reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
		headers[public.SDOSS_REQUEST_ID] = reqid
	}

	for i := 0; i < len(list); i++ {
		fileUrl := "http://" + list[start] + "/" + fid
		filename, content, mType, err = util.DownloadFile(fileUrl, headers)
		if err == nil {
			fsize = int64(len(content))
			break
		}
		start = (start + 1) % int64(len(list))
	}
	return
}

func DownloadInCollectionwithlastmodify(fid, collection, ms string) (filename string, content []byte, mType string, lastmodify int64, err error) {
	parts := strings.SplitN(fid, ",", 2)
	if len(parts) != 2 {
		err = errors.New("Bad Fid " + fid)
		return
	}
	lookup, err := Lookup(ms, parts[0], collection, nil)
	if err != nil {
		return
	}

	list := lookup.List()
	if len(list) <= 0 {
		err = errors.New("list volume ip is 0,fid: " + fid)
		return
	}
	start := time.Now().Unix() % int64(len(list))

	for i := 0; i < len(list); i++ {
		fileUrl := "http://" + list[start] + "/" + fid
		filename, content, mType, lastmodify, err = util.DownloadFilewithlastmodify(fileUrl)
		if err == nil && len(content) > 0 {
			break
		}
		start = (start + 1) % int64(len(list))
	}
	if err == nil && len(content) <= 0 {
		err = errors.New("Failed to download file:" + fid)
	}
	return
}
