package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
	"context"
)

type CollectionBasicResult struct {
	PartitionCount int32
	Rp             string
	Error          string
}
type CollectionBasic struct {
	PartitionCount int32
	Rp             string
}
type CollectionBasicCache struct {
	collectioinMap map[string]CollectionBasic
	lock           sync.Mutex
}

var collectionBasicCache = CollectionBasicCache{collectioinMap: make(map[string]CollectionBasic)}

func (colBasicCache *CollectionBasicCache) get(collection string) (cb *CollectionBasic) {
	colBasicCache.lock.Lock()
	defer colBasicCache.lock.Unlock()
	if cbc, ok := colBasicCache.collectioinMap[collection]; ok {
		return &cbc
	} else {
		return nil
	}
}
func (colBasicCache *CollectionBasicCache) set(collection string, cb *CollectionBasic) {
	colBasicCache.lock.Lock()
	defer colBasicCache.lock.Unlock()
	collectionBasicCache.collectioinMap[collection] = *cb
	return
}
func GetCollectionPartCountRp(master string, collection string,ctx *context.Context) (int, string, error) {
	cb := collectionBasicCache.get(collection)
	if cb != nil {
		return int(cb.PartitionCount), cb.Rp, nil
	}
	headers:=make(map[string]string)
	if ctx!=nil{
		reqid,_:=((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
		headers[public.SDOSS_REQUEST_ID]=reqid
	}


	urlstr:="/collection/basic?collection="+collection+"&tryLocal=y"

	resp, err := util.MakeRequest_timeout(master, urlstr, "GET", nil, headers)
	if err != nil {
		sleep := 1
		for i := 0; i <= 3; i++ {
			time.Sleep(time.Duration(sleep) * time.Second)
			resp, err = util.MakeRequest_timeout(master, urlstr, "GET", nil, nil)
			if err == nil {
				break
			}
			sleep += sleep
		}
	}
	if err == nil {
		defer resp.Body.Close()
		if data, err := ioutil.ReadAll(resp.Body); err == nil {
			ret := CollectionBasicResult{}
			if err = json.Unmarshal(data, &ret); err == nil {
				erroStr := ret.Error
				if erroStr != "" {
					err = fmt.Errorf(erroStr)
					return 0, "", err
				}
				cb := CollectionBasic{}
				cb.PartitionCount = ret.PartitionCount
				cb.Rp = ret.Rp
				collectionBasicCache.set(collection, &cb)
				if cb.PartitionCount == 0 && collection != public.FOLDER && collection != public.EC{
					return 0, "", errors.New("Unexpected Partition Count!")
				}
				return int(cb.PartitionCount), cb.Rp, nil
			}
		}
	}
	return 0, "", err
}
