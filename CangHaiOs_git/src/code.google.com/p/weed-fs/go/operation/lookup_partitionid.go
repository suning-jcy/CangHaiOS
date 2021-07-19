package operation

import (
	"code.google.com/p/weed-fs/go/glog"
	"encoding/json"
	"errors"
	"hash/crc32"
	"net/url"
	"strconv"
	"time"

	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
	"context"
	"io/ioutil"
)

type PartitionLookupResult struct {
	Id        string     `json:"partitionId,omitempty"`
	Locations []Location `json:"locations,omitempty"`
	Error     string     `json:"error,omitempty"`
}

func (plr *PartitionLookupResult) List() (list []string) {
	for _, l := range plr.Locations {
		list = append(list, l.Url)
	}
	return
}

var (
	parcache = ParidCache{cache: make(map[string][]ParidInfo)}
)

func (cache *ParidCache) PartitionLookup(server string, Parid string, replication string, collection string, direct bool, ctx *context.Context) (ret *PartitionLookupResult, err error) {
	if direct {
		ret, err = do_Partitionlookup(server, public.MASTER_PARTITION_LOOKUP, Parid, replication, collection, ctx)
		if err != nil {
			sleep := 1
			for i := 0; i <= 3; i++ {
				time.Sleep(time.Duration(sleep) * time.Second)
				ret, err = do_Partitionlookup(server, public.MASTER_PARTITION_LOOKUP, Parid, replication, collection, ctx)
				if err == nil {
					break
				}
				sleep += sleep
			}
		}
		if err == nil && ret.Locations != nil && len(ret.Locations) > 0 {
			cache.Set(collection, Parid, ret.Locations, 5*time.Minute)
		}
		return
	}
	locations, cache_err := cache.Get(collection, Parid)
	if cache_err != nil {
		ret, err = do_Partitionlookup(server, public.MASTER_PARTITION_LOOKUP, Parid, replication, collection, ctx)
		/*
			if err != nil {
				sleep := 1
				for i := 0; i <= 3; i++ {
					time.Sleep(time.Duration(sleep) * time.Second)
					ret, err = do_Partitionlookup(server, public.MASTER_PARTITION_LOOKUP, Parid, replication, collection)
					if err == nil {
						break
					}
					sleep += sleep
				}
			}
		*/

		if err == nil && ret.Locations != nil && len(ret.Locations) > 0 {
			cache.Set(collection, Parid, ret.Locations, 5*time.Minute)
		} else {
			//没获取到最新的，沿用旧的，持续时间30S（此时数据正确性是打折扣的，虽然一般都是正确的）
			if locations != nil {
				cache.Set(collection, Parid, locations, 30*time.Second)
				ret = &PartitionLookupResult{Id: Parid, Locations: locations}
			} else {
				//旧的也没有，就空的吧。空的最不值得信任，10S过期
				cache.Set(collection, Parid, []Location{}, 10*time.Second)
				ret = &PartitionLookupResult{Id: Parid, Locations: []Location{}}
			}
			err = nil
		}
	} else {
		ret = &PartitionLookupResult{Id: Parid, Locations: locations}
	}
	if ret != nil && len(ret.Locations) < 1 {
		err = errors.New("NotFoundLocationsFor:" + Parid + " " + replication + " " + collection)
	}
	return
}
func PartitionLookup(server string, Parid string, replication string, collection string, direct bool, ctx *context.Context) (ret *PartitionLookupResult, err error) {
	if direct {
		ret, err = do_Partitionlookup(server, public.MASTER_PARTITION_LOOKUP, Parid, replication, collection, ctx)
		if err != nil {
			sleep := 1
			for i := 0; i <= 3; i++ {
				time.Sleep(time.Duration(sleep) * time.Second)
				ret, err = do_Partitionlookup(server, public.MASTER_PARTITION_LOOKUP, Parid, replication, collection, ctx)
				if err == nil {
					break
				}
				sleep += sleep
			}
		}
		if err == nil && ret.Locations != nil && len(ret.Locations) > 0 {
			parcache.Set(collection, Parid, ret.Locations, 5*time.Minute)
		}
		return
	}
	locations, cache_err := parcache.Get(collection, Parid)
	if cache_err != nil {
		ret, err = do_Partitionlookup(server, public.MASTER_PARTITION_LOOKUP, Parid, replication, collection, ctx)
		/*
			if err != nil {
				sleep := 1
				for i := 0; i <= 3; i++ {
					time.Sleep(time.Duration(sleep) * time.Second)
					ret, err = do_Partitionlookup(server, public.MASTER_PARTITION_LOOKUP, Parid, replication, collection)
					if err == nil {
						break
					}
					sleep += sleep
				}
			}
		*/

		if err == nil && ret.Locations != nil && len(ret.Locations) > 0 {
			parcache.Set(collection, Parid, ret.Locations, 5*time.Minute)
		} else {
			//没获取到最新的，沿用旧的，持续时间30S（此时数据正确性是打折扣的，虽然一般都是正确的）
			if locations != nil {
				parcache.Set(collection, Parid, locations, 30*time.Second)
				ret = &PartitionLookupResult{Id: Parid, Locations: locations}
			} else {
				//旧的也没有，就空的吧。空的最不值得信任，10S过期
				parcache.Set(collection, Parid, []Location{}, 10*time.Second)
				ret = &PartitionLookupResult{Id: Parid, Locations: []Location{}}
			}
			err = nil
		}
	} else {
		ret = &PartitionLookupResult{Id: Parid, Locations: locations}
	}
	if ret != nil && len(ret.Locations) < 1 {
		glog.V(0).Infoln("err:",cache_err,"server:",server,len(ret.Locations),Parid)
		err = errors.New("NotFoundLocationsFor:" + Parid + " " + replication + " " + collection)
	}
	return
}

func do_Partitionlookup(server string, path string, Parid string, replication string, collection string, ctx *context.Context) (*PartitionLookupResult, error) {
	values := make(url.Values)
	values.Add("partitionId", Parid)
	if replication != "" {
		values.Add("replication", replication)
	}
	if collection != "" {
		values.Add("collection", collection)
	}
	values.Add("tryLocal", "y")
	headers := make(map[string]string)
	if ctx != nil {
		reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
		headers[public.SDOSS_REQUEST_ID] = reqid
	}

	resp, err := util.MakeHttpRequest("http://"+server+path, "GET", values, headers, true)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	jsonBlob, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var ret PartitionLookupResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}
func InvalidateFPCache(node string) {
	parcache.Invalidate(node)
}
func PartitionLookup2(server, hashStr, collection string, direct bool, ctx *context.Context) (ret *PartitionLookupResult, parId string, err error) {
	parnum, rep, err := GetCollectionPartCountRp(server, collection, ctx)
	if err != nil || parnum <= 0 {
		if parnum <= 0 {
			glog.V(0).Infoln("PartitionCount is 0, make sure the collection: " + collection + " specified has been constructed!",err,server,parnum,hashStr,direct)
			err = errors.New("PartitionCount is 0, make sure the collection: " + collection + " specified has been constructed!")
		}
		return nil, "", err
	}
	parId = strconv.Itoa(HashPos(hashStr, parnum))
	ret, err = PartitionLookup(server, parId, rep, collection, direct, ctx)
	if err != nil {
		return nil, "", err
	}
	if ret == nil || len(ret.Locations) < 1 {
		return nil, "", errors.New("SERVICE-ERROR,fail to get collection:" + collection + "information")
	}
	return
}
func HashPos(path string, parNum int) int {
	hash := crc32.ChecksumIEEE([]byte(path))
	pos := hash % uint32(parNum)
	return int(pos)
}
func PartitionPos(server, hashStr, collection string, ctx *context.Context) (pos string, err error) {
	parnum, _, err := GetCollectionPartCountRp(server, collection, ctx)
	if err != nil || parnum == 0 {
		if parnum == 0 {
			err = errors.New("Failed to get partition count from server:" + server)
		}
		return "", err
	}
	return strconv.Itoa(HashPos(hashStr, parnum)), nil
}
