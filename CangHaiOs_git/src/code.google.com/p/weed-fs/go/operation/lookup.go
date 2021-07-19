package operation

import (
	"encoding/json"
	"errors"
	"math/rand"
	"net/url"
	"sort"
	"strings"
	"time"

	"context"
	"io/ioutil"

	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
)

var UNEXPTECTEDOPTERR = errors.New("UnexptectedOptions!")

type Location struct {
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
	SerialNum string `json:"sn"`
}
type LookupResult struct {
	VolumeId  string     `json:"volumeId,omitempty"`
	Locations []Location `json:"locations,omitempty"`
	Error     string     `json:"error,omitempty"`
}

func (lr *LookupResult) List() (nlist []string) {
	for _, v := range lr.Locations {
		nlist = append(nlist, v.PublicUrl)
	}
	sort.Sort(sort.StringSlice(nlist))
	return
}
func Lookup(server, id, collection string, ctx *context.Context) (ret *LookupResult, err error) {
	locations, cache_err := vc.Get(collection, id)
	if cache_err != nil {
		ret, err = do_lookup(server, id, collection, ctx)
		if err == nil {
			vc.Set(collection, id, ret.Locations, 5*time.Minute)
		} else {
			//没获取到最新的，沿用旧的，持续时间30S（此时数据正确性是打折扣的，虽然一般都是正确的）
			if locations != nil {
				vc.Set(collection, id, locations, 30*time.Second)
				ret = &LookupResult{VolumeId: id, Locations: locations}
			} else {
				//旧的也没有，就空的吧。
				vc.Set(collection, id, []Location{}, 10*time.Second)
				ret = &LookupResult{VolumeId: id, Locations: []Location{}}
			}
			err = nil
		}
		/*
			else {
				sleep := 1
				for i := 0; i <= 3; i++ {
					time.Sleep(time.Duration(sleep) * time.Second)
					ret, err = do_lookup(server, id, collection)
					if err == nil {
						vc.Set(collection, id, ret.Locations, 1*time.Minute)
						break
					}
					sleep += sleep
				}
			}
		*/

	} else {
		ret = &LookupResult{VolumeId: id, Locations: locations}
	}
	return
}

func do_lookup(server, id, collection string, ctx *context.Context) (*LookupResult, error) {
	values := make(url.Values)
	values.Add("volumeId", id)
	values.Add("collection", collection)
	values.Add("tryLocal", "y")
	headers := make(map[string]string)
	if ctx != nil {
		reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
		headers[public.SDOSS_REQUEST_ID] = reqid
	}

	resp, err := util.MakeHttpRequest("http://"+server+"/dir/lookup", "GET", values, headers, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	jsonBlob, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var ret LookupResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}

func LookupFileId(server, fid, collection string, ctx *context.Context) (fullUrl string, err error) {
	parts := strings.Split(fid, ",")
	if len(parts) != 2 {
		return "", errors.New("Invalid fileId " + fid)
	}
	lookup, lookupError := Lookup(server, parts[0], collection, ctx)
	if lookupError != nil {
		return "", lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", errors.New("File Not Found")
	}
	randVal:=rand.Intn(len(lookup.Locations))
	if collection == public.EC{
		return "http://" + lookup.Locations[randVal].PublicUrl + "/" + fid+"?serialNum="+lookup.Locations[randVal].SerialNum, nil
	}
	return "http://" + lookup.Locations[randVal].PublicUrl + "/" + fid, nil
}

func LookupVolumeIds(server string, vids []string, ctx *context.Context) (map[string]LookupResult, error) {
	values := make(url.Values)
	for _, vid := range vids {
		values.Add("volumeId", vid)
	}

	headers := make(map[string]string)
	if ctx != nil {
		reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
		headers[public.SDOSS_REQUEST_ID] = reqid
	}

	resp, err := util.MakeHttpRequest("http://"+server+"/dir/lookup", "GET", values, headers, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	jsonBlob, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	ret := make(map[string]LookupResult)
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, errors.New(err.Error() + " " + string(jsonBlob))
	}
	return ret, nil
}
func InvalidateVlCache(node string) {
	vc.Invalidate(node)
}

func InvalidateVlCacheById(vid string) {
	vc.InvalidateById(vid)
}

func GetVlCache(vid string)([]Location, error){
	return vc.GetById(vid)
}

func LookupInCache(server, id, collection string, vCache *CollectionVolumeLocCache, ctx *context.Context) (ret *LookupResult, err error) {
	locations, cache_err := vCache.Get(collection, id)
	if cache_err != nil {
		ret, err = do_lookup(server, id, collection, ctx)
		if err == nil {
			vc.Set(collection, id, ret.Locations, 5*time.Minute)
		} else {
			sleep := 1
			for i := 0; i <= 3; i++ {
				time.Sleep(time.Duration(sleep) * time.Second)
				ret, err = do_lookup(server, id, collection, ctx)
				if err == nil {
					vCache.Set(collection, id, ret.Locations, 5*time.Minute)
					break
				}
				sleep += sleep
			}
		}
	} else {
		ret = &LookupResult{VolumeId: id, Locations: locations}
	}
	return
}
