package operation

import (
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

var (
	sepCach = SeparationCache{cache: &SeparationInfo{}}
)

func SeparationLookup(master string, direct bool, ctx *context.Context) (res string, err error) {
	if direct {
		res, err = do_Separationlookup(master, public.MASTER_SEPARATION_LEADER_LOOKUP, ctx)
		if err != nil {
			sleep := 1
			for i := 0; i <= 3; i++ {
				time.Sleep(time.Duration(sleep) * time.Second)
				res, err = do_Separationlookup(master, public.MASTER_SEPARATION_LEADER_LOOKUP, ctx)
				if err == nil {
					break
				}
				sleep += sleep
			}
		}
		if err == nil && res != "" {
			sepCach.Set(res, 5*time.Minute)
		}
		return
	}
	res, cacheErr := sepCach.Get()
	if cacheErr != nil {
		res, err = do_Separationlookup(master, public.MASTER_SEPARATION_LEADER_LOOKUP, ctx)
		if err == nil && res != "" {
			sepCach.Set(res, 5*time.Minute)
		} else {
			if res != "" {
				sepCach.Set(res, 30*time.Second)
			} else {
				sepCach.Set(res, 10*time.Second)
			}
			err = nil
		}
	}
	return
}

func do_Separationlookup(server string, path string, ctx *context.Context) (res string, err error) {
	headers := make(map[string]string)
	if ctx != nil {
		reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
		headers[public.SDOSS_REQUEST_ID] = reqid
	}

	resp, err := util.MakeHttpRequest("http://"+server+path, "GET", nil, headers, true)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode!=http.StatusOK{
		return "", errors.New("Get separation server err: "+strconv.Itoa(resp.StatusCode))
	}
	data,err:=ioutil.ReadAll(resp.Body)
	if err!=nil{
		return "", errors.New("Get separation server err: "+strconv.Itoa(resp.StatusCode))
	}
	var result SeparationServers
	err=json.Unmarshal(data,&result)
	if err!=nil{
		return "", errors.New("Get separation server err: "+strconv.Itoa(resp.StatusCode))
	}
	for _,v:=range result.List{
		if v.IsLeader{
			res=v.Separationserver
			break
		}
	}
	if res == "" {
		return "", errors.New("Get empty separation leader")
	}
	return
}

type SeparationServers struct {
	List       map[string]*SeparationServerEntity `json:"List,omitempty"`
	lostleader int
}
type SeparationServerEntity struct {
	// Separation的地址
	Separationserver string `json:"Separationserver,omitempty"`
	//上次上报时间
	ExpiredTime int64
	//leader?
	IsLeader bool `json:"IsLeader,omitempty"`
}