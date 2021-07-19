package cache

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/public"
)

type McmCacheType struct {
	managers    []*MemcacheManager
	expire time.Time
}
type McmCache struct {
	m    map[int]*McmCacheType
	lock sync.Mutex
}

var (
	mcmCache = McmCache{m: make(map[int]*McmCacheType)}
)

func (mcmc *McmCache) get(pos int) []*MemcacheManager {
	if mcm, ok := mcmc.m[pos]; ok {
		if mcm.expire.After(time.Now()) {
			return mcm.managers
		}
	}
	return nil
}
func (mcmc *McmCache) set(pos int,accessUrl []string) {
	mcmc.lock.Lock()
	defer mcmc.lock.Unlock()
	needUpdate := false
	urlLen := len(accessUrl)
	if urlLen <= 0 {
		return
	}
	if mcm, ok := mcmc.m[pos]; ok {
		managerLen := len(mcm.managers)
			if managerLen == urlLen{
				for index,accessUrl := range accessUrl {
				if mcm.managers[index].url != accessUrl {
					needUpdate = true
					break
				}
			}
		}
		if !needUpdate {
			mcm.expire = time.Now().Add(1 * time.Minute)
			return
		}
	
	}else {
		needUpdate = true
	}
	if needUpdate {
		var managers []*MemcacheManager
		for _,accessUrl := range accessUrl {
		   mcm,_ := NewMemcacheManager(accessUrl)
		   managers = append(managers,mcm)
		}
		mcmc.m[pos] = &McmCacheType{managers:managers,expire:time.Now().Add(1 * time.Minute)}
	}

}
func LookupMemcache(master string,hashkey string,direct bool) ([]*MemcacheManager, error) {
	parnum, _, err := operation.GetCollectionPartCountRp(master, public.MEMCACHE,nil)
	if err != nil || parnum == 0 {
		if parnum == 0 {
			err = errors.New("PartitionCount is 0, make sure the collection: " + public.MEMCACHE + " specified has been constructed!")
		}
		glog.V(0).Infoln("Failed to get partition num and replication:", err.Error())
		return nil, err
	}
	parId := operation.HashPos(hashkey, parnum)
	mcms := mcmCache.get(parId)
	if direct == true || mcms == nil {
		lookup, _, err := operation.PartitionLookup2(master, hashkey, public.MEMCACHE, false,nil)
		if err != nil {
			return nil, err
		}
		mcmCache.set(parId, lookup.List())
		mcms = mcmCache.get(parId)

	}
	if mcms != nil {
		return mcms, nil
	} else {
		return nil, errors.New("NOT-FOUND-ERROR")
	}
}
func LookupAllMemcache(master string, direct bool) ([][]*MemcacheManager, error) {
	parnum, rep, err := operation.GetCollectionPartCountRp(master, public.MEMCACHE,nil)
	if err != nil || parnum == 0 {
		if parnum == 0 {
			err = errors.New("PartitionCount is 0, make sure the collection: " + public.MEMCACHE + " specified has been constructed!")
		}
		glog.V(0).Infoln("Failed to get partition num and replication:", err.Error())
		return nil, err
	}
	var res [][]*MemcacheManager
	for parId := 0; parId < parnum; parId++ {
		mcms := mcmCache.get(parId)
		if direct == true || mcms == nil {
			lookup, err := operation.PartitionLookup(master, strconv.Itoa(parId), rep, public.MEMCACHE, direct,nil)
			if err != nil {
				continue
			}
			mcmCache.set(parId, lookup.List())
			mcms = mcmCache.get(parId)
		}
		if mcms == nil {
			return nil, errors.New("NOT-FOUND-ERROR")
		}
		res = append(res, mcms)
	}
	return res, err
}
