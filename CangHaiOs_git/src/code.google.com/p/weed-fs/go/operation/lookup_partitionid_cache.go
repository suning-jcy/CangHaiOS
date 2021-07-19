package operation

import (
	"code.google.com/p/weed-fs/go/glog"
	"errors"
	"strconv"
	"sync"
	"time"
)

type ParidInfo struct {
	Locations       []Location
	NextRefreshTime time.Time
}
type ParidCache struct {
	cache map[string][]ParidInfo
	sync.RWMutex
}

func NewParidCache() *ParidCache {
	cache := &ParidCache{}
	cache.cache = make(map[string][]ParidInfo)
	return cache
}
func (vc *ParidCache) Get(collection, vid string) ([]Location, error) {
	vc.RLock()
	defer vc.RUnlock()
	parcache, ok := vc.cache[collection]
	if !ok {
		return nil, errors.New("Not Found")
	}
	id, _ := strconv.Atoi(vid)
	if 0 <= id && id < len(parcache) {
		if parcache[id].Locations == nil {
			return nil, errors.New("Not Set")
		}
		if parcache[id].NextRefreshTime.Before(time.Now()) {
			return parcache[id].Locations, errors.New("Expired")
		}
		return parcache[id].Locations, nil
	}
	return nil, errors.New("Not Found")
}
func (vc *ParidCache) Set(collection, vid string, locations []Location, duration time.Duration) {
	vc.Lock()
	defer vc.Unlock()
	if len(locations) <= 1 {
		duration = 1 * time.Second
	}
	parcache, ok := vc.cache[collection]
	if !ok {
		vc.cache[collection] = make([]ParidInfo, 16)
		parcache, ok = vc.cache[collection]
	}
	id, _ := strconv.Atoi(vid)
	if id >= len(parcache) {
		for i := id - len(parcache); i >= 0; i-- {
			parcache = append(parcache, ParidInfo{})
		}
	}
	parcache[id].Locations = locations
	parcache[id].NextRefreshTime = time.Now().Add(duration)
	vc.cache[collection] = parcache
	glog.V(1).Infoln("set collection:",collection,"id:",id," duration:",duration," len:",len(locations))
}
func (vc *ParidCache) Invalidate(node string) {
	vc.Lock()
	defer vc.Unlock()
	m := make(map[string][]ParidInfo)
	for col, colCache := range vc.cache {
		newColCache := make([]ParidInfo, 0)
		for pid, pif := range colCache {
			for _, loc := range pif.Locations {
				if loc.Url == node {
					glog.V(0).Infoln("clean partition cache,node:",node,"pid:",pid)
					pif.Locations = nil
					break
				}
			}
			newColCache = append(newColCache, pif)
		}
		m[col] = newColCache
	}
	vc.cache = m
}
