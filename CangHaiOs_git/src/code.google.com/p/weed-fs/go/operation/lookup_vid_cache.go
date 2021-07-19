package operation

import (
	"errors"
	"strings"
	"sync"
	"time"

	"code.google.com/p/weed-fs/go/glog"
)

type VidInfo struct {
	Locations       []Location
	NextRefreshTime time.Time
}
type VidCache struct {
	cache []VidInfo
}

type CollectionVolumeLocCache struct {
	colCache map[string]*VidInfo
	sync.RWMutex
}

var (
	vc = CollectionVolumeLocCache{colCache: make(map[string]*VidInfo)}
)

func NewVidCache() *CollectionVolumeLocCache {
	return &CollectionVolumeLocCache{colCache: make(map[string]*VidInfo)}
}
func (vc *CollectionVolumeLocCache) Get(collection, vid string) ([]Location, error) {
	key := collection + "_" + vid
	vc.RLock()
	defer vc.RUnlock()
	if cac, ok := vc.colCache[key]; ok {
		if cac.NextRefreshTime.After(time.Now()) {
			return cac.Locations, nil
		} else {
			return cac.Locations, errors.New("Expired")
		}
	}
	return nil, errors.New("Not Found")
}

func (vc *CollectionVolumeLocCache) GetById(vid string) ([]Location, error) {
	vc.RLock()
	defer vc.RUnlock()
	for k,v:=range vc.colCache{
		myid:=strings.Split(k,"_")[1]
		if myid == vid {
			return v.Locations,nil
		}
	}
	return nil, errors.New("Not Found")
}

func (vc *CollectionVolumeLocCache) Set(collection, vid string, locations []Location, duration time.Duration) {
	key := collection + "_" + vid
	cache := VidInfo{Locations: locations,
		NextRefreshTime: time.Now().Add(duration)}
	vc.Lock()
	defer vc.Unlock()
	vc.colCache[key] = &cache
}

func (vc *CollectionVolumeLocCache) Invalidate(node string) {
	vc.Lock()
	defer vc.Unlock()
	glog.V(0).Infoln("need to clean cache of node:", node)
	for colvid, colvidCach := range vc.colCache {
		for _, loc := range colvidCach.Locations {
			if strings.Contains(loc.Url, node) {
				glog.V(0).Infoln("clean cache of node:", node,"success")
				delete(vc.colCache, colvid)
				break
			}
		}
	}
}

func (vc *CollectionVolumeLocCache) InvalidateById(vid string) {
	vc.Lock()
	defer vc.Unlock()
	glog.V(0).Infoln("need to clean cache of vid:", vid)
	for colvid, _ := range vc.colCache {
		myid:=strings.Split(colvid,"_")[1]
		if myid == vid {
			delete(vc.colCache,colvid)
			glog.V(0).Infoln("clean cache of vid:", vid,"success")
			return
		}
	}
}
