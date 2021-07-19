package operation

import (
	"errors"
	"sync"
	"time"
)

type SeparationInfo struct {
	Location        string
	NextRefreshTime time.Time
}

type SeparationCache struct {
	cache *SeparationInfo
	sync.RWMutex
}

var (
	sc = SeparationCache{cache: &SeparationInfo{}}
)

func NewSeparationCache() *SeparationCache {
	return &SeparationCache{cache: &SeparationInfo{}}
}

func (sc *SeparationCache) Get() (string, error) {
	sc.RLock()
	defer sc.RUnlock()
	if sc.cache.Location != "" {
		if sc.cache.NextRefreshTime.After(time.Now()) {
			return sc.cache.Location, nil
		} else {
			return sc.cache.Location, errors.New("Expired")
		}
	}
	return "", errors.New("Not Found")
}

func (sc *SeparationCache) Set(loc string, duration time.Duration) {
	cache := SeparationInfo{
		Location:        loc,
		NextRefreshTime: time.Now().Add(duration),
	}
	sc.Lock()
	defer sc.Unlock()
	sc.cache = &cache
}
