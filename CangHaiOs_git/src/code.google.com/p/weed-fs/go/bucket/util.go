package bucket

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
	"strconv"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/util"
)

func NewReplicaPlacementFromString(id string) (*storage.ReplicaPlacement, error) {
	return storage.NewReplicaPlacementFromString(id)
}

func BADPARIDERR(id string) error {
	return errors.New("BadParID:" + id)
}
func BADREPTYPEERR(rp string) error {
	return errors.New("BadRepType:" + rp)
}

type IBucketUpdator interface {
	Push(string, string, int64, int64)
	Update()
}
type UpdateBuckCache struct {
	acnt        string
	buck        string
	objectCount int
	byteUsed    int64
	reqCnt      int
}
type BucketUpdator struct {
	lock            sync.Mutex
	cache           map[string]*UpdateBuckCache
	needKick        chan bool
	thresholdReqCnt int
	thresholdTime   int
	master          *string
	threshBukCnt    int
}

func NewBuckUpdator(thresholdCnt int, thresholdTime int, threshBukCnt int, master *string) (buckUpdator *BucketUpdator) {
	buckUpdator = &BucketUpdator{}
	buSo := make(map[string]*UpdateBuckCache)
	buckUpdator.cache = buSo
	buckUpdator.needKick = make(chan bool, 1)
	buckUpdator.thresholdReqCnt = thresholdCnt
	buckUpdator.thresholdTime = thresholdTime
	buckUpdator.threshBukCnt = threshBukCnt
	buckUpdator.master = master
	buckUpdator.loop()
	return buckUpdator
}

func (bu *BucketUpdator) Push(acnt string, buck string, byteUsedNew int64, byteUsedOld int64) {
	bu.lock.Lock()
	defer bu.lock.Unlock()
	objCnt := 1
	if byteUsedOld > 0 && byteUsedNew > 0 {
		objCnt = 0
	} else if byteUsedOld > 0 && byteUsedNew == 0 {
		objCnt = -1
	} else if byteUsedOld < 0 || byteUsedNew < 0 {
		return
	}
	byteUsed := byteUsedNew - byteUsedOld
	key := acnt + "/" + buck
	bukUpt, ok := (bu.cache)[key]
	if ok {
		bukUpt.objectCount += objCnt
		bukUpt.byteUsed += byteUsed
		bukUpt.reqCnt += 1
		(bu.cache)[key] = bukUpt
		if bukUpt.reqCnt >= bu.thresholdReqCnt {
			bu.needKick <- true
		}
	} else {
		bukUpt := UpdateBuckCache{
			acnt:        acnt,
			buck:        buck,
			objectCount: objCnt,
			byteUsed:    byteUsed,
			reqCnt:      1}
		(bu.cache)[key] = &bukUpt
		if len(bu.cache) >= bu.threshBukCnt {
			bu.needKick <- true
		}
	}
}

func (bu *BucketUpdator) loop() {
	go func() {
		for {
			select {
			case <-bu.needKick:
				go bu.Update()
			case <-time.After(time.Duration(bu.thresholdTime) * time.Second):
				go bu.Update()
			}
		}
	}()
}
func (bu *BucketUpdator) Update() {
	bu.lock.Lock()
	cache := bu.cache
	bu.cache = make(map[string]*UpdateBuckCache)
	bu.lock.Unlock()
	ret := http.StatusOK
	for _, buk := range cache {
		if buk.objectCount == 0 && buk.byteUsed == 0 {
			continue
		}
		for i := time.Duration(0); i < 3; i++ {
			direct := (ret == http.StatusOK)
			if ret == http.StatusMovedPermanently {
				time.Sleep(i * 5 * time.Second)
			}
			locations, pos, err := operation.PartitionLookup2(*(bu.master), buk.acnt, public.BUCKET, direct,nil)
			if err != nil {
				glog.V(3).Infoln("Statistics: failed to PartitionLookup2:", buk, err)
				break
			}
			path := "/updateObj?"
			path = path + "&" + "account=" + buk.acnt
			path = path + "&" + "bucket=" + buk.buck
			path = path + "&" + "objectNum=" + strconv.Itoa(buk.objectCount)
			path = path + "&" + "objectSize=" + fmt.Sprint(buk.byteUsed)
			path = path + "&" + "parId=" + pos
			ret, _, err = util.MakeRequests(locations.List(), path, "DELETE", nil, nil)
			if err != nil {
				glog.V(0).Infoln("Statistics: failed to MakeReqeusts:", buk, locations.List(), err)
			}
			if ret == http.StatusOK {
				break
			}
		}
	}
}
func ConcatPath(path, key, value string) (newPath string) {
	newPath = path + "&"
	newPath = newPath + key + "=" + value
	return newPath
}
