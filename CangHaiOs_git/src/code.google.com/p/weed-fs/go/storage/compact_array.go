package storage

import (
	"code.google.com/p/weed-fs/go/glog"
	"github.com/pkg/errors"
	"sync"
)

var BatchCount = 10000
var listCount = 1000
type needleArray struct {
	array []NeedleValue
}

type CompactArray struct {
	arrayList []needleArray
	location  map[Key]int
	count     int
	cmLock    sync.RWMutex
}

func NewCompactArray() CompactArray {
	newCA:=CompactArray{
		arrayList:make([]needleArray,listCount),
		location: make(map[Key]int,BatchCount),
	}
	newCA.arrayList[0].array = make([]NeedleValue,BatchCount)
	return newCA
}

func(cm *CompactArray)idxTrans(location int,add bool)(int,int,error){
	idx := location / BatchCount
	seq := location % BatchCount
	if !add && len(cm.arrayList) <= idx{
		glog.V(0).Infoln("needlemap maybe corrucpt!", idx, len(cm.arrayList))
		return idx,seq,errors.New("needlemap maybe corrucpt")
	}
	return idx,seq,nil
}

func (cm *CompactArray) Set(key Key, offset uint32, size uint32) uint32 {
	ret := uint32(0)
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()
	if v, ok := cm.location[key]; ok {
		idx, seq, err := cm.idxTrans(v,false)
		if err != nil {
			return 0
		}
		p := &cm.arrayList[idx].array[seq]
		p.Offset, p.Size = offset, size
	} else {
		idx, seq ,_ := cm.idxTrans(cm.count,true)
		if len(cm.arrayList) == idx {
			newNA := make([]needleArray, listCount)
			cm.arrayList = append(cm.arrayList, newNA...)
			glog.V(0).Infoln("append a arrayList!",len(cm.arrayList),idx)
		}
		if cm.arrayList[idx].array == nil {
			cm.arrayList[idx].array = make([]NeedleValue, BatchCount)
			glog.V(0).Infoln("append a array!",len(cm.arrayList[idx].array),idx)
		}
		cm.location[key] = cm.count
		p := &cm.arrayList[idx].array[seq]
		p.Key, p.Offset, p.Size = key, offset, size
		cm.count++
		//glog.V(0).Infoln("add a needle! locaton:", cm.count-1, "key:", p.Key, len(cm.arrayList),"size:", p.Size, "idx:", idx, "seq:", seq, "Count:", cm.count)
	}
	return ret
}

func (cm *CompactArray) Delete(key Key) uint32 {
	toDeleteIndex := 0
	ret := uint32(0)
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()
	if _,ok:=cm.location[key]; !ok {
		return ret
	}
	toDeleteIndex = cm.location[key]

	myIdx, mySeq, err := cm.idxTrans(toDeleteIndex,false)
	if err != nil {
		return ret
	}
	idx, seq,err := cm.idxTrans(cm.count - 1,false)
	if err != nil {
		return ret
	}
	myNV := &cm.arrayList[myIdx].array[mySeq]
	ret = cm.arrayList[myIdx].array[mySeq].Size //oldSize
	vlLen := cm.count
	if toDeleteIndex >= 0 && vlLen > 0 && vlLen > toDeleteIndex {
		lastNV := &cm.arrayList[idx].array[seq]
		if toDeleteIndex == vlLen-1 {
		} else if toDeleteIndex == 0 {
			myNV.Key, myNV.Offset, myNV.Size = lastNV.Key, lastNV.Offset, lastNV.Size
			cm.location[myNV.Key] = toDeleteIndex
		} else {
			myNV.Key, myNV.Offset, myNV.Size = lastNV.Key, lastNV.Offset, lastNV.Size
			cm.location[myNV.Key] = toDeleteIndex
		}
		lastNV.Key, lastNV.Size, lastNV.Offset = 0, 0, 0
		delete(cm.location, key)
		cm.count--
		//glog.V(0).Infoln("delete a needle! locaton:", toDeleteIndex, "key:", key, "idx:", myIdx, "seq:", mySeq, "Count:", cm.count)
	}
	return ret
}

func (cm *CompactArray) Get(key Key) (*NeedleValue, bool) {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()
	if v, ok := cm.location[key]; ok {
		idx, seq,err := cm.idxTrans(v,false)
		if err != nil {
			return nil, false
		}
		return &cm.arrayList[idx].array[seq], true
	}
	return nil, false
}

func (cm *CompactArray) Visit(visit func(NeedleValue) error) error {
	newArray:=make([]NeedleValue,cm.count)
	k:=0
	cm.cmLock.RLock()
	for i:=0;i<cm.count;i++ {
		idx,seq,err := cm.idxTrans(i,false)
		if err != nil {
			continue
		}
		newArray[k] = cm.arrayList[idx].array[seq]
		k++
	}
	cm.cmLock.RUnlock()
	for _, nm := range newArray {
		if err := visit(nm); err != nil {
			return err
		}
	}
	return nil
}
