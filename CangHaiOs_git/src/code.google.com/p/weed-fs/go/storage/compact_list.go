package storage

import (
	"fmt"
	"sort"
	"sync"
)

type compactSection struct {
	values  []NeedleValue
	start   Key
	end     Key
	counter int
}

func newCompactSection(start Key) *compactSection {
	return &compactSection{
		values: make([]NeedleValue, batch),
		start:  start,
	}
}

//return old entry size
func (cs *compactSection) Set(key Key, offset uint32, size uint32) (uint32, bool) {
	ret := uint32(0)
	ok := true
	if cs.counter >= batch {
		return ret, false
	}
	if key > cs.end {
		cs.end = key
	}
	if cs.start == 0 || key < cs.start {
		cs.start = key
	}
	if i := cs.binarySearchValues(key); i >= 0 {
		ret = cs.values[i].Size
		//fmt.Println("key", key, "old size", ret)
		cs.values[i].Offset, cs.values[i].Size = offset, size
	} else {
		p := &cs.values[cs.counter]
		p.Key, p.Offset, p.Size = key, offset, size
		//fmt.Println("added index", cs.counter, "key", key, cs.values[cs.counter].Key)
		cs.counter++
		for j := cs.counter - 1; j > 0; j-- {
			if cs.values[j].Key < cs.values[j-1].Key {
				cs.values[j], cs.values[j-1] = cs.values[j-1], cs.values[j]
			} else {
				break
			}

		}

	}
	return ret, ok
}

//return old entry size
func (cs *compactSection) Delete(key Key) uint32 {
	ret := uint32(0)
	if i := cs.binarySearchValues(key); i >= 0 {
		if cs.values[i].Size > 0 {
			ret = cs.values[i].Size
			cs.values[i].Size = 0
		}
	}
	return ret
}
func (cs *compactSection) Get(key Key) (*NeedleValue, bool) {
	if i := cs.binarySearchValues(key); i >= 0 {
		return &cs.values[i], true
	}
	return nil, false
}
func (cs *compactSection) popLast() (key Key, offset uint32, size uint32) {
	cs.counter--
	cs.end = cs.values[cs.counter-1].Key
	nv := &cs.values[cs.counter]
	key, offset, size = nv.Key, nv.Offset, nv.Size
	nv.Key = 0
	return
}
func (cs *compactSection) binarySearchValues(key Key) int {
	l, h := 0, cs.counter-1
	if h >= 0 && cs.values[h].Key < key {
		return -2
	}
	//println("looking for key", key)
	for l <= h {
		m := (l + h) / 2
		//println("mid", m, "key", cs.values[m].Key, cs.values[m].Offset, cs.values[m].Size)
		if cs.values[m].Key < key {
			l = m + 1
		} else if key < cs.values[m].Key {
			h = m - 1
		} else {
			//println("found", m)
			return m
		}
	}
	return -1
}

//This map assumes mostly inserting increasing keys
type CompactList struct {
	list     []*compactSection
	overflow map[Key]NeedleValue
	cmLock   sync.Mutex
}

func NewCompactList() CompactList {
	return CompactList{
		overflow: make(map[Key]NeedleValue, batch),
	}
}

func (cm *CompactList) Set(key Key, offset uint32, size uint32) uint32 {
	ret := uint32(0)
	ok := true
	err := true
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		//println(x, "creating", len(cm.list), "section, starting", key)
		cm.list = append(cm.list, newCompactSection(0))
		x = len(cm.list) - 1
		if x >= 1 && key < cm.list[x-1].end {
			cm.list[x].Set(cm.list[x-1].popLast())
			ret, err = cm.list[x-1].Set(key, offset, size)
			if err == false {
				fmt.Println("cm place err 1", key)
			}
		} else {
			ret, err = cm.list[x].Set(key, offset, size)
			if err == false {
				fmt.Println("cm place err 2", key)
			}
		}
	} else {
		if ret, ok = cm.list[x].Set(key, offset, size); !ok {
			if x == len(cm.list)-2 && cm.list[x+1].counter < batch {
				if cm.list[x].end < key {
					ret, err = cm.list[x+1].Set(key, offset, size)
					if err == false {
						fmt.Println("cm place err 3", key)
					}
				} else {
					cm.list[x+1].Set(cm.list[x].popLast())
					ret, err = cm.list[x].Set(key, offset, size)
					if err == false {
						fmt.Println("cm place err 4", key)
					}
				}
			} else {
				if oldValue, found := cm.overflow[key]; found {
					ret = oldValue.Size
				}
				cm.overflow[key] = NeedleValue{Key: key, Offset: offset, Size: size}
				fmt.Println("cm.overflow", len(cm.overflow), "key", key)
			}
		}

	}
	/*
		if err == false {
		   fmt.Println("cm place err",key)
		}
	*/
	/*
		x = len(cm.list) - 1
		if x >0 && cm.list[x].start < cm.list[x-1].end {
		  fmt.Println("cm disorder ",cm.list[x].start,cm.list[x-1].end)
		}
	*/
	return ret
}
func (cm *CompactList) Delete(key Key) uint32 {
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()
	x := cm.binarySearchCompactSectionRead(key)
	if x < 0 {
		if v, found := cm.overflow[key]; found {
			delete(cm.overflow, key)
			return v.Size
		}
		return uint32(0)
	}
	return cm.list[x].Delete(key)
}
func (cm *CompactList) Get(key Key) (*NeedleValue, bool) {
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()
	x := cm.binarySearchCompactSectionRead(key)
	if x < 0 {
		if v, found := cm.overflow[key]; found {
			return &v, true
		}
		return nil, false
	}
	return cm.list[x].Get(key)
}
func (cm *CompactList) binarySearchCompactSection(key Key) int {
	l, h := 0, len(cm.list)-1
	if h < 0 {
		return -5
	}
	if cm.list[h].start == key {
		return h
	}
	if cm.list[h].start < key {
		if cm.list[h].counter < batch {
			return h
		} else {
			return -4
		}
	}
	for l <= h {
		m := (l + h) / 2
		if key < cm.list[m].start {
			h = m - 1
		} else { // cm.list[m].start <= key
			if cm.list[m+1].start <= key {
				l = m + 1
			} else {
				return m
			}
		}
	}
	return l
}
func (cm *CompactList) binarySearchCompactSectionRead(key Key) int {
	l, h := 0, len(cm.list)-1
	if h < 0 {
		return -5
	}
	if cm.list[h].start == key {
		return h
	}
	if cm.list[h].start < key {
		if cm.list[h].counter <= batch {
			return h
		} else {
			return -4
		}
	}
	for l <= h {
		m := (l + h) / 2
		if key < cm.list[m].start {
			h = m - 1
		} else { // cm.list[m].start <= key
			if cm.list[m+1].start <= key {
				l = m + 1
			} else {
				return m
			}
		}
	}
	return l
}

type NeedleValueArray []NeedleValue

func (h NeedleValueArray) Len() int           { return len(h) }
func (h NeedleValueArray) Less(i, j int) bool { return h[i].Key < h[j].Key }
func (h NeedleValueArray) Swap(i, j int) {
	h[i].Key, h[j].Key = h[j].Key, h[i].Key
	h[i].Offset, h[j].Offset = h[j].Offset, h[i].Offset
	h[i].Size, h[j].Size = h[j].Size, h[i].Size
}
func (cm *CompactList) Visit(visit func(NeedleValue) error) error {
	cm.cmLock.Lock()
	overflow := make(NeedleValueArray, len(cm.overflow))
	insPos := 0
	for _, v := range cm.overflow {
		overflow[insPos] = v
		insPos++
	}
	cm.cmLock.Unlock()
	sort.Sort(overflow)
	for _, cs := range cm.list {
		for _, v := range cs.values {
			for len(overflow) > 0 && overflow[0].Key < v.Key {
				if err := visit(overflow[0]); err != nil {
					return err
				}
				overflow = overflow[1:]
			}
			if err := visit(v); err != nil {
				return err
			}

		}
	}
	return nil
}
