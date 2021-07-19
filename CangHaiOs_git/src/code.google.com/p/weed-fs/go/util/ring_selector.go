package util

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
	_ "time"
)

type Element struct {
	next, prev       *Element
	elementId        string
	scheduleTotal    int64
	loadBalanceTotal int64
	maxSeekStep      int
	value            interface{}
}
type RingSelector struct {
	name         string
	count        int
	participarts map[string]*Element
	ringHead     *Element
	selector     func(interface{}) bool
	sync.RWMutex
}

func NewRingSelector(name string, selector func(interface{}) bool) *RingSelector {
	rs := &RingSelector{name: name, selector: selector}
	rs.participarts = make(map[string]*Element, 10)
	/*
		go func() {
			for {
				str := ""
				//	for k, v := range rs.participarts {
				rs.RLock()
				p := rs.ringHead
				for p != nil {
					str += "[" + p.elementId + "]"
					str += fmt.Sprintf("schTot=%d,lbTot=%d,msStep=%d ", p.scheduleTotal, p.loadBalanceTotal, p.maxSeekStep)
					value := reflect.ValueOf(p.value).Elem()
					t := value.Type()
					for i := 0; i < value.NumField(); i++ {
						vv := simpleValueToString(value.Field(i))
						if t.Field(i).Name == "RWMutex" {
							continue
						}
						str += fmt.Sprintf("%s=%v ", t.Field(i).Name, vv)

					}
					p = p.next
					if p == rs.ringHead {
						p = nil
					}
				}
				rs.RUnlock()
				fmt.Println("name:", rs.name, "members:", str)
				time.Sleep(1 * time.Second)
			}

		}()
	*/
	return rs
}

func (rs *RingSelector) Get(elementId string) interface{} {
	rs.RLock()
	defer rs.RUnlock()
	ele, ok := rs.participarts[elementId]
	if ok {
		return ele.value
	}
	return nil
}
func (rs *RingSelector) Set(elementId string, value interface{}) interface{} {
	rs.Lock()
	defer rs.Unlock()
	ele, ok := rs.participarts[elementId]
	if ok {
		return ele.value
	}
	ele = &Element{elementId: elementId, value: value}
	if rs.ringHead == nil {
		rs.ringHead = ele
		ele.next = ele
		ele.prev = ele
	} else {
		p := rs.ringHead
		ele.next = p
		ele.prev = p.prev
		p.prev.next = ele
		p.prev = ele
	}
	rs.participarts[elementId] = ele
	rs.count++
	return ele.value

}
func (rs *RingSelector) Remove(elementId string) (value interface{}, err error) {
	rs.Lock()
	defer rs.Unlock()
	ele, ok := rs.participarts[elementId]
	if !ok {
		return nil, errors.New("element not exist")
	}
	if ele.next == ele {
		rs.ringHead = nil
	} else {
		ele.prev.next = ele.next
		ele.next.prev = ele.prev
		if rs.ringHead == ele {
			rs.ringHead = ele.next
		}
	}
	ele.next = nil
	ele.prev = nil
	delete(rs.participarts, elementId)
	rs.count--
	return ele.value, nil

}
func (rs *RingSelector) SelectFromHead(elementId string) string {
	rs.RLock()
	defer rs.RUnlock()
	if rs.count == 0 || rs.ringHead == nil {
		return elementId
	}
	p := rs.ringHead
	for p != nil {
		if rs.selector(p.value) {
			return p.elementId
		}
		p = p.next
		if p == rs.ringHead {
			p = nil
		}
	}
	return elementId
}
func (rs *RingSelector) SelectFromCurrent(elementId string) string {
	rs.RLock()
	defer rs.RUnlock()
	if rs.count == 0 {
		return elementId
	}
	ele, ok := rs.participarts[elementId]
	if !ok {
		return elementId
	}
	ele.scheduleTotal++
	i := 0
	p := ele
	for p != nil {
		if rs.selector(p.value) {
			if p != ele {
				ele.loadBalanceTotal++
				if i > ele.maxSeekStep {
					ele.maxSeekStep = i
				}
			}
			return p.elementId
		}
		p = p.next
		i++
		if p == ele {
			p = nil
		}
	}
	return elementId
}
func (rs *RingSelector) SelectFromRand(elementId string, bl map[string]time.Time) string {
	rs.RLock()
	defer rs.RUnlock()
	if rs.count == 0 {
		return elementId
	}

	ele, ok := rs.participarts[elementId]
	if !ok {
		return elementId
	}
	ele.scheduleTotal++
	i := 0
	p := ele
	for p != nil {
		//判断是否在黑名单中，在就跳过
		etime, ok := bl[p.elementId]
		if ok && etime.Before(time.Now()) {
			delete(bl, p.elementId)
			ok = false
		}

		if !ok && rs.selector(p.value) {
			if p != ele {
				ele.loadBalanceTotal++
				if i > ele.maxSeekStep {
					ele.maxSeekStep = i
				}
			}
			return p.elementId
		}
		p = p.next
		i++
		if p == ele {
			p = nil
		}
	}
	return elementId
}

func (rs *RingSelector) Clean() {
	rs.Lock()
	defer rs.Unlock()
	for _, ele := range rs.participarts {
		ele.scheduleTotal = 0
		ele.loadBalanceTotal = 0
	}
}
func (rs *RingSelector) GetDebugInfo() string {
	str := ""
	rs.RLock()
	str += fmt.Sprintf("tot=%d", rs.count)
	p := rs.ringHead
	for p != nil {
		str += "[" + p.elementId + "]"
		str += fmt.Sprintf("schTot=%d,lbTot=%d,msStep=%d ", p.scheduleTotal, p.loadBalanceTotal, p.maxSeekStep)
		value := reflect.ValueOf(p.value).Elem()
		t := value.Type()
		for i := 0; i < value.NumField(); i++ {
			vv := simpleValueToString(value.Field(i))
			if t.Field(i).Name == "RWMutex" {
				continue
			}
			str += fmt.Sprintf("%s=%v ", t.Field(i).Name, vv)

		}

		p = p.next
		if p == rs.ringHead {
			p = nil
		}
	}
	rs.RUnlock()
	return str
}
