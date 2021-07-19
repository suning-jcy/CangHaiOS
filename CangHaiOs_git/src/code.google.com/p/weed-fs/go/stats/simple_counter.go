package stats

import (
	"code.google.com/p/weed-fs/go/glog"
	"sync"
)

const (
	INTMAX = 9223372036854775807
)

type SimpleValue struct {
	val   int
	delay int64
}

func NewSimpleValue(val int, delay int64) *SimpleValue {
	return &SimpleValue{delay: delay, val: val}
}

type SimpleWarn struct {
	Ip     string
	Info   string
}

func NewSimpleWarn(ip,info string) *SimpleWarn {
	return &SimpleWarn{Ip:ip,Info: info}
}


type WarnStatic struct {
	WarnInfo map[string]string
	lock sync.RWMutex
}

type RequestStatic struct {
	requeststodo int64
	requests     int64
	delay        []int64
	delayIndex   int
	delayMax     int64
}

type RequestStaticInfo struct {
	Requeststodo int64
	Requests     int64
	DelayAvg     int64
	DelayMax     int64
}

func NewRequestStatic() *RequestStatic {
	return &RequestStatic{requeststodo: int64(0), requests: int64(0), delay: make([]int64, 3), delayIndex: 0, delayMax: int64(0)}
}

func NewWarnStatic() *WarnStatic{
	return &WarnStatic{
		WarnInfo:make(map[string]string),
	}
}

func (rs *WarnStatic) Add(sw *SimpleWarn) {
	rs.lock.Lock()
	rs.WarnInfo[sw.Ip]=sw.Info
	rs.lock.Unlock()
}

func (rs *WarnStatic) ReInit() {
	rs.lock.Lock()
	rs.WarnInfo=make(map[string]string)
	rs.lock.Unlock()
}

func (rs *WarnStatic) GetRequests() *WarnStatic{
	rs.lock.Lock()
	defer rs.lock.Unlock()
	return &WarnStatic{WarnInfo:rs.WarnInfo}
}

func (rs *RequestStatic) Add(sv *SimpleValue) {
	if sv.delay < 0 {
		return
	}
	rs.requeststodo += int64(sv.val)
	if rs.requeststodo < 0 { //队列长度不能为负值
		glog.V(0).Infoln("val:",sv.val,"requeststodo:",rs.requeststodo)
		rs.requeststodo = 0
	}
	if sv.val > 0 {
		//request open
		return
	}
	//request close
	rs.requests++
	if INTMAX-rs.delay[rs.delayIndex] < sv.delay {
		rs.delayIndex++
	}

	rs.delay[rs.delayIndex] += sv.delay

	if rs.delayMax < sv.delay {
		rs.delayMax = sv.delay
	}
}

func (rs *RequestStatic) DelayAvg() (avg int64) {
	if rs.requests <= 0 {
		return
	}
	for i := 0; i <= rs.delayIndex; i++ {
		avg += rs.delay[i] / rs.requests
	}
	return
}

func (rs *RequestStatic) DelayMax() int64 {
	return rs.delayMax
}

func (rs *RequestStatic) RequstsTodo() int64 {
	return rs.requeststodo
}

func (rs *RequestStatic) Requests() int64 {
	return rs.requests
}

func (rs *RequestStatic) ReInit() {
	//rs.requeststodo		= int64(0)
	rs.requests = int64(0)
	rs.delayIndex = 0
	rs.delayMax = int64(0)
	for i := 0; i < len(rs.delay); i++ {
		rs.delay[i] = int64(0)
	}
}

func (rs *RequestStatic) GetRequests() *RequestStaticInfo {
	return &RequestStaticInfo{Requeststodo: rs.RequstsTodo(), Requests: rs.Requests(), DelayAvg: rs.DelayAvg(), DelayMax: rs.DelayMax()}
}
