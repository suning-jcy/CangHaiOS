package ossfs

import "sync"

//打开文件的使用统计
type openFilesstatistics struct {
	//写操作有几个，最多有1个
	locwritecount int
	//读操作有几个,
	locreadcount int

	//自己，包括自己的子节点，一共多少个写
	tolwritecount int
	//自己，包括自己的子节点，一共多少个读
	tolreadcount int
	sync.RWMutex
}

func (o *openFilesstatistics) addloc(r, w int) {
	o.Lock()
	defer o.Unlock()
	o.locreadcount += r
	o.locwritecount += w
	o.tolreadcount += r
	o.tolwritecount += w
}

func (o *openFilesstatistics) addtol(r, w int) {
	o.Lock()
	defer o.Unlock()
	o.tolreadcount += r
	o.tolwritecount += w
}

//自己上几个读，几个写，一共几个
func (o *openFilesstatistics) GetLocOpenedCount() (r, w, t int) {
	o.RLock()
	defer o.RUnlock()
	return o.locreadcount, o.locwritecount, o.locreadcount + o.locwritecount
}

//包括子节点一共几个读，几个写，一共几个
func (o *openFilesstatistics) GetTolOpenedCount() (r, w, t int) {
	o.RLock()
	defer o.RUnlock()
	return o.tolreadcount, o.tolwritecount, o.tolreadcount + o.tolwritecount
}

//handle的使用统计
type handlestatistics struct {
	loccount int
	tolcount int
	sync.RWMutex
}

func (h *handlestatistics) addloc(count int) {
	h.Lock()
	defer h.Unlock()
	h.loccount += count
	h.tolcount += count
}

func (h *handlestatistics) getloc() int {
	h.RLock()
	defer h.RUnlock()
	return h.loccount
}

func (h *handlestatistics) addtol(count int) {
	h.Lock()
	defer h.Unlock()
	h.tolcount += count
}

func (h *handlestatistics) gettol() int {
	h.RLock()
	defer h.RUnlock()
	return h.tolcount
}
