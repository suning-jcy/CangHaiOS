package util

import (
	"time"
)

type ChanQueue struct {
	timeout int
	ch      chan byte
	length  int
}

func NewChanQueue(cp int, to int) *ChanQueue {
	if cp <= 0 || to < 0 {
		return nil
	}
	cq := &ChanQueue{}
	cq.timeout = to
	cq.length = cp
	cq.ch = make(chan byte, cq.length)
	return cq
}
func (this *ChanQueue) Wait() bool {
	if this.timeout > 0 {
		select {
		case this.ch <- 'c':
			return true
		case <-time.After(time.Duration(this.timeout) * time.Second):
			return false
		}
	} else {
		this.ch <- 'c'
		return true
	}
}
func (this *ChanQueue) Done() {
	<-this.ch
}
func (this *ChanQueue) Length() int{
	return len(this.ch)
}
func (this *ChanQueue) Cap() int{
	return cap(this.ch)
}
