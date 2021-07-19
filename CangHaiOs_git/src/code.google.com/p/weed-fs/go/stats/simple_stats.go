package stats

import (
	"time"
)

//同4xx统计一样，接口已经被使用，且字段被固定， 下回抽空调整吧，需要跟portal一块
const (
	STATS_READ   = "READ"
	STATS_WRITE  = "WRITE"
	STATS_DELETE = "DELETE"
	STATS_QUERY  = "QUERY"
)

const (
	STATS_WRITEBINLOG = "WRITEBINLOG"
	STATS_DISPENSE    = "DISPENSE"
	STATS_GETDATA     = "GETDATA"
	STATS_BACKUPDATA  = "BACKUPDATA"
	STATS_PULLBINLOGERR = "PULLBINLOGERR"
	STATS_DISPENSEBINLOGERR = "DISPENSEBINLOGERR"
	STATS_STARTWORKERERR = "STARTWORKERERR"
	STATS_BINLOGWRITEERR = "BINLOGWRITEERR"
	STATS_REMOTESERVERSTATUSERR = "REMOTESERVERSTATUSERR"  //获取主集群的binlogserver leader状态异常
)

const (
	TIMEDIVISOR = 1000
)

type FilerServerStats struct {
	FilerReadRequests   *RequestStatic
	FilerWriteRequests  *RequestStatic
	FilerDeleteRequests *RequestStatic
	FilerQueryRequests  *RequestStatic
}

type BinLogStats struct {
	FilerWriteRequests *RequestStatic
}

type VolumeServerStats struct {
	VolumeReadRequests   *RequestStatic
	VolumeWriteRequests  *RequestStatic
	VolumeDeleteRequests *RequestStatic
	VolumeQueryRequests  *RequestStatic
}

type RedisServerStats struct {
	RedisGetRequests    *RequestStatic
	RedisPutRequests    *RequestStatic
	RedisDeleteRequests *RequestStatic
}

type RegionServerStats struct {
	RegionReadRequests   *RequestStatic
	RegionWriteRequests  *RequestStatic
	RegionDeleteRequests *RequestStatic
	RegionQueryRequests  *RequestStatic
}

type SimpleChannels struct {
	ReadRequests   chan *SimpleValue
	WriteRequests  chan *SimpleValue
	DeleteRequests chan *SimpleValue
	QueryRequests  chan *SimpleValue
}

type BinlogChannels struct {
	WriteRequests chan *SimpleValue
}

var (
	FilerChan  *SimpleChannels
	VolumeChan *SimpleChannels
	BinlogChan *BinlogChannels
	RedisChan  *SimpleChannels
	RegionChan *SimpleChannels
)

func init() {
	FilerChan = &SimpleChannels{
		ReadRequests:   make(chan *SimpleValue, 100),
		WriteRequests:  make(chan *SimpleValue, 100),
		DeleteRequests: make(chan *SimpleValue, 100),
		QueryRequests:  make(chan *SimpleValue, 100),
	}

	VolumeChan = &SimpleChannels{
		ReadRequests:   make(chan *SimpleValue, 100),
		WriteRequests:  make(chan *SimpleValue, 100),
		DeleteRequests: make(chan *SimpleValue, 100),
		QueryRequests:  make(chan *SimpleValue, 100),
	}

	BinlogChan = &BinlogChannels{
		WriteRequests: make(chan *SimpleValue, 100),
	}

	RedisChan = &SimpleChannels{
		ReadRequests:   make(chan *SimpleValue, 1000),
		WriteRequests:  make(chan *SimpleValue, 1000),
		DeleteRequests: make(chan *SimpleValue, 1000),
		QueryRequests:  make(chan *SimpleValue, 1000),
	}
	RegionChan = &SimpleChannels{
		ReadRequests:   make(chan *SimpleValue, 100),
		WriteRequests:  make(chan *SimpleValue, 100),
		DeleteRequests: make(chan *SimpleValue, 100),
		QueryRequests:  make(chan *SimpleValue, 100),
	}

}

func NewFilerServerStats() *FilerServerStats {
	return &FilerServerStats{
		FilerReadRequests:   NewRequestStatic(),
		FilerWriteRequests:  NewRequestStatic(),
		FilerDeleteRequests: NewRequestStatic(),
		FilerQueryRequests:  NewRequestStatic(),
	}
}

func NewBinLogStats() *BinLogStats {
	return &BinLogStats{
		FilerWriteRequests: NewRequestStatic(),
	}
}

func NewVolumeServerStats() *VolumeServerStats {
	return &VolumeServerStats{
		VolumeReadRequests:   NewRequestStatic(),
		VolumeWriteRequests:  NewRequestStatic(),
		VolumeDeleteRequests: NewRequestStatic(),
		VolumeQueryRequests:  NewRequestStatic(),
	}
}
func NewRedisServerStats() *RedisServerStats {
	return &RedisServerStats{
		RedisGetRequests:    NewRequestStatic(),
		RedisPutRequests:    NewRequestStatic(),
		RedisDeleteRequests: NewRequestStatic(),
	}
}

func NewRegionServerStats() *RegionServerStats {
	return &RegionServerStats{
		RegionReadRequests:   NewRequestStatic(),
		RegionWriteRequests:  NewRequestStatic(),
		RegionDeleteRequests: NewRequestStatic(),
		RegionQueryRequests:  NewRequestStatic(),
	}
}

//hujf add the filer and volume requests
//-------------------filer--------------------
func FilerRequestOpen(action string) {
	switch action {
	case STATS_READ:
		FilerChan.ReadRequests <- NewSimpleValue(1, int64(0))
	case STATS_QUERY:
		FilerChan.QueryRequests <- NewSimpleValue(1, int64(0))
	case STATS_DELETE:
		FilerChan.DeleteRequests <- NewSimpleValue(1, int64(0))
	case STATS_WRITE:
		FilerChan.WriteRequests <- NewSimpleValue(1, int64(0))
	}
}

func BinlogOpen(action string) {
	switch action {
	case STATS_WRITE:
		BinlogChan.WriteRequests <- NewSimpleValue(1, int64(0))
	}
}

func RegionRequestOpen(action string) {
	switch action {
	case STATS_READ:
		RegionChan.ReadRequests <- NewSimpleValue(1, int64(0))
	case STATS_QUERY:
		RegionChan.QueryRequests <- NewSimpleValue(1, int64(0))
	case STATS_DELETE:
		RegionChan.DeleteRequests <- NewSimpleValue(1, int64(0))
	case STATS_WRITE:
		RegionChan.WriteRequests <- NewSimpleValue(1, int64(0))
	}
}

func FilerRequestClose(action string, startTime int64) {
	delay := (time.Now().UnixNano() - startTime) / TIMEDIVISOR
	switch action {
	case STATS_READ:
		FilerChan.ReadRequests <- NewSimpleValue(-1, delay)
	case STATS_QUERY:
		FilerChan.QueryRequests <- NewSimpleValue(-1, delay)
	case STATS_DELETE:
		FilerChan.DeleteRequests <- NewSimpleValue(-1, delay)
	case STATS_WRITE:
		FilerChan.WriteRequests <- NewSimpleValue(-1, delay)
	}
}

func BinlogClose(action string, startTime int64) {
	delay := (time.Now().UnixNano() - startTime) / TIMEDIVISOR
	switch action {
	case STATS_WRITE:
		BinlogChan.WriteRequests <- NewSimpleValue(-1, delay)
	}
}

func RegionRequestClose(action string, delay int64) {
	switch action {
	case STATS_READ:
		RegionChan.ReadRequests <- NewSimpleValue(-1, delay)
	case STATS_QUERY:
		RegionChan.QueryRequests <- NewSimpleValue(-1, delay)
	case STATS_DELETE:
		RegionChan.DeleteRequests <- NewSimpleValue(-1, delay)
	case STATS_WRITE:
		RegionChan.WriteRequests <- NewSimpleValue(-1, delay)
	}
}

/*
func FilerReadRequestOpen(){
	FilerChan.ReadRequests <- NewSimpleValue(1, int64(0))
}

func FilerReadRequestClose(delay int64){
	FilerChan.ReadRequests <- NewSimpleValue(-1, delay)
}

func FilerWriteRequestOpen(){
	FilerChan.WriteRequests <- NewSimpleValue(1, int64(0))
}

func FilerWriteRequestClose(delay int64){
	FilerChan.WriteRequests <- NewSimpleValue(-1, delay)
}

func FilerDeleteRequestOpen(){
	FilerChan.DeleteRequests <- NewSimpleValue(1, int64(0))
}

func FilerDeleteRequestClose(delay int64){
	FilerChan.DeleteRequests <- NewSimpleValue(-1, delay)
}

func FilerQueryRequestOpen(){
	FilerChan.QueryRequests <- NewSimpleValue(1, int64(0))
}

func FilerQueryRequestClose(delay int64){
	FilerChan.QueryRequests <- NewSimpleValue(-1, delay)
}
*/
//-------------------filer--------------------

//-------------------redis--------------------

func RedisRequestOpen(action string) {
	switch action {
	case STATS_READ:
		RedisChan.ReadRequests <- NewSimpleValue(1, int64(0))
	case STATS_QUERY:
		RedisChan.QueryRequests <- NewSimpleValue(1, int64(0))
	case STATS_DELETE:
		RedisChan.DeleteRequests <- NewSimpleValue(1, int64(0))
	case STATS_WRITE:
		RedisChan.WriteRequests <- NewSimpleValue(1, int64(0))
	}
}

func RedisRequestClose(action string, startTime int64) {
	delay := (time.Now().UnixNano() - startTime) / TIMEDIVISOR
	switch action {
	case STATS_READ:
		RedisChan.ReadRequests <- NewSimpleValue(-1, delay)
	case STATS_QUERY:
		RedisChan.QueryRequests <- NewSimpleValue(-1, delay)
	case STATS_DELETE:
		RedisChan.DeleteRequests <- NewSimpleValue(-1, delay)
	case STATS_WRITE:
		RedisChan.WriteRequests <- NewSimpleValue(-1, delay)
	}
}

//-------------------volume--------------------

func VolumeRequestOpen(action string) {
	switch action {
	case STATS_READ:
		VolumeChan.ReadRequests <- NewSimpleValue(1, int64(0))
	case STATS_QUERY:
		VolumeChan.QueryRequests <- NewSimpleValue(1, int64(0))
	case STATS_DELETE:
		VolumeChan.DeleteRequests <- NewSimpleValue(1, int64(0))
	case STATS_WRITE:
		VolumeChan.WriteRequests <- NewSimpleValue(1, int64(0))
	}
}

func VolumeRequestClose(action string, startTime int64) {
	delay := (time.Now().UnixNano() - startTime) / TIMEDIVISOR
	switch action {
	case STATS_READ:
		VolumeChan.ReadRequests <- NewSimpleValue(-1, delay)
	case STATS_QUERY:
		VolumeChan.QueryRequests <- NewSimpleValue(-1, delay)
	case STATS_DELETE:
		VolumeChan.DeleteRequests <- NewSimpleValue(-1, delay)
	case STATS_WRITE:
		VolumeChan.WriteRequests <- NewSimpleValue(-1, delay)
	}
}

/*
func VolumeReadRequestOpen(){
	VolumeChan.ReadRequests <- NewSimpleValue(1, int64(0))
}

func VolumeReadRequestClose(delay int64){
	VolumeChan.ReadRequests <- NewSimpleValue(-1, delay)
}

func VolumeWriteRequestOpen(){
	VolumeChan.WriteRequests <- NewSimpleValue(1, int64(0))
}

func VolumeWriteRequestClose(delay int64){
	VolumeChan.WriteRequests <- NewSimpleValue(-1, delay)
}

func VolumeDeleteRequestOpen(){
	VolumeChan.DeleteRequests <- NewSimpleValue(1, int64(0))
}

func VolumeDeleteRequestClose(delay int64){
	VolumeChan.DeleteRequests <- NewSimpleValue(-1, delay)
}

func VolumeQueryRequestOpen(){
	VolumeChan.QueryRequests <- NewSimpleValue(1, int64(0))
}

func VolumeQueryRequestClose(delay int64){
	VolumeChan.QueryRequests <- NewSimpleValue(-1, delay)
}
*/
//-------------------volume--------------------

func (fss *FilerServerStats) Start() {
	for {
		select {
		case sv := <-FilerChan.ReadRequests:
			fss.FilerReadRequests.Add(sv)
		case sv := <-FilerChan.WriteRequests:
			fss.FilerWriteRequests.Add(sv)
		case sv := <-FilerChan.DeleteRequests:
			fss.FilerDeleteRequests.Add(sv)
		case sv := <-FilerChan.QueryRequests:
			fss.FilerQueryRequests.Add(sv)
		}
	}
}

func (bs *BinLogStats) Start() {
	for {
		select {
		case sv := <-BinlogChan.WriteRequests:
			bs.FilerWriteRequests.Add(sv)
		}
	}
}

func (fss *FilerServerStats) GetUpdateRequests() int64 {
	return fss.FilerWriteRequests.RequstsTodo() + fss.FilerDeleteRequests.RequstsTodo()
}
func (vss *VolumeServerStats) Start() {
	for {
		select {
		case sv := <-VolumeChan.ReadRequests:
			vss.VolumeReadRequests.Add(sv)
		case sv := <-VolumeChan.WriteRequests:
			vss.VolumeWriteRequests.Add(sv)
		case sv := <-VolumeChan.DeleteRequests:
			vss.VolumeDeleteRequests.Add(sv)
		case sv := <-VolumeChan.QueryRequests:
			vss.VolumeQueryRequests.Add(sv)
		}
	}
}

func (rss *RedisServerStats) Start() {
	for {
		select {
		case sv := <-RedisChan.ReadRequests:
			rss.RedisGetRequests.Add(sv)
		case sv := <-RedisChan.WriteRequests:
			rss.RedisPutRequests.Add(sv)
		case sv := <-RedisChan.DeleteRequests:
			rss.RedisDeleteRequests.Add(sv)
		case <-RedisChan.QueryRequests:
		}
	}
}

func (rss *RegionServerStats) Start() {
	for {
		select {
		case sv := <-RegionChan.ReadRequests:
			rss.RegionReadRequests.Add(sv)
		case sv := <-RegionChan.WriteRequests:
			rss.RegionWriteRequests.Add(sv)
		case sv := <-RegionChan.DeleteRequests:
			rss.RegionDeleteRequests.Add(sv)
		case sv := <-RegionChan.QueryRequests:
			rss.RegionQueryRequests.Add(sv)
		}
	}
}

func (fss *FilerServerStats) Stats() *StatsCollection {
	sc := &StatsCollection{
		ReadRequests:   fss.FilerReadRequests.GetRequests(),
		WriteReuests:   fss.FilerWriteRequests.GetRequests(),
		DeleteRequests: fss.FilerDeleteRequests.GetRequests(),
		QueryRequests:  fss.FilerQueryRequests.GetRequests(),
	}
	//fmt.Printf("ret" , sc.writeReuests)
	return sc
}

func (vss *VolumeServerStats) Stats() *StatsCollection {
	sc := &StatsCollection{
		ReadRequests:   vss.VolumeReadRequests.GetRequests(),
		WriteReuests:   vss.VolumeWriteRequests.GetRequests(),
		DeleteRequests: vss.VolumeDeleteRequests.GetRequests(),
		QueryRequests:  vss.VolumeQueryRequests.GetRequests(),
	}
	return sc
}

func (vss *RedisServerStats) Stats() *StatsCollection {
	sc := &StatsCollection{
		ReadRequests:   vss.RedisGetRequests.GetRequests(),
		WriteReuests:   vss.RedisPutRequests.GetRequests(),
		DeleteRequests: vss.RedisDeleteRequests.GetRequests(),
	}
	return sc
}

func (bs *BinLogStats) Stats() *StatsCollection {
	sc := &StatsCollection{
		WriteReuests: bs.FilerWriteRequests.GetRequests(),
	}
	return sc
}

func (rss *RegionServerStats) Stats() *StatsCollection {
	sc := &StatsCollection{
		ReadRequests:   rss.RegionReadRequests.GetRequests(),
		WriteReuests:   rss.RegionWriteRequests.GetRequests(),
		DeleteRequests: rss.RegionDeleteRequests.GetRequests(),
		QueryRequests:  rss.RegionQueryRequests.GetRequests(),
	}
	return sc
}

func (fss *FilerServerStats) ReInit() {
	fss.FilerDeleteRequests.ReInit()
	fss.FilerWriteRequests.ReInit()
	fss.FilerReadRequests.ReInit()
	fss.FilerQueryRequests.ReInit()
}

func (vss *VolumeServerStats) ReInit() {
	vss.VolumeDeleteRequests.ReInit()
	vss.VolumeWriteRequests.ReInit()
	vss.VolumeReadRequests.ReInit()
	vss.VolumeQueryRequests.ReInit()
}

func (rss *RedisServerStats) ReInit() {
	rss.RedisDeleteRequests.ReInit()
	rss.RedisPutRequests.ReInit()
	rss.RedisGetRequests.ReInit()
}

func (bs *BinLogStats) ReInit() {
	bs.FilerWriteRequests.ReInit()
}

func (vss *RegionServerStats) ReInit() {
	vss.RegionDeleteRequests.ReInit()
	vss.RegionWriteRequests.ReInit()
	vss.RegionReadRequests.ReInit()
	vss.RegionQueryRequests.ReInit()
}

type StatsCollection struct {
	ReadRequests   *RequestStaticInfo
	WriteReuests   *RequestStaticInfo
	DeleteRequests *RequestStaticInfo
	QueryRequests  *RequestStaticInfo
}
