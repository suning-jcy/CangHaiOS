package stats

import (
	"time"
)

type ServerStats struct {
	Requests       *DurationCounter
	Connections    *DurationCounter
	AssignRequests *DurationCounter
	ReadRequests   *DurationCounter
	WriteRequests  *DurationCounter
	DeleteRequests *DurationCounter
	BytesIn        *DurationCounter
	BytesOut       *DurationCounter
}

type Channels struct {
	Connections    chan *TimedValue
	Requests       chan *TimedValue
	AssignRequests chan *TimedValue
	ReadRequests   chan *TimedValue
	WriteRequests  chan *TimedValue
	DeleteRequests chan *TimedValue
	BytesIn        chan *TimedValue
	BytesOut       chan *TimedValue
}

var (
	Chan *Channels
)

func init() {
	Chan = &Channels{
		Connections:    make(chan *TimedValue, 100),
		Requests:       make(chan *TimedValue, 100),
		AssignRequests: make(chan *TimedValue, 100),
		ReadRequests:   make(chan *TimedValue, 100),
		WriteRequests:  make(chan *TimedValue, 100),
		DeleteRequests: make(chan *TimedValue, 100),
		BytesIn:        make(chan *TimedValue, 100),
		BytesOut:       make(chan *TimedValue, 100),
	}
}

func NewServerStats() *ServerStats {
	return &ServerStats{
		Requests:       NewDurationCounter(),
		Connections:    NewDurationCounter(),
		AssignRequests: NewDurationCounter(),
		ReadRequests:   NewDurationCounter(),
		WriteRequests:  NewDurationCounter(),
		DeleteRequests: NewDurationCounter(),
		BytesIn:        NewDurationCounter(),
		BytesOut:       NewDurationCounter(),
	}
}

func ConnectionOpen() {
	Chan.Connections <- NewTimedValue(time.Now(), 1)
}
func ConnectionClose() {
	Chan.Connections <- NewTimedValue(time.Now(), -1)
}
func RequestOpen() {
	Chan.Requests <- NewTimedValue(time.Now(), 1)
}
func RequestClose() {
	Chan.Requests <- NewTimedValue(time.Now(), -1)
}
func AssignRequest() {
	Chan.AssignRequests <- NewTimedValue(time.Now(), 1)
}
func ReadRequest() {
	Chan.ReadRequests <- NewTimedValue(time.Now(), 1)
}
func WriteRequest() {
	Chan.WriteRequests <- NewTimedValue(time.Now(), 1)
}
func DeleteRequest() {
	Chan.DeleteRequests <- NewTimedValue(time.Now(), 1)
}
func BytesIn(val int64) {
	Chan.BytesIn <- NewTimedValue(time.Now(), val)
}
func BytesOut(val int64) {
	Chan.BytesOut <- NewTimedValue(time.Now(), val)
}

func (ss *ServerStats) Start() {
	for {
		select {
		case tv := <-Chan.Connections:
			ss.Connections.Add(tv)
		case tv := <-Chan.Requests:
			ss.Requests.Add(tv)
		case tv := <-Chan.AssignRequests:
			ss.AssignRequests.Add(tv)
		case tv := <-Chan.ReadRequests:
			ss.ReadRequests.Add(tv)
		case tv := <-Chan.WriteRequests:
			ss.WriteRequests.Add(tv)
		case tv := <-Chan.ReadRequests:
			ss.ReadRequests.Add(tv)
		case tv := <-Chan.DeleteRequests:
			ss.DeleteRequests.Add(tv)
		case tv := <-Chan.BytesIn:
			ss.BytesIn.Add(tv)
		case tv := <-Chan.BytesOut:
			ss.BytesOut.Add(tv)
		}
	}
}

//binlogserver的时延统计
type BinLogServerDelayStats struct {
	//binlog落盘成功-binlogid时间 时间差
	BinlogWriteRequests *RequestStatic
	//binlog分发成功到-binlogid时间 时间差
	BinlogDispenseRequests *RequestStatic
	//binlog处理成功(收到backupack)到-binlogid时间 时间差
	DataWriteRequests *RequestStatic

	WriteRequestsChan     chan *SimpleValue  //binlog已经扔到待分发同步队列，等待backup回应的个数
	DispenseRequestsChan  chan *SimpleValue  //binlog待分发队列中，待扔到backup的文件的队列长度
	DataWriteRequestsChan chan *SimpleValue  //binlog分发到backup后，等待backup回应的个数
}

type BinLogWarnStats struct {
	//备binlog leader从主集群拉取binlog时，监测到状态异常的次数
	PullBinlogErr *WarnStatic
	//备binlog leader向备backup分发binlog，监测到状态异常的次数
	DispenseBinlogErr *WarnStatic
	//binlog leader启动filer处理线程失败，监测到启动失败的次数
	CreateWorkerErr *WarnStatic

	//binlogserver的非leader从leader拉取
	BinlogWriteErr *WarnStatic

	ServerStatusErr *WarnStatic

	PullBinLogErrChan     chan *SimpleWarn
	DispenseBinLogErrChan chan *SimpleWarn
	CreateWorkerErrChan   chan *SimpleWarn
	BinlogWriteErrChan    chan *SimpleWarn
	ServerStatusErrChan   chan *SimpleWarn

}

func NewBinLogServerDelayStats() *BinLogServerDelayStats {
	return &BinLogServerDelayStats{
		//binlog落盘成功-binlogid时间 时间差
		BinlogWriteRequests: NewRequestStatic(),
		//binlog分发成功到-binlogid时间 时间差
		BinlogDispenseRequests: NewRequestStatic(),
		//binlog处理成功(收到backupack)到-binlogid时间 时间差
		DataWriteRequests: NewRequestStatic(),

		WriteRequestsChan:     make(chan *SimpleValue, 100),
		DispenseRequestsChan:  make(chan *SimpleValue, 100),
		DataWriteRequestsChan: make(chan *SimpleValue, 100),

	}
}

func NewBinLogWarnStats() *BinLogWarnStats {
	return &BinLogWarnStats{
		PullBinlogErr:     NewWarnStatic(),
		DispenseBinlogErr: NewWarnStatic(),
		CreateWorkerErr:   NewWarnStatic(),
		BinlogWriteErr:    NewWarnStatic(),
		ServerStatusErr:   NewWarnStatic(),

		PullBinLogErrChan:     make(chan *SimpleWarn, 100),
		DispenseBinLogErrChan: make(chan *SimpleWarn, 100),
		CreateWorkerErrChan:   make(chan *SimpleWarn, 100),
		BinlogWriteErrChan:    make(chan *SimpleWarn, 100),
		ServerStatusErrChan:   make(chan *SimpleWarn, 100),
	}
}

func (bss *BinLogServerDelayStats) Start() {
	for {
		select {
		case sv := <-bss.WriteRequestsChan:
			bss.BinlogWriteRequests.Add(sv)
		case sv := <-bss.DispenseRequestsChan:
			bss.BinlogDispenseRequests.Add(sv)
		case sv := <-bss.DataWriteRequestsChan:
			bss.DataWriteRequests.Add(sv)
		}
	}
}

func (bss *BinLogWarnStats) Start() {
	for {
		select {
		case sv := <-bss.PullBinLogErrChan:
			bss.PullBinlogErr.Add(sv)
		case sv := <-bss.DispenseBinLogErrChan:
			bss.DispenseBinlogErr.Add(sv)
		case sv := <-bss.CreateWorkerErrChan:
			bss.CreateWorkerErr.Add(sv)
		case sv := <-bss.BinlogWriteErrChan:
			bss.BinlogWriteErr.Add(sv)
		case sv := <-bss.ServerStatusErrChan:
			bss.ServerStatusErr.Add(sv)
		}
	}
}

func (bss *BinLogServerDelayStats) ReInit() {
	bss.BinlogWriteRequests.ReInit()
	bss.BinlogDispenseRequests.ReInit()
	bss.DataWriteRequests.ReInit()
}

func (bss *BinLogWarnStats) ReInit() {
	bss.PullBinlogErr.ReInit()
	bss.DispenseBinlogErr.ReInit()
	bss.CreateWorkerErr.ReInit()
	bss.BinlogWriteErr.ReInit()
	bss.ServerStatusErr.ReInit()
}

type BinlogStatsCollection struct {
	BinlogWriteRequests   *RequestStaticInfo
	BinlogDispenseReuests *RequestStaticInfo
	DataWriteRequests     *RequestStaticInfo
}

type BinlogWarnCollection struct {
	PullBinlogErr         *WarnStatic
	DispenseBinlogErr     *WarnStatic
	CreateWorkerErr       *WarnStatic
	BinlogWriteErr        *WarnStatic
	ServerStatusErr       *WarnStatic
}
func (bss *BinLogServerDelayStats) Stats() *BinlogStatsCollection {
	sc := &BinlogStatsCollection{
		BinlogWriteRequests:   bss.BinlogWriteRequests.GetRequests(),
		BinlogDispenseReuests: bss.BinlogDispenseRequests.GetRequests(),
		DataWriteRequests:     bss.DataWriteRequests.GetRequests(),
	}
	return sc
}

func (bss *BinLogWarnStats) Stats() *BinlogWarnCollection {
	sc := &BinlogWarnCollection{
		PullBinlogErr:         bss.PullBinlogErr.GetRequests(),
		DispenseBinlogErr:     bss.DispenseBinlogErr.GetRequests(),
		CreateWorkerErr:       bss.CreateWorkerErr.GetRequests(),
		BinlogWriteErr:        bss.BinlogWriteErr.GetRequests(),
		ServerStatusErr:       bss.ServerStatusErr.GetRequests(),
	}
	return sc
}

//backup的时延统计
type BackupServerDelayStats struct {
	//统计binlog执行请求过来到请求完成（返回ack）的时延
	BackupWriteRequests   *RequestStatic
	DataWriteRequestsChan chan *SimpleValue
}

func NewBackupServerDelayStats() *BackupServerDelayStats {
	return &BackupServerDelayStats{
		BackupWriteRequests:   NewRequestStatic(),
		DataWriteRequestsChan: make(chan *SimpleValue, 100),
	}
}

func (bs *BackupServerDelayStats) Start() {
	for {
		select {
		case sv := <-bs.DataWriteRequestsChan:
			bs.BackupWriteRequests.Add(sv)
		}
	}
}

func (bs *BackupServerDelayStats) ReInit() {
	bs.BackupWriteRequests.ReInit()
}

type BackupStatsCollection struct {
	DataWriteRequests *RequestStaticInfo
}

func (bs *BackupServerDelayStats) Stats() *BackupStatsCollection {
	sc := &BackupStatsCollection{
		DataWriteRequests: bs.BackupWriteRequests.GetRequests(),
	}
	return sc
}
