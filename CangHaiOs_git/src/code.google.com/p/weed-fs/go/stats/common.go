package stats

import (
	"time"
)

//backup的tps
var BackupServer_Tps_Stats *BackupServerTpsStats

//backup的平均时延
var BackupServer_Delay_Stats *BackupServerDelayStats

//binlogserver 的tps
var BinlogServer_Tps_Stats *BinlogserverTpsStats

//binlogserver的平均时延
var BinlogServer_Delay_Stats *BinLogServerDelayStats

var BinlogServer_Warn_Stats *BinLogWarnStats

func StatisticsStart(module string) {
	switch module {
	case "binlogserver":
		BinlogServer_Tps_Stats = NewBinlogserverTpsStats()
		go BinlogServer_Tps_Stats.Start()

		BinlogServer_Delay_Stats = NewBinLogServerDelayStats()
		go BinlogServer_Delay_Stats.Start()

		BinlogServer_Warn_Stats = NewBinLogWarnStats()
		go BinlogServer_Warn_Stats.Start()
		break
	case "backupserver":
		BackupServer_Tps_Stats = NewBackupServerTpsStats()
		go BackupServer_Tps_Stats.Start()

		BackupServer_Delay_Stats = NewBackupServerDelayStats()
		go BackupServer_Delay_Stats.Start()
		break
	case "filer":

	case "volume":
	case "folder":
	case "master":
	case "partition":

	}
}

//action  只能是GET、POST
//get 的bodyLength 是接收到的binlog记录条目数
//post的bodyLength是接收到的backup的ack的那条记录的大小
func BinlogserverTps(action string, status int, bodyLength int64) {
	switch action {
	case "GET": //获取到的binlog
		BinlogServer_Tps_Stats.BinlogTpsChan.GetTps <- NewTpsValue(status, bodyLength)
	case "POST": //已经被执行的binlog
		BinlogServer_Tps_Stats.BinlogTpsChan.PostTps <- NewTpsValue(status, bodyLength)

	case "GetFromFiler": //从filer收到了多少条
		BinlogServer_Tps_Stats.BinlogTpsFromFilerChan.GetTps <- NewTpsValue(status, bodyLength)
	}
}

//backup的tps
func BackupTps(action string, status int, bodyLength int64) {
	switch action {
	case "GET":
		BackupServer_Tps_Stats.BackupTpsChan.GetTps <- NewTpsValue(status, bodyLength)
	}
}

//binlogserver的时延统计
func BinlogRequestOpen(action string, count int) {
	switch action {
	case STATS_WRITEBINLOG:
		BinlogServer_Delay_Stats.WriteRequestsChan <- NewSimpleValue(count, int64(0))
	case STATS_DISPENSE:
		BinlogServer_Delay_Stats.DispenseRequestsChan <- NewSimpleValue(count, int64(0))
	case STATS_GETDATA:
		BinlogServer_Delay_Stats.DataWriteRequestsChan <- NewSimpleValue(count, int64(0))
	}
}

func BinlogRequestClose(action string, startTime int64) {
	delay := (time.Now().UnixNano() - startTime) / TIMEDIVISOR
	switch action {
	case STATS_WRITEBINLOG:
		BinlogServer_Delay_Stats.WriteRequestsChan <- NewSimpleValue(-1, delay)
	case STATS_DISPENSE:
		BinlogServer_Delay_Stats.DispenseRequestsChan <- NewSimpleValue(-1, delay)
	case STATS_GETDATA:
		BinlogServer_Delay_Stats.DataWriteRequestsChan <- NewSimpleValue(-1, delay)
	}
}

//backupserver的时延统计
func BackupServerRequestOpen(action string) {
	switch action {
	case STATS_BACKUPDATA:
		BackupServer_Delay_Stats.DataWriteRequestsChan <- NewSimpleValue(1, int64(0))
	}
}

func BackupServerRequestClose(action string, startTime int64) {
	delay := (time.Now().UnixNano() - startTime) / TIMEDIVISOR
	switch action {
	case STATS_BACKUPDATA:
		BackupServer_Delay_Stats.DataWriteRequestsChan <- NewSimpleValue(-1, delay)
	}
}

const (
	PullBinLogErr = "pull bin log err.ip:"  //ip指向拉取哪个filer的binlog失败
	DispenseBinLogErr = "dispense bin log err.ip:"  //ip指向 哪个filer的binlog分发失败
	CreateWorkerErr = "create worker err.ip:"  //ip指向 主binlogserver leader创建哪个filer的工作线程失败

	//此异常是指某个记录在filer侧的binlog文件是在第X行，但是在binlogserver leader刷写时，却准备写往第Y行。
	//这个问题一般是leader的binlog文件很早就没有更新（如最新的binlog序号已经到了100，但是
	//leader的binlog文件序号一直停留在50），此时leader收到了filer的post binlog请求后，只能创建第100个文件，然后继续写，这样刷写时的行数与binlog的应处行数必然不一致
	//出现这种问题的解决方法：
	// 1、查看该leader在哪个集群；
	// 2、然后停止集群内所有binlogserver进程，
	// 3、手动补全每个binlogserver节点binlog目录下异常的binlog文件（如果leader为主集群节点，则与对应filer节点的binlog相比较，如果leader为备集群节点，则与主集群
	// 的binlogserver leader的目录内binlog文件进行比较），保证集群内各个binlogserver下的binlog文件一致。
	// 4、启动所有binlogserver进程

	BinlogCopyErr = "write bin log err.ip:"  //ip指向 binlogserver leader刷写哪个filer的binlog异常.

	RemoteBinlogServerStatusErr = "Get Remote BinlogServer Leader Status Error.ip:" //获取主集群binlogserver leader的状态异常
)

//binlogserver的时延统计
func BinlogWarnInfo(action , ip, info string) {
	switch action {
	case STATS_PULLBINLOGERR:
		BinlogServer_Warn_Stats.PullBinLogErrChan <- NewSimpleWarn(ip,PullBinLogErr+ip+info)
	case STATS_DISPENSEBINLOGERR:
		BinlogServer_Warn_Stats.DispenseBinLogErrChan <- NewSimpleWarn(ip,DispenseBinLogErr+ip+info)
	case STATS_STARTWORKERERR:
		BinlogServer_Warn_Stats.CreateWorkerErrChan <- NewSimpleWarn(ip,CreateWorkerErr+ip+info)
	case STATS_BINLOGWRITEERR:
		BinlogServer_Warn_Stats.BinlogWriteErrChan <- NewSimpleWarn(ip,BinlogCopyErr+ip+info)
	case STATS_REMOTESERVERSTATUSERR:
		BinlogServer_Warn_Stats.ServerStatusErrChan <- NewSimpleWarn(ip,RemoteBinlogServerStatusErr+ip+info)
	}
}