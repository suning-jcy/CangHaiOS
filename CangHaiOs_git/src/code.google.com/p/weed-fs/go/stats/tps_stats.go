package stats

//FilerTpsStats  VolumeTpsStats RedisTpsStats 实际上都一样。。。能调整下，不过要跟portal一块，后面有空跟portal一块改

type FilerTpsStats struct {
	FilerGet  *TpsStatic
	FilerPost *TpsStatic
}

type VolumeTpsStats struct {
	VolumeGet  *TpsStatic
	VolumePost *TpsStatic
}

type RedisTpsStats struct {
	RedisGet  *TpsStatic
	RedisPost *TpsStatic
}

type RegionTpsStats struct {
	RegionGet  *TpsStatic
	RegionPost *TpsStatic
}

type TpsChannels struct {
	GetTps  chan *TpsValue
	PostTps chan *TpsValue
}

var (
	FilerTpsChan  *TpsChannels
	VolumeTpsChan *TpsChannels
	RedisTpsChan  *TpsChannels
	RegionTpsChan *TpsChannels
)

func (tc *TpsChannels) UnmarshalJSON(b []byte) (err error) {

	return nil
}

func (tc *TpsChannels) MarshalJSON() ([]byte, error) {
	return []byte(`""`), nil
}

func init() {
	FilerTpsChan = &TpsChannels{
		GetTps:  make(chan *TpsValue, 100),
		PostTps: make(chan *TpsValue, 100),
	}

	VolumeTpsChan = &TpsChannels{
		GetTps:  make(chan *TpsValue, 1000),
		PostTps: make(chan *TpsValue, 1000),
	}

	RedisTpsChan = &TpsChannels{
		GetTps:  make(chan *TpsValue, 2000),
		PostTps: make(chan *TpsValue, 2000),
	}

	RegionTpsChan = &TpsChannels{
		GetTps:  make(chan *TpsValue, 1000),
		PostTps: make(chan *TpsValue, 1000),
	}

}

func NewFilerTpsStats() *FilerTpsStats {
	return &FilerTpsStats{
		FilerGet:  NewTpsStatic(),
		FilerPost: NewTpsStatic(),
	}
}

func NewVolumeTpsStats() *VolumeTpsStats {
	return &VolumeTpsStats{
		VolumeGet:  NewTpsStatic(),
		VolumePost: NewTpsStatic(),
	}
}
func NewRedisTpsStats() *RedisTpsStats {
	return &RedisTpsStats{
		RedisGet:  NewTpsStatic(),
		RedisPost: NewTpsStatic(),
	}
}

func NewRegionTpsStats() *RegionTpsStats {
	return &RegionTpsStats{
		RegionGet:  NewTpsStatic(),
		RegionPost: NewTpsStatic(),
	}
}

func FilerTps(action string, status int, bodyLength int64) {
	switch action {
	case "GET":
		FilerTpsChan.GetTps <- NewTpsValue(status, bodyLength)
	case "HEAD":
		FilerTpsChan.GetTps <- NewTpsValue(status, bodyLength)
	case "POST":
		FilerTpsChan.PostTps <- NewTpsValue(status, bodyLength)
	case "PUT":
		FilerTpsChan.PostTps <- NewTpsValue(status, bodyLength)
	}
}

func VolumeTps(action string, status int, bodyLength int64) {
	switch action {
	case "GET":
		VolumeTpsChan.GetTps <- NewTpsValue(status, bodyLength)
	case "POST":
		VolumeTpsChan.PostTps <- NewTpsValue(status, bodyLength)
	}
}

//bodyLength都为0，redis不关是value还有key
func RedisTps(action string, status int, bodyLength int64) {
	switch action {
	case "GET":
		RedisTpsChan.GetTps <- NewTpsValue(status, bodyLength)
	case "POST":
		RedisTpsChan.PostTps <- NewTpsValue(status, bodyLength)
	}
}

func RegionTps(action string, status int, bodyLength int64) {
	switch action {
	case "GET":
		RegionTpsChan.GetTps <- NewTpsValue(status, bodyLength)
	case "POST":
		RegionTpsChan.PostTps <- NewTpsValue(status, bodyLength)
	}
}

func (fts *FilerTpsStats) Start() {
	for {
		select {
		case tv := <-FilerTpsChan.GetTps:
			fts.FilerGet.Add(tv)
		case tv := <-FilerTpsChan.PostTps:
			fts.FilerPost.Add(tv)
		}
	}
}

func (vts *VolumeTpsStats) Start() {
	for {
		select {
		case tv := <-VolumeTpsChan.GetTps:
			vts.VolumeGet.Add(tv)
		case tv := <-VolumeTpsChan.PostTps:
			vts.VolumePost.Add(tv)
		}
	}
}

func (rts *RedisTpsStats) Start() {
	for {
		select {
		case tv := <-RedisTpsChan.GetTps:
			rts.RedisGet.Add(tv)
		case tv := <-RedisTpsChan.PostTps:
			rts.RedisPost.Add(tv)
		}
	}
}

func (rts *RegionTpsStats) Start() {
	for {
		select {
		case tv := <-RegionTpsChan.GetTps:
			rts.RegionGet.Add(tv)
		case tv := <-RegionTpsChan.PostTps:
			rts.RegionPost.Add(tv)
		}
	}
}

func (fts *FilerTpsStats) ReInit() {
	fts.FilerGet.ReInit()
	fts.FilerPost.ReInit()
}

func (vts *VolumeTpsStats) ReInit() {
	vts.VolumeGet.ReInit()
	vts.VolumePost.ReInit()
}
func (rts *RedisTpsStats) ReInit() {
	rts.RedisPost.ReInit()
	rts.RedisGet.ReInit()
}

func (rts *RegionTpsStats) ReInit() {
	rts.RegionGet.ReInit()
	rts.RegionPost.ReInit()
}

type BackupServerTpsStats struct {
	BackupGet     *TpsStatic
	BackupTpsChan *TpsChannels
}

func NewBackupServerTpsStats() *BackupServerTpsStats {
	return &BackupServerTpsStats{
		BackupGet: NewTpsStatic(),
		BackupTpsChan: &TpsChannels{
			GetTps: make(chan *TpsValue, 1000),
		},
	}
}

func (bts *BackupServerTpsStats) Start() {
	for {
		select {
		case tv := <-bts.BackupTpsChan.GetTps:
			bts.BackupGet.Add(tv)
		}
	}
}

func (bts *BackupServerTpsStats) ReInit() {
	bts.BackupGet.ReInit()
}

type BinlogserverTpsStats struct {
	//binlogserver作为备集群节点时，从主集群接收到多少条（落盘）
	BinlogGet *TpsStatic

	//binlogserver作为备集群节点时，从主集群接收到binlog后，被执行的条目数（收到ack）
	BinlogExec *TpsStatic

	//binlog作为备集群节点时，从主集群的获取到的binlog与最后处理的binlog的统计
	BinlogTpsChan *TpsChannels

	//binlogserver作为主集群节点时，从本集群的filer节点的接收到多少（落盘）
	BinlogGetFromFiler *TpsStatic

	//binlogserver作为主集群节点时，从本集群的filer获取到的binlog的条目数
	BinlogTpsFromFilerChan *TpsChannels
}

func NewBinlogserverTpsStats() *BinlogserverTpsStats {
	return &BinlogserverTpsStats{
		BinlogGet:  NewTpsStatic(),
		BinlogExec: NewTpsStatic(),
		BinlogTpsChan: &TpsChannels{
			//被用作binlog的接收统计
			GetTps: make(chan *TpsValue, 5000),
			//被用作binlog的执行统计
			PostTps: make(chan *TpsValue, 5000),
		},
		BinlogGetFromFiler:NewTpsStatic(),
		BinlogTpsFromFilerChan: &TpsChannels{
			GetTps: make(chan *TpsValue, 5000),
		},
	}
}

func (bts *BinlogserverTpsStats) Start() {
	for {
		select {
		case tv := <-bts.BinlogTpsChan.GetTps:
			bts.BinlogGet.Tps_20X += tv.bodyLength

		case tv := <-bts.BinlogTpsChan.PostTps:
			bts.BinlogExec.Add(tv)

		case tv := <-bts.BinlogTpsFromFilerChan.GetTps:
			bts.BinlogGetFromFiler.Tps_20X += tv.bodyLength
		}
	}
}

func (bts *BinlogserverTpsStats) ReInit() {
	bts.BinlogGet.ReInit()

	bts.BinlogExec.ReInit()

	bts.BinlogGetFromFiler.ReInit()
}
