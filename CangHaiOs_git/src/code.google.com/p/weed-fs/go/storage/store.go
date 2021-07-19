package storage

import (
	"code.google.com/p/weed-fs/go/public"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/stats"
	"code.google.com/p/weed-fs/go/util"
	"path"
	"sort"
)

type DiskLocation struct {
	Directory      string
	MaxVolumeCount int
	readOnly       bool
	OfflineStatus  bool
	volumes        map[VolumeId]*Volume  //当前从磁盘中加载到的卷列表
	NotJoin        bool
	sysOpvolume    map[VolumeId]*Volume  //正在进行迁移/修复的卷.更大的作用是作为标识
	Record         map[VolumeId]VolInfo //该磁盘中应当存有的卷列表[随着磁盘损坏，不是所有卷最后都能加载成功]
	sync.RWMutex
}
type VolInfo struct{
	Vid VolumeId
	SerialNum string
}

//占个坑
//实际卷不存在或者还在数据传输中。
func (disk *DiskLocation) GetSysOpSlot(vid VolumeId,serialNum string, status int,size uint64) *Volume {
	disk.Lock()
	defer disk.Unlock()
	if disk.MaxVolumeCount <= len(disk.volumes)+len(disk.sysOpvolume) {
		return nil
	}
	v := &Volume{
		Id:             vid,
		dir:            disk.Directory,
		volSysOpStatus: NewVolStausSt(vid, "", disk.Directory, nil,size),
		SerialNum:      serialNum,
	}
	v.volSysOpStatus.status = status
	disk.sysOpvolume[vid] = v
	return v
}

func (disk *DiskLocation) SetSysOpSlot(vid VolumeId,serialNum string, status int,size uint64) error {
	disk.Lock()
	defer disk.Unlock()
	v,ok:=disk.volumes[vid]
	if !ok{
		return errors.New("no such volume")
	}
	if serialNum != "" && v.SerialNum != serialNum{
		return errors.New("no such serialNum")
	}
	v.volSysOpStatus.status = status
	v.volSysOpStatus.needSize = size
	disk.sysOpvolume[vid] = v
	return nil
}
//修复/迁移动作结束了，不管是失败了还是成功了，释放这个系统操作槽位.
//这个是正常情况的移除槽位
func (disk *DiskLocation) ReleaseSysOpSlot(vid VolumeId,serialNum string) {
	disk.Lock()
	defer disk.Unlock()
	if v, ok := disk.sysOpvolume[vid]; ok {
		if serialNum != "" && v.SerialNum != serialNum{
			return
		}
		delete(disk.sysOpvolume, vid)
	}
}

//异常情况，强制删除被占用的槽位，这个强制动作由前台显示给予
//会同时清楚一些中间文件
func (disk *DiskLocation) DestroySysOpSlot(vid VolumeId,serialNum string) {
	disk.Lock()
	defer disk.Unlock()
	if v, ok := disk.sysOpvolume[vid]; ok {
		if serialNum != "" && v.SerialNum != serialNum{
			return
		}
		status := v.volSysOpStatus.getStatus()
		//清除reb/rep的中间文件
		if status == VOL_REB {
			if v.volSysOpStatus.rebalancestatus.tempdatFile != nil {
				info, _ := v.volSysOpStatus.rebalancestatus.tempdatFile.Stat()
				v.volSysOpStatus.rebalancestatus.tempdatFile.Close()
				if info != nil {
					os.Remove(path.Join(disk.Directory, info.Name()))
				}
			}
			if v.volSysOpStatus.rebalancestatus.tempidxFile != nil {
				info, _ := v.volSysOpStatus.rebalancestatus.tempidxFile.Stat()
				v.volSysOpStatus.rebalancestatus.tempidxFile.Close()
				if info != nil {
					os.Remove(path.Join(disk.Directory, info.Name()))
				}
			}
		} else if status == VOL_REP {
			if v.volSysOpStatus.repairstatus.tempdatFile != nil {
				info, _ := v.volSysOpStatus.repairstatus.tempdatFile.Stat()
				v.volSysOpStatus.repairstatus.tempdatFile.Close()
				if info != nil {
					os.Remove(path.Join(disk.Directory, info.Name()))
				}
			}
			if v.volSysOpStatus.repairstatus.tempidxFile != nil {
				info, _ := v.volSysOpStatus.repairstatus.tempidxFile.Stat()
				v.volSysOpStatus.repairstatus.tempidxFile.Close()
				if info != nil {
					os.Remove(path.Join(disk.Directory, info.Name()))
				}
			}
		}
		delete(disk.sysOpvolume, vid)
	}

}

//删除已有卷的实时状态
func (disk *DiskLocation) DestroyVolumeSlot(vid VolumeId) {
	disk.Lock()
	defer disk.Unlock()
	if v, ok := disk.volumes[vid]; ok {
		status := v.volSysOpStatus.getStatus()
		//清除reb/rep的中间文件
		if status == VOL_REB {
			if v.volSysOpStatus.rebalancestatus.tempdatFile != nil {
				info, _ := v.volSysOpStatus.rebalancestatus.tempdatFile.Stat()
				v.volSysOpStatus.rebalancestatus.tempdatFile.Close()
				if info != nil {
					os.Remove(path.Join(disk.Directory, info.Name()))
				}
			}
			if v.volSysOpStatus.rebalancestatus.tempidxFile != nil {
				info, _ := v.volSysOpStatus.rebalancestatus.tempidxFile.Stat()
				v.volSysOpStatus.rebalancestatus.tempidxFile.Close()
				if info != nil {
					os.Remove(path.Join(disk.Directory, info.Name()))
				}
			}
		} else if status == VOL_REP {
			if v.volSysOpStatus.repairstatus.tempdatFile != nil {
				info, _ := v.volSysOpStatus.repairstatus.tempdatFile.Stat()
				v.volSysOpStatus.repairstatus.tempdatFile.Close()
				if info != nil {
					os.Remove(path.Join(disk.Directory, info.Name()))
				}
			}
			if v.volSysOpStatus.repairstatus.tempidxFile != nil {
				info, _ := v.volSysOpStatus.repairstatus.tempidxFile.Stat()
				v.volSysOpStatus.repairstatus.tempidxFile.Close()
				if info != nil {
					os.Remove(path.Join(disk.Directory, info.Name()))
				}
			}
		}
		v.volSysOpStatus.reset()
	}

}

func (mn *DiskLocation) Reset() {
}
func (disk *DiskLocation) SetNotJoin() {
	disk.Lock()
	disk.NotJoin = true
	disk.Unlock()
}

func (disk *DiskLocation) IsNotJoin() (notJoin bool) {
	disk.RLock()
	notJoin = disk.NotJoin
	disk.RUnlock()
	return notJoin
}
func (disk *DiskLocation) SetJoin() {
	disk.Lock()
	disk.NotJoin = false
	disk.Unlock()
}

func (disk *DiskLocation) SetAllVolumeReadOnly(value bool) {
	disk.Lock()
	for _, v := range disk.volumes {
		v.SwitchReadOnly(value)
	}
	disk.Unlock()
}

type MasterNodes struct {
	nodes    []string
	lastNode int
	ShieldNode string //被屏蔽的节点
}

func NewMasterNodes(bootstrapNode string) (mn *MasterNodes) {
	mn = &MasterNodes{nodes: []string{bootstrapNode}, lastNode: -1}
	return
}
func (mn *MasterNodes) Reset() {
	glog.V(2).Info("Reset master node :", len(mn.nodes), mn.lastNode)
	/*
		if len(mn.nodes) > 1 && mn.lastNode > 0 {
			mn.lastNode = -mn.lastNode
		}
	*/
	mn.lastNode = -1
	mn.ShieldNode = ""
}

func (mn *MasterNodes) ShieldMasterNode(node string) {
	mn.ShieldNode = node
	if node != "" {
		glog.V(0).Infoln("shield master node:",node)
	}
}

func (mn *MasterNodes) FindMaster() (string, error) {
	if len(mn.nodes) == 0 {
		return "", errors.New("No master node found!")
	}
	if mn.lastNode < 0 || len(mn.nodes) < 2 || mn.ShieldNode != ""{
		for _, m := range mn.nodes {
			if masters, e := operation.ListMasters(m); e == nil {
				if len(masters) == 0 {
					continue
				}
				//mn.nodes = append(mn.nodes,masters...)
				mn.updateMasters(masters)
				lastNode := mn.lastNode
				mn.lastNode = rand.Intn(len(mn.nodes))
				if mn.ShieldNode != "" {
					if mn.ShieldNode == "restore"{
					}else if lastNode >= 0 && lastNode < len(mn.nodes) && mn.nodes[lastNode] != mn.ShieldNode{
						mn.lastNode = lastNode //如果没有命中坏的node，就还用老的node
						glog.V(0).Info("current master node does not changed:", mn.nodes[mn.lastNode]," shielded master node",mn.ShieldNode)
					}else {
						if mn.nodes[mn.lastNode] == mn.ShieldNode {
							mn.lastNode++
							if mn.lastNode >= len(mn.nodes) {
								mn.lastNode = 0
							}
						}
					}
					mn.ShieldNode = ""
				}
				glog.V(0).Infoln("current master node have :", mn.nodes,"current master node is :", mn.nodes[mn.lastNode])
				break
			}
		}
	}
	if mn.lastNode < 0 {
		return "", errors.New("No master node avalable!")
	}
	return mn.nodes[mn.lastNode], nil
}
func (mn *MasterNodes) updateMasters(masters []string) {
	for _, m := range masters {
		if m == "" {
			continue
		}
		isExist := false
		for _, node := range mn.nodes {
			if m == node {
				isExist = true
				break
			}
		}
		if !isExist {
			mn.nodes = append(mn.nodes, m)
		}
	}
}

//hujf
func (store *Store) FindMaster() string {
	if master, err := store.masterNodes.FindMaster(); err == nil {
		return master
	}
	return ""
}
func (store *Store) Reset() {
	store.masterNodes.Reset()
}

type Store struct {
	Port            int
	Ip              string
	PublicUrl       string
	Collection      string
	Locations       []*DiskLocation
	ServerReadOnly	bool
	dataCenter      string //optional informaton, overwriting master setting if exists
	rack            string //optional information, overwriting master setting if exists
	connected       bool
	volumeSizeLimit uint64 //read from the master
	masterNodes     *MasterNodes
	readQueue       chan int
	repRepairStatus RepRepairStaus
	DiskThreshold   uint64
	readQueueLen    int
	migDir          string //For volume migration
	migDate         time.Time

	recordChan      chan struct{}
	ECSliceLimit    int
	SysOpChan       chan struct{}
	SysOpLock sync.RWMutex
	SysOpHis  map[string]*SysOpStatus
	SysOpSequence  uint64

	NodeLock       sync.RWMutex
	NodeList       map[string]bool  //separation leader节点
}

//func NewStore(port int, ip, publicUrl string, dirnames []string, maxVolumeCounts []int) (s *Store){
func NewStore(port int, ip, publicUrl string, dirnames []string, maxVolumeCounts []int, diskThreshold uint64, collection string, readQueueLen int, dirnum int, inxfilenum int) (s *Store) {
	//s = &Store{Port: port, Ip: ip, PublicUrl: publicUrl}
	s = &Store{Port: port, Ip: ip, PublicUrl: publicUrl, DiskThreshold: diskThreshold * 1024 * 1024, Collection: collection, readQueueLen: readQueueLen, ServerReadOnly: false}
	s.Locations = make([]*DiskLocation, 0)
	s.SysOpHis = make(map[string]*SysOpStatus)
	s.SysOpChan = make(chan struct{},1)
	s.recordChan = make(chan struct{},len(dirnames))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	runtimeCnt := make(chan int, dirnum)
	glog.V(0).Infoln("dirnum " + strconv.Itoa(dirnum) + "inxfilenum" + strconv.Itoa(inxfilenum))
	for i := 0; i < len(dirnames); i++ {
		wg.Add(1)
		runtimeCnt <- 1
		glog.V(0).Infoln("cycle the " + strconv.Itoa(i) + "th time")
		go func(s *Store, runtimeCnt chan int, i int, inxfilenum int, mutex *sync.Mutex) {
			defer wg.Done()
			location := &DiskLocation{Directory: dirnames[i], MaxVolumeCount: maxVolumeCounts[i]}
			location.volumes = make(map[VolumeId]*Volume)
			location.sysOpvolume = make(map[VolumeId]*Volume)
			location.Record = make(map[VolumeId]VolInfo)
			location.loadExistingVolumes(inxfilenum,collection)
			mutex.Lock()
			s.Locations = append(s.Locations, location)
			mutex.Unlock()
			<-runtimeCnt
		}(s, runtimeCnt, i, inxfilenum, &mutex)

	}
	wg.Wait()
	s.readQueue = make(chan int, s.readQueueLen)
	go s.LoadDiskRecord()
	go s.DiskRecordUpdateLoop()
	return
}
func (s *Store) AddVolume(volumeListString string, collection string, replicaPlacement string,serialNum string) error {
	rt, e := NewReplicaPlacementFromString(replicaPlacement)
	if e != nil {
		return e
	}
	glog.V(0).Infoln("try to add vol str:",volumeListString,serialNum,collection)
	for _, range_string := range strings.Split(volumeListString, ",") {
		if strings.Index(range_string, "-") < 0 {
			id_string := range_string
			id, err := NewVolumeId(id_string)
			if err != nil {
				return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", id_string)
			}
			e = s.addVolume(VolumeId(id),serialNum ,collection, rt)
		} else {
			pair := strings.Split(range_string, "-")
			start, start_err := strconv.ParseUint(pair[0], 10, 64)
			if start_err != nil {
				return fmt.Errorf("Volume Start Id %s is not a valid unsigned integer!", pair[0])
			}
			end, end_err := strconv.ParseUint(pair[1], 10, 64)
			if end_err != nil {
				return fmt.Errorf("Volume End Id %s is not a valid unsigned integer!", pair[1])
			}
			for id := start; id <= end; id++ {
				if err := s.addVolume(VolumeId(id),serialNum, collection, rt); err != nil {
					e = err
				}
			}
		}
	}
	return e
}
func (s *Store) DeleteCollection(collection string) (e error) {
	for _, location := range s.Locations {
		location.Lock()
		for k, v := range location.volumes {
			if v.Collection == collection {
				e = v.Destroy()
				if e != nil {
					location.Unlock()
					return
				}
				delete(location.volumes, k)
			}
		}
		location.Unlock()
	}
	return
}
func (s *Store) findVolume(vid VolumeId,sn string) *Volume {
	for _, location := range s.Locations {
		location.RLock()

		if v, found := location.volumes[vid]; found {
			if sn != "" && v.SerialNum != sn {
				location.RUnlock()
				continue
			}
			location.RUnlock()
			return v
		}
		location.RUnlock()
	}
	return nil
}
func (s *Store) findSysOpVolume(vid VolumeId,sn string) *Volume {
	for _, location := range s.Locations {
		location.RLock()
		if v, found := location.sysOpvolume[vid]; found {
			if sn != "" && v.SerialNum != sn {
				location.RUnlock()
				continue
			}
			location.RUnlock()
			return v
		}
		location.RUnlock()
	}
	return nil
}

func (s *Store) GetDirLocation(dir string) *DiskLocation {
	for _, v := range s.Locations {
		if v.Directory == dir {
			return v
		}
	}
	return nil
}

func (s *Store) FindLocation(vid VolumeId,sn string) *DiskLocation {
	for _, location := range s.Locations {
		location.RLock()
		if v, found := location.volumes[vid]; found {
			if sn != "" && v.SerialNum != sn {
				location.RUnlock()
				continue
			}
			location.RUnlock()
			return location
		}
		location.RUnlock()
	}
	return nil
}

func (s *Store) findFreeLocation(vid VolumeId) (ret *DiskLocation) {
	max := 0
	for _, location := range s.Locations {
		location.RLock()
		if _,ok:=location.volumes[vid];ok{//如果已经有卷了，就不要再加了
			location.RUnlock()
			continue
		}
		nowcount := len(location.volumes) + len(location.sysOpvolume)
		location.RUnlock()
		currentFreeCount := location.MaxVolumeCount - nowcount
		if currentFreeCount > max {
			max = currentFreeCount
			ret = location
		}
	}
	return ret
}

type LocationInfo struct {
	FreeVolumeCount int
	index int
}

type Nodes []LocationInfo

func (w Nodes) Len() int {
	return len(w)
}

func (w Nodes) Less(i, j int) bool {
	return w[i].FreeVolumeCount > w[j].FreeVolumeCount
}

func (w Nodes) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}

func (s *Store) findFreeLocationAndOccupy(needSize uint64,vid VolumeId,serialNum string, status int) (ret *DiskLocation,v *Volume,err error) {
	var hasFreeSpace bool //是否有空余的磁盘空间
	var hasFreeVolSlot bool //是否有空的卷的槽位
	var hasAvailAbleDisk bool //是否有空余的磁盘 [一个磁盘上只能有一个副本或者一个EC卷的一个切片]
	length := len(s.Locations)
	sortlocations := make(Nodes, length)

	for i := 0; i< length; i++ {
		location := s.Locations[i]
		location.Lock()
		nowcount := len(location.volumes) + len(location.sysOpvolume)
		sortlocations[i].FreeVolumeCount = location.MaxVolumeCount - nowcount
		sortlocations[i].index = i
		location.Unlock()
	}
	sort.Sort(sortlocations)
	for i := 0; i< length; i++ {
		glog.V(4).Infoln("sortlocations[",i,"]:",sortlocations[i].FreeVolumeCount,sortlocations[i].index)
	}

	//for _, location := range s.Locations {
	for i := 0; i < length; i++ {
		location := s.Locations[sortlocations[i].index]

		location.Lock()
		if _, found := location.volumes[vid]; found {
			location.Unlock()
			continue
		}
		if _, found := location.sysOpvolume[vid]; found {
			location.Unlock()
			continue
		}
		hasAvailAbleDisk = true
		if needSize != 0 {
			var diskRemain uint64 = 0
			diskStatus := stats.NewDiskStatus(location.Directory)
			var extraSize uint64 //该磁盘上正在执行的其他修复或者迁移任务需要占用的空间

			for _,v:=range location.sysOpvolume {
				if v.volSysOpStatus.status == VOL_REB || v.volSysOpStatus.status == VOL_REP {
					extraSize += v.volSysOpStatus.needSize
				}
			}
			if diskStatus.Available < diskStatus.Free {
				diskRemain = diskStatus.Available
			} else {
				diskRemain = diskStatus.Free
			}
			hasAvailableSpace := diskRemain > (s.DiskThreshold + needSize + extraSize)
			glog.V(0).Infoln("diskStatus:",diskStatus.Free,diskStatus.Available,s.DiskThreshold,"diskRemain:",diskRemain," Directory:",location.Directory," needSize:",needSize," extraSize:",extraSize," hasAvailableSpace:",hasAvailableSpace,"length:",length,"idx:",i)
			if hasAvailableSpace {
				hasFreeSpace = true //表明曾经有磁盘是有剩余空间的
			}else{
				location.Unlock()
				continue
			}
		}
		nowcount := len(location.volumes) + len(location.sysOpvolume)
		currentFreeCount := location.MaxVolumeCount - nowcount
		if currentFreeCount > 0{
			hasFreeVolSlot = true
			ret = location
			v = &Volume{
				Id:             vid,
				dir:            location.Directory,
				volSysOpStatus: NewVolStausSt(vid, "", location.Directory, nil,needSize),
				SerialNum: serialNum,
			}
			v.volSysOpStatus.status = status
			location.sysOpvolume[vid] = v
		}
		location.Unlock()
		if ret != nil {
			break
		}
	}
	if hasFreeSpace && !hasFreeVolSlot {
		err = errors.New("Has free disk space,but has no free volume slot")
	}
	if !hasFreeSpace {
		err = errors.New("No free disk space")
	}
	if !hasAvailAbleDisk {//一般是磁盘有空间有槽位，但是该磁盘已经有了该卷
		err = errors.New("No available disk")
	}
	return ret,v,err
}

func (s *Store) addVolume(vid VolumeId,serialNum, collection string, replicaPlacement *ReplicaPlacement) error {
	if s.findVolume(vid,serialNum) != nil {
		return fmt.Errorf("Volume Id %d already exists!", vid)
	}
	if location := s.findFreeLocation(vid); location != nil {
		location.Lock()
		defer location.Unlock()
		nowcount := len(location.volumes) + len(location.sysOpvolume)
		if location.MaxVolumeCount - nowcount <= 0{ //并发引起的槽位争抢，概率很低，此时重试即可
			err := errors.New("no free slot")
			glog.V(0).Infoln("no more free slot for this disk,try another disk!")
			return err
		}
		glog.V(0).Infoln("In dir", location.Directory, "adds volume =", vid, ", collection =", collection, ", serialNum =",serialNum,", replicaPlacement =", replicaPlacement)
		if volume, err := NewVolume(location.Directory,collection, serialNum, vid, replicaPlacement); err == nil {
			volume.Fallocate(1, 0, int64(s.volumeSizeLimit))
			location.volumes[vid] = volume
			s.AddAnVolRecord(location.Directory,vid,serialNum)
			return nil
		} else {
			return err
		}
	}
	return fmt.Errorf("No more free space left")
}

func (s *Store) FreezeVolume(volumeIdString,serialNum string) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString,serialNum)
	}
	if v := s.findVolume(vid,serialNum); v != nil {
		if v.readOnly {
			return fmt.Errorf("Volume %s is already read-only", volumeIdString,serialNum)
		}
		return v.freeze()
	}
	return fmt.Errorf("volume id %d is not found during freeze!", vid,serialNum)
}

func (s *Store) ChangeDiskReadOnly(par string, sta bool) error {
	found := 0
	if len(par) <= 0 {
		return fmt.Errorf("partions parameter length  is invalid")
	}

	for _, location := range s.Locations {
		if strings.Contains(location.Directory, par+"/") {
			found = 1
			location.readOnly = sta
			location.Lock()
			for id, v := range location.volumes {
				if v.readOnly == sta {
					glog.V(2).Infoln("Volume", id, "read-only is already ", sta)
				} else {
					v.SwitchReadOnly(sta)
				}
			}
			location.Unlock()
			break
		}
	}
	if found == 0 {
		return fmt.Errorf("partions not found,do not freeze")
	} else {
		return nil
	}
}

func (s *Store) ShowDiskReadOnly(par string) (bool, error) {
	var sta bool
	found := 0
	if len(par) <= 0 {
		return false, fmt.Errorf("partions parameter length  is invalid")
	}

	for _, location := range s.Locations {
		if strings.Contains(location.Directory, par+"/") {
			found = 1
			sta = location.readOnly
			break
		}
	}
	if found == 0 {
		return false, fmt.Errorf("partions not found,do not freeze")
	} else {
		return sta, nil
	}
}

func (s *Store) ChangeVolumeReadOnly(vid,serialNum string, sta bool) error {
	if vid == "" {
		return fmt.Errorf("vid parameter length  is invalid")
	}
	for _, location := range s.Locations {
		location.Lock()
		for _, v := range location.volumes {
			if v.Id.String() == vid && v.SerialNum == serialNum {
				v.SwitchReadOnly(sta)
				location.Unlock()
				return nil
			}
		}
		location.Unlock()
	}
	return fmt.Errorf("partions not found,do not freeze")
}

func (s *Store) ShowVolumeReadOnly(vid,serialNum string) (bool,error){
	readonly:=false
	if vid == "" {
		return readonly,fmt.Errorf("vid parameter length is invalid")
	}
	for _, location := range s.Locations {
		location.Lock()
		for _, v := range location.volumes {
			if v.Id.String() == vid && v.SerialNum == serialNum  {
				readonly=v.readOnly
				location.Unlock()
				return readonly,nil
			}
		}
		location.Unlock()
	}
	return readonly,fmt.Errorf("partions not found,do not freeze")
}


func (l *DiskLocation) loadExistingVolumes(inxfilenum int,collection string)(newVid uint32) {
	length:=0
	if dirs, err := ioutil.ReadDir(l.Directory); err == nil {

		var wg sync.WaitGroup
		runtimeCnt := make(chan int, inxfilenum)
		glog.V(1).Infoln("inxfilenum" + strconv.Itoa(inxfilenum))
		for _, dir := range dirs {

			wg.Add(1)
			runtimeCnt <- 1
			go func(dir os.FileInfo, runtimeCnt chan int) {
				defer func() {
					<-runtimeCnt
					wg.Done()
				}()
				name := dir.Name()
				if !dir.IsDir() && strings.HasSuffix(name, ".dat") {
					coll := ""
					base := name[:len(name)-len(".dat")]
					sn:=""
					i := strings.Index(base, "_")
					vid:=""
					if i > 0 {
						coll, base = base[0:i], base[i+1:]
						snIdx:=strings.Index(base,"_")
						if snIdx > 0{
							sn = strings.Split(base,"_")[1]
							vid = strings.Split(base,"_")[0]
						}else {
							vid = base
						}
					}
					if coll != "" && collection != coll{
						glog.V(0).Infoln("found a bad vid:", name)
						return
					}
 					if vid, err := NewVolumeId(vid); err == nil {
							l.Lock()
							x := l.volumes[vid]
							if x == nil {
								if v, e := NewVolume(l.Directory, coll,sn, vid, nil); e == nil {
									atomic.AddUint32(&newVid,1)
									l.volumes[vid] = v
									glog.V(0).Infoln("data file", l.Directory+"/"+name, "replicaPlacement =", v.ReplicaPlacement, "version =", v.Version(), "size =", v.Size())
								}
							}
							length = len(l.volumes)
							l.Unlock()
					}
				}
			}(dir, runtimeCnt)
		}
		wg.Wait()
	}
	if newVid > 0{
		glog.V(0).Infoln("Store started on dir:", l.Directory, "with", length, "volumes", "max", l.MaxVolumeCount," newFoundedVolume:",newVid)
	}
	return
}
func (s *Store) Status(all bool) []VolumeInfo2 {
	var vStats []VolumeInfo2
	var canWrite bool
	var diskRemain uint64

	for _, location := range s.Locations {
		canWrite = false
		diskStatus := stats.NewDiskStatus(location.Directory)

		if diskStatus.Available < diskStatus.Free {
			diskRemain = diskStatus.Available
		} else {
			diskRemain = diskStatus.Free
		}
		if diskRemain > s.DiskThreshold {
			canWrite = true
		}
		location.RLock()
		for k, v := range location.volumes {
			if v.Collection != s.Collection && all == false{
				continue
			}
			if v.Version() == 0 && all == false{
				continue
			}
			vCanWrite := canWrite && !(v == nil || v.readOnly || !v.volSysOpStatus.canWrite())
			s :=VolumeInfo2{
				VolumeInfo{
					Id:               VolumeId(k),
					SerialNum:        v.SerialNum,
					Size:             uint64(v.Size()),
					Collection:       v.Collection,
					MaxFileKey:       v.nm.MaxFileKey(),
					ReplicaPlacement: v.ReplicaPlacement,
					Version:          v.Version(),
					FileCount:        v.nm.FileCount(),
					DeleteCount:      v.nm.DeletedCount(),
					DeletedByteCount: v.nm.DeletedSize(),
					ReadOnly:         v.readOnly,
					CanWrite:         vCanWrite,
					Mnt:              location.Directory},
					v.Mode,
			}
			vStats = append(vStats, s)
		}
		//Version =0 是一个比较明显的特征用来区分是不是正在进行的系统操作
		for k, v := range location.sysOpvolume {
			s := VolumeInfo2{
				VolumeInfo{
					Id:         k,
					SerialNum:  v.SerialNum,
					Collection: v.Collection,
					ReadOnly:   true,
					CanWrite:   false,
					Mnt:        location.Directory},
					v.Mode,
			}
			vStats = append(vStats, s)
		}
		location.RUnlock()
	}
	return vStats
}
func (s *Store) GetVolumeInfo(vid,serialNum string) *VolumeInfo2 {
	id, err := NewVolumeId(vid)
	if err != nil {
		return nil
	}
	canWrite := false
	var diskRemain uint64
	if v := s.findVolume(id,serialNum); v != nil {
		location := s.GetDirLocation(v.dir)
		diskStatus := stats.NewDiskStatus(location.Directory)

		if diskStatus.Available < diskStatus.Free {
			diskRemain = diskStatus.Available
		} else {
			diskRemain = diskStatus.Free
		}
		if diskRemain > s.DiskThreshold {
			canWrite = true
		}
		canWrite = canWrite && !(v == nil || v.readOnly || !v.volSysOpStatus.canWrite())
		s := VolumeInfo{Id: id,
			Size: uint64(v.Size()),
			Collection: v.Collection,
			MaxFileKey: v.nm.MaxFileKey(),
			ReplicaPlacement: v.ReplicaPlacement,
			Version: v.Version(),
			FileCount: v.nm.FileCount(),
			DeleteCount: v.nm.DeletedCount(),
			DeletedByteCount: v.nm.DeletedSize(),
			ReadOnly: v.readOnly,
			CanWrite: canWrite,
			Mnt:v.dir}
		ss := &VolumeInfo2{
			s,
			 v.Mode,
		}
		return ss
	}
	return nil
}
func (s *Store) DevicesStatus() []stats.DeviceStat {
	var vStats []stats.DeviceStat
	for _, location := range s.Locations {
		location.RLock()
		diskStatus := stats.NewDiskStatus(location.Directory)
		device := stats.DeviceStat{
			All:         diskStatus.All,
			Available:   diskStatus.Available,
			Free:        diskStatus.Free,
			Used:        diskStatus.Used,
			Device:      diskStatus.Dir,
			VolumeCount: len(location.volumes),
			Max:         location.MaxVolumeCount,
		}
		location.RUnlock()
		vStats = append(vStats, device)
	}
	return vStats
}
func (s *Store) SetDataCenter(dataCenter string) {
	s.dataCenter = dataCenter
}
func (s *Store) SetRack(rack string) {
	s.rack = rack
}
func (s *Store) GetRack() string {
	return s.rack
}
func (s *Store) SetBootstrapMaster(bootstrapMaster string) {
	s.masterNodes = NewMasterNodes(bootstrapMaster)
}
func (s *Store) Join() (masterNode string, e error) {
	masterNode, e = s.masterNodes.FindMaster()
	if e != nil {
		glog.V(0).Infoln(e)
		return
	}
	var volumeMessages  []*operation.DiskVolInfoMessage
	maxVolumeCount := 0
	var maxFileKey uint64
	var canWrite bool
	var diskRemain uint64
	var capacityAll, capacityUsed, capacityAvail uint64

	for _, location := range s.Locations {
		if location.OfflineStatus {
			glog.V(4).Infoln("volume disk is offline,not connect master,disk is:", location.Directory)
			continue
		}
		diskInfo:=&operation.DiskVolInfoMessage{
			Path:proto.String(location.Directory),
		}
		var vms []*operation.VolInfoMessage
		canWrite = false
		diskStatus := stats.NewDiskStatus(location.Directory)

		capacityAll += diskStatus.All
		capacityUsed += diskStatus.Used
		capacityAvail += diskStatus.Available
		maxVolumeCount = maxVolumeCount + location.MaxVolumeCount
		if diskStatus.Available < diskStatus.Free {
			diskRemain = diskStatus.Available
		} else {
			diskRemain = diskStatus.Free
		}
		if diskRemain > s.DiskThreshold {
			canWrite = true
		}
		location.RLock()
		for k, v := range location.volumes {
			vCanWrite := canWrite && !(v == nil || v.readOnly || !v.volSysOpStatus.canWrite() || s.ServerReadOnly)
			if v.Collection != s.Collection{
				continue
			}
			volumeMessage := &operation.VolInfoMessage{
				Id:               proto.Uint32(uint32(k)),
				Sn:				  proto.String(v.SerialNum),
				Size:             proto.Uint64(uint64(v.Size())),
				MaxFileKey:       proto.Uint64(v.nm.MaxFileKey()),
				Collection:       proto.String(v.Collection),
				FileCount:        proto.Uint64(uint64(v.nm.FileCount())),
				DeleteCount:      proto.Uint64(uint64(v.nm.DeletedCount())),
				DeletedByteCount: proto.Uint64(v.nm.DeletedSize()),
				ReadOnly:         proto.Bool(v.readOnly),
				//CanWrite:         proto.Bool(canWrite),
				CanWrite:         proto.Bool(vCanWrite),
				ReplicaPlacement: proto.Uint32(uint32(v.ReplicaPlacement.Byte())),
				Version:          proto.Uint32(uint32(v.Version())),
			}
			vms = append(vms, volumeMessage)
			if maxFileKey < v.nm.MaxFileKey() {
				maxFileKey = v.nm.MaxFileKey()
			}
		}
		diskInfo.Volumes = vms
		location.RUnlock()
		volumeMessages=append(volumeMessages,diskInfo)
	}

	joinMessage := &operation.JoinMessage{
		IsInit:         proto.Bool(!s.connected),
		Ip:             proto.String(s.Ip),
		Port:           proto.Uint32(uint32(s.Port)),
		PublicUrl:      proto.String(s.PublicUrl),
		MaxVolumeCount: proto.Uint32(uint32(maxVolumeCount)),
		MaxFileKey:     proto.Uint64(maxFileKey),
		DataCenter:     proto.String(s.dataCenter),
		Rack:           proto.String(s.rack),
		DiskVolumes:    volumeMessages,
		Collection:     proto.String(s.Collection),
		CapacityAll:    proto.Uint64(capacityAll),
		CapacityUsed:   proto.Uint64(capacityUsed),
		CapacityAvail:  proto.Uint64(capacityAvail),
	}

	data, err := proto.Marshal(joinMessage)
	if err != nil {
		glog.V(0).Infoln(err)
		return "", err
	}

	//jsonBlob, err := util.PostBytes("http://"+masterNode+"/dir/join", data)
	jsonBlob, err := util.PostBytes_timeout("http://"+masterNode+"/dir/join", data)
	if err != nil {
		glog.V(0).Infoln(err)
		s.masterNodes.Reset()
		return "", err
	}
	var ret operation.JoinResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		glog.V(0).Infoln(err, string(jsonBlob))
		return masterNode, err
	}
	if ret.Error != "" {
		glog.V(0).Infoln(ret.Error)
		return masterNode, errors.New(ret.Error)
	}
	s.volumeSizeLimit = ret.VolumeSizeLimit
	s.connected = true
	return
}
func (s *Store) Close() {
	for _, location := range s.Locations {
		location.RLock()
		for _, v := range location.volumes {
			v.Close()
		}
		location.RUnlock()
	}
}
func (s *Store) Write(i VolumeId, n *Needle,serialNum string) (size uint32, err error) {
	if v := s.findVolume(i,serialNum); v != nil {
		if v.readOnly {
			err = fmt.Errorf("Volume %d is read only!", i)
			return
		} else {
			if MaxPossibleVolumeSize >= v.ContentSize()+uint64(size) {
				size, err = v.write(n)
				if size < 0 {
					glog.V(0).Infoln("write size", i, size)
				}
			} else {
				err = fmt.Errorf("Volume Size Limit %d Exceeded! Current size is %d", s.volumeSizeLimit, v.ContentSize())
			}
			if s.volumeSizeLimit < v.ContentSize()+3*uint64(size) {
				glog.V(0).Infoln("volume", i, "size", v.ContentSize(), "will exceed limit", s.volumeSizeLimit)
				if _, e := s.Join(); e != nil {
					glog.V(0).Infoln("error when reporting size:", e)
				}
			}
		}
		return
	}
	glog.V(0).Infoln("volume", i, "not found!")
	err = fmt.Errorf("Volume %d not found!", i)
	return
}
func (s *Store) Delete(i VolumeId, n *Needle,serialNum string) (uint32, error) {
	if v := s.findVolume(i,serialNum); v != nil {
		if v.volSysOpStatus.canDelete() == false{
			return 0,errors.New("cannot delete")
		}
		return v.delete(n)
	}
	return 0, nil
}
func (s *Store) Read(i VolumeId, n *Needle,serialNum,reqid string) (int, error) {
	s.readQueue <- 1
	defer func() {
		<-s.readQueue
	}()
	if v := s.findVolume(i,serialNum); v != nil {
		if v.Mode == 2 {
			ips:=make([]string,0)
			s.NodeLock.RLock()
			for k,_:=range s.NodeList{
				ips = append(ips,k)
			}
			s.NodeLock.RUnlock()
			return s.ReadCopyDownNeedle(v,n,serialNum,reqid,ips)
		}
		return v.read(n)
	}
	return 0, fmt.Errorf("Volume %v not found!", i)
}

//读降级后的副本卷上的数据
func (s *Store) ReadCopyDownNeedle(v *Volume, n *Needle,serialNum,reqid string,ip []string) (int, error) {
	offset,size,err:=v.GetInfo(n)
	if err != nil {
		glog.V(0).Infoln("get needle info err:",err)
		return 0,err
	}
	if v.Desc == nil {
		err = errors.New("desc nil")
		glog.V(0).Infoln("desc nil:",err)
		return 0,err
	}
	if v.Desc.Count == 0 && v.Desc.Size > 0{
		glog.V(0).Infoln("invalid desc:",err)
		return 0,err
	}
	firstBlockSize:=v.Desc.ObjectParts[0].Size
	start:=offset/firstBlockSize
	end:=start
	if offset+int64(size) > start * firstBlockSize{
		end++
	}
	ranges := "bytes="+fmt.Sprintf("%d-%d",offset,int64(size)+offset-1)
	data,err := ReadCopyDownVolume(ranges,reqid,v.Desc,ip)
	if err != nil {
		glog.V(0).Infoln("read copy down volume err:",err)
		return 0,err
	}
	if len(data) >= int(size){
		data = data[:size]
	}else{
		glog.V(0).Infoln("want datalen:",size,"but get:",len(data))
		err = errors.New("read err")
		return 0,err
	}
	ret,err := n.ParseData(data)
	if err != nil {
		glog.V(0).Infoln("ParseData data err!",err)
		return 0,err
	}
	return ret, nil
}

func ReadCopyDownVolume(ranges,reqid string,desc *operation.ECMultiPartObject,ip []string)(data []byte,err error){
	data=make([]byte,0)
	rngs, err := operation.ParseRange(ranges, desc.Size)
	if err != nil {
		glog.V(0).Infoln("ParseRange err:",err,ranges,desc.Size)
		return nil,err
	}
	if len(rngs) != 1 {
		err = errors.New("invalid ranges")
		glog.V(0).Infoln("invalid ranges!",rngs,ranges)
		return nil,err
	}
	ra := rngs[0]
	multiPartsRangs, err := desc.Range(ra)
	if err != nil {
		glog.V(0).Infoln("resolve range information error:", err.Error())
		return nil, err
	}
	iplen:=len(ip)
	headers := make(map[string]string)
	headers[public.SDOSS_REQUEST_ID] = reqid
	for _, rng := range multiPartsRangs {
		var tempData []byte
		partNum:=rng.PartNum
		if desc.Count < partNum{
			glog.V(0).Infoln("invalid partNum:",partNum, desc.Count)
			return nil,err
		}
		blockFid:=desc.ObjectParts[partNum].Fid
		rngLen:=rng.Length
		req:="/readECBlock" +"?fileId="+blockFid+"&offset="+fmt.Sprintf("%d",rng.Start)+"&size="+fmt.Sprintf("%d",rngLen)+"&partNum="+fmt.Sprintf("%d",partNum)+"&filename="+desc.Key
		for i:=1;i<operation.MAX_RETRY_NUM;i++{
			idx:=rand.Intn(iplen)
			visitUrl:="http://"+ip[idx]+req
			glog.V(1).Infoln("visitUrl:",visitUrl)
			resp,err1:=util.MakeHttpRequest(visitUrl,"GET",nil,headers,true)
			err = err1
			if err != nil || resp == nil{
				glog.V(0).Infoln("get volume status err!",err,"url:",visitUrl)
				continue
			}else{
				var errMsg string
				defer resp.Body.Close()
				tempData,err = ioutil.ReadAll(resp.Body)
				if err != nil {
					glog.V(0).Infoln("read body err!",err,"url:",visitUrl)
					continue
				}
				if resp.StatusCode == http.StatusOK{
					data = append(data,tempData...)
				}else{
					err = errors.New("read body err")
					_=json.Unmarshal(tempData,&errMsg)
					glog.V(0).Infoln("read body err!",errMsg,"status:",resp.StatusCode,"url:",visitUrl)
					continue
				}
				break
			}
		}
		if err != nil {
			return nil,err
		}
	}
	return
}

func (s *Store) GetVolume(i VolumeId,serialNum string) *Volume {
	return s.findVolume(i,serialNum)
}

func (s *Store) HasVolume(i VolumeId,serialNum string) bool {
	v := s.findVolume(i,serialNum)
	return v != nil
}

func (s *Store) SyncVolumes() {
	for _, location := range s.Locations {
		location.RLock()
		for key, v := range location.volumes {
			if err := v.Sync(); err == nil {
				glog.V(5).Infoln("Sync volume", key, "success")
			} else {
				glog.V(0).Infoln("Sync volume", key, "failed,error", err)
			}
		}
		location.RUnlock()
	}
}



func (s *Store) deleteVolume(vid VolumeId) (e error) {
	for _, location := range s.Locations {
		location.Lock()
		for k, v := range location.volumes {
			if v.Id == vid {
				e = v.Destroy()
				if e != nil {
					location.Unlock()
					return
				}
				delete(location.volumes, k)
			}
		}
		location.Unlock()
	}
	return
}

func (s *Store) Check(i VolumeId, n *Needle,serialNum string) bool {
	if v := s.findVolume(i,serialNum); v != nil {
		return v.check(n)
	}
	return false
}

//
func (s *Store) UnloadDisk(dir string) error {
	for _, l := range s.Locations {
		if strings.TrimSuffix(l.Directory, "/") != strings.TrimSuffix(dir, "/") {
			continue
		}
		l.SetNotJoin()
		time.Sleep(15 * time.Second)
		l.Lock()
		for vid, volume := range l.volumes {
			delete(l.volumes, vid)
			volume.Close()
		}
		l.Unlock()
	}
	return nil
}
//检查有没有
func (s *Store) LoadDiskCheck(dir string) error {
	for _, l := range s.Locations {
		if strings.TrimSuffix(l.Directory, "/") == strings.TrimSuffix(dir, "/") {
			newVidNum:=l.loadExistingVolumes(1,s.Collection)
			if newVidNum > 0 {
				glog.V(0).Infoln("Disk:", dir," find",newVidNum,"new volumes")
				s.Join()
			}
			return nil
		}
	}
	return errors.New("cannot load a not existed disk")
}

func (s *Store) LoadDisk(dir string) error {
	for _, l := range s.Locations {
		if strings.TrimSuffix(l.Directory, "/") == strings.TrimSuffix(dir, "/") {
			if l.IsNotJoin() == false {
				return errors.New("can not reload disk")
			}
			l.loadExistingVolumes(1,s.Collection)
			l.NotJoin = false
			s.Join()
			return nil
		}
	}
	return errors.New("cannot load a not existed disk")
}
func (s *Store) UnloadVolume(vid VolumeId,serialNum string,getAsleep bool) error {
	if v := s.findVolume(vid,serialNum); v != nil {
		v.SetNotJoin()
		s.Join()
		if getAsleep{
			time.Sleep(15 * time.Second)
		}
		for _, location := range s.Locations {
			location.Lock()
			v2, ok := location.volumes[vid]
			if ok && v2.SerialNum == serialNum {
				glog.V(0).Infoln("delete volume:vid:",vid,"dir:",location.Directory,"serialNum:",serialNum)
				delete(location.volumes, vid)
			}
			location.Unlock()
		}
		v.Close()
	} else {
		return errors.New("No volume specified!")
	}
	return nil
}

func (s *Store) LoadVolume(vid VolumeId,serialNum,collection string) (err error) {
	if v := s.findVolume(vid,serialNum); v != nil {
		err = errors.New("can not reload an existing volume")
		return err
	}
	fileName:=""
	for _, l := range s.Locations {
		if collection == "" {
			fileName = path.Join(l.Directory, vid.String()+"_"+serialNum)
		}else if collection != public.EC{
			fileName = path.Join(l.Directory, collection+"_"+vid.String())
		}else{
			fileName = path.Join(l.Directory, collection+"_"+vid.String()+"_"+serialNum)
		}
		_, err = os.Stat(fileName+".dat")
		if err != nil {
			if os.IsNotExist(err) {
				err = nil
				continue
			} else {
				glog.V(0).Infoln("failed to load volume", err)
				return err
			}
		}
		_, err = os.Stat(fileName+".idx")
		if err != nil {
			if os.IsNotExist(err) {
				err = nil
				continue
			} else {
				glog.V(0).Infoln("failed to load volume", err)
				return err
			}
		}
		if v, err := NewVolume(l.Directory, collection,serialNum, vid, nil); err == nil {
			l.Lock()
			l.volumes[vid] = v
			l.Unlock()
			s.Join()
		} else {
			glog.V(0).Infoln("falied to load volume", err)
			return err
		}
	}
	return nil
}
func (s *Store) CanWrite(vid VolumeId,serialNum string) bool {
	v := s.findVolume(vid,serialNum)
	/*
		if v == nil || v.CompactStatus != NotCompact || v.RebalanceStatus != NotRebalance || v.RepairStatus != VOL_REPAIR_NOT_REPAIR || v.readOnly != false {
			return false
		}
	*/
	if v == nil || v.readOnly || !v.volSysOpStatus.canWrite() {
		return false
	}
	return true
}

func (s *Store) ChangeDiskOfflineStatus(par string, sta bool) error {
	if len(par) <= 0 {
		glog.V(0).Infoln("disk offline status not change,partion parameter len is invalid")
		return fmt.Errorf("volume partions %s is invalid!", par)
	}

	for _, location := range s.Locations {
		if strings.Contains(location.Directory, par+"/") {
			location.OfflineStatus = sta
			return nil
		}
	}
	return fmt.Errorf("volume partions %s is not found!", par)
}

func (s *Store) ShowDiskOfflineStatus(par string) (bool, error) {
	var sta bool
	if len(par) <= 0 {
		glog.V(0).Infoln("disk offline status not change,partion parameter len is invalid")
		return false, fmt.Errorf("volume partions %s is invalid!", par)
	}

	for _, location := range s.Locations {
		if strings.Contains(location.Directory, par+"/") {
			sta = location.OfflineStatus
			return sta, nil
		}
	}
	return false, fmt.Errorf("volume partions %s is not found!", par)
}

func (s *Store) AddSysOpStatus(status *SysOpStatus,maxConcurrent int)error {
	s.SysOpLock.Lock()
	defer s.SysOpLock.Unlock()
	if status.Optype == "vacuum" {
		dir:=status.Mnt
		var num = 0
		var disks =make(map[string]bool)
		for _,v:=range s.SysOpHis{//查看当前磁盘上正在vacuum的卷的个数
			disks[v.Mnt]=true
			if v.Optype == "vacuum" && v.Mnt == dir && v.IsOver == false{
				return errors.New("disk is vacuuming")
			}
		}
		//查看当前有多少磁盘在vacuum
		for k,_:=range disks{
			for _,v1:=range s.SysOpHis{
				if v1.Optype == "vacuum" && v1.Mnt == k && v1.IsOver == false{
					num++
					break
				}
			}
		}
		if num >= maxConcurrent {
			glog.V(0).Infoln("num:",num,"maxConcurrent:",maxConcurrent)
			return errors.New("too many disk are vacuuming")
		}
	}

	s.SysOpHis[status.Uuid] = status
	status.TaskSeq=s.SysOpSequence
	s.SysOpSequence++
	for k, v := range s.SysOpHis {
		if v.IsOver && time.Since(v.Endtime) > time.Duration(24*30)*time.Hour {
			delete(s.SysOpHis, k)
		}
	}
	return nil
}
func (s *Store) RecoverVolume(vid,serialNum,collection,uuid string,success,del string) (error) {
	volumeId, err := NewVolumeId(vid)
	if err != nil {
		glog.V(0).Infoln("invalid vid",vid,err)
		return err
	}
	v:=s.GetVolume(volumeId,serialNum)//找到临时卷
	if v == nil {
		err = errors.New("vid not found")
		glog.V(0).Infoln("vid not found! vid:",volumeId,"serialNum:",serialNum,"collection:",collection)
		return err
	}
	tempV:=*v
	tempV.Collection = s.Collection

	oldIdx:=v.FileName()+".idx"
	oldDat:=v.FileName()+".dat"
	newIdx:=tempV.FileName()+".idx"
	newDat:=tempV.FileName()+".dat"
	if oldIdx == newIdx{//本来的临时EC卷提前变成了正式的EC卷，中间可能发生了未知的变化，停止此次操作
		err = errors.New("volume maybe changed")
		glog.V(0).Infoln("volume maybe changed!",oldIdx,newIdx)
		return err
	}
	//检查后台任务信息，如果任务的流水号等信息对应不上，放弃操作
	v = s.findSysOpVolume(volumeId,serialNum)
	if v == nil {
		return fmt.Errorf("Volume Id %s has not been prepared for repairs yet.", vid)
	}
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	v.volSysOpStatus.lastUse=time.Now()
	if v.volSysOpStatus.uuid != uuid {
		return fmt.Errorf("Volume Id %s has different uuid.want:%s,but:%s", vid, v.volSysOpStatus.uuid, uuid)
	}
	if v.volSysOpStatus.getStatus() != VOL_REP {
		return fmt.Errorf("Volume Id %s has different status.want:%d,but:%d", vid, VOL_REP, v.volSysOpStatus.getStatus())
	}
	dirloc := s.GetDirLocation(v.dir)
	if dirloc == nil {
		err = errors.New("dir not found")
		glog.V(0).Infoln("dir not found",volumeId,serialNum,collection,v.dir)
		return err
	}
	defer func() {
		if dirloc != nil {
			dirloc.DestroySysOpSlot(volumeId,serialNum)//移除该卷的后台任务操作信息
		}
	}()
	if err = s.UnloadVolume(volumeId,serialNum,false); err != nil {//卸载临时卷
		glog.V(0).Infoln("UnloadVolume err",err)
		return err
	}
	//如果修复失败了，bak下
	if success == "false" {
		if del == "true" {
			_= os.Remove(oldIdx)
			_= os.Remove(oldDat)
			glog.V(0).Infoln("remove temp files",oldIdx,oldDat)
		}else{
			newIdx = oldIdx+".bak"
			newDat = oldDat+".bak"
			_=os.Rename(oldIdx,newIdx)
			_=os.Rename(oldDat,newDat)
			glog.V(0).Infoln("backup temp files",oldIdx,oldDat)
		}
		return nil
	}
	err = os.Rename(oldIdx,newIdx)
	if err != nil {
		glog.V(0).Infoln("bak old volume idx file err.",err)
		return err
	}

	defer func() {
		if err != nil{
			os.Rename(newIdx,oldIdx)
		}
	}()

	err = os.Rename(oldDat,newDat)
	if err != nil {
		glog.V(0).Infoln("bak old volume data file err.",err)
		return err
	}
	newVid:=dirloc.loadExistingVolumes(1,s.Collection)
	if newVid > 0{
		dirloc.Lock()
		s.AddAnVolRecord(dirloc.Directory,volumeId,serialNum)
		dirloc.Unlock()
	}
	s.Join()
	return nil
}

func (s *Store) ReadVolume(vid string,offset,size int64,typeStr string) (data []byte,err error) {
	volumeId, err := NewVolumeId(vid)
	if err != nil {
		glog.V(0).Infoln("invalid vid",vid,err)
		return nil,err
	}

	v:=s.GetVolume(volumeId,"")
	if v == nil {
		err = errors.New("vid not found")
		glog.V(0).Infoln("vid not found",volumeId)
		return nil,err
	}
	//Idx:=v.FileName()+".idx"
	filename:=v.FileName()
	if typeStr == "idx"{
		filename+=".idx"
	}else if typeStr == "dat"{
		filename+=".dat"
	}else{
		err = errors.New("invalid filetype")
		glog.V(0).Infoln("invalid filetype",typeStr)
		return nil,err
	}

	file,err := os.Open(filename)
	if err != nil {
		glog.V(0).Infoln("Open file err",err)
		return nil,err
	}
	defer file.Close()
	data = make([]byte,size)
	ret,err := file.ReadAt(data,offset)
	glog.V(1).Infoln("read file size:",size,"ret:",ret,"err:",err,"offset:",offset)
	if err != nil && err != io.EOF{
		glog.V(0).Infoln("read file err",err)
		return nil,err
	}
	if err == io.EOF {
		data=data[:ret]
		return data,nil
	}
	data=data[:ret]
	return
}

func (s *Store) StopSysOpTask(vid VolumeId,serialNum,uuid string)(status int,err error) {
	status = http.StatusOK
	v := s.GetVolume(vid,serialNum)
	if v == nil {
		err=errors.New("volume not find")
		glog.V(0).Infoln("volume not find",vid)
		status = http.StatusNotFound
		return
	}
	err=v.volSysOpStatus.tryForceQuit(uuid)
	if err != nil {
		glog.V(0).Infoln("tryForceQuit err:",err,"vid:",vid)
		status = http.StatusBadRequest
		return
	}
	return
}

func (s *Store) AddAnVolRecord(dir string,vid VolumeId,serialNum string) bool {
	var oldRecord map[VolumeId]VolInfo
	for _, v := range s.Locations {
		if vol,ok:=v.Record[vid];ok{
			if vol.SerialNum == serialNum {
				oldRecord = v.Record
				break
			}
		}
	}
	for _, v := range s.Locations {
		if v.Directory == dir {
			if vol,ok:=v.Record[vid];ok{
				if vol.SerialNum == serialNum{
					return true
				}else{
					glog.V(0).Infoln("update a ec volume record from serialNum:"+vol.SerialNum,"to",serialNum,"vid:",vid)
				}
			}else{
				glog.V(0).Infoln("add a volume record.vid:",vid,"serialNum:",serialNum,"to disk:",v.Directory)
			}
			newVol:=VolInfo{
				Vid:vid,
				SerialNum:serialNum,
			}
			if oldRecord != nil {
				delete(oldRecord,vid) //删除旧记录
			}
			v.Record[vid] = newVol
			select {
			case s.recordChan <- struct{}{}:
			case <-time.After(time.Second*time.Duration(3)):
				glog.V(0).Infoln("add a volume record timeout")
			}
			return true
		}
	}
	return false
}

func (s *Store) DeleteAnVolRecord(dir string,vid VolumeId,serialNum string) bool {
	for _, v := range s.Locations {
		if v.Directory == dir {
			if vol,ok:=v.Record[vid];ok{
				if vol.SerialNum == serialNum{
					delete(v.Record,vid)
					select {
					case s.recordChan <- struct{}{}:
						glog.V(0).Infoln("del a volume record.vid:",vid,"serialNum:",serialNum,"to disk:",v.Directory)
					case <-time.After(time.Second*time.Duration(3)):
						glog.V(0).Infoln("del a volume record timeout")
					}
					return true
				}
			}
		}
	}
	return false
}

func (s *Store) LookupDiskRecord(vid VolumeId,serialNum string) (dir string) {
	for _, v := range s.Locations {
		v.RLock()
		if vol,ok:=v.Record[vid];ok{
			if serialNum == ""{
				v.RUnlock()
				return v.Directory
			}
			if vol.SerialNum == serialNum {
				v.RUnlock()
				return v.Directory
			}
		}
		v.RUnlock()
	}
	return ""
}

func (s *Store)ShowDiskRecord(dir string) (vols map[string]struct{}) {
	vols = make(map[string]struct{})
	for _, v := range s.Locations {
		v.RLock()
		if dir != "" {
			if v.Directory != dir {
				v.RUnlock()
				continue
			}
		}
		for _, v1 := range v.Record {
			vol := fmt.Sprintf("%s/%s_%d", v.Directory,s.Collection, v1.Vid)
			if v1.SerialNum != "" {
				vol += fmt.Sprintf("_%s", v1.SerialNum)
			}
			vols[vol] = struct{}{}
		}
		v.RUnlock()
	}
	if dir != "" && len(vols) == 0{
		return nil
	}
	return vols
}

//对比初始磁盘中的卷记录和现在已经加载好的卷个数，找出未能加载的卷
func (s *Store)CheckBadVols(dir string) (vols []string) {
	vols = make([]string,0)
	for _, v := range s.Locations {
		v.RLock()
		if dir != "" {
			if v.Directory != dir {
				v.RUnlock()
				continue
			}
		}
		for _, v1 := range v.Record {
			if v2,ok:=v.volumes[v1.Vid];ok{
				if v2.SerialNum == v1.SerialNum {
					continue
				}
			}
			vol := fmt.Sprintf("%s_%d", v.Directory, v1.Vid)
			if v1.SerialNum != "" {
				vol += fmt.Sprintf("_%s", v1.SerialNum)
			}
			vols=append(vols,vol)
		}
		v.RUnlock()
	}
	return vols
}

func (s *Store)ResetDiskRecord() (err error) {
	var disksVols = make(map[string](map[VolumeId]VolInfo))
	for _, dir := range s.Locations {
		if vols, err := ioutil.ReadDir(dir.Directory); err == nil {
			dirVols:=make(map[VolumeId]VolInfo)
			for _, vol := range vols {
				name := vol.Name()
				if !vol.IsDir() && strings.HasSuffix(name, ".dat") {
					coll := ""
					base := name[:len(name)-len(".dat")]
					sn:=""
					i := strings.Index(base, "_")
					vid:=""
					if i > 0 {
						coll, base = base[0:i], base[i+1:]
						snIdx:=strings.Index(base,"_")
						if snIdx > 0{
							sn = strings.Split(base,"_")[1]
							vid = strings.Split(base,"_")[0]
						}else {
							vid = base
						}
					}
					if coll != "" && s.Collection != coll{
						continue
					}
					if vid, err := NewVolumeId(vid); err == nil {
						v:=VolInfo{
							Vid:vid,
							SerialNum:sn,
						}
						dirVols[vid]=v
					}
				}
			}
			disksVols[dir.Directory] = dirVols
		}else{
			glog.V(0).Infoln("scan disk dir:",dir.Directory,"err:",err,".reset failed!")
			err = errors.New("scan disk failed")
			return err
		}
	}
	data,err:=json.Marshal(disksVols)
	if err != nil {
		glog.V(0).Infoln("ResetDiskRecord Marshal failed!",err)
		return err
	}
	logDir:=glog.GetlogDir()
	err = s.RewriteDiskRecord(logDir,data)
	if err != nil {
		glog.V(0).Infoln("ResetDiskRecord RewriteDiskRecord failed!",err)
		return err
	}
	for _,v:=range s.Locations{
		v.Lock()
		if vols,ok:=disksVols[v.Directory];ok{
			v.Record = vols
			glog.V(0).Infoln("loading disk record.disk:",v.Directory,"vol count:",len(vols))
		}
		v.Unlock()
	}
	return nil
}

func (s *Store)RewriteDiskRecord(logdir string,data []byte) (err error){
	fileName := filepath.Join(logdir, "diskRecord.log")
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		glog.V(0).Infoln("failed to open log file for write", err)
		return err
	}
	defer file.Close()
	_, err = file.Write(data)
	if err != nil {
		glog.V(0).Infoln("failed to write log", err)
		return err
	}
	return nil
}

func (s *Store)LoadDiskRecord(){
	logDir:=glog.GetlogDir()
	fileName := filepath.Join(logDir, "diskRecord.log")
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			err = s.ResetDiskRecord()
			glog.V(0).Infoln("disk record file doest not exists but already reset it,err:", err)
			return
		}
		glog.V(0).Infoln("failed to open disk record file:", err)
		return
	}
	defer file.Close()
	tempBuf:=make([]byte,1024*1024)
	var buf []byte
	for {
		size, err := file.Read(tempBuf)
		if err != nil {
			if err == io.EOF{
				buf=append(buf,tempBuf[:size]...)
				break
			}
			glog.V(0).Infoln("failed to write log", err)
			return
		}
		buf=append(buf,tempBuf[:size]...)
	}
	var disksVols map[string](map[VolumeId]VolInfo)
	err=json.Unmarshal(buf,&disksVols)
	if err != nil {
		glog.V(0).Infoln("LoadDiskRecord Unmarshal err:", err)
		return
	}
	//如果初始磁盘记录比当前配置的磁盘目录个数要少，说明要么是新增磁盘了，要么就是加载失败了。
	if len(disksVols) < len(s.Locations){
		glog.V(0).Infoln("orginal disk record does not equal disk:", err)
		return
	}
	for _,v:=range s.Locations{
		v.Lock()
		if vols,ok:=disksVols[v.Directory];ok{
			v.Record=vols
			glog.V(0).Infoln("loading disk record.disk:",v.Directory,".count:",len(vols))
		}
		v.Unlock()
	}
	var justAdd = false
	badVols := s.CheckBadVols("")
	if len(badVols) == 0{//磁盘记录中的卷在已加载的卷中都能查找到
		for _,v:=range s.Locations{
			v.RLock()
			for k1,v1:=range v.volumes{
				temp,ok:=v.Record[k1]
				if !ok ||(ok == true && temp.SerialNum != v1.SerialNum){
					justAdd = true
					glog.V(0).Infoln("need to update disk record file.add disk:",v.Directory,"vid:",k1,"serialNum:",v1.SerialNum)
				}
			}
			v.RUnlock()
		}
	}
	if justAdd {
		err = s.ResetDiskRecord()
		glog.V(0).Infoln("update disk record file,err:", err)
	}
	return
}

func(s *Store)DiskRecordUpdateLoop() {
	var num int
	var interval = 60//seconds
	for {
		select {
			case <- s.recordChan:
				num++
				continue
			case <-time.After(time.Second*time.Duration(interval)):
				if num <= 0 {
					continue
				}
		}
		num = 0
		var disksVols = make(map[string](map[VolumeId]VolInfo))
		for _, dir := range s.Locations {
			dir.RLock()
			disksVols[dir.Directory]=dir.Record
			dir.RUnlock()
		}
		data,err:=json.Marshal(disksVols)
		if err != nil {
			glog.V(0).Infoln("Marshal failed!",err)
			continue
		}
		logDir:=glog.GetlogDir()
		err=s.RewriteDiskRecord(logDir,data)
		glog.V(0).Infoln("RewriteDiskRecord err:",err)
	}
}