package storage

import (
	"code.google.com/p/weed-fs/go/public"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"net/http"
)

const (
	REPREPAIR_INIT     = iota
	REPREPAIR_PREPARED
	REPREPAIR_COMMIT   = 3
)

type RepRepairStaus struct {
	status      int
	tempdatFile *os.File
	tempidxFile *os.File
	v           *Volume
}

func (s *Store) GetSysOpStatus(uuid string,isOver string) (interface{}) {
	s.SysOpLock.RLock()
	defer s.SysOpLock.RUnlock()
	if uuid != "" {
		if v, ok := s.SysOpHis[uuid]; ok {
			return v
		}
	}else{
		var status  SysOpStatusSort
		for _,v:=range s.SysOpHis{
			vv:=*v
			if isOver == "false"{
				if vv.IsOver == false {
					status=append(status,vv)
				}
			}else{
				status=append(status,vv)
			}
		}
		sort.Sort(status)
		return  status
	}
	return nil
}

//确定能找到卷，并能进行异步修复之后就返回
func (s *Store) RepairRepVolume(r *http.Request) (error, string) {
	r.ParseForm()
	vidStr := r.FormValue("volumeId")
	dstAddr := r.FormValue("dstNode")
	//dstdir := r.FormValue("dstDir")
	force := r.FormValue("force")
	dests := strings.Split(dstAddr, ",")
	if len(dests) == 0 || dstAddr == "" {
		return errors.New("dstNode can not be null"), ""
	}

	vid, err := NewVolumeId(vidStr)
	if err != nil {
		err = fmt.Errorf("Storage: bad vid %s", vidStr)
		return err, ""
	}

	v := s.findVolume(vid,"")
	if v == nil {
		return ErrVolumeNotFound, ""
	}

	if VolBackOpEnable == false {
		glog.V(0).Infoln("background task is not permitted now,Vid:",vid)
		err = errors.New("Background task is disabled")
		return err,""
	}

	//获取
	if err = v.volSysOpStatus.trySetREP(); err != nil {
		glog.V(0).Infoln("error while trySetREP ,other TRANSFER is runing")
		return err, ""
	}

	v.accessLock.Lock()
	//获取大小
	vinfo, err := v.dataFile.Stat()
	if err != nil {
		v.accessLock.Unlock()
		v.volSysOpStatus.reset()
		glog.V(0).Infoln("get datafile size err:",err)
		return err,""
	}
	dataSize := vinfo.Size()
	v.accessLock.Unlock()

	nowTime := time.Now()
	nowTimeStr := fmt.Sprintf("%d-%d-%d_%d-%d-%d", nowTime.Year(), nowTime.Month(), nowTime.Day(), nowTime.Hour(), nowTime.Minute(), nowTime.Second())
	opstatus := &SysOpStatus{
		Uuid:      fmt.Sprintf("repair_vol_%d_from_%s_time_%s",vid,s.Ip,nowTimeStr),
		Optype:    "repair",
		Result:    false,
		Starttime: nowTime,
		Id:        int(vid),
		Dest:      dstAddr,
		Size:      dataSize,
		Mnt:       v.dir,
	}

	v.volSysOpStatus.uuid = opstatus.Uuid

	s.AddSysOpStatus(opstatus,0)

	go func() {
		var err error
		defer func() {
			//不管执行结果如何，最终要归还占位
			v.volSysOpStatus.reset()
			if err == nil {
				opstatus.Result = true
				glog.V(0).Infoln("do RepairRepVolume success.uuid:", opstatus.Uuid,"Vid:",vid)
			} else {
				glog.V(0).Infoln("do RepairRepVolume error:", err," uuid:", opstatus.Uuid,"Vid:",vid)
				opstatus.Result = false
				opstatus.Err = err.Error()
			}
			opstatus.IsOver = true
			opstatus.Endtime = time.Now()
		}()

		Factors := []*Factor{}
		for _, v := range dests {
			parms := strings.Split(v, "|")
			if len(parms) == 0 {
				continue
			}
			f := &Factor{
				Dest: parms[0],
				Vid:  vidStr,
				Uuid: opstatus.Uuid,
			}
			if len(parms) >= 2 {
				f.DestDir = parms[1]
			}
			Factors = append(Factors, f)
		}
		srcRack:=""
		if v.ReplicaPlacement != nil { //副本模式为00X时，需要目的节点和源节点在同一个机架上 jjj
			rp := v.ReplicaPlacement.String()
			if len(rp) > 2 {
				if rp[0] == '0' && rp[1] == '0'{
					srcRack = s.rack
				}
			}
		}
		//让对端准备
		if err = s.RepRequestPrepare(Factors, force == "true",dataSize,srcRack); err != nil {
			glog.V(0).Infoln("failed to RebRequestPrepare.uuid:", opstatus.Uuid,"Vid:",vid)
			return
		}

		//准备本端的文件传输
		//传输动作要带上uuid
		porter := NewPorter(v.FileName(), v.Id.String(), Factors, nil, opstatus.Uuid, VOL_REP)
		if porter == nil {
			glog.V(0).Infoln("failed to create porter.uuid:", opstatus.Uuid,"Vid:",vid)
			err = errors.New("failed to create porter")
			return
		}

		//等待本地状态上报为不可写，同时等待可能的已经分配过的写进来
		time.Sleep(15 * time.Second)
		if VolBackOpEnable == false {
			glog.V(0).Infoln("background task is not permitted now,uuid",opstatus.Uuid,"Vid:",vid)
			err = errors.New("Background task is disabled")
			return
		}
		//传输数据
		if err = porter.transferVolBase(""); err != nil {
			glog.V(0).Infoln("transferVolBase error:", err,"uuid:", opstatus.Uuid,"Vid:",vid)
			porter.finMigration(false,"")
			return
		}

		time.Sleep(5 * time.Second)
		if VolBackOpEnable == false {
			glog.V(0).Infoln("background task is not permitted now.uuid:",opstatus.Uuid,"Vid:",vid)
			err = errors.New("Background task is disabled")
			return
		}
		//传输中途可能的 被追加到dat/idx 上的文件（理论上都是删除动作）
		if err = porter.transferVolExt(""); err != nil {
			glog.V(0).Infoln("transferVolExt error:", err,"uuid:", opstatus.Uuid,"Vid:",vid)
			porter.finMigration(false,"")
			return
		}

		//最终动作，发送commit_rebfile
		if err = porter.finMigration(true,""); err != nil {
			glog.V(0).Infoln("finMigration error:", err,"uuid:", opstatus.Uuid,"Vid:",vid)
			return
		}
		//join动作会自动进行，不手动执行了

	}()
	glog.V(0).Infoln("start repair task.uuid:",opstatus.Uuid," volumeId:",vidStr," dstAddr:",dstAddr," force:",force)
	return nil, opstatus.Uuid

}

func (s *Store) RepairByNodeVidRepVolume(volumeIdString string, dstNodeListStr string,serialNum string) (error, bool) {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString), false
	}
	dstNodeListTmp := strings.Split(dstNodeListStr, ",")
	dstNodeList := make([]string, 0, 16)
	count := 0
	for _, node := range dstNodeListTmp {
		if node != "" {
			dstNodeList = append(dstNodeList, node)
			count++
			if count >= 16 {
				return fmt.Errorf("Too many nodes!"), false
			}
		}
	}
	if v := s.findVolume(vid,serialNum); v != nil {
		return v.RepairByNodeVidRep(dstNodeList), true
	}
	return fmt.Errorf("volume id %d is not found during rep repair!", vid), false
}

//准备进行迁移动作，是作为源端，向目的端发送的信息
//通知对端占好槽位，
func (s *Store) RepRequestPrepare(dests []*Factor, force bool,Size int64,srcRack string) error {
	if len(dests) <= 0 {
		return errors.New("Replicate volume:No dests!")
	}
	passdests := []*Factor{}
	for _, dest := range dests {
		if dest.Dest == "" {
			continue
		}
		url := "/admin/prepare_repfile?volumeId=" + dest.Vid + "&uuid=" + dest.Uuid+"&size="+fmt.Sprintf("%d",Size)
		if srcRack != "" {
			url += "&srcRack="+srcRack
		}
		if dest.DestDir != "" {
			url += "&dir=" + dest.DestDir
		}
		if force {
			url += "&force=true"
		}
		glog.V(0).Infoln(url, dest.Dest, dest.DestDir)
		rsp, err := util.MakeRequest(dest.Dest, url, "", nil, nil)
		if err != nil {
			s.RepairPrepareRollBack(passdests)
			return err
		}
		if rsp.StatusCode != http.StatusOK {
			s.RepairPrepareRollBack(passdests)
			data, _ := ioutil.ReadAll(rsp.Body)
			rsp.Body.Close()
			if data != nil && len(data) > 0 {
				var temp map[string]string
				Unmarshalerr := json.Unmarshal(data, &temp)
				if Unmarshalerr != nil {
					glog.V(0).Infoln(Unmarshalerr)
					err = errors.New(string(data))
				} else {
					err = errors.New(temp["error"])
				}
			} else {
				err = errors.New("Can not replicate now!")
			}
			return err
		}
		passdests = append(passdests, dest)
	}
	return nil
}

//受端接收到修复命令之后的动作
//假如原本存在，必须带有force标识才会继续
//force 标识会把原来的旧的文件删除
//假如指定了位置，就用指定的位置，
//没指定就随机
//被动接受端，所以肯定有几率导致
func (s *Store) RepFilePrepare(volumeIdString,dir string, uuid,serialNum string, force bool,size,rep,srcRack,collection string) error {
	var dirloc *DiskLocation
	var v *Volume
	needSize,err := strconv.ParseInt(size,10,64)
	if err != nil {
		glog.V(0).Infoln("invalid size:",size)
		return fmt.Errorf("get size err:%s", err)
	}
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		glog.V(0).Infoln("invalid vid:",volumeIdString)
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}
	if srcRack != "" && srcRack != s.rack{
		glog.V(0).Infoln("src node and dst node are not in the same rack!", srcRack,s.rack)
		return fmt.Errorf("src node and dst node are not in the same rack!")
	}
	if collection == public.ECTemp && rep == "" {
		glog.V(0).Infoln("replication cannot be null when in ec mode")
		return fmt.Errorf("replication is null")
	}
	select {
		case s.SysOpChan <- struct{}{}:
		case <- time.After(time.Second*time.Duration(15))://等待时间太久，要检查下是卡在哪个任务里了
			glog.V(0).Infoln("wait too long for channel,give up")
			return errors.New("wait timeout")
	}
	defer func() {
		<- s.SysOpChan
	}()
	if collection == public.ECTemp || collection == public.EC{
		err := s.CheckECSliceLimit(vid,serialNum)
		if err != nil {
			if err == ErrTooManySlices && force == true{
				glog.V(0).Infoln("too many ec slices but has a force command! vid:",volumeIdString,"serialNum:",serialNum)
				err = nil
			}else if err == ErrSliceExists &&force == true{
				glog.V(0).Infoln("ec slice already exists but has a force command! vid:",volumeIdString,"serialNum:",serialNum)
				err = nil
			}else{
				glog.V(0).Infoln("ec slice limit check failed!err:",err,"vid:",volumeIdString,"serialNum:",serialNum)
				return err
			}
		}
	}
	v = s.findVolume(vid,serialNum)
	if v == nil {
		//占一个系统槽位
		v = s.findSysOpVolume(vid,serialNum)
		if v != nil {
			if force { //强制移除原有的操作痕迹
				dirloc = s.GetDirLocation(v.dir)
				dirloc.DestroySysOpSlot(vid,serialNum)
			} else {
				return fmt.Errorf("Volume Id %s is exist at %s! ,and it's in repair or rebalance ,retry as force request!", volumeIdString, v.dir)
			}
		}
		v = nil
		dirloc = nil
		//找一个位置
		if dir == "" {
			if dirloc,v,err = s.findFreeLocationAndOccupy(uint64(needSize),vid,serialNum, VOL_REP); err != nil {
				glog.V(0).Infoln("can not repair:",err)
				return fmt.Errorf(err.Error())
			}else{
				glog.V(0).Infoln("use dir:",dirloc.Directory,"to repair vid:",v.Id," needRepairedSize:",needSize)
			}
		} else {
			if dirloc = s.GetDirLocation(dir); dirloc == nil {
				glog.V(0).Infoln("No more free space left,can not do rep repair.")
				return fmt.Errorf("No more free space left,can not do rep repair.")
			}
			dirloc.RLock()
			if _, found := dirloc.volumes[vid]; found {
				dirloc.RUnlock()
				glog.V(0).Infoln("disk ",dirloc.Directory,"already has this volume!",vid,serialNum)
				return fmt.Errorf("same volume on one disk")
			}
			if _, found := dirloc.sysOpvolume[vid]; found {
				dirloc.RUnlock()
				glog.V(0).Infoln("disk ",dirloc.Directory,"already has this volume in progressing!",vid,serialNum)
				return fmt.Errorf("same volume on one disk")
			}
			dirloc.RUnlock()
			//构建一个vol卷,保留状态信息，不参与上报
			if v = dirloc.GetSysOpSlot(vid,serialNum,VOL_REP,uint64(needSize)); v == nil {
				glog.V(0).Infoln("No more free space left,can not do rep repair.")
				return fmt.Errorf("No more free space left,can not do rep repair.")
			}
		}
	} else {
		dirloc = s.GetDirLocation(v.dir)
		if dirloc == nil {
			return fmt.Errorf(" can not get %s's dir:%s ", volumeIdString, v.dir)
		}
		glog.V(0).Infoln("find vol on disk:",dirloc.Directory,"vid:",vid,"serialNum:",serialNum)
		//现在原本就存在volume，把这个卷的状态变更了
		if err = v.volSysOpStatus.trySetREP(); err != nil {
			//说明有其他的修复还在进行
			if force { //强制移除原有的操作痕迹
				dirloc.DestroyVolumeSlot(vid)
				if err = v.volSysOpStatus.trySetREP(); err != nil {
					return fmt.Errorf("Volume Id %s is exist at %s! ,and it's in repair or rebalance 2", volumeIdString, v.dir)
				}
			} else {
				return fmt.Errorf("Volume Id %s is exist at %s! ,and it's in repair or rebalance 3", volumeIdString, v.dir)
			}
		}
		err = dirloc.SetSysOpSlot(vid,serialNum,VOL_REP,uint64(needSize))
		if err != nil {
			dirloc.DestroyVolumeSlot(vid)
			glog.V(0).Infoln("set sys op slot err:",err, "vid:",vid,"serialNum:",serialNum)
			return fmt.Errorf("set sys op slot err")
		}
	}

	//设置uuid
	v.volSysOpStatus.uuid = uuid

	//建立临时文件
	fileName := dirloc.Directory + "/" + s.Collection + "_" + vid.String()
	if collection == public.ECTemp{
		if serialNum != "" {
			fileName = fileName + "_" + serialNum
		}
		dirloc.Lock()
		defer dirloc.Unlock()
		_=os.Remove(fileName + ".idx")
		_=os.Remove(fileName + ".dat")
		rt, e := NewReplicaPlacementFromString(rep)
		if e != nil {
			return e
		}
		glog.V(0).Infoln("In dir", dirloc.Directory, "adds volume =", vid, ", collection =", collection, ", serialNum =",serialNum,", replicaPlacement =", rep)
		if volume, err := NewVolume(dirloc.Directory,collection, serialNum, vid, rt); err == nil {
			volume.Fallocate(1, 0, int64(s.volumeSizeLimit))
			dirloc.volumes[vid] = volume
			return nil
		}
		return err
	}
	v.volSysOpStatus.repairstatus.tempdatFile, err = os.OpenFile(fileName+".dat.rep", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		glog.V(0).Infoln("can not create file", fileName+".dat.rep"+err.Error())
		return fmt.Errorf("can not create file" + fileName + ".dat.rep" + err.Error())
	}

	v.volSysOpStatus.repairstatus.tempidxFile, err = os.OpenFile(fileName+".idx.rep", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		glog.V(0).Infoln("can not create file", fileName+".idx.rep"+err.Error())
		return fmt.Errorf("can not create file" + fileName + ".idx.rep" + err.Error())
	}
	return nil

}

//接收到要修复的文件
//1 找到卷
//2 确认卷状态
//3 确认uuid
//4 写入
func (s *Store) RepFileReceive(volumeIdString, ext, sum, uuid,serialNum string, data []byte) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}
	vol := s.findVolume(vid,serialNum)
	if vol == nil {
		vol = s.findSysOpVolume(vid,serialNum)
	}
	if vol == nil {
		return fmt.Errorf("Volume Id %s has not been prepared for repairs yet.", volumeIdString)
	}
	if vol.volSysOpStatus.uuid != uuid {
		return fmt.Errorf("Volume Id %s has different uuid.want : %s , but: %s", volumeIdString, vol.volSysOpStatus.uuid, uuid)
	}
	if vol.volSysOpStatus.getStatus() != VOL_REP {
		return fmt.Errorf("Volume Id %s has different status.want : %d , but: %d", volumeIdString, VOL_REP, vol.volSysOpStatus.getStatus())
	}

	var file *os.File
	if ext == "dat" {
		file = vol.volSysOpStatus.repairstatus.tempdatFile
	} else if ext == "idx" {
		file = vol.volSysOpStatus.repairstatus.tempidxFile
	} else {
		return errors.New("Bad transfer type!")
	}
	ctxMD5 := md5.New()
	ctxMD5.Reset()
	ctxMD5.Write(data)
	if hex.EncodeToString(ctxMD5.Sum(nil)) != sum {
		return ErrStorageMigBadRcv
	}
	ln, err := file.Write(data)
	if err != nil {
		return err
	}
	if ln != len(data) {
		return errors.New("Can not write local file!")
	}
	return nil
}

//修复动作已经结束了，把信息
//1 找到卷
//2 确认卷状态
//3 确认uuid
//4
//5
func (s *Store) RepFileCommit(volumeIdString string, rslt string, uuid,serialNum string) error {

	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}
	oldexit := true
	vol := s.findVolume(vid,serialNum)
	if vol == nil {
		vol = s.findSysOpVolume(vid,serialNum)
		oldexit = false
	}

	if vol == nil {
		return fmt.Errorf("Volume Id %s has not been prepared for repairs yet.", volumeIdString)
	}
	if vol.volSysOpStatus.uuid != uuid {
		return fmt.Errorf("Volume Id %s has different uuid.want : %s , but: %s", volumeIdString, vol.volSysOpStatus.uuid, uuid)
	}
	if vol.volSysOpStatus.getStatus() != VOL_REP {
		return fmt.Errorf("Volume Id %s has different status.want : %d , but: %d", volumeIdString, VOL_REP, vol.volSysOpStatus.getStatus())
	}

	dirloc := s.GetDirLocation(vol.dir)
	//失败了
	if rslt != "1" {
		//找到dir，并移除
		dirloc.DestroySysOpSlot(vid,serialNum)
		return nil
	}
	if vol.volSysOpStatus.repairstatus.tempidxFile != nil {
		vol.volSysOpStatus.repairstatus.tempidxFile.Close()
		vol.volSysOpStatus.repairstatus.tempidxFile = nil
	}
	if vol.volSysOpStatus.repairstatus.tempdatFile != nil {
		vol.volSysOpStatus.repairstatus.tempdatFile.Close()
		vol.volSysOpStatus.repairstatus.tempdatFile = nil
	}
	if oldexit {
		//存在旧的
		vol.Backup(true)
		s.deleteVolumeM(vid,serialNum)
		s.Join()
		time.Sleep(5 * time.Second)
	}
	if err = s.RepairFinTransfer(volumeIdString,serialNum, dirloc.Directory); err != nil {
		return err
	}
	newVid:=dirloc.loadExistingVolumes(1,s.Collection)
	if newVid > 0{
		dirloc.Lock()
		s.AddAnVolRecord(dirloc.Directory,vid,serialNum)
		dirloc.Unlock()
	}
	//释放
	dirloc.ReleaseSysOpSlot(vid,serialNum)
	s.Join()
	return nil
}

func (s *Store) RepairFinTransfer(volumeIdString,serialNum string, dir string) (err error) {

	base := s.Collection + "_" + volumeIdString
	if serialNum!=""{
		base = base + "_" + serialNum
	}

	oldName := path.Join(dir, base+".dat.rep")
	newName := path.Join(dir, base+".dat")
	if err = os.Rename(oldName, newName); err != nil {
		return err
	}
	oldName = path.Join(dir, base+".idx.rep")
	newName = path.Join(dir, base+".idx")
	if err = os.Rename(oldName, newName); err != nil {
		return err
	}

	return nil
}

func (s *Store) RepairPrepareRollBack(dests []*Factor) {
	for _, dest := range dests {
		if dest.Dest == "" {
			continue
		}
		url := "/admin/commit_repfile?volumeId=" + dest.Vid
		url = url + "&rslt=0" + "&uuid=" + dest.Uuid
		rsp, err := util.MakeRequest(dest.Dest, url, "PUT", nil, nil)
		if err != nil {
			glog.V(0).Infoln(err)
			continue
		}
		if rsp.StatusCode != http.StatusOK {
			glog.V(0).Infoln("can not rollback dst node :", dest, ",volume:", dest.Vid, " maybe need reprepair")
			continue
		}
	}
	return
}