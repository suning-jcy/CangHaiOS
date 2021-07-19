package storage

import (
	"code.google.com/p/weed-fs/go/public"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"io/ioutil"
)

type RebalanceStaus struct {
	tempdatFile *os.File
	tempidxFile *os.File
}

var VolBackOpEnable = true //是否允许后台任务，修复/迁移
var VolBackOpInternal = 0  //后台任务间隔，修复/迁移任务的发包间隔
var ErrStorageMigNoSpace = errors.New("storage:no space for vol migration")
var ErrStorageMigBadRcv = errors.New("storage: bad rcvd data for vol migration")
var ErrVolumeNotFound = errors.New("storage: volume not found")
var FlagOpenTmpFile = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
var ErrTxBusy = errors.New("One transfer is now in progress")

//迁移/修复动作的元素
type Factor struct {
	//目的端IP
	Dest string
	//目录
	DestDir string

	Vid  string
	Uuid string
}

func (s *Store) RebalanceVolume(r *http.Request) (error, string) {

	r.ParseForm()
	vidStr := r.FormValue("volumeId")
	serialNum := r.FormValue("serialNum")
	dstAddr := r.FormValue("dstNode")
	delVol := "true" == r.FormValue("delVol")
	force := r.FormValue("force")

	dests := strings.Split(dstAddr, ",")
	if len(dests) == 0 || dstAddr == "" {
		return errors.New("dstNode can not be null"), ""
	}
	if s.Collection == public.EC && serialNum == "" {
		return errors.New("serialNum in ec collection can not be null"), ""
	}
	if s.Collection != public.EC && serialNum != "" {
		return errors.New("serialNum in non-ec collection should be null"), ""
	}
	glog.V(4).Infoln("get rebalance quest", vidStr, dstAddr, delVol, force)

	vid, err := NewVolumeId(vidStr)
	if err != nil {
		err = fmt.Errorf("Storage: bad vid %s", vidStr)
		return err, ""
	}

	v := s.findVolume(vid,r.FormValue("serialNum"))
	if v == nil {
		return ErrVolumeNotFound, ""
	}
	lookupResult, lookupErr := operation.Lookup(s.FindMaster(), vidStr, s.Collection, nil)
	if lookupErr != nil || lookupResult == nil || len(lookupResult.Locations) == 0 {
		return ErrVolumeNotFound, ""
	}
	peers := []string{}
	for _, v := range lookupResult.Locations {
		if serialNum != "" {
			if serialNum == v.SerialNum {
				peers = append(peers, v.PublicUrl)
			}
		}else{
			peers = append(peers, v.PublicUrl)
		}
	}

	if VolBackOpEnable == false {
		glog.V(0).Infoln("background task is not permitted now,Vid:", vid)
		err = errors.New("Background task is disabled")
		return err, ""
	}

	ips, err := s.getVidLoc(vidStr)
	if err != nil {
		glog.V(0).Infoln("get vid loc err:", err, "vid:", vid)
		return err, ""
	}

	//获取
	if err = v.volSysOpStatus.trySetREB(); err != nil {
		glog.V(0).Infoln("error while trySetREB ,other TRANSFER is runing")
		return err, ""
	}

	v.accessLock.Lock()
	//获取大小
	vinfo, err := v.dataFile.Stat()
	if err != nil {
		v.accessLock.Unlock()
		v.volSysOpStatus.reset()
		glog.V(0).Infoln("get datafile size err:", err)
		return err, ""
	}
	dataSize := vinfo.Size()
	v.accessLock.Unlock()

	nowTime := time.Now()
	nowTimeStr := fmt.Sprintf("%d-%d-%d_%d-%d-%d", nowTime.Year(), nowTime.Month(), nowTime.Day(), nowTime.Hour(), nowTime.Minute(), nowTime.Second())
	opstatus := &SysOpStatus{
		Uuid:      fmt.Sprintf("rebalance_vol_%d_from_%s_time_%s", vid, s.Ip, nowTimeStr),
		Optype:    "rebalance",
		Result:    false,
		IsOver:    false,
		Starttime: nowTime,
		Id:        int(vid),
		Sn:        serialNum,
		Dest:      dstAddr,
		Size:      dataSize,
		Mnt:       v.dir,
	}
	glog.V(4).Infoln("create opstatus uuid:", opstatus.Uuid, "Vid:", vid)
	v.volSysOpStatus.uuid = opstatus.Uuid

	s.AddSysOpStatus(opstatus,0)

	go func() {
		var err error
		defer func() {
			//不管执行结果如何，最终要归还占位
			v.volSysOpStatus.reset()
			if err == nil {
				opstatus.Result = true
				glog.V(0).Infoln("do RebalanceVolume success.uuid:", opstatus.Uuid, "Vid:", vid)
			} else {
				glog.V(0).Infoln("do RebalanceVolume error:", err, "uuid:", opstatus.Uuid, "Vid:", vid)
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
				Dest: v,
				Vid:  vidStr,
				Uuid: opstatus.Uuid,
			}
			if len(parms) >= 2 {
				f.DestDir = parms[1]
			}
			Factors = append(Factors, f)
		}

		//让对端准备
		if err = s.RebRequestPrepare(Factors, force == "true", dataSize,serialNum,s.rack); err != nil {
			glog.V(0).Infoln("failed to RebRequestPrepare.uuid:", opstatus.Uuid, "Vid:", vid)
			return
		}

		//准备本端的文件传输
		//传输动作要带上uuid
		porter := NewPorter(v.FileName(), v.Id.String(), Factors, peers, opstatus.Uuid, VOL_REB)
		if porter == nil {
			glog.V(0).Infoln("failed to create porter, uuid:", opstatus.Uuid, "Vid:", vid)
			err = errors.New("failed to create porter")
			return
		}

		//等待本地状态上报为不可写，同时等待可能的已经分配过的写进来
		time.Sleep(15 * time.Second)
		//传输数据
		if VolBackOpEnable == false {
			glog.V(0).Infoln("background task is not permitted now", opstatus.Uuid, "Vid:", vid)
			err = errors.New("Background task is disabled")
			return
		}
		if err = porter.transferVolBase(serialNum); err != nil {
			glog.V(0).Infoln("transferVolBase error:", err, " uuid:", opstatus.Uuid, "Vid:", vid)
			porter.finMigration(false,serialNum)
			return
		}

		time.Sleep(5 * time.Second)
		if VolBackOpEnable == false {
			glog.V(0).Infoln("background task is not permitted now", opstatus.Uuid, "Vid:", vid)
			err = errors.New("Background task is disabled")
			return
		}
		//传输中途可能的 被追加到dat/idx 上的文件（理论上都是删除动作）
		if err = porter.transferVolExt(serialNum); err != nil {
			glog.V(0).Infoln("transferVolExt error:", err, " uuid:", opstatus.Uuid, "Vid:", vid)
			porter.finMigration(false,serialNum)
			return
		}

		//最终动作，发送commit_rebfile
		if err = porter.finMigration(true,serialNum); err != nil {
			glog.V(0).Infoln("finMigration error:", err, " uuid:", opstatus.Uuid, "Vid:", vid)
			return
		}
		if err = porter.backupPeers(delVol,serialNum); err != nil {
			glog.V(0).Infoln("backupPeers", err, " uuid:", opstatus.Uuid, "Vid:", vid)
			return
		}

		err = s.DisableFilerCache(vid)
		if err != nil {
			glog.V(0).Infoln("disable filer cache error:", err, " uuid:", opstatus.Uuid, "Vid:", vid, "delVol:", delVol)
		}

		err = s.DisableVolCache(ips, vid)
		if err != nil {
			glog.V(0).Infoln("disable vol cache error:", err, " uuid:", opstatus.Uuid, "Vid:", vid, "delVol:", delVol)
		}
	}()
	return nil, opstatus.Uuid
}

//准备进行迁移动作，是作为源端，向目的端发送的信息
func (s *Store) RebRequestPrepare(dests []*Factor, force bool, Size int64,serialNum,rack string) error {
	if len(dests) <= 0 {
		return errors.New("Replicate volume:No dests!")
	}
	passdests := []*Factor{}
	for _, dest := range dests {
		if dest.Dest == "" {
			continue
		}
		url := "/admin/prepare_rebfile?volumeId=" + dest.Vid + "&uuid=" + dest.Uuid + "&size=" + fmt.Sprintf("%d", Size)
		if dest.DestDir != "" {
			url += "&dir=" + dest.DestDir
		}
		if force {
			url += "&force=true"
		}
		finalUrl :=url+ "&serialNum="+serialNum+"&rack="+rack
		glog.V(0).Infoln(finalUrl, dest.Dest, dest.DestDir)
		rsp, err := util.MakeRequest(dest.Dest, finalUrl, "", nil, nil)
		if err != nil {
			s.RebalancePrepareRollBack(passdests)
			return err
		}
		if rsp.StatusCode != http.StatusOK {
			s.RebalancePrepareRollBack(passdests)
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
func (s *Store) RebFilePrepare(volumeIdString, dir string, uuid,serialNum string, force bool, size string) error {
	var dirloc *DiskLocation
	var v *Volume
	needSize, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return fmt.Errorf("get size err:%s", err)
	}
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
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
	if s.Collection == public.EC{
		err := s.CheckECSliceLimit(vid,serialNum)
		if err != nil {
			if err == ErrTooManySlices && force == true{
				glog.V(0).Infoln("ec slice limit check failed but has a force command! vid:",volumeIdString,"serialNum:",serialNum)
				err = nil
			}else{
				glog.V(0).Infoln("ec slice limit check failed!err:",err,"vid:",volumeIdString,"serialNum:",serialNum)
				return err
			}
		}
	}
	//迁移动作一定是现在没有的
	v = s.findVolume(vid,serialNum)
	if v == nil {

		//占一个系统槽位
		v = s.findSysOpVolume(vid,serialNum)
		if v != nil {
			if force { //强制移除原有的操作痕迹
				dirloc = s.GetDirLocation(v.dir)
				dirloc.DestroySysOpSlot(vid,serialNum)
			} else {
				return fmt.Errorf("Volume Id %s is exist at %s! ,and it's in repair or rebalance ,retry as force rebalance!", volumeIdString, v.dir)
			}
		}
		v = nil
		dirloc = nil
		//找一个位置
		if dir == "" {
			if dirloc, v, err = s.findFreeLocationAndOccupy(uint64(needSize), vid,serialNum, VOL_REB); err != nil {
				glog.V(0).Infoln("can not rebalance:", err,".serialNum:",serialNum)
				return fmt.Errorf(err.Error())
			} else {
				glog.V(0).Infoln("use dir:", dirloc.Directory, "to rebalance vid:", v.Id,"serialNum:",serialNum, " needRebalancedSize:", needSize)
			}
		} else {
			if dirloc = s.GetDirLocation(dir); dirloc == nil {
				glog.V(0).Infoln("No more free space left,can not do rep rebalance.")
				return fmt.Errorf("No more free space left,can not do rep rebalance.")
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
		}
	} else {
		return fmt.Errorf("Volume Id %s is exist at %s! ", volumeIdString, v.dir)
	}

	//设置uuid
	v.volSysOpStatus.uuid = uuid

	//建立临时文件
	fileName := dirloc.Directory + "/" + s.Collection + "_" + vid.String()
	if serialNum != ""{
		fileName+= "_" + serialNum
	}
	v.volSysOpStatus.rebalancestatus.tempdatFile, err = os.OpenFile(fileName+".dat.reb", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		glog.V(0).Infoln("can not create file" + fileName + ".dat.reb :" + err.Error())
		return fmt.Errorf("can not create file" + fileName + ".dat.reb " + err.Error())
	}

	v.volSysOpStatus.rebalancestatus.tempidxFile, err = os.OpenFile(fileName+".idx.reb", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		glog.V(0).Infoln("can not create file", fileName+".idx.reb"+err.Error())
		return fmt.Errorf("can not create file" + fileName + ".idx.reb " + err.Error())
	}
	return nil

}

//接收到要修复的文件
//1 找到卷
//2 确认卷状态
//3 确认uuid
//4 写入
func (s *Store) RebFileReceive(volumeIdString, ext, sum, uuid,serialNum string, data []byte) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}
	vol := s.findVolume(vid,serialNum)
	if vol == nil {
		vol = s.findSysOpVolume(vid,serialNum)
	}
	if vol == nil {
		return fmt.Errorf("Volume Id %s has not been prepared for rebalance yet.", volumeIdString)
	}
	if vol.volSysOpStatus.uuid != uuid {
		return fmt.Errorf("Volume Id %s has different uuid.want : %s , but: %s", volumeIdString, vol.volSysOpStatus.uuid, uuid)
	}
	if vol.volSysOpStatus.getStatus() != VOL_REB {
		return fmt.Errorf("Volume Id %s has different status.want : %d , but: %d", volumeIdString, VOL_REB, vol.volSysOpStatus.getStatus())
	}

	var file *os.File
	if ext == "dat" {
		file = vol.volSysOpStatus.rebalancestatus.tempdatFile
	} else if ext == "idx" {
		file = vol.volSysOpStatus.rebalancestatus.tempidxFile
	} else {
		return errors.New("Bad transfer type!")
	}
	if file == nil {
		return errors.New("Bad transfer file!")
	}
	//glog.V(4).Infoln("write", len(data), " to ", file.Name())
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

func (s *Store) RebFileCommit(volumeIdString string,serialNum, rslt string, uuid string) error {

	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}

	vol := s.findSysOpVolume(vid,serialNum)

	if vol == nil {
		return fmt.Errorf("Volume Id %s has not been prepared for repairs yet.", volumeIdString)
	}
	if vol.volSysOpStatus.uuid != uuid {
		return fmt.Errorf("Volume Id %s has different uuid.want : %s , but: %s", volumeIdString, vol.volSysOpStatus.uuid, uuid)
	}
	if vol.volSysOpStatus.getStatus() != VOL_REB {
		return fmt.Errorf("Volume Id %s has different status.want : %d , but: %d", volumeIdString, VOL_REB, vol.volSysOpStatus.getStatus())
	}

	dirloc := s.GetDirLocation(vol.dir)
	//失败了
	if rslt != "1" {
		//找到dir，并移除
		dirloc.DestroySysOpSlot(vid,serialNum)
		return nil
	}
	if vol.volSysOpStatus.rebalancestatus.tempidxFile != nil {
		vol.volSysOpStatus.rebalancestatus.tempidxFile.Close()
		vol.volSysOpStatus.rebalancestatus.tempidxFile = nil
	}
	if vol.volSysOpStatus.rebalancestatus.tempdatFile != nil {
		vol.volSysOpStatus.rebalancestatus.tempdatFile.Close()
		vol.volSysOpStatus.rebalancestatus.tempdatFile = nil
	}
	//最终替换错误
	if err = s.RebalanceFinTransfer(volumeIdString, dirloc.Directory,serialNum); err != nil {
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

func (s *Store) RebalanceFinTransfer(volumeIdString string, dir,serialNum string) (err error) {
	base := s.Collection + "_" + volumeIdString
	if serialNum != ""{
		base+= "_" + serialNum
	}
	oldName := path.Join(dir, base+".dat.reb")
	newName := path.Join(dir, base+".dat")
	if err = os.Rename(oldName, newName); err != nil {
		return err
	}
	oldName = path.Join(dir, base+".idx.reb")
	newName = path.Join(dir, base+".idx")
	if err = os.Rename(oldName, newName); err != nil {
		return err
	}

	return nil
}

func (s *Store) BackupVolume(id string, delVol bool,serialNum string) (err error) {
	vid, err := NewVolumeId(id)
	if err != nil {
		return errors.New("Storage: invalid volume id:" + id)
	}
	if v := s.findVolume(vid,serialNum); v != nil {
		v.accessLock.Lock()
		defer v.accessLock.Unlock()
		if err = v.Backup(delVol); err == nil {
			s.deleteVolumeM(vid,serialNum)
			s.Join()
		} else {
			return err
		}
	} else {
		return ErrVolumeNotFound
	}
	return nil
}

func (s *Store) deleteVolumeM(vid VolumeId,serialNum string) (e error) {
	for _, location := range s.Locations {
		location.Lock()
		for k, v := range location.volumes {
			if v.Id == vid  && v.SerialNum == serialNum{
				glog.V(0).Infoln("delete volume vid:",vid,"serialNum:",serialNum)
				delete(location.volumes, k)
				s.DeleteAnVolRecord(location.Directory,vid,serialNum)
			}
		}
		location.Unlock()
	}
	return
}

//make all str & dst volumeserver status to nomal
//str:  if delVol==true  reprepair  ? ====>no ,just,volume will be unuseable until reprepair by master
//
//dst:delete temp file ,make sure vs.status,
//指向的同一个vol，
func (s *Store) RebalancePrepareRollBack(dests []*Factor) {
	for _, dest := range dests {
		if dest.Dest == "" {
			continue
		}
		url := "/admin/commit_rebfile?volumeId=" + dest.Vid
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

//移除指定目录下所有的bak文件
func (s *Store) DelBakFile(diskname string) error {
	if diskname != "" {
		dl := s.GetDirLocation(diskname)
		if dl == nil {
			return errors.New("can not get dir named:" + diskname)
		}
		if dirs, err := ioutil.ReadDir(dl.Directory); err == nil {
			for _, dir := range dirs {
				name := dir.Name()
				if !dir.IsDir() && strings.HasSuffix(name, ".bak") {
					//移除.bak文件
					os.Remove(dl.Directory + "/" + name)
				}
			}
		}
	} else {

		for _, v := range s.Locations {
			if dirs, err := ioutil.ReadDir(v.Directory); err == nil {
				for _, dir := range dirs {
					name := dir.Name()
					if !dir.IsDir() && strings.HasSuffix(name, ".bak") {
						//移除.bak文件
						os.Remove(v.Directory + "/" + name)
					}
				}
			}
		}

	}
	return nil
}

func (s *Store) getVidLoc(vid string) (ip []string, err error) {
	//get filers ip
	masterNode, e := s.masterNodes.FindMaster()
	if e != nil {
		glog.V(0).Infoln("find master node err:", e)
		return nil, errors.New("find master node err")
	}
	//get vid Location
	r, err := util.MakeRequest(masterNode, "/dir/lookup?volumeId="+vid+"&collection="+s.Collection, "POST", nil, nil)
	if err != nil {
		glog.V(0).Infoln("volume lookup Post error:", err, "server:", masterNode, "volume:", vid)
		return nil, err
	}
	defer r.Body.Close()
	jsonBlob, err := ioutil.ReadAll(r.Body)
	//jsonBlob, err := util.Post("http://"+server+"/dir/lookup", values)
	if err != nil {
		glog.V(0).Infoln("volume lookup Post error:", err, "server:", masterNode, "volume:", vid)
		return nil, err
	}
	var ret operation.LookupResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		glog.V(0).Infoln("volume lookup Unmarshal error:", err, "server:", masterNode, "volume:", vid)
		return nil, err
	}
	if ret.Error != "" {
		glog.V(0).Infoln("volume lookup error :", ret.Error, "server:", masterNode, "volume:", vid)
		return nil, errors.New(ret.Error)
	}
	length := len(ret.Locations)
	ips := make([]string, length)
	var num = 0
	for _, v := range ret.Locations {
		glog.V(1).Infoln("get vid:", vid, "location.PublicUrl:", v.PublicUrl,"serialNum:",v.SerialNum, "url", v.Url, "s.Ip:", s.Ip)
		ips[num] = v.PublicUrl
		num++
	}
	if num == 0 {
		return nil, errors.New("find no extra vol") //找不到其他volume位置信息
	}
	return ips, nil
}

func (s *Store) DisableFilerCache(vid VolumeId) error {
	//get filers ip
	var filers []string
	masterNode, e := s.masterNodes.FindMaster()
	if e != nil {
		glog.V(0).Infoln("find master node err:", e)
		return errors.New("find master node err")
	}
	url := "/sdoss/filer/status"
	rsp, e := util.MakeRequest(masterNode, url, "GET", nil, nil)
	if e != nil {
		glog.V(0).Infoln("Can not get filers status. err:", e, masterNode)
		return e
	}
	if rsp.StatusCode != 200 {
		glog.V(0).Infoln("Can not get filers status", rsp.StatusCode)
		return errors.New("Can not get filers status")
	} else {
		data, e := ioutil.ReadAll(rsp.Body)
		if e != nil {
			glog.V(0).Infoln("read body err", e)
			return errors.New("read body err")
		}
		var msg map[string]interface{}
		e = json.Unmarshal(data, &msg)
		if e != nil {
			glog.V(0).Infoln("Unmarshal data err", e)
			return errors.New("Unmarshal data err")
		}
		if _, ok := msg["Filers Available"]; ok {
			filersIP := msg["Filers Available"].([]interface{})
			filers = make([]string, len(filersIP))
			for k, v := range filersIP {
				filers[k] = v.(string)
			}
		} else {
			return errors.New("find no filers")
		}
	}
	//disable filers' partitionCache
	for _, v := range filers {
		urlTail := "/sdoss/filer/delVC?pvid=" + fmt.Sprintf("%d", vid)
		url := "http://" + v + urlTail
		glog.V(1).Infoln("try to disable filer cache url:", url)
		b, e := util.PostBytes_timeout(url, nil)
		if e != nil {
			glog.V(0).Infoln("disable filer cache node:", url, " failed:", e)
			continue
		}
		var msg map[string]interface{}
		e = json.Unmarshal(b, &msg)
		if e != nil {
			glog.V(0).Infoln("disable filer cache node:", url, " failed:", e)
			continue
		}
		glog.V(1).Infoln("disable filer cache node:", url, " ok")
	}
	return nil
}

func (s *Store) DisableVolCache(dstAddr []string, vid VolumeId) error {
	//get filers ip
	for _, v := range dstAddr {
		urlTail := "/vol/disablevlcache?pvid=" + fmt.Sprintf("%d", vid)
		url := "http://" + v + urlTail
		b, e := util.PostBytes_timeout(url, nil)
		if e != nil {
			glog.V(0).Infoln("disable vol cache node:", url, " failed:", e)
			continue
		}
		var msg map[string]interface{}
		e = json.Unmarshal(b, &msg)
		if e != nil {
			glog.V(0).Infoln("disable vol cache node:", url, " failed:", e)
			continue
		}
		glog.V(1).Infoln("disable vol cache node:", url, " ok")
	}

	return nil
}