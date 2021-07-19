package storage

import (
	"fmt"
	"os"
	"strconv"

	"code.google.com/p/weed-fs/go/glog"
	"time"
)

type VacuumStaus struct {
	nmCPD       NeedleMapper
	oldNM       NeedleMapper
	OldDataSize int64
	OldIdxSize  int64

	CompactSize      uint64
	CompactFileCount int
}

func (s *Store) CheckCompactVolume(volumeIdString string, garbageThresholdString,serialNum string, limitFreeSize uint64) (err error, needVcm bool) {

	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		needVcm = false
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString), false
	}
	garbageThreshold, e := strconv.ParseFloat(garbageThresholdString, 32)
	if e != nil {
		needVcm = false
		return fmt.Errorf("garbageThreshold %s is not a valid float number!", garbageThresholdString), false
	}
	if v := s.findVolume(vid,serialNum); v != nil {
		//剩余空间默认是卷的最大值
		//假如带过来参数limitFreeSize就使用limitFreeSize
		//假如卷实际空间大于lsize，就使用实际空间
		lsize := s.volumeSizeLimit
		if limitFreeSize != 0 {
			lsize = limitFreeSize
		}
		finfo, err := v.dataFile.Stat()
		if err != nil || finfo == nil{
			glog.V(0).Infoln("data file stat failed:",err)
			return err, false
		}
		filesize := uint64(finfo.Size()) + 500*1024*1024

		if filesize > lsize {
			lsize = filesize
		}

		if garbageThreshold > v.garbageLevel() {
			glog.V(0).Infoln("garbageThreshold:",garbageThreshold,v.nm.DeletedSize(),v.nm.ContentSize(),"vid:",vid)
			return nil, false
		}

		if err := v.DiskCanVacuum(lsize); err != nil {
			return err, false
		}

		return nil, true
	}
	return fmt.Errorf("check:volume id %d is not found during check compact!", vid), false
}
func (s *Store) CompactVolume(volumeIdString,serialNum, method string) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		glog.V(0).Infoln("Volume Id %s is not a valid unsigned integer!", volumeIdString)
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}
	if v := s.findVolume(vid,serialNum); v != nil {
		//return v.Compact()
		//v.CompactStatus = PreCompact
		//s.Join()
		//waiting for extra writing file
		//time.Sleep(15 * time.Second)
		//v.CompactStatus = UnderCompact
		err = v.Compact(method)
		s.Join()
		return err
	}
	return fmt.Errorf("volume id %d is not found during compact!", vid)
}
func (s *Store) CommitCompactVolume(volumeIdString,serialNum string) error {

	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}
	if v := s.findVolume(vid,serialNum); v != nil {
		//return v.commitCompact()

		err := v.commitCompact()
		s.Join()
		return err
	}
	return fmt.Errorf("volume id %d is not found during commit compact!", vid)
}

func (s *Store) DeleteCompactFileOnVacuumFail(volumeIdString,serialNum string) error {

	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}
	if v := s.findVolume(vid,serialNum); v != nil {
		if err := v.DeleteCompactFileOnVacuumFail(); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) CheckVolumes(limitFreeSize uint64) map[string]interface{} {
	ret := make(map[string]interface{})
	for _, l := range s.Locations {
		lRet := make(map[string]string)
		isShadowCpxRemainErr := false
		l.RLock()
		for id, volume := range l.volumes {
			if err := volume.IsIdxDatConsistent(); err != nil {
				lRet[id.String()] = err.Error()
				continue
			}

			lsize := s.volumeSizeLimit
			if limitFreeSize != 0 {
				lsize = limitFreeSize
			}
			finfo, err := volume.dataFile.Stat()
			if err != nil || finfo == nil{
				glog.V(0).Infoln("data file stat failed:",err)
				lRet[id.String()] = err.Error()
				continue
			}
			filesize := uint64(finfo.Size()) + 500*1024*1024
			if finfo != nil && filesize > lsize {
				lsize = filesize
			}

			if err := volume.DiskCanVacuum(lsize); err != nil && !isShadowCpxRemainErr {
				lRet[l.Directory] = err.Error()
				isShadowCpxRemainErr = true
			}
			lRet[id.String()] = "OK"
		}
		l.RUnlock()
		ret[l.Directory] = lRet
	}
	return ret
}
func (s *Store) Vacuum(volume, threshold, method,serialNum string, ommitCpxErr bool, limitFreeSize uint64,maxConcurrent int) (int64, error) {
	bsize := int64(0)
	asize := int64(0)

	vid, _ := NewVolumeId(volume)
	v := s.GetVolume(vid,serialNum)
	if v == nil {
		return 0, fmt.Errorf("can not get volume:" + volume)
	}

	//检测是不是需要回收
	err, needVcm := s.CheckCompactVolume(volume, threshold,serialNum, limitFreeSize)
	if err != nil {
		if err == ErrCpXRemain{
			if ommitCpxErr {
				err = nil
				needVcm = true
			}else{
				glog.V(0).Infoln("error while trySetVCM ,because this disk now is",err,"status:",v.volSysOpStatus.status,"vid:",vid)
				return 0, ErrVacuumBusy
			}
		}else{
			return 0,err
		}
	}
	if !needVcm {
		glog.V(2).Infoln("volume", volume, "need not to vac.",ommitCpxErr,"vid:",vid,"err:",err)
		return 0, fmt.Errorf("check:no need to gc")
	}

	//设置为节点的回收状态。
	canvacerr := v.volSysOpStatus.trySetVCM()
	if canvacerr != nil {
		glog.V(0).Infoln("error while trySetVCM ,other TRANSFER is runing")
		return 0, canvacerr
	}

	vinfo, err := v.dataFile.Stat()
	if err != nil {
		v.volSysOpStatus.reset()
		return 0, err
	}
	bsize = vinfo.Size()

	nowTime := time.Now()
	nowTimeStr := fmt.Sprintf("%d-%d-%d_%d-%d-%d", nowTime.Year(), nowTime.Month(), nowTime.Day(), nowTime.Hour(), nowTime.Minute(), nowTime.Second())
	opstatus := &SysOpStatus{
		Uuid:       fmt.Sprintf("vacuum_vol_%d_time_%s",vid,nowTimeStr),
		Optype:    "vacuum",
		Result:    false,
		IsOver:    false,
		Starttime: nowTime,
		Id:        int(vid),
		Dest:      "",
		Mnt:       v.dir,
	}
	v.volSysOpStatus.uuid = opstatus.Uuid
	err = s.AddSysOpStatus(opstatus,maxConcurrent)
	if err != nil {
		glog.V(0).Infoln("create vacuum opstatus Uuid:", opstatus.Uuid, " vid:", volume,"disk:",opstatus.Mnt,"err:",err)
		v.volSysOpStatus.reset()
		return 0,ErrVacuumBusy
	}

	glog.V(0).Infoln("create vacuum opstatus Uuid:", opstatus.Uuid, " vid:", volume,"disk:",opstatus.Mnt)
	defer func() {
		//不管执行结果如何，最终要归还占位
		v.volSysOpStatus.reset()
		if err == nil {
			opstatus.Result = true
		} else {
			opstatus.Result = false
			opstatus.Err = err.Error()
		}
		opstatus.IsOver = true
		opstatus.Endtime = time.Now()
	}()
	if v.Mode == CopyDownMode{
		glog.V(0).Infoln("volume in copy down mode,can not vacuum! vid:",v.Id)
		return 0,nil
	}
	//回收
	if err = s.CompactVolume(volume,serialNum, method); err != nil {
		return 0, err
	}
	//提交
	if err = s.CommitCompactVolume(volume,serialNum); err != nil {
		return 0, err
	}
	vinfo, err = v.dataFile.Stat()
	if err != nil {
		return 0, err
	}
	asize = vinfo.Size()
	return bsize - asize, nil
}
func (s *Store) GetVacuumVolume() map[string]string {
	ret := make(map[string]string)
	for _, l := range s.Locations {
		l.RLock()
		for vid, volume := range l.volumes {
			status := volume.volSysOpStatus.getStatus()
			switch status {
			case VOL_VCM:
				ret[vid.String()] = "UnderCompact"
			}
		}
		l.RUnlock()
	}
	return ret
}
func (s *Store) ScanVolume(vid,serialNum string)(fids []string,err error) {
	volumeId,err:=strconv.Atoi(vid)
	if err != nil {
		glog.V(0).Infoln("invalid vid",vid)
		return nil,err
	}
	vol:=s.findVolume(VolumeId(volumeId),serialNum)
	if vol == nil {
		glog.V(0).Infoln("not find vid",vid,"serialNum:",serialNum)
		return nil,err
	}
	location:=s.FindLocation(VolumeId(volumeId),serialNum)
	if location == nil {
		glog.V(0).Infoln("not find vid location",vid,"serialNum:",serialNum)
		return nil,err
	}
	fileName:=vol.FileName()
	idxFile ,err := os.OpenFile(fileName+".idx", os.O_RDONLY, 0644)
	if err != nil {
		fmt.Println("open index file error")
		return  nil,err
	}
	defer idxFile.Close()
	dataFile, err := os.OpenFile(fileName+".dat", os.O_RDONLY, 0644)
	if err != nil {
		fmt.Println("open data file error")
		return nil,err
	}
	defer dataFile.Close()
	sb,err := ReadSuperBlock(dataFile)
	if err != nil {
		fmt.Println("read super block error")
		return nil,err
	}
	fids = make([]string,0)
	files:=make(map[uint64]string)
	var MaxKey uint64
	err = WalkIndexFile(idxFile, func(key uint64, offset, size uint32) error {
		n := new(Needle)
		n.Read(dataFile, int64(offset)*NeedlePaddingSize, size, sb.Version)
		ffid:=fmt.Sprintf("%s,%06x%08x",vid, n.Id, n.Cookie)
		fileInfo:=ffid
		glog.V(1).Infoln("fid:",ffid,"size:",n.Size,"",n.DataSize)
		if n.Size == 0{
			delete(files,key)
		}else{
			if MaxKey < key {
				MaxKey = key
			}
			files[key] = fileInfo
		}
		return nil
	})
	var i uint64
	for i=0;i<=MaxKey;i++{
		if v,ok:=files[i];ok{
			fids=append(fids,v)//排序
		}
	}
	return fids,nil
}