package storage

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

var ErrTooManySlices = errors.New("too many ec slices")
var ErrSliceExists = errors.New("slice already exists")
func(s *Store)DowngradeVolume(vid,uuid,srcIp string,data []byte)(status int,err error){
	status = http.StatusOK
	volumeId, err := NewVolumeId(vid)
	if err != nil {
		glog.V(0).Infoln("downgrade err:", err,"vid:",vid,"uuid:",uuid)
		status = http.StatusBadRequest
		return
	}
	v := s.GetVolume(volumeId,"")
	if v == nil {
		err=errors.New("volume not find")
		glog.V(0).Infoln("downgrade err:", err,"vid:",vid,"uuid:",uuid)
		status = http.StatusNotFound
		return
	}
	if v.Mode == CopyDownMode {//该卷已经降级
		glog.V(0).Infoln("do not need to downgrade!","vid:",vid,"uuid:",uuid)
		return
	}
	err = s.CheckVolumeUUID(v.Id,uuid)
	if err != nil {
		glog.V(0).Infoln("downgrade err:", err,"vid:",vid,"uuid:",uuid)
		status = http.StatusBadRequest
		return
	}
	var idxData []byte = nil
	//if srcIp != s.PublicUrl{
	idxData,err = s.CopyIdxFile(srcIp,vid,s.Collection)
	if err != nil {
		glog.V(0).Infoln("downgrade err:", err,"vid:",vid,"uuid:",uuid)
		status = http.StatusInternalServerError
		return
	}
	//}
	filename:=v.FileName()
	rep:=v.ReplicaPlacement
	//写临时文件
	err = s.writeDowngradeTempFile(filename,rep ,data,idxData)
	if err != nil {
		glog.V(0).Infoln("write downgrade temp file err:", err,"vid:",vid,"uuid:",uuid)
		status = http.StatusInternalServerError
		return
	}
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	v.nm.Close()
	_ = v.dataFile.Close()
	defer func() {
		//不管成功与否，都要重新加载该卷
		err1 := v.load(true,false,true) //v.l.LoadVolume(volumeId,"",s.Collection)
		if err1 != nil {
			glog.V(0).Infoln("Load err",err1,uuid)
			status = http.StatusInternalServerError
		}
		if err == nil && err1 == nil {
			//返回成功
			glog.V(0).Infoln("downgrade success! vid:",vid,"uuid:",uuid)
			return
		}
		if err == nil && err1 != nil {
			err = err1
		}
	}()
	err = s.writeDowngradeVolume(filename)
	if err != nil {
		glog.V(0).Infoln("downgrade err:", err,"vid:",vid,"uuid:",uuid)
		status = http.StatusInternalServerError
	}
	return
}

func(s *Store)writeDowngradeTempFile(filename string,rep *ReplicaPlacement,data,idxData []byte) (err error) {
	var writeErr error
	defer func() {
		if writeErr != nil {
			_ = os.Remove(filename+".idx_down_temp")
			_ = os.Remove(filename+".dat_down_temp")
			err = writeErr
			return
		}
	}()
	idxFile, writeErr := os.OpenFile(filename+".idx_down_temp", os.O_WRONLY|os.O_CREATE, 0644)
	if writeErr != nil {
		glog.V(0).Infoln("failed to open idx file:",filename,writeErr)
		return writeErr
	}
	defer idxFile.Close()
	ret,writeErr := idxFile.Write(idxData)
	if writeErr != nil {
		glog.V(0).Infoln("failed to write idx file:",filename,writeErr)
		return writeErr
	}
	if ret != len(idxData){
		writeErr = errors.New("write err")
		glog.V(0).Infoln("failed to write idx file:",filename,writeErr,ret,len(idxData))
		return writeErr
	}
	idxFile.Sync()
	dataFile, writeErr := os.OpenFile(filename+".dat_down_temp", os.O_WRONLY|os.O_CREATE, 0644)
	if writeErr != nil {
		glog.V(0).Infoln("failed to open dat file:",filename,writeErr)
		return writeErr
	}
	defer dataFile.Close()
	writeErr = TransCopyStatus(dataFile,rep,CopyDownMode)
	if writeErr != nil {
		glog.V(0).Infoln("failed to rewrite dat file:", filename,writeErr)
		return writeErr
	}
	desc:=DescBlock{Size:len(data)}
	_,writeErr = dataFile.Write(desc.Bytes())
	if writeErr != nil {
		glog.V(0).Infoln("write datafile err:", filename,writeErr,string(desc.Bytes()))
		return writeErr
	}
	if len(data) % NeedlePaddingSize != 0{
		extra:= NeedlePaddingSize - len(data)% NeedlePaddingSize
		data=append(data,make([]byte,extra)...)
	}
	size,writeErr := dataFile.Write(data)
	if writeErr != nil {
		glog.V(0).Infoln("write datafile err:", filename,writeErr)
		return writeErr
	}
	if size != len(data){
		writeErr = errors.New("write err")
		glog.V(0).Infoln("write datafile err:", filename,size,len(data))
		return writeErr
	}
	dataFile.Sync()
	return
}

func(s *Store)writeDowngradeVolume(filename string) (err error) {
	var renameErr error
	defer func() {
		if renameErr != nil {
			err1 := os.Rename(filename+".dat_down",filename+".dat")
			if err1 != nil {
				glog.V(0).Infoln("reset dat file.err?",err1)
			}
			err1 = os.Rename(filename+".idx_down",filename+".idx")
			if err1 != nil {
				glog.V(0).Infoln("reset idx file.err?",err1)
			}
			err1 = os.Remove(filename+".idx_down_temp")
			if err1 != nil {
				glog.V(0).Infoln("remove temp idx file.err?",err1)
			}
			err1 = os.Remove(filename+".dat_down_temp")
			if err1 != nil {
				glog.V(0).Infoln("remove temp dat file.err?",err1)
			}
			err = renameErr
		}
	}()
	renameErr = os.Rename(filename+".dat",filename+".dat_down")
	if renameErr != nil {
		glog.V(0).Infoln("backup dat file err?",renameErr)
		return
	}
	renameErr = os.Rename(filename+".idx",filename+".idx_down")
	if renameErr != nil {
		glog.V(0).Infoln("backup idx file err?",renameErr)
		return
	}
	renameErr = os.Rename(filename+".dat_down_temp",filename+".dat")
	if renameErr != nil {
		glog.V(0).Infoln("reset dat file.err?",renameErr)
		return
	}
	renameErr = os.Rename(filename+".idx_down_temp",filename+".idx")
	if renameErr != nil {
		glog.V(0).Infoln("reset idx file.err?",renameErr)
		return
	}
	return
}

func(s *Store)SetVolumeDowngrading(vid VolumeId,v *Volume,action,reload bool,uuid,errMsg string)error{
	if action == true{
		err := v.volSysOpStatus.trySetDowngrade(uuid)
		if err != nil {
			return err
		}
		if reload == true{
			glog.V(0).Infoln("reload opstatus. uuid:", uuid)
			return nil
		}
		opstatus := &SysOpStatus{
			Uuid:      fmt.Sprintf("%s", uuid),
			Optype:    "downgrade",
			Result:    false,
			IsOver:    false,
			Starttime: time.Now(),
			Id:        int(vid),
			Mnt:       v.dir,
		}
		glog.V(0).Infoln("create opstatus uuid:", opstatus.Uuid, "Vid:", vid)
		v.volSysOpStatus.uuid = opstatus.Uuid
		s.AddSysOpStatus(opstatus,0)
	}else{
		if v.volSysOpStatus.status == VOL_DOWNGRADE {
			err:=v.volSysOpStatus.resetWithUUID(uuid)
			if err != nil {
				return err
			}
			s.SysOpLock.Lock()
			if _,ok:=s.SysOpHis[uuid];!ok{
				s.SysOpLock.Unlock()
				glog.V(0).Infoln("not such sysop history.uuid:", uuid)
				return nil
			}
			opstatus:=s.SysOpHis[uuid]
			//不管执行结果如何，最终要归还占位
			if errMsg == "" {
				opstatus.Result = true
				glog.V(0).Infoln("downgrade volume success.uuid:", uuid)
			} else {
				glog.V(0).Infoln("downgrade volume error:", errMsg, " uuid:", uuid)
				opstatus.Result = false
				opstatus.Err = errMsg
			}
			opstatus.IsOver = true
			opstatus.Endtime = time.Now()
			s.SysOpLock.Unlock()
		}
	}
	return nil
}

func(s *Store)SetVolumeUpgrading(vid VolumeId,v *Volume,action bool,uuid,errMsg string)error{
	if action == true{
		err:=v.volSysOpStatus.trySetUpgrade(uuid)
		if err != nil {
			return err
		}
		opstatus := &SysOpStatus{
			Uuid:      fmt.Sprintf("%s", uuid),
			Optype:    "upgrade",
			Result:    false,
			IsOver:    false,
			Starttime: time.Now(),
			Id:        int(vid),
			Mnt:       v.dir,
		}
		glog.V(0).Infoln("create opstatus uuid:", opstatus.Uuid, "Vid:", vid)
		v.volSysOpStatus.uuid = opstatus.Uuid
		s.AddSysOpStatus(opstatus,0)
	}else{
		if v.volSysOpStatus.status == VOL_UPGRADE {
			err:=v.volSysOpStatus.resetWithUUID(uuid)
			if err != nil {
				return err
			}
			s.SysOpLock.Lock()
			if _,ok:=s.SysOpHis[uuid];!ok{
				s.SysOpLock.Unlock()
				glog.V(0).Infoln("not such sysop history.uuid:", uuid)
				return nil
			}
			opstatus:=s.SysOpHis[uuid]
			//不管执行结果如何，最终要归还占位
			if errMsg == "" {
				opstatus.Result = true
				glog.V(0).Infoln("upgrade volume success.uuid:", uuid)
			} else {
				glog.V(0).Infoln("upgrade volume error:", errMsg, " uuid:", uuid)
				opstatus.Result = false
				opstatus.Err = errMsg
			}
			opstatus.IsOver = true
			opstatus.Endtime = time.Now()
			s.SysOpLock.Unlock()
		}
	}
	return nil
}

//专门用于EC卷的修复
func(s *Store)SetVolumeRepairing(vid VolumeId,serialNum string,v *Volume,action bool,uuid,errMsg string)error{
	if action == true{
		err:=v.volSysOpStatus.trySetECREP(uuid)
		if err != nil {
			return err
		}
		opstatus := &SysOpStatus{
			Uuid:      fmt.Sprintf("%s", uuid),
			Optype:    "repair",
			Result:    false,
			IsOver:    false,
			Sn:        serialNum,
			Starttime: time.Now(),
			Id:        int(vid),
		}
		glog.V(0).Infoln("create opstatus uuid:", opstatus.Uuid, "Vid:", vid)
		v.volSysOpStatus.uuid = opstatus.Uuid
		s.AddSysOpStatus(opstatus,0)
	}else{
		if v.volSysOpStatus.status == VOL_ECREP {
			err:=v.volSysOpStatus.resetWithUUID(uuid)
			if err != nil {
				return err
			}
			s.SysOpLock.Lock()
			if _,ok:=s.SysOpHis[uuid];!ok{
				s.SysOpLock.Unlock()
				glog.V(0).Infoln("not such sysop history.uuid:", uuid)
				return nil
			}
			opstatus:=s.SysOpHis[uuid]
			//不管执行结果如何，最终要归还占位
			if errMsg == "" {
				opstatus.Result = true
				glog.V(0).Infoln("do repair ec volume success.uuid:", uuid)
			} else {
				glog.V(0).Infoln("do repair ec volume error:", errMsg, " uuid:", uuid)
				opstatus.Result = false
				opstatus.Err = errMsg
			}
			opstatus.IsOver = true
			opstatus.Endtime = time.Now()
			s.SysOpLock.Unlock()
		}
	}
	return nil
}

func(s *Store)CheckVolumeUUID(vid VolumeId,uuid string)error{
	v := s.findVolume(vid,"")
	if v == nil {
		return ErrVolumeNotFound
	}
	return v.volSysOpStatus.checkUUID(uuid)
}

func(s *Store)CopyIdxFile(srcIp,vid,collection string) (data []byte,err error) {
	var offset=""
	var size = ""
	var idx = 0
	var readSize=1024*1024*10
	for {
		var tempData []byte
		var err1 error
		offset=fmt.Sprintf("%d",idx*readSize)
		size=fmt.Sprintf("%d",(idx+1)*readSize)
		for i:=0;i<operation.MAX_RETRY_NUM;i++{
			tempData = tempData[:0]
			var resp *http.Response
			visitUrl := "http://" + srcIp + "/volume/read" +"?vid="+vid+"&collection="+collection+"&offset="+offset+"&size="+size+"&type="+"idx"
			resp,err1 = util.MakeHttpRequest(visitUrl,"GET",nil,nil,false)
			if err1 != nil {
				glog.V(0).Infoln("volumeRead 1 err!",err1,"url:",visitUrl)
				continue
			}else{
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK{
					err1 = errors.New("read volume err")
					glog.V(0).Infoln("volumeRead 3 err!",err1,resp.StatusCode,"url:",visitUrl)
					continue
				}else{
					tempData,err1 = ioutil.ReadAll(resp.Body)
					if err1 != nil {
						glog.V(0).Infoln("volumeRead 2 err!",err1,"url:",visitUrl)
						continue
					}
					glog.V(0).Infoln("volumeRead ok!size:",len(tempData)," visitUrl:",visitUrl)
					break
				}
			}
		}
		if err1 != nil {
			err = err1
			return
		}
		if len(tempData) == 0{
			break
		}
		data = append(data,tempData...)
		idx++
	}
	return
}

//将已降级的卷恢复为副本模式 jjj
func (s *Store)UpgradeCopyDownVolume(v *Volume,uuid,reqid string,interval int)(status int,err error){
	if v.Mode != CopyDownMode {
		glog.V(0).Infoln("do no need to upgrade! uuid:",uuid)
		return
	}
	err = s.CheckVolumeUUID(v.Id,uuid)
	if err != nil {
		glog.V(0).Infoln("upgrade err:", err,"uuid:",uuid)
		status = http.StatusBadRequest
		return
	}
	fi, err := v.dataFile.Stat()
	if err != nil {
		glog.V(0).Infoln("failed to stat volume file", err)
		status = http.StatusInternalServerError
		return
	}
	descSize,err:=v.readDescBlock()
	if err != nil {
		status = http.StatusInternalServerError
		glog.V(0).Infoln("read descBlock err:", err)
		return
	}
	oldDataSize := SuperBlockSize+DescSize+int64(descSize) //超级块，8字节表示描述文件的大小，描述文件大小

	if descSize % NeedlePaddingSize != 0{
		extra:= NeedlePaddingSize - descSize % NeedlePaddingSize
		oldDataSize+=int64(extra)
	}

	filePath := v.FileName()
	ips:=make([]string,0)
	s.NodeLock.RLock()
	for k,_:=range s.NodeList{
		ips = append(ips,k)
	}
	s.NodeLock.RUnlock()
	if v.Desc.Count < 1 || len(v.Desc.ObjectParts) < 0{
		status = http.StatusInternalServerError
		err = errors.New("bad copydown desc")
		glog.V(0).Infoln("invalid desc,bad copydown dat file")
		return
	}
	var dst  *os.File
	if dst, err = os.OpenFile(filePath+".dat_up_temp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		glog.V(0).Infoln("create temp file err:",err)
		status = http.StatusInternalServerError
		return
	}
	defer func() {
		if err != nil {
			dst.Close()
			os.Remove(filePath+".dat_up_temp")
		}
	}()

	firstBlockSize:=v.Desc.ObjectParts[0].Size
	var sizeSum int64
	for i:=0;i<v.Desc.Count;i++{
		if interval != 0 {
			time.Sleep(time.Millisecond * time.Duration(interval))
		}
		if v.volSysOpStatus.forceQuit == true {
			err = errors.New("force quit")
			glog.V(0).Infoln("force quit!uuid:",uuid)
			status = http.StatusBadRequest
			return
		}
		start:=int64(i)*firstBlockSize
		end := int64(i+1)*firstBlockSize
		if end > v.Desc.Size{
			end = v.Desc.Size
		}
		ranges := "bytes="+fmt.Sprintf("%d-%d",start,end-1)
		data,err1 := ReadCopyDownVolume(ranges,reqid,v.Desc,ips)
		if err1 != nil {
			err = err1
			glog.V(0).Infoln("read copy down volume err:",err)
			status = http.StatusInternalServerError
			return
		}
		if len(data) == 0 {
			break
		}
		sizeSum+=int64(len(data))
		glog.V(1).Infoln("read size:",len(data),sizeSum,"count:",i,"ranges:",ranges)
		_,err1=dst.Write(data)
		if err1 != nil {
			err = err1
			glog.V(0).Infoln("write cpd file err:",err)
			status = http.StatusInternalServerError
			return
		}
		_=dst.Sync()
	}
	if sizeSum != v.Desc.Size {
		err = errors.New("size not matched")
		glog.V(0).Infoln("size not matched:",sizeSum,v.Desc.Size)
		status = http.StatusInternalServerError
		return
	}
	fi, err = v.dataFile.Stat()
	if err != nil {
		glog.V(0).Infoln("failed to stat volume file", err)
		status = http.StatusInternalServerError
		return 0,err
	}
	newDataSize := fi.Size() //获取目前大小
	needSize:=newDataSize-oldDataSize
	glog.V(1).Infoln("newDataSize", newDataSize,"oldDataSize:",oldDataSize)
	if needSize > 0 {
		_,err = v.dataFile.Seek(oldDataSize,os.SEEK_SET)
		if err != nil {
			glog.V(0).Infoln("seed err", err)
			status = http.StatusInternalServerError
			return
		}
		var buf = make([]byte,needSize)
		ret,err:=v.dataFile.Read(buf)
		if int64(ret) != needSize {
			err = errors.New("invalid read size")
			glog.V(0).Infoln("Read err", ret,needSize)
			status = http.StatusInternalServerError
			return status,err
		}
		_,err = dst.Write(buf)
		if err != nil {
			glog.V(0).Infoln("write err", err)
			status = http.StatusInternalServerError
			return status,err
		}
	}
	_=dst.Sync()
	_=dst.Close()
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	_=v.dataFile.Close()
	v.nm.Close()
	defer func() {
		err = v.load(true,false,true)//重新加载dat文件 jj
		if err != nil {
			glog.V(0).Infoln("load vid err", v.Id,err)
			status = http.StatusInternalServerError
			return
		}
	}()
	err = os.Rename(filePath+".dat",filePath+".dat_up")
	if err != nil {
		glog.V(0).Infoln("rename err", err)
		status = http.StatusInternalServerError
		return
	}

	defer func() {
		if err != nil { //重命名失败，尝试重命名回来
			err1 := os.Rename(filePath+".dat_up",filePath+".dat")
			if err1 != nil {
				glog.V(0).Infoln("rename err:",err1)
			}
		}
	}()
	err = os.Rename(filePath+".dat_up_temp",filePath+".dat")
	if err != nil {
		glog.V(0).Infoln("rename err", err)
		status = http.StatusInternalServerError
		return
	}
	v.readOnly = false
	return
}

func(s *Store) RecoverDowngradedVolume(vid,uuid string) (status int,err error) {
	status = http.StatusOK
	volumeId, err := NewVolumeId(vid)
	if err != nil {
		glog.V(0).Infoln("parsing error:", err)
		status = http.StatusBadRequest
		return
	}
	err = s.CheckVolumeUUID(volumeId,uuid)
	if err != nil {
		glog.V(0).Infoln("invalid uuid!", err)
		status = http.StatusBadRequest
		return
	}
	v := s.GetVolume(volumeId,"")
	if v == nil {
		err=errors.New("volume not find")
		glog.V(0).Infoln("volume not find",vid)
		status = http.StatusNotFound
		return
	}
	if v.Collection == public.EC {
		err=errors.New("cannot recover downgrade for ec volume")
		status = http.StatusBadRequest
		return
	}
	if v.Mode == CopyMode {
		return
	}
	filename:=v.FileName()
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	v.nm.Close()
	_ = v.dataFile.Close()
	v.readOnly = false
	defer func() {
		err1 := v.load(true,false,true) //v.l.LoadVolume(volumeId,"",s.Collection)
		if err1 != nil {
			glog.V(0).Infoln("LoadVolume err",err1,vid)
			status = http.StatusInternalServerError
		}
		if err == nil && err1 == nil {
			_=s.SetVolumeDowngrading(volumeId,v,false,false,uuid,"recoverDowngraded")
			return
		}
		if err == nil && err1 != nil {
			err = err1
		}
		return
	}()
	//将已经降级的bak下
	_=os.Rename(filename+".dat",filename+".dat_down_bak")
	_=os.Rename(filename+".idx",filename+".idx_down_bak")

	//将未降级的备份文件重新启用
	err = os.Rename(filename+".dat_down",filename+".dat")
	if err != nil {
		err = os.Rename(filename+".dat_down",filename+".dat")
		if err != nil {
			glog.V(0).Infoln("rename err:",err)
			status = http.StatusInternalServerError
			return status,err
		}
	}
	err = os.Rename(filename+".idx_down",filename+".idx")
	if err != nil {
		err = os.Rename(filename+".idx_down",filename+".idx")
		if err != nil {
			glog.V(0).Infoln("rename err",err)
			status = http.StatusInternalServerError
			return status,err
		}
	}
	return
}

func (s *Store) RecoverUpgradedVolume(vid,uuid string) (status int,err error) {
	volumeId, err := NewVolumeId(vid)
	if err != nil {
		glog.V(0).Infoln("parsing error:", err)
		status = http.StatusBadRequest
		return
	}
	v := s.GetVolume(volumeId,"")
	if v == nil {
		err = errors.New("volume not find")
		glog.V(0).Infoln("volume not find",vid)
		status = http.StatusNotFound
		return
	}
	err = s.CheckVolumeUUID(volumeId,uuid)
	if err != nil {
		glog.V(0).Infoln("CheckVolumeUUID err:", err,uuid)
		status = http.StatusBadRequest
		return
	}
	if v.Collection == public.EC {
		err = errors.New("cannot recover upgrade for ec volume")
		status = http.StatusBadRequest
		return
	}
	if v.Mode == CopyDownMode { //该卷已经降级
		return
	}
	filename:=v.FileName()
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	v.nm.Close()
	_ = v.dataFile.Close()
	defer func() {
		err1 := v.load(true,false,true)
		if err1 != nil {
			glog.V(0).Infoln("Load err",err1,uuid)
			status = http.StatusInternalServerError
		}
		if err == nil && err1 == nil {
			_=s.SetVolumeUpgrading(volumeId,v,false,uuid,"recoverUpgraded")
			return
		}
		if err == nil && err1 != nil {
			err = err1
		}
	}()
	err = os.Rename(filename+".dat_up",filename+".dat")
	if err != nil {
		err = os.Rename(filename+".dat_up",filename+".dat")
		if err != nil {
			glog.V(0).Infoln("rename err",err,uuid)
			status = http.StatusInternalServerError
			return
		}
	}
	return
}

func GetECSliceLimit(masterIp string) (int,error) {
	var config string
	for i:=0;i<5;i++{
		if i>0{
			time.Sleep(time.Second*time.Duration(i))
		}
		url := "http://" + masterIp + "/sdoss/getEcConfig"+"?type=sliceLimit"
		data, err := util.PostBytes_timeout(url, nil)
		if  err != nil {
			glog.V(0).Infoln("getEcConfig from master:", masterIp,"err:",err,"times:",i)
			continue
		}
		err = json.Unmarshal(data,&config)
		if err != nil {
			glog.V(0).Infoln("Unmarshal err:",err, "master:", masterIp,"times:",i)
			continue
		}
		break
	}
	limit:=util.ParseInt(config,0)
	if limit == 0 {
		glog.V(0).Infoln("get ec sliceLimit err,config:",config)
		return 0,errors.New("can not get sliceLimit")
	}
	return limit,nil
}

func (s *Store)GetECSliceLimit()(error){
	if s.ECSliceLimit > 0 {
		return nil
	}
	masterNode, e := s.masterNodes.FindMaster()
	if e != nil {
		glog.V(0).Infoln("find master node err:", e)
		return errors.New("find master node err")
	}
	sliceLimit,err:=GetECSliceLimit(masterNode)
	if err == nil {
		s.ECSliceLimit = sliceLimit
	}
	return err
}

func (s *Store)CheckECSliceLimit(vid VolumeId,sn string)(err error){
	if sn == "" {
		return errors.New("bad serialNum")
	}
	err = s.GetECSliceLimit()
	if err != nil {
		glog.V(0).Infoln("get ec slice limit err:", err)
		return errors.New("get ec sliceLimit err")
	}
	sliceNum:=0
	for _, location := range s.Locations {
		location.RLock()
		if v, found := location.volumes[vid]; found {
			if v.SerialNum == sn{
				location.RUnlock()
				glog.V(0).Infoln("volume slice already exists! vid:",vid,"serialNum:",sn)
				return ErrSliceExists
			}
			sliceNum++
		}
		if v, found := location.sysOpvolume[vid]; found {
			if v.SerialNum == sn{
				location.RUnlock()
				glog.V(0).Infoln("volume slice already progressing! vid:",vid,"serialNum:",sn)
				return errors.New("slice already progressing")
			}
			sliceNum++
		}
		location.RUnlock()
	}
	if sliceNum >= s.ECSliceLimit {
		glog.V(0).Infoln("ec volume has too many slices on this node.vid:",vid,"target serialNum:",sn,"sliceNum:",sliceNum,"limit:",s.ECSliceLimit)
		err = ErrTooManySlices
	}
	return err
}
