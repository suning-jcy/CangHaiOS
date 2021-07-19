package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/stats"
)

var (
	ErrFinishScan      = errors.New("finish scanning file!")
	ErrDataCorrup      = errors.New("data corrupted!")
	ErrSkipMaxNeedle   = errors.New("skip more than 3 times")
	ErrCpXRemain       = errors.New("cpd or cpx remain")
	ErrVacuumNoSpace   = errors.New("no sufficient space")
	ErrVacuumThreshold = errors.New("no need to vacuum")
	ErrVacuumBusy      = errors.New("vacuum busy")
)

const (
	VACUUM_DEF = "0"
	VACUUM_IDX = "1"
	VACUUM_MIX = "2"
)
const (
	VACUUM_MAX_SKIP = 3
)

func (v *Volume) garbageLevel() float64 {
	if v.Mode == CopyDownMode{
		return float64(v.nm.DeletedSize()) / float64(v.Desc.Size)
	}
	return float64(v.nm.DeletedSize()) / float64(v.ContentSize())
}

func (v *Volume) Compact(method string) (err error) {
	v.accessLock.Lock()
	//	defer v.accessLock.Unlock()

	glog.V(3).Infof("Compacting ...")
	filePath := v.FileName()

	fi, err := v.dataFile.Stat()
	if err != nil {
		glog.V(0).Infoln("failed to stat volume file", err)
		v.accessLock.Unlock()
		return err
	}
	v.volSysOpStatus.vacuumstatus.OldDataSize = fi.Size()
	v.volSysOpStatus.vacuumstatus.CompactSize = v.nm.ContentSize() - v.nm.DeletedSize()
	v.volSysOpStatus.vacuumstatus.CompactFileCount = v.nm.FileCount() - 2*v.nm.DeletedCount()
	idxFile, err := os.Open(v.FileName() + ".idx")
	if err != nil {
		glog.V(0).Infoln("failed to open idx file", v.FileName(), err)
		v.accessLock.Unlock()
		return err
	}

	if v.volSysOpStatus.vacuumstatus.oldNM, err = LoadNeedleMap(idxFile); err != nil {
		idxFile.Close()
		glog.V(0).Infoln("failed to load needle map", err)
		v.accessLock.Unlock()
		return err
	}
	defer v.volSysOpStatus.vacuumstatus.oldNM.Close()
	idxIndicatedLen, err := GetIdxIndicatedLen(idxFile)
	if err != nil {
		glog.V(0).Infoln("failed to calculate idxIndicatedLen", err)
		v.accessLock.Unlock()
		return err
	}
	if idxIndicatedLen < fi.Size() {
		glog.V(0).Infof("dat file len %d indicated by idx is less than dat file len %d", idxIndicatedLen, fi.Size())
		v.accessLock.Unlock()
		return fmt.Errorf(" dat len indicated by idx %d less than dat file len %d", idxIndicatedLen, fi.Size())
	}
	fi, err = v.nm.(*NeedleMap).indexFile.Stat()
	if err != nil {
		glog.V(0).Infoln("failed to stat volume file", err)
		v.accessLock.Unlock()
		return err
	}
	v.volSysOpStatus.vacuumstatus.OldIdxSize = fi.Size()
	glog.V(3).Infof("creating copies for volume %d ,last offset %d...", v.Id, v.volSysOpStatus.vacuumstatus.OldIdxSize)
	v.accessLock.Unlock()
	v.Sync()
	err = v.copyDataAndGenerateIndexFile(filePath+".cpd", filePath+".cpx", method)
	if err != nil {
		glog.V(0).Infoln("failed to copy volume file", err)

		v.DeleteCompactFileOnVacuumFail()
	}
	return err
}
func (v *Volume) DeleteCompactFileOnVacuumFail() error {
	filePath := v.FileName()
	glog.V(3).Infof("Remove cpx file for volume %d ...", v.Id)
	if err := os.Remove(filePath + ".cpd"); err != nil {
		if os.IsNotExist(err) == false {
			return err
		}
	}
	if err := os.Remove(filePath + ".cpx"); err != nil {
		if os.IsNotExist(err) == false {
			return err
		}
	}
	return nil
}
func (v *Volume) commitCompact() error {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	glog.V(3).Infof("Committing vacuuming...")
	fi, err := v.dataFile.Stat()
	if fi == nil || err != nil{
		glog.V(0).Infoln("stat data file failed:",err)
		return err
	}
	if cpdFile, err := os.OpenFile(v.FileName()+".cpd", os.O_WRONLY, 0); err != nil {
		return err
	} else {
		if fi.Size() > v.volSysOpStatus.vacuumstatus.OldDataSize {
			extraBytes := make([]byte, fi.Size()-v.volSysOpStatus.vacuumstatus.OldDataSize)
			glog.V(0).Info("Moving extra data on volume ", v.FileName(), ",size: ", len(extraBytes), "B")
			v.dataFile.ReadAt(extraBytes, v.volSysOpStatus.vacuumstatus.OldDataSize)
			cpdFile.Seek(0, os.SEEK_END)
			if n, err := cpdFile.Write(extraBytes); err != nil {
				glog.V(0).Infoln("Write dat error", err)
			} else {
				glog.V(0).Infoln("Write Size", n)
			}
		}
		cpdFile.Close()
	}

	if cpxFile, err := os.OpenFile(v.FileName()+".cpx", os.O_WRONLY, 0); err != nil {
		return err
	} else {
		if fi, _ := v.nm.(*NeedleMap).indexFile.Stat(); fi.Size() > v.volSysOpStatus.vacuumstatus.OldIdxSize {
			extraBytes := make([]byte, fi.Size() - v.volSysOpStatus.vacuumstatus.OldIdxSize)
			glog.V(0).Info("Moving extra index on volume ", v.FileName(), ",size: ", len(extraBytes), "B")
			v.nm.(*NeedleMap).indexFile.ReadAt(extraBytes, v.volSysOpStatus.vacuumstatus.OldIdxSize)
			cpxFile.Seek(0, os.SEEK_END)
			if n, err := cpxFile.Write(extraBytes); err != nil {
				glog.V(0).Infoln("Write idx error", err)
			} else {
				glog.V(0).Infoln("Write Size", n)
			}
		}
		cpxFile.Close()
	}

	_ = v.dataFile.Close()
	isOldDiskFile := false
	var e error
	if e = os.Rename(v.FileName()+".dat", v.FileName()+".dat.bak"); e != nil {
		glog.V(0).Infoln("vacuum commit:failed to bak dat", v.FileName(), e)
		return e
	}
	if e = os.Rename(v.FileName()+".idx", v.FileName()+".idx.bak"); e != nil {
		glog.V(0).Infoln("vacuum commit:failed to bak idx", v.FileName(), e)
		return e
	}
	if e = os.Rename(v.FileName()+".cpd", v.FileName()+".dat"); e == nil {
		if e = os.Rename(v.FileName()+".cpx", v.FileName()+".idx"); e != nil {
			glog.V(0).Infoln("vacuum commit:failed to mv cpx to idx", v.FileName(), e)
		} else {
			isOldDiskFile = true
		}
	} else {
		glog.V(0).Infoln("vacuum commit:failed to mv cpd to dat", v.FileName(), e)
	}
	if e != nil {
		if e = os.Rename(v.FileName()+".dat.bak", v.FileName()+".dat"); e != nil {
			glog.V(0).Infoln("vacuum commit:failed to move .dat.bak to dat", v.FileName(), e)
			return e
		}
		if e = os.Rename(v.FileName()+".idx.bak", v.FileName()+".idx"); e != nil {
			glog.V(0).Infoln("vacuum commit:failed to idx.bak to  idx", v.FileName(), e)
			return e
		}
	}
	glog.V(0).Infoln("try to load vid",v.Id.String()," isOldDiskFile",isOldDiskFile)
	if e = v.load(true, false,false); e != nil && !isOldDiskFile {
		if e = os.Rename(v.FileName()+".dat", v.FileName()+".cpd"); e != nil {
			glog.V(0).Infoln("vacuum commit:failed to load volume, now move .dat to .cpd", v.FileName(), e)
			return e
		}
		if e = os.Rename(v.FileName()+".idx", v.FileName()+".cpx"); e != nil {
			glog.V(0).Infoln("vacuum commit:failed to load volume, now move .idx to .cpx", v.FileName(), e)
			return e
		}
		if e = os.Rename(v.FileName()+".dat.bak", v.FileName()+".dat"); e != nil {
			glog.V(0).Infoln("vacuum commit:failed to mv dat.bak to dat", v.FileName(), e)
			return e
		}
		if e = os.Rename(v.FileName()+".idx.bak", v.FileName()+".idx"); e != nil {
			glog.V(0).Infoln("vacuum commit:failed to mv idx.bak to idx", v.FileName(), e)
			return e
		}
		if e = v.load(true, false,false); e != nil {
			glog.V(0).Infoln("vacuum commit:falied to load volume for the 2nd time", e)
			return e
		}
		if e = os.Remove(v.FileName() + ".cpd"); e != nil {
			glog.V(0).Infoln("vacuum commit:failed to remove cpd file", v.FileName(), e)
			return e
		}
		if e = os.Remove(v.FileName() + ".cpx"); e != nil {
			glog.V(0).Infoln("vacuum commit:failed to remove cpx file", v.FileName(), e)
			return e
		}
	} else {
		if e = os.Remove(v.FileName() + ".dat.bak"); e != nil {
			glog.V(0).Infoln("vacuum commit:failed to remove bak file", v.FileName()+".dat.bak", e)
			return e
		}
		if e = os.Remove(v.FileName() + ".idx.bak"); e != nil {
			glog.V(0).Infoln("vacuum commit:failed to remove bak file", v.FileName()+".idx.bak", e)
			return e
		}
	}
	return nil
}

func (v *Volume) copyDataAndGenerateIndexFile(dstName, idxName, method string) (err error) {
	switch method {
	case VACUUM_DEF:
		err = v.copyByDefault(dstName, idxName)
	case VACUUM_IDX:
		err = v.copyByIndex(dstName, idxName)
	case VACUUM_MIX:
		err = v.copyByMix(dstName, idxName)
	default:
		err = v.copyByDefault(dstName, idxName)
	}
	return err
}

//能不能回收
//剩余空间要足
//同文件夹不能有cpd cpx 文件
func (v *Volume) DiskCanVacuum(volumeSizeLimit uint64) error {
	disk := stats.NewDiskStatus(v.dir)
	if volumeSizeLimit > 0 && volumeSizeLimit > disk.Available {
		glog.V(0).Infoln("faild to check", volumeSizeLimit, disk.Available, ErrVacuumNoSpace,v.Id.String())
		return ErrVacuumNoSpace
	}
	err := filepath.Walk(v.dir, func(path string, info os.FileInfo, err error) error {
		if info == nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(info.Name(), ".cpd") {
			glog.V(0).Infoln("failed to check:", ErrCpXRemain, info.Name())
			return ErrCpXRemain
		}
		if strings.HasSuffix(info.Name(), ".cpx") {
			glog.V(0).Infoln("failed to check:", ErrCpXRemain, info.Name())
			return ErrCpXRemain
		}
		return nil
	})
	return err
}
func (v *Volume) IsIdxDatConsistent() error {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	info, err := v.dataFile.Stat()
	idxFile, err := os.Open(v.FileName() + ".idx")
	if err != nil {
		glog.V(0).Infoln("failed to open idx file", v.FileName(), err)
		return err
	}
	idxIndicatedLen, err := GetIdxIndicatedLen(idxFile)
	if err != nil {
		glog.V(0).Infoln("failed to get idx indicated dat len", err)
		idxFile.Close()
		return err
	}
	defer idxFile.Close()
	if idxIndicatedLen != info.Size() {
		return fmt.Errorf("volume %s on disk %s may corrupted", v.Id.String(), v.dir)
	}
	return nil

}
func (v *Volume) copyByDefault(dstName, idxName string) (err error) {
	var (
		dst, idx *os.File
	)
	if dst, err = os.OpenFile(dstName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return
	}
	defer dst.Close()
	defer dst.Sync()

	if idx, err = os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return
	}
	defer idx.Close()

	nm := NewNeedleMap(idx)
	new_offset := int64(SuperBlockSize)
	err = ScanVolumeFile(v.dir, v.Collection,v.SerialNum, v.Id, func(superBlock SuperBlock) error {
		_, err = dst.Write(superBlock.Bytes())
		return err
	}, true, func(n *Needle, offset int64) error {
		//glog.V(0).Infoln("offset", offset)

		nv, ok := v.nm.Get(n.Id)
		glog.V(4).Infoln("needle expected offset ", offset, "ok", ok, "nv", nv)
		if ok && int64(nv.Offset)*NeedlePaddingSize == offset && nv.Size > 0 {
			if _, err = nm.Put(n.Id, uint32(new_offset/NeedlePaddingSize), n.Size); err != nil {
				return fmt.Errorf("cannot put needle: %s", err)
			}
			if _, err = n.Append(dst, v.Version(),1); err != nil {
				return fmt.Errorf("cannot append needle: %s", err)
			}
			new_offset += n.DiskSize()
			glog.V(3).Infoln("saving key", n.Id, "volume offset", offset, "=>", new_offset, "data_size", n.Size)
		}
		return nil
	})
	if err != nil && err != ErrFinishScan {
		glog.V(0).Infoln("failed to vacuum compact:", err)
		return err
	}
	if nm.ContentSize() < v.volSysOpStatus.vacuumstatus.CompactSize {
		glog.V(0).Infoln("dat may have been corrupted when compact", v.Id)
		return fmt.Errorf("Compact %s: cbd size%d < expected size%d", v.Id.String(), nm.ContentSize(), v.volSysOpStatus.vacuumstatus.CompactSize)
	}
	if nm.FileCount()-nm.DeletedCount() < v.volSysOpStatus.vacuumstatus.CompactFileCount {
		glog.V(0).Infoln("dat may have been corrupted when compact", v.Id)
		return fmt.Errorf("Compact %s: cbd file count %d less than expceted file count %d", v.Id.String(), nm.FileCount(), v.volSysOpStatus.vacuumstatus.CompactFileCount)
	}
	if fstat, err := idx.Stat(); err == nil {
		nm.Offset = fstat.Size()
		v.volSysOpStatus.vacuumstatus.nmCPD = nm
	}
	return
}
func (v *Volume) copyByIndex(dstName, idxName string) (err error) {
	var (
		dst, idx *os.File
	)
	if dst, err = os.OpenFile(dstName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return
	}
	defer dst.Close()
	defer dst.Sync()
	if idx, err = os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return err
	}
	defer idx.Close()
	nm := NewNeedleMap(idx)
	new_offset := int64(SuperBlockSize)
	skipNeedleCount := 0
	err = ScanVolumeFileByIdx(v.volSysOpStatus.vacuumstatus.oldNM, v.dir, v.Collection,v.SerialNum, v.Id, v.volSysOpStatus.vacuumstatus.OldDataSize, func(superBlock SuperBlock) error {
		_, err = dst.Write(superBlock.Bytes())
		return err
	}, true, func(n *Needle, offset int64, size uint32, isSkip bool) error {
		if isSkip {
			skipNeedleCount++
			if skipNeedleCount > VACUUM_MAX_SKIP {
				glog.V(0).Infoln("failed to vacuum compact:", ErrBadNeedleData)
				return ErrSkipMaxNeedle
			}
			v.volSysOpStatus.vacuumstatus.CompactFileCount = v.volSysOpStatus.vacuumstatus.CompactFileCount - 1
			v.volSysOpStatus.vacuumstatus.CompactSize = v.volSysOpStatus.vacuumstatus.CompactSize - uint64(size)
			return nil
		}

		if _, err = nm.Put(n.Id, uint32(new_offset/NeedlePaddingSize), n.Size); err != nil {
			return fmt.Errorf("cannot put needle: %s", err)
		}
		new_offset += n.DiskSize()
		if new_offset > v.volSysOpStatus.vacuumstatus.OldDataSize {
			return fmt.Errorf("cannot append needle: %s", err)
		}
		if _, err = n.Append(dst, v.Version(),1); err != nil {
			return fmt.Errorf("cannot append needle: %s", err)
		}

		glog.V(3).Infoln("saving key", n.Id, "volume offset", offset, "=>", new_offset, "data_size", n.Size)
		return nil
	})
	if err != nil {
		glog.V(0).Infoln("failed to vacuum compact:", err)
		return err
	}
	if nm.ContentSize() < v.volSysOpStatus.vacuumstatus.CompactSize {
		glog.V(0).Infoln("dat may have been corrupted when compact", v.Id)
		return fmt.Errorf("Compact %s: cbd size%d < expected size%d", v.Id.String(), nm.ContentSize(), v.volSysOpStatus.vacuumstatus.CompactSize)
	}
	if nm.FileCount()-nm.DeletedCount() < v.volSysOpStatus.vacuumstatus.CompactFileCount {
		glog.V(0).Infoln("dat may have been corrupted when compact", v.Id)
		return fmt.Errorf("Compact %s: cbd file count %d less than expceted file count %d", v.Id.String(), nm.FileCount(), v.volSysOpStatus.vacuumstatus.CompactFileCount)
	}
	if fstat, err := idx.Stat(); err == nil {
		nm.Offset = fstat.Size()
		v.volSysOpStatus.vacuumstatus.nmCPD = nm
	}
	return nil
}
func (v *Volume) copyByMix(dstName, idxName string) (err error) {
	if err = v.copyByDefault(dstName, idxName); err != nil {
		glog.V(0).Infoln("try to compact dat by default method, failed for", err, "try method by idx")
		if err = v.copyByIndex(dstName, idxName); err != nil {
			glog.V(0).Infoln("failed to vacuum copmact when try method by idx:", err)
			return err
		}
	}
	return nil
}
