package storage

import (
	"bytes"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sync"
	_ "syscall"
	"time"

	"code.google.com/p/weed-fs/go/glog"
)

const (
	SuperBlockSize = 8

	NotCompact    = 0
	PreCompact    = 1
	UnderCompact  = 2
	CommitCompact = 3

	NotRebalance    = 0
	PreRebalance    = 1
	UnderRebalance  = 2
	CommitRebalance = 3

	VOL_REPAIR_NOT_REPAIR        = 0
	VOL_REPAIR_UNDER_REPAIR      = 1
	VOL_REPAIR_TRANSFER_DAT_FAIL = 2
	VOL_REPAIR_TRANSFER_IDX_FAIL = 3
	VOL_REPAIR_TRANSFER_OKAY     = 4
	VOL_REPAIR_COMMIT_REPAIR     = 5
	FlagBadMask                  = uint64(0xFF00000000000000)

	CopyMode = 0
	ECMode = 1
	CopyDownMode = 2
	DescSize = 8
)

type SuperBlock struct {
	Version          Version
	ReplicaPlacement *ReplicaPlacement
	Mode             byte  //用来区分存储模式 0:副本卷 1:ec卷  2:副本卷,但已经被降级为EC【使用EC切割了副本卷】
}

func (s *SuperBlock) Bytes() []byte {
	header := make([]byte, SuperBlockSize)
	header[0] = byte(s.Version)
	header[1] = s.ReplicaPlacement.Byte()
	header[2] = s.Mode
	return header
}
type DescBlock struct {
	Size int
}
func (d *DescBlock) Bytes() []byte {
	header := make([]byte, DescSize)
	header = []byte(fmt.Sprintf("%08x",d.Size))
	return header
}

type Volume struct {
	Id         VolumeId
	dir        string
	Collection string
	SerialNum  string  //序列号，区分一组卷中的不同卷
	dataFile   *os.File
	nm         NeedleMapper
	readOnly   bool

	SuperBlock

	accessLock sync.Mutex

	//volume 的系统操作，包括修复/均衡/回收，状态的判定等都在这个状态内进行
	volSysOpStatus *VolStausSt
	Desc  *operation.ECMultiPartObject
	notJoin bool
}

func NewVolume(dirname string, collection ,serialNum string, id VolumeId, replicaPlacement *ReplicaPlacement) (v *Volume, e error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id, SerialNum: serialNum}
	v.volSysOpStatus = NewVolStausSt(id, collection, dirname, replicaPlacement,0) // &VolStausSt{status: VOL_FRE}
	v.volSysOpStatus.repairstatus = &RepRepairStaus{}
	v.volSysOpStatus.rebalancestatus = &RebalanceStaus{}
	v.volSysOpStatus.vacuumstatus = &VacuumStaus{}
	var mode = byte(CopyMode)
	if (collection == public.EC || collection == public.ECTemp)&& replicaPlacement != nil{
		mode = byte(ECMode)
	}
	v.SuperBlock = SuperBlock{ReplicaPlacement: replicaPlacement,Mode:mode}
	e = v.load(true, true,false)
	return
}
func loadVolumeWithoutIndex(dirname string, collection,serialNum string, id VolumeId) (v *Volume, e error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id,SerialNum:serialNum}
	v.SuperBlock = SuperBlock{}
	e = v.load(false, false,false)
	return
}
func (v *Volume) FileName() (fileName string) {
	if v.Collection == "" {
		fileName = path.Join(v.dir, v.Id.String()+"_"+v.SerialNum)
	}else if v.Collection == public.EC || v.Collection == public.ECTemp{
		fileName = path.Join(v.dir, v.Collection+"_"+v.Id.String()+"_"+v.SerialNum)
	}else{
		fileName = path.Join(v.dir, v.Collection+"_"+v.Id.String())
	}
	return
}

func (v *Volume) LocalBackUpAndReplaceRep() error {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	oldDatFileName := v.dataFile.Name()
	oldIdxFileName := v.nm.(*NeedleMap).indexFile.Name()
	if err := os.Rename(oldDatFileName, oldDatFileName+".backup"); err != nil {
		return errors.New("Not able to back up data file " + err.Error())
	}
	if err := os.Rename(oldDatFileName+".rep", oldDatFileName); err != nil {
		return errors.New("Not able to rename data file " + err.Error())
	}
	if err := os.Rename(oldIdxFileName, oldIdxFileName+".backup"); err != nil {
		return errors.New("Not able to back up data file " + err.Error())
	}
	if err := os.Rename(oldIdxFileName+".rep", oldIdxFileName); err != nil {
		return errors.New("Not able to rename data file " + err.Error())
	}
	if err := v.load(true, false,false); err != nil {
		glog.V(0).Infoln("Not able to reload datafile", err.Error())
		return errors.New("Not able to reload datafile" + err.Error())
	}
	if err := os.Remove(oldDatFileName + ".backup"); err != nil {
		if os.IsNotExist(err) {
			return nil
		} else {
			return err
		}
	}
	if err := os.Remove(oldIdxFileName + ".backup"); err != nil {
		if os.IsNotExist(err) {
			return nil
		} else {
			return err
		}
	}
	return nil
}
func (v *Volume) load(alsoLoadIndex bool, createDatIfMissing bool,reload bool) error {
	var e error
	var readOnly = false
	fileName := v.FileName()
	defer func() {
		glog.V(0).Infoln("load vid:",v.Id.String(),"status:", e," isReadOnly",v.readOnly)
	}()
	if exists, canRead, canWrite, _ := checkFile(fileName + ".dat"); exists {
		if !canRead {
			e = fmt.Errorf("cannot read Volume Data file %s.dat", fileName)
			return e
		}
		if canWrite {
			v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
		} else {
			glog.V(0).Infoln("opening " + fileName + ".dat in READONLY mode")
			v.dataFile, e = os.Open(fileName + ".dat")
			readOnly = true
		}
	} else {
		if createDatIfMissing {
			v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
		} else {
			e = fmt.Errorf("Volume Data file %s.dat does not exist.", fileName)
			return e
		}
	}

	if e != nil {
		if !os.IsPermission(e) {
			e = fmt.Errorf("cannot load Volume Data %s.dat: %s", fileName, e.Error())
			return e
		}
	}

	if v.ReplicaPlacement == nil || reload == true{
		e = v.readSuperBlock()
	} else {
		e = v.maybeWriteSuperBlock()
	}
	if e == nil && alsoLoadIndex {
		if readOnly {
			if v.ensureConvertIdxToCdb(fileName) {
				v.nm, e = OpenCdbMap(fileName + ".cdb")
				return e
			}
		}
		var indexFile *os.File
		if readOnly {
			glog.V(1).Infoln("open to read file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDONLY, 0644); e != nil {
				e = fmt.Errorf("cannot read Volume Index %s.idx: %s", fileName, e.Error())
				return e
			}
		} else {
			glog.V(1).Infoln("open to write file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDWR|os.O_CREATE, 0644); e != nil {
				e = fmt.Errorf("cannot write Volume Index %s.idx: %s", fileName, e.Error())
				return e
			}
		}
		if readOnly {
			v.readOnly = true
		}
		if v.Mode == CopyDownMode{//经过降级的卷，只可删可读，不可写
			_,e = v.readDescBlock()
			if e == nil {
				v.readOnly = true
			}else{
				e = fmt.Errorf("cannot read desc block %s.idx: %s", fileName, e.Error())
				return e
			}
		}else{
			v.Desc = nil
		}
		glog.V(0).Infoln("loading file", fileName+".idx", " isReadonly?", v.readOnly)
		if v.volSysOpStatus.vacuumstatus.nmCPD != nil {
			if v.nm != nil {
				v.nm.Close()
			}
			v.nm, e = IncLoadNeedleMap(indexFile, (v.volSysOpStatus.vacuumstatus.nmCPD).(*NeedleMap))
			if v.nm.MaxFileKey()&FlagBadMask != 0 {
				v.readOnly = true
			}
		} else {
			if v.nm != nil {
				v.nm.Close()
			}
			if v.nm, e = LoadNeedleMap(indexFile); e != nil {
				glog.V(0).Infoln("loading error:", e)
			}
			if v.nm.MaxFileKey()&FlagBadMask != 0 {
				v.readOnly = true
			}
		}
	}
	return e
}

func (v *Volume) Version() Version {
	return v.SuperBlock.Version
}
func (v *Volume) Size() int64 {
	stat, e := v.dataFile.Stat()
	if e != nil {
		//when data file is closed , we can't use v.dataFile.Stat() to get the stat of file
		fileName := v.FileName()
		tmpFile, _ := os.Open(fileName + ".dat")
		defer tmpFile.Close()
		stat, e = tmpFile.Stat()
	}
	if e == nil {
		return stat.Size()
	}
	glog.V(0).Infof("Failed to read file size %s %s", v.dataFile.Name(), e.Error())
	return -1
}
func (v *Volume) Fallocate(mode uint32, off int64, length int64) (err error) {
	if runtime.GOOS != "linux" {
		return
	}
	return //syscall.Fallocate(int(v.dataFile.Fd()),mode,off,length)

}
func (v *Volume) Close() {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	v.nm.Close()
	_ = v.dataFile.Close()
}
func (v *Volume) maybeWriteSuperBlock() error {
	stat, e := v.dataFile.Stat()
	if e != nil {
		glog.V(0).Infof("failed to stat datafile %s: %s", v.dataFile, e.Error())
		return e
	}
	if stat.Size() == 0 {
		v.SuperBlock.Version = CurrentVersion
		_, e = v.dataFile.Write(v.SuperBlock.Bytes())
		if e != nil && os.IsPermission(e) {
			//read-only, but zero length - recreate it!
			if v.dataFile, e = os.Create(v.dataFile.Name()); e == nil {
				if _, e = v.dataFile.Write(v.SuperBlock.Bytes()); e == nil {
					v.readOnly = false
				}
			}
		}
	}
	return e
}
func (v *Volume) readSuperBlock() (err error) {
	if _, err = v.dataFile.Seek(0, 0); err != nil {
		return fmt.Errorf("cannot seek to the beginning of %s: %s", v.dataFile.Name(), err.Error())
	}
	header := make([]byte, SuperBlockSize)
	if _, e := v.dataFile.Read(header); e != nil {
		return fmt.Errorf("cannot read superblock: %s", e.Error())
	}
	v.SuperBlock, err = ParseSuperBlock(header)
	return err
}

func (v *Volume) readDescBlock() (size uint32,err error) {
	if _, err = v.dataFile.Seek(SuperBlockSize, 0); err != nil {
		return 0,fmt.Errorf("cannot seek to the beginning of %s: %s", v.dataFile.Name(), err.Error())
	}
	header := make([]byte, DescSize)
	if _, e := v.dataFile.Read(header); e != nil {
		return 0,fmt.Errorf("cannot read descblocksize: %s", e.Error())
	}
	sizeByte, e := hex.DecodeString(string(header))
	if  e != nil {
		return 0,fmt.Errorf("cannot parse size: %s", e.Error())
	}
	size=util.BytesToUint32(sizeByte)
	if _, err = v.dataFile.Seek(SuperBlockSize+DescSize, 0); err != nil {
		return 0,fmt.Errorf("cannot seek to the beginning of %s: %s", v.dataFile.Name(), err.Error())
	}
	if v.Desc != nil {
		return size,nil
	}
	descBuf := make([]byte,size)
	if _, e := v.dataFile.Read(descBuf); e != nil {
		return 0,fmt.Errorf("cannot read descblock: %s", e.Error())
	}
	var multiPart operation.ECMultiPartObject
	e = json.Unmarshal(descBuf,&multiPart)
	if e != nil {
		glog.V(0).Infoln("unmarshal err",descBuf,len(descBuf),string(descBuf))
		return 0,fmt.Errorf("cannot parse descblock: %s", e.Error())
	}
	v.Desc = &multiPart
	return size,err
}

func TransCopyStatus(file *os.File,rep *ReplicaPlacement, value byte) error {
	if file == nil {
		return errors.New("file not open")
	}
	superBlock := SuperBlock{ReplicaPlacement: rep,Mode:value}
	superBlock.Version=CurrentVersion
	_,err := file.Write(superBlock.Bytes())
	return err
}

func ReadSuperBlock(vol *os.File) (superBlock SuperBlock, err error) {
	if _, err = vol.Seek(0, 0); err != nil {
		return
	}
	header := make([]byte, SuperBlockSize)
	if _, err = vol.Read(header); err != nil {
		return
	}
	return ParseSuperBlock(header)
}

func ParseSuperBlock(header []byte) (superBlock SuperBlock, err error) {
	superBlock.Version = Version(header[0])
	superBlock.Mode = header[2]
	if superBlock.ReplicaPlacement, err = NewReplicaPlacementFromByte(header[1]); err != nil {
		err = fmt.Errorf("cannot read replica type: %s", err.Error())
	}
	return
}
func (v *Volume) NeedToReplicate() bool {
	return v.ReplicaPlacement.GetCopyCount() > 1
}

func (v *Volume) isFileUnchanged(n *Needle) int {
	nv, ok := v.nm.Get(n.Id)
	if ok && nv.Offset > 0 {
		oldNeedle := new(Needle)
		oldNeedle.Read(v.dataFile, int64(nv.Offset)*NeedlePaddingSize, nv.Size, v.Version())
		if oldNeedle.Checksum == n.Checksum && bytes.Equal(oldNeedle.Data, n.Data) {
//			glog.V(0).Infof("needle is unchanged,", n.Id, nv.Offset, nv.Size, n.Checksum)
			n.Size = oldNeedle.Size
			return 1
		}
		if oldNeedle.Checksum != n.Checksum || !bytes.Equal(oldNeedle.Data, n.Data){
			return 2
		}
	}
	return 0
}

func (v *Volume) Destroy() (err error) {
	if v.readOnly {
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	v.Close()
	err = os.Remove(v.dataFile.Name())
	if err != nil {
		return
	}
	err = v.nm.Destroy()
	return
}

func (v *Volume) write(n *Needle) (size uint32, err error) {
	//glog.V(4).Infof("writing needle %s", NewFileIdFromNeedle(v.Id, n).String())
	if v.readOnly {
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	if !v.volSysOpStatus.canWrite() {
		err = fmt.Errorf("%s is under compacting", v.dataFile.Name())
		return
	}
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	ret := v.isFileUnchanged(n)
	if ret == 1{
		size = n.Size
	//	glog.V(0).Infof("needle is unchanged!")
		return
	}
	if ret == 2 { //fid已存在，要先删除才能写
		err = fmt.Errorf("needle already exists")
		return
	}
	var offset int64
	if offset, err = v.dataFile.Seek(0, 2); err != nil {
		return
	}

	//ensure file writing starting from aligned positions
	if offset%NeedlePaddingSize != 0 {
		offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
		if offset, err = v.dataFile.Seek(offset, 0); err != nil {
			glog.V(4).Infof("failed to align in datafile %s: %s", v.dataFile.Name(), err.Error())
			return
		}
	}

	if size, err = n.Append(v.dataFile, v.Version(),1); err != nil {
		if e := v.dataFile.Truncate(offset); e != nil {
			err = fmt.Errorf("%s\ncannot truncate %s: %s", err, v.dataFile.Name(), e.Error())
		}
		if size < 0 {
			glog.V(0).Infof("needle append size =", size)
		}
		return
	}
	nv, ok := v.nm.Get(n.Id)
	if !ok || int64(nv.Offset)*NeedlePaddingSize < offset {
		if _, err = v.nm.Put(n.Id, uint32(offset/NeedlePaddingSize), n.Size); err != nil {
			glog.V(4).Infof("failed to save in needle map %d: %s", n.Id, err.Error())
		}
	}
	return
}

func (v *Volume) delete(n *Needle) (uint32, error) {
	glog.V(4).Infof("delete needle %s", NewFileIdFromNeedle(v.Id, n).String())
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	nv, ok := v.nm.Get(n.Id)
	//fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok {
		size := nv.Size
		if err := v.nm.Delete(n.Id); err != nil {
			return size, err
		}
		if _, err := v.dataFile.Seek(0, 2); err != nil {
			return size, err
		}
		n.Data = make([]byte, 0)
		_, err := n.Append(v.dataFile, v.Version(),0)
		return size, err
	}
	return 0, nil
}

func (v *Volume) read(n *Needle) (int, error) {
/*	if !v.volSysOpStatus.canRead() {
		err := fmt.Errorf("%s is hanging up ", v.dataFile.Name())
		glog.V(2).Infoln("read error,volume is hanging up")
		return -1, err
	}*/

	nv, ok := v.nm.Get(n.Id)
	if ok && nv.Offset > 0 {
		return n.Read(v.dataFile, int64(nv.Offset)*NeedlePaddingSize, nv.Size, v.Version())
	}
	return -1, errors.New("Not Found")
}

func (v *Volume) GetInfo(n *Needle) (int64, uint32,error) {
	nv, ok := v.nm.Get(n.Id)
	if ok && nv.Offset > 0 {
		return  int64(nv.Offset)*NeedlePaddingSize, nv.Size+NeedleHeaderSize+NeedleChecksumSize, nil
	}
	return 0,0, errors.New("Not Found")
}

/*
func (v *Volume) check(n *Needle) bool {
	_, ok := v.nm.Get(n.Id)
	return ok
}
*/

func (v *Volume) check(n *Needle) bool {
	nv, ok := v.nm.Get(n.Id)
	if ok && nv != nil {
		return nv.Offset > 0 && nv.Size > 0
	} else {
		return false
	}
}

func (v *Volume) freeze() error {
	if v.readOnly {
		return nil
	}
	nm, ok := v.nm.(*NeedleMap)
	if !ok {
		return nil
	}
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	bn, _ := baseFilename(v.dataFile.Name())
	cdbFn := bn + ".cdb"
	glog.V(0).Infof("converting %s to %s", nm.indexFile.Name(), cdbFn)
	err := DumpNeedleMapToCdb(cdbFn, nm)
	if err != nil {
		return err
	}
	if v.nm, err = OpenCdbMap(cdbFn); err != nil {
		return err
	}
	nm.indexFile.Close()
	os.Remove(nm.indexFile.Name())
	v.readOnly = true
	return nil
}

func ScanVolumeFileByIdx(nm NeedleMapper, dirname string, collection string,serialNum string, id VolumeId, limitSize int64,
	visitSuperBlock func(SuperBlock) error,
	readNeedleBody bool,
	visitNeedle func(n *Needle, offset int64, size uint32, isSkip bool) error) (err error) {
	var v *Volume

	if v, err = NewVolume(dirname, collection,serialNum ,id, nil); err != nil {
		return errors.New("Failed to load volume:" + err.Error())
	}

	defer v.nm.Close()
	defer v.dataFile.Close()
	if err = visitSuperBlock(v.SuperBlock); err != nil {
		return errors.New("Failed to read super block:" + err.Error())
	}
	version := v.Version()
	offset := int64(SuperBlockSize)
	if headerFileInfo, err := os.Stat(v.FileName() + ".idx"); err == nil {
		if headerFileInfo.Size() == 0 {
			return nil
		}
	}
	newOffset := offset
	indexFile, err := os.OpenFile(v.FileName()+".idx", os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open index file %s.idx: %s", v.FileName(), err)
	}
	defer indexFile.Close()

	if nm == nil {
		nm, err = LoadNeedleMap(indexFile)
		if err != nil {
			return fmt.Errorf("failed to load in needlemap from file:%s.idx: %s", v.FileName(), err)
		}
	}
	//
	err = nm.Visit(func(nv NeedleValue) error {

		glog.V(4).Infoln("needmap:", nv.Key, nv.Size, nv.Offset);
		if nv.Size == 0 {
			glog.V(4).Infoln("needmap:", nv.Key, "Size == 0")
			return nil
		}
		offset := int64(nv.Offset) * NeedlePaddingSize
		if offset >= limitSize {
			glog.V(4).Infoln("needmap:", nv.Key, "Size == 0")
			return ErrFinishScan
		}
		n, rest, e := ReadNeedleHeader(v.dataFile, version, offset)
		if e == io.EOF {
			glog.V(4).Infoln("needmap:", nv.Key, "Size == 0")
			return nil
		}
		if e != nil {
			glog.V(4).Infoln("needmap:", nv.Key, "Size == 0")
			return fmt.Errorf("cannot read needle header: %s, skip", e)
		}
		if n == nil {
			glog.V(0).Infoln("cannot  read needle header, skip!")
			return nil
		}
		bodyOffset := offset + int64(NeedleHeaderSize)
		if bodyOffset+int64(rest) > limitSize {
			glog.V(0).Infoln("skip a bad needle.")
			return ErrFinishScan
		}
		if n.Id == 0 {
			glog.V(0).Infof("Scan VolumeId %s Key %d offset %d size %d has corrupted:%s, skip!\n", id, nv.Key, nv.Offset, nv.Size, err)
			return visitNeedle(n, offset, nv.Size, true)
		}
		if Key(n.Id) != nv.Key {
			glog.V(0).Infoln("skip a bad needle.")
			return visitNeedle(n, offset, nv.Size, true)
		}
		if readNeedleBody {
			if err = n.ReadNeedleBody(v.dataFile, version, bodyOffset, rest); err != nil {
				if err == ErrDataCorrup || err == ErrBadNeedleData {
					glog.V(0).Infof("Scan VolumeId %s Key %d offset %d size %d has corrupted:%s, skip!\n", id, nv.Key, nv.Offset, nv.Size, err)
					return visitNeedle(n, offset, nv.Size, true)
				} else {
					glog.V(0).Infof("Scan VolumeId %s Key %d offset %d size %d has corrupted:%s, return!\n", id, nv.Key, nv.Offset, nv.Size, err)
					err = fmt.Errorf("cannot read needle body: %s", err)
					return err
				}
			}
		}
		if err = visitNeedle(n, offset, 0, false); err != nil {
			glog.V(0).Infoln("failed to vist needel:", err)
			return err
		}
		newOffset = newOffset + int64(NeedleHeaderSize) + int64(rest)
		return nil

	})
	return
}
func ScanVolumeFile(dirname string, collection,serialNum string, id VolumeId,
	visitSuperBlock func(SuperBlock) error,
	readNeedleBody bool,
	visitNeedle func(n *Needle, offset int64) error) (err error) {
	var v *Volume
	if v, err = loadVolumeWithoutIndex(dirname, collection,serialNum, id); err != nil {
		return errors.New("Failed to load volume:" + err.Error())
	}
	if err = visitSuperBlock(v.SuperBlock); err != nil {
		return errors.New("Failed to read super block:" + err.Error())
	}
	version := v.Version()

	offset := int64(SuperBlockSize)
	if headerFileInfo, err := os.Stat(v.FileName() + ".idx"); err == nil {
		if headerFileInfo.Size() == 0 {
			return nil
		}
	}
	n, rest, e := ReadNeedleHeader(v.dataFile, version, offset)
	if e != nil {
		if e == io.EOF {
			err = nil
		} else {
			err = fmt.Errorf("cannot read needle header: %s", e)
		}
		return
	}
	for n != nil {
		if readNeedleBody {
			if err = n.ReadNeedleBody(v.dataFile, version, offset+int64(NeedleHeaderSize), rest); err != nil {
				glog.V(0).Infof("Scan volume %s  offset %d, error: %s\n", id, offset, err)
				if err == ErrBadNeedleData {
					glog.V(0).Infoln("bad needle data:", n.Id, n.DataSize)
				} else {
					err = fmt.Errorf("cannot read needle body: %s", err)
					return
				}
			}
		}
		if err == nil {
			if err = visitNeedle(n, offset); err != nil {
				return
			}
		}
		offset += int64(NeedleHeaderSize) + int64(rest)
		if n, rest, err = ReadNeedleHeader(v.dataFile, version, offset); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("cannot read needle header: %s", err)
		}
	}

	return
}

func (v *Volume) ContentSize() uint64 {
	return v.nm.ContentSize()
}

func checkFile(filename string) (exists, canRead, canWrite bool, modTime time.Time) {
	exists = true
	fi, err := os.Stat(filename)
	if os.IsNotExist(err) {
		exists = false
		return
	}
	if fi == nil {
		glog.V(0).Infoln("stat file:",filename," err:",err)
		exists = false
		return
	}
	if fi.Mode()&0400 != 0 {
		canRead = true
	}
	if fi.Mode()&0200 != 0 {
		canWrite = true
	}
	modTime = fi.ModTime()
	return
}
func (v *Volume) ensureConvertIdxToCdb(fileName string) (cdbCanRead bool) {
	var indexFile *os.File
	var e error
	_, cdbCanRead, cdbCanWrite, cdbModTime := checkFile(fileName + ".cdb")
	_, idxCanRead, _, idxModeTime := checkFile(fileName + ".idx")
	if cdbCanRead && cdbModTime.After(idxModeTime) {
		return true
	}
	if !cdbCanWrite {
		return false
	}
	if !idxCanRead {
		glog.V(0).Infoln("Can not read file", fileName+".idx!")
		return false
	}
	glog.V(2).Infoln("opening file", fileName+".idx")
	if indexFile, e = os.Open(fileName + ".idx"); e != nil {
		glog.V(0).Infoln("Failed to read file", fileName+".idx !")
		return false
	}
	defer indexFile.Close()
	glog.V(0).Infof("converting %s.idx to %s.cdb", fileName, fileName)
	if e = ConvertIndexToCdb(fileName+".cdb", indexFile); e != nil {
		glog.V(0).Infof("error converting %s.idx to %s.cdb: %s", fileName, fileName, e.Error())
		return false
	}
	return true
}
func (v *Volume) SwitchReadOnly(s bool) {
	v.readOnly = s
	glog.V(0).Infoln("switch vid:",v.Id.String(),"serialNum:",v.SerialNum," to readonly?",s)
}
func (v *Volume) Sync() (err error) {
	if !v.volSysOpStatus.canFlush() {
		return nil
	}
	return v.dataFile.Sync()
}
func (v *Volume) SetNotJoin() {
	v.notJoin = true
}
func (v *Volume) GetNotJion() bool {
	return v.notJoin
}
func (v *Volume) Dir() string {
	return v.dir
}
const (
	//什么都没再做
	VOL_FRE = iota

	VOL_VCM

	VOL_REB

	VOL_REP

	VOL_DOWNGRADE

	VOL_UPGRADE

	VOL_ECREP
)

var ErrVolumeBusy = errors.New("storage:Volume is now busy")
var ErrVolumeBadUUID = errors.New("storage:Invalid uuid")
type VolStausSt struct {
	uuid string
	//非VOL_FRE状态不可新写文件
	status int

	//基本信息
	lastUse    time.Time
	vid        VolumeId
	collection string
	location   string
	rp         *ReplicaPlacement

	//
	repairstatus    *RepRepairStaus
	rebalancestatus *RebalanceStaus
	vacuumstatus    *VacuumStaus
	needSize        uint64 //需要多少空间
	forceQuit       bool //强制任务退出
	sync.RWMutex
}

func NewVolStausSt(vid VolumeId, collection string, dir string, rp *ReplicaPlacement,size uint64) *VolStausSt {
	vstatus := &VolStausSt{
		status:     VOL_FRE,
		vid:        vid,
		collection: collection,
		location:   dir,
		rp:         rp,
		needSize:   size,
	}
	vstatus.repairstatus = &RepRepairStaus{}
	vstatus.rebalancestatus = &RebalanceStaus{}
	vstatus.vacuumstatus = &VacuumStaus{}
	return vstatus
}

func (vs *VolStausSt) trySetVCM() error {
	return vs.set(VOL_VCM)
}

func (vs *VolStausSt) trySetREB() error {
	return vs.set(VOL_REB)

}

func (vs *VolStausSt) trySetREP() error {
	return vs.set(VOL_REP)
}

func (vs *VolStausSt) trySetDowngrade(uuid string) error {
	return vs.setWithUUID(VOL_DOWNGRADE,uuid)
}

func (vs *VolStausSt) trySetUpgrade(uuid string) error {
	return vs.setWithUUID(VOL_UPGRADE,uuid)
}

func (vs *VolStausSt) trySetECREP(uuid string) error {
	return vs.setWithUUID(VOL_ECREP,uuid)
}

func (vs *VolStausSt) isFree() bool {
	return vs.status == VOL_FRE
}

func (vs *VolStausSt) tryForceQuit(uuid string) error {
	vs.Lock()
	defer vs.Unlock()
	if vs.uuid != uuid{
		return fmt.Errorf("%s,now uuid:%s bad uuid:%s",ErrVolumeBadUUID.Error(),vs.uuid,uuid)
	}
	vs.forceQuit = true
	return nil
}

func (vs *VolStausSt) canDelete() bool {
	vs.RLock()
	defer vs.RUnlock()
	if vs.status > VOL_REP {
		return false
	}
	return true
}

//是不是可写状态
//不管有没有缓存，所有非free都不可写
func (vs *VolStausSt) canWrite() bool {
	vs.RLock()
	defer vs.RUnlock()
	if vs.status > VOL_FRE {
		return false
	}
	return true
}

func (vs *VolStausSt) canFlush() bool {
	vs.RLock()
	defer vs.RUnlock()
	//空闲或者压缩进行时可以进行下刷动作
	if vs.status == VOL_FRE || vs.status == VOL_VCM  {
		return true
	}
	//包括了rebalance进行时，repair进行时，所有commit动作，实际上也没有刷的动作
	return false
}

//设置成某种状态
func (vs *VolStausSt) set(staus int) error {
	vs.Lock()
	defer vs.Unlock()

	if vs.status != VOL_FRE {
		return ErrVolumeBusy
	}
	vs.status = staus
	vs.lastUse = time.Now()
	return nil
}

//设置成某种状态
func (vs *VolStausSt) checkUUID(uuid string) error {
	vs.Lock()
	defer vs.Unlock()
	if vs.uuid != uuid{
		return fmt.Errorf("%s,now uuid:%s bad uuid:%s",ErrVolumeBadUUID.Error(),vs.uuid,uuid)
	}
	return nil
}
//设置成某种状态
func (vs *VolStausSt) setWithUUID(staus int,uuid string) error {
	vs.Lock()
	defer vs.Unlock()

	if vs.status != VOL_FRE || vs.uuid != ""{
		return ErrVolumeBusy
	}
	vs.uuid = uuid
	vs.status = staus
	vs.lastUse = time.Now()
	return nil
}
//复位
func (vs *VolStausSt) resetWithUUID(uuid string)error {
	vs.Lock()
	defer vs.Unlock()
	if vs.uuid != uuid {
		return fmt.Errorf("%s,now uuid:%s bad uuid:%s",ErrVolumeBadUUID.Error(),vs.uuid,uuid)
	}
	vs.status = VOL_FRE
	vs.uuid = ""
	vs.forceQuit = false
	vs.lastUse = time.Now()
	return nil
}

//复位
func (vs *VolStausSt) reset() {
	vs.Lock()
	defer vs.Unlock()
	vs.status = VOL_FRE
	vs.uuid = ""
	vs.lastUse = time.Now()
	vs.forceQuit = false
	return
}

//获取现在的状态
func (vs *VolStausSt) getStatus() int {
	vs.RLock()
	defer vs.RUnlock()
	res := vs.status
	return res
}

type SysOpStatus struct {
	Uuid      string
	Optype    string
	IsOver    bool
	Result    bool
	Starttime time.Time
	Endtime   time.Time
	Err       string
	Id        int //卷号
	Sn        string //序列号
	Dest      string  //目的
	Size      int64  //待修复数据大小，根据dat文件的大小
	Mnt       string //磁盘路径
	TaskSeq   uint64
}

type SysOpStatusSort []SysOpStatus
func (s SysOpStatusSort) Len() int { return len(s) }
func (s SysOpStatusSort) Swap(i, j int){ s[i], s[j] = s[j], s[i] }
func (s SysOpStatusSort) Less(i, j int) bool { return s[i].TaskSeq < s[j].TaskSeq }
