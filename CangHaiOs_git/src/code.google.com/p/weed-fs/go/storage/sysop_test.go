package storage

import (
	"testing"
	"fmt"
	"time"
	"math/rand"
	"crypto/md5"
	"encoding/hex"
	"os"
)

//Store.SysOpHis 在NewStore初始化，
// 新增状态以uuid为主键。
// 每次新增动作触发遍历。结束超过1个月的会被删除
func TestStore_AddSysOpStatus(t *testing.T) {
	st := NewStore(8080, "127.0.0.1", "127.0.0.1", nil, nil, 0, "coll", 0)
	opstatus := &SysOpStatus{
		Uuid:      (fmt.Sprint(time.Now().UnixNano()) + fmt.Sprint(rand.Intn(100))),
		Optype:    "repair",
		Result:    false,
		Starttime: time.Now(),
	}
	st.AddSysOpStatus(opstatus)
	println("add SysOpStatus ", opstatus.Uuid, "  ok .IsOver:false  ")

	opstatus2 := &SysOpStatus{
		Uuid:      (fmt.Sprint(time.Now().UnixNano()) + fmt.Sprint(rand.Intn(100))),
		Optype:    "repair",
		Result:    false,
		Starttime: time.Now().Add(-31 * 24 * time.Hour),
	}
	st.AddSysOpStatus(opstatus2)
	println("add SysOpStatus ", opstatus2.Uuid, "  ok .IsOver:false  ")

	opstatus2.IsOver = true
	opstatus2.Endtime = time.Now().Add(-30 * 24 * time.Hour)
	//能访问到1、2

	v := st.GetSysOpStatus(opstatus.Uuid)
	if v == nil {
		t.Fatal(opstatus.Uuid, " missing")
	}
	println("get SysOpStatus ", opstatus.Uuid, "  ok .IsOver :", opstatus.IsOver)

	v = st.GetSysOpStatus(opstatus2.Uuid)
	if v == nil {
		t.Fatal(opstatus2.Uuid, " missing")
	}
	println("get SysOpStatus ", opstatus2.Uuid, "  ok .IsOver: ", opstatus2.IsOver, " end time:", opstatus2.Endtime.String() )

	opstatus3 := &SysOpStatus{
		Uuid:      (fmt.Sprint(time.Now().UnixNano()) + fmt.Sprint(rand.Intn(100))),
		Optype:    "repair",
		Result:    false,
		Starttime: time.Now(),
	}
	st.AddSysOpStatus(opstatus3)

	v = st.GetSysOpStatus(opstatus2.Uuid)
	if v != nil {
		t.Fatal(opstatus2.Uuid, " old  exist")
	}
	//结束且时间超过30天的会被移除
	println("after add  opstatus3 ,", opstatus2.Uuid, "has been deleted ")

}

//获取一个系统槽位
//1 用于原来不存在的volume，需要获取一个位置，防止出现并发冲突，导致一个磁盘上出现多个卷
//2
func TestDiskLocation_SysOpSlot(t *testing.T) {
	location := &DiskLocation{Directory: "mydir", MaxVolumeCount: 20}
	location.volumes = make(map[VolumeId]*Volume)
	location.sysOpvolume = make(map[VolumeId]*Volume)
	v := &Volume{dir: location.Directory, Collection: "coll", Id: 1}
	v.volSysOpStatus = NewVolStausSt(v.Id, v.Collection, v.dir, nil)
	location.volumes[v.Id] = v

	v2 := &Volume{dir: location.Directory, Collection: "coll", Id: 2}
	v2.volSysOpStatus = NewVolStausSt(v2.Id, v.Collection, v2.dir, nil)
	location.volumes[v2.Id] = v2

	for i := 3; i <= 10; i++ {
		location.GetSysOpSlot(VolumeId(i), VOL_REB)
	}
	for i := 11; i <= 20; i++ {
		location.GetSysOpSlot(VolumeId(i), VOL_REP)
	}
	println("GetSysOpSlot ok,20 solt ,20 op")

	res := location.GetSysOpSlot(VolumeId(21), VOL_REP)
	if res != nil {
		t.Fatal("Slot is full but get one")
	}
	println("can not get solt while Slot is full")

	location.DestroySysOpSlot(VolumeId(20))
	if _, ok := location.sysOpvolume[VolumeId(20)]; ok {
		t.Fatal("Destroy but get it")
	}

	println("DestroySysOpSlot test ok ")
}

func TestStore_FindfreeLocation(t *testing.T) {
	st := NewStore(8080, "127.0.0.1", "127.0.0.1", nil, nil, 0, "coll", 0)
	location := &DiskLocation{Directory: "dira", MaxVolumeCount: 2}
	location.volumes = make(map[VolumeId]*Volume)
	location.sysOpvolume = make(map[VolumeId]*Volume)
	location.GetSysOpSlot(VolumeId(0), VOL_REB)
	location.GetSysOpSlot(VolumeId(1), VOL_REB)
	st.Locations = append(st.Locations, location)

	location2 := &DiskLocation{Directory: "dirb", MaxVolumeCount: 2}
	location2.volumes = make(map[VolumeId]*Volume)
	location2.sysOpvolume = make(map[VolumeId]*Volume)
	location2.GetSysOpSlot(VolumeId(2), VOL_REB)
	st.Locations = append(st.Locations, location2)

	location3 := &DiskLocation{Directory: "dirc", MaxVolumeCount: 2}
	location3.volumes = make(map[VolumeId]*Volume)
	location3.sysOpvolume = make(map[VolumeId]*Volume)
	st.Locations = append(st.Locations, location3)


	for i := 0; i < 100; i++ {
		ret := st.findFreeLocation()
		if ret.Directory == "dira" {
			t.Fatal("get full dir ")
		}
	}
	println("FindfreeLocation test ok ")
}

func TestStore_RebFilePrepare(t *testing.T) {
	//模拟要进行准备动作
	st := NewStore(8080, "127.0.0.1", "127.0.0.1", nil, nil, 0, "collreb", 0)
	location := &DiskLocation{
		Directory:      "/mnt/data/sdoss/volume",
		MaxVolumeCount: 2,
		sysOpvolume:    make(map[VolumeId]*Volume),
	}
	st.Locations = append(st.Locations, location)

	err := st.RebFilePrepare("9999999", "", "myuuid1", false)
	if err != nil {
		t.Fatal("RebFilePrepare error :", err)
	}
	//希望失败
	err = st.RebFilePrepare("9999999", "", "myuuid2", false)
	if err == nil {
		t.Fatal("RebFilePrepare repeated err ")
	}
	//希望成功
	err = st.RebFilePrepare("9999999", "", "myuuid3", true)
	if err != nil {
		t.Fatal("RebFilePrepare with force error :", err)
	}
}

func TestStore_RebFileReceive(t *testing.T) {
	st := NewStore(8080, "127.0.0.1", "127.0.0.1", nil, nil, 0, "collreb", 0)
	location := &DiskLocation{Directory: "/mnt/data/sdoss/volume",
		MaxVolumeCount: 2,
		sysOpvolume: make(map[VolumeId]*Volume),}
	st.Locations = append(st.Locations, location)
	err := st.RebFilePrepare("9999999", "", "myuuid1", true)
	if err != nil {
		t.Fatal("RebFilePrepare error :", err)
	}
	data := []byte("hello")
	ctxMD5 := md5.New()
	ctxMD5.Reset()
	ctxMD5.Write(data)
	sum := hex.EncodeToString(ctxMD5.Sum(nil))
	err = st.RebFileReceive("9999999", "dat", sum, "myuuid1", data)
	if err != nil {
		t.Fatal("RebFileReceive error :", err)
	}
	err = st.RebFileCommitTest("9999999", "1", "myuuid1")
	if err != nil {
		t.Fatal("RebFileCommit error :", err)
	}
	//检查文件是不是存在
	if len(location.sysOpvolume) != 0 {
		t.Fatal("RebFileCommit error :", err)
	}
	_, err = os.Stat("/mnt/data/sdoss/volume/collreb_9999999.dat")
	if err != nil {
		t.Fatal("RebFileCommit error :", err)
	}
	_, err = os.Stat("/mnt/data/sdoss/volume/collreb_9999999.idx")
	if err != nil {
		t.Fatal("RebFileCommit error :", err)
	}

	err = st.RebFilePrepare("9999998", "", "myuuid1", true)
	if err != nil {
		t.Fatal("RebFilePrepare error :", err)
	}
	data = []byte("hello")
	ctxMD5 = md5.New()
	ctxMD5.Reset()
	ctxMD5.Write(data)
	sum = hex.EncodeToString(ctxMD5.Sum(nil))
	err = st.RebFileReceive("9999998", "dat", sum, "myuuid1", data)
	if err != nil {
		t.Fatal("RebFileReceive error :", err)
	}
	err = st.RebFileCommitTest("9999998", "0", "myuuid1")
	if err != nil {
		t.Fatal("RebFileCommit error :", err)
	}
	if len(location.sysOpvolume) != 0 {
		t.Fatal("RebFileCommit error :", err)
	}
	//检查reb文件是不是存在
	_, err = os.Stat("/mnt/data/sdoss/volume/collreb_9999998.dat")
	if err == nil {
		t.Fatal("RebFileCommit with rslt 0 error :", err)
	}
	_, err = os.Stat("/mnt/data/sdoss/volume/collreb_9999998.idx")
	if err == nil {
		t.Fatal("RebFileCommit with rslt 0 error :", err)
	}
}

func TestStore_RepFilePrepare(t *testing.T) {
	//模拟要进行准备动作
	st := NewStore(8080, "127.0.0.1", "127.0.0.1", nil, nil, 0, "collrep", 0)
	location := &DiskLocation{
		Directory:      "/mnt/data/sdoss/volume",
		MaxVolumeCount: 2,
		sysOpvolume:    make(map[VolumeId]*Volume),
	}
	st.Locations = append(st.Locations, location)

	err := st.RepFilePrepare("9999999", "", "myuuid1", false)
	if err != nil {
		t.Fatal("RepFilePrepare error :", err)
	}
	err = st.RepFilePrepare("9999999", "", "myuuid2", false)
	if err == nil {
		t.Fatal("RepFilePrepare repeated err ")
	}
	err = st.RepFilePrepare("9999999", "", "myuuid3", true)
	if err != nil {
		t.Fatal("RepFilePrepare with force error :", err)
	}
}

func TestStore_RepFileReceive(t *testing.T) {
	st := NewStore(8080, "127.0.0.1", "127.0.0.1", nil, nil, 0, "collrep", 0)
	location := &DiskLocation{
		Directory:      "/mnt/data/sdoss/volume",
		MaxVolumeCount: 2,
		sysOpvolume:    make(map[VolumeId]*Volume),
	}
	st.Locations = append(st.Locations, location)
	err := st.RepFilePrepare("9999999", "", "myuuid1", true)
	if err != nil {
		t.Fatal("RebFilePrepare error :", err)
	}
	data := []byte("hello")
	ctxMD5 := md5.New()
	ctxMD5.Reset()
	ctxMD5.Write(data)
	sum := hex.EncodeToString(ctxMD5.Sum(nil))
	err = st.RepFileReceive("9999999", "dat", sum, "myuuid1", data)
	if err != nil {
		t.Fatal("RebFileReceive error :", err)
	}
	err = st.RepFileCommitTest("9999999", "1", "myuuid1")
	if err != nil {
		t.Fatal("RebFileCommit error :", err)
	}
	//检查文件是不是存在
	if len(location.sysOpvolume) != 0 {
		t.Fatal("RebFileCommit error :", err)
	}
	_, err = os.Stat("/mnt/data/sdoss/volume/collrep_9999999.dat")
	if err != nil {
		t.Fatal("RebFileCommit error :", err)
	}
	_, err = os.Stat("/mnt/data/sdoss/volume/collrep_9999999.idx")
	if err != nil {
		t.Fatal("RebFileCommit error :", err)
	}

	err = st.RepFilePrepare("9999998", "", "myuuid1", true)
	if err != nil {
		t.Fatal("RebFilePrepare error :", err)
	}
	data = []byte("hello")
	ctxMD5 = md5.New()
	ctxMD5.Reset()
	ctxMD5.Write(data)
	sum = hex.EncodeToString(ctxMD5.Sum(nil))
	err = st.RepFileReceive("9999998", "dat", sum, "myuuid1", data)
	if err != nil {
		t.Fatal("RebFileReceive error :", err)
	}
	err = st.RepFileCommitTest("9999998", "0", "myuuid1")
	if err != nil {
		t.Fatal("RebFileCommit error :", err)
	}
	if len(location.sysOpvolume) != 0 {
		t.Fatal("RebFileCommit error :", err)
	}
	//检查reb文件是不是存在
	_, err = os.Stat("/mnt/data/sdoss/volume/collrep_9999998.dat")
	if err == nil {
		t.Fatal("RebFileCommit error :", err)
	}
	_, err = os.Stat("/mnt/data/sdoss/volume/collrep_9999998.idx")
	if err == nil {
		t.Fatal("RebFileCommit error :", err)
	}
}

//把卷信息
func TestStore_BackupVolume(t *testing.T) {

}

//删除bak文件
func TestStore_DelBakFile(t *testing.T) {

}

func (s *Store) RebFileCommitTest(volumeIdString string, rslt string, uuid string) error {

	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}

	vol := s.findSysOpVolume(vid)

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
		dirloc.DestroySysOpSlot(vid)
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
	if err = s.RebalanceFinTransfer(volumeIdString, dirloc.Directory); err != nil {
		return err
	}
	//释放
	dirloc.ReleaseSysOpSlot(vid)

	return nil
}

func (s *Store) RepFileCommitTest(volumeIdString string, rslt string, uuid string) error {

	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}
	oldexit := true
	vol := s.findVolume(vid)
	if vol == nil {
		vol = s.findSysOpVolume(vid)
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
		dirloc.DestroySysOpSlot(vid)
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
		vol.Backup(false)
		s.deleteVolumeM(vid)
		time.Sleep(5 * time.Second)
	}
	if err = s.RepairFinTransfer(volumeIdString, dirloc.Directory); err != nil {
		return err
	}
	//释放
	dirloc.ReleaseSysOpSlot(vid)
	return nil
}
