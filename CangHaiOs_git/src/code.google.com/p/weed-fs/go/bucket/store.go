package bucket

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"code.google.com/p/goprotobuf/proto"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/util"
	_ "github.com/go-sql-driver/mysql"
)

type BucketStore struct {
	Port             int
	Ip               string
	PublicUrl        string
	dataCenter       string //optional informaton, overwriting master setting if exists
	rack             string //optional information, overwriting master setting if exists
	connected        bool
	masterNodes      *storage.MasterNodes
	storetype        string
	ReplicaPlacement *storage.ReplicaPlacement
	createIfNotExist bool
	bucketManagers   map[PartitionId]BucketManager
	managersLock     sync.RWMutex
	accessLock       sync.Mutex
	util.DBStoreCommon
}
type StoreBucket struct {
	ParId  string
	Bucket Bucket
}

func NewBucketStore(port int, ip string, publicUrl string, storedir string, storetype string, rack string, replication string) (s *BucketStore, err error) {
	s = &BucketStore{Port: port, Ip: ip, PublicUrl: publicUrl, storetype: storetype, rack: rack, createIfNotExist: true}
	s.ReplicaPlacement, err = storage.NewReplicaPlacementFromString(replication)
	if err != nil {
		return
	}
	s.InitStoreCom(storedir)
	s.bucketManagers = make(map[PartitionId]BucketManager)
	s.loadExsitingBucketPartition()
	return
}

func (s *BucketStore) addBucketPartition(id string, needCreate, insTopo bool) (err error) {
	parId, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}

	if s.HasBucketManager(parId) {
		return nil
	}
	glog.V(0).Infoln("adds BucketPartition =", id)

	publicdb, dbprefix, err := s.GetPublicMysqlDB()
	if err != nil {
		glog.V(0).Infoln("get public dsn err", err.Error())
		return err
	}
	defer publicdb.Close()
	dbname := "bucket" + "_" + parId.String()
	if needCreate {
		_, err = publicdb.Exec("create database " + dbname)
		if err != nil {
			glog.V(0).Infoln("create database err", dbname, err.Error())
			return err
		}
		glog.V(0).Infoln("create database ", dbname)
	}
	if insTopo {
		dsn := dbprefix + "/" + dbname
		if bucketMngr, err := NewMysqlBucketManager(dsn, parId.String(), needCreate); err == nil {
			s.SetPartition(parId, bucketMngr)
			s.Join()
			storage.WriteNewParToConf(s.GetConfDir(), dsn, parId.String(), "")
			return nil
		}
		glog.V(0).Infoln("create bucket ok ", dsn, parId.String(), needCreate)
	}
	return err
}
func (s *BucketStore) AddBucketPartition(idList string, needCreate, insTopo bool) (err error) {
	s.accessLock.Lock()
	defer s.accessLock.Unlock()
	count := 0
	glog.V(0).Infoln("AddBucketPartition check point 1. start:", idList, needCreate, insTopo)
	for _, range_string := range strings.Split(idList, ",") {
		if strings.Index(range_string, "-") < 0 {
			id_string := range_string
			err = s.addBucketPartition(id_string, needCreate, insTopo)
			if err != nil {
				return
			}
		} else {
			pair := strings.Split(range_string, "-")
			start, start_err := strconv.ParseUint(pair[0], 10, 64)
			if start_err != nil {
				glog.V(0).Infoln("AddBucketPartition check point 3:", idList, needCreate, insTopo, start, pair[0])
				return fmt.Errorf("Partition Start Id %s is not a valid unsigned integer!", pair[0])
			}
			end, end_err := strconv.ParseUint(pair[1], 10, 64)
			if end_err != nil {
				glog.V(0).Infoln("AddBucketPartition check point 4:", idList, needCreate, insTopo, start, pair[1])
				return fmt.Errorf("Partition End Id %s is not a valid unsigned integer!", pair[1])
			}
			glog.V(0).Infoln("AddBucketPartition check point 5:", idList, needCreate, insTopo, start, end)
			for id := start; id <= end; id++ {
				if err = s.addBucketPartition(fmt.Sprint(id), needCreate, insTopo); err != nil {
					glog.V(0).Infoln("AddBucketPartition check point 6:", idList, needCreate, insTopo, start, end, id, err)
					return err
				}
			}
		}
		count++
	}
	glog.V(0).Infoln("AddBucketPartition check point 7. end:", idList, needCreate, insTopo, count)
	return
}

//load existing bucket from bukcet dir
func (s *BucketStore) loadExsitingBucketPartition() {
	confFile, err := os.OpenFile(filepath.Join(s.GetConfDir(), "mysql.conf"), os.O_RDONLY, 0644)
	if err != nil {
		if !os.IsNotExist(err) {
			panic(err.Error())
		}
		return
	}

	lines := bufio.NewReader(confFile)
	for {
		line, err := util.Readln(lines)
		if err != nil && err != io.EOF {
			panic(err.Error())
			return
		}
		if err == io.EOF {
			break
		}
		str := string(line)
		if strings.HasPrefix(str, "#") {
			continue
		}
		if str == "" {
			continue
		}
		parts := strings.Split(str, "=")
		if len(parts) == 0 {
			continue
		}
		switch parts[0] {
		case "partition":
			parstr := parts[1]
			dsn := ""
			base := ""
			parts := strings.Split(parstr, ",")
			lens := len(parts)
			if lens != 2 {
				continue
			}
			base, dsn = parts[0], parts[1]
			if parID, err := NewPartitionId(base); err == nil {
				if !s.HasBucketManager(parID) {
					if bucketMngr, e := NewMysqlBucketManager(dsn, parID.String(), false); e == nil {
						glog.V(0).Infoln("Add bucket partition:", dsn, parID.String())
						s.SetPartition(parID, bucketMngr)
					}
				}
			}
		default:
			continue
		}

	}
	glog.V(0).Infoln("BucketPartitionStore started on dir:", s.GetConfDir())
}

//SetPartition
func (s *BucketStore) SetPartition(id PartitionId, bucketMngr BucketManager) {
	s.managersLock.Lock()
	s.bucketManagers[id] = bucketMngr
	s.managersLock.Unlock()
}

//create a new bucket
//func  (s *BucketStore) AddBucket(interface){}
//get a bucket specified by collection and bucket ID
func (s *BucketStore) GetBucketManager(id PartitionId) (BucketManager, bool) {
	s.managersLock.RLock()
	v, ok := s.bucketManagers[id]
	s.managersLock.RUnlock()
	return v, ok
}

//bucket manager already existed
func (s *BucketStore) HasBucketManager(id PartitionId) bool {
	s.managersLock.RLock()
	_, ok := s.bucketManagers[id]
	s.managersLock.RUnlock()
	return ok
}

func (s *BucketStore) SetDataCenter(dataCenter string) {
	s.dataCenter = dataCenter
}
func (s *BucketStore) SetRack(rack string) {
	s.rack = rack
}

func (s *BucketStore) SetBootstrapMaster(bootstrapMaster string) {
	s.masterNodes = storage.NewMasterNodes(bootstrapMaster)
}
func (s *BucketStore) Join() (masterNode string, e error) {
	masterNode, e = s.masterNodes.FindMaster()
	if e != nil {
		return
	}
	var BucketMessages []*operation.PartitionInformationMessage
	s.managersLock.RLock()
	for k, _ := range s.bucketManagers {
		BucketMessage := &operation.PartitionInformationMessage{
			Id:               proto.Uint32(uint32(k)),
			ReplicaPlacement: proto.Uint32(uint32(s.ReplicaPlacement.Byte())),
			Collection:       proto.String(public.BUCKET),
		}
		BucketMessages = append(BucketMessages, BucketMessage)
	}
	s.managersLock.RUnlock()
	joinMessage := &operation.PartitionJoinMessage{
		IsInit:     proto.Bool(!s.connected),
		Ip:         proto.String(s.Ip),
		Port:       proto.Uint32(uint32(s.Port)),
		PublicUrl:  proto.String(s.PublicUrl),
		DataCenter: proto.String(s.dataCenter),
		Rack:       proto.String(s.rack),
		Partitions: BucketMessages,
		Collection: proto.String(public.BUCKET),
	}

	data, err := proto.Marshal(joinMessage)
	if err != nil {
		return "", err
	}
	jsonBlob, err := util.PostBytes_timeout("http://"+masterNode+"/bucketpartition/join", data)
	if err != nil {
		s.masterNodes.Reset()
		return "", err
	}
	var ret operation.JoinResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return masterNode, err
	}
	if ret.Error != "" {
		return masterNode, errors.New(ret.Error)
	}
	glog.V(4).Infoln("Store succeeds to join master:", masterNode, BucketMessages)
	s.connected = true
	return
}
func (s *BucketStore) Create(storeBuk StoreBucket) (err error) {
	pID, err := NewPartitionId(storeBuk.ParId)
	if err != nil {
		return BADPARIDERR(storeBuk.ParId)
	}
	if bukMgr, ok := s.GetBucketManager(pID); ok {
		return bukMgr.Create(storeBuk.Bucket)
	}
	return errors.New("NoBucketPartitionAvailable")

}
func (s *BucketStore) Delete(id string, account string, bucket string) error {
	pid, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if bukMgr, ok := s.GetBucketManager(pid); ok {
		return bukMgr.Delete(account, bucket)
	}
	return errors.New("No Partition Found!")
}
func (s *BucketStore) Get(id string, account string, bucket string) (buk *Bucket, err error) {
	pid, err := NewPartitionId(id)
	if err != nil {
		return nil, BADPARIDERR(id)
	}
	if bukMgr, ok := s.GetBucketManager(pid); ok {
		return bukMgr.Get(account, bucket)
	}
	return nil, errors.New("NoBucket")
}
func (s *BucketStore) Update(id, account, bucket string, num int, size int64) (err error) {
	pid, err := NewPartitionId(id)
	if err != nil {
		return errors.New("BadParID" + id)
	}
	if bukMgr, ok := s.GetBucketManager(pid); ok {
		return bukMgr.Update(account, bucket, num, size)
	}
	return errors.New("NoBucket")
}
func (s *BucketStore) ModifyAuth(id string, account string, bucket string, accessType string, timeStamp string, isSync bool) error {
	pid, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if bukMgr, ok := s.GetBucketManager(pid); ok {
		return bukMgr.ModifyAuth(account, bucket, accessType, timeStamp, isSync)
	}
	return errors.New("NoBucket")
}
func (s *BucketStore) List(parId, acc, marker, prefix string, maxNum int) (bukList *BucketList, err error) {
	pid, err := NewPartitionId(parId)
	if err != nil {
		return nil, BADPARIDERR(parId)
	}
	if bukMgr, ok := s.GetBucketManager(pid); ok {
		if bukList, err = bukMgr.List(acc, marker, prefix, maxNum); err != nil {
			return nil, err
		}
	}
	return
}
func (s *BucketStore) ListAll(parId string) (bukList *BucketList, err error) {
	pid, err := NewPartitionId(parId)
	if err != nil {
		return nil, BADPARIDERR(parId)
	}
	if bukMgr, ok := s.GetBucketManager(pid); ok {
		if bukList, err = bukMgr.ListAll(); err != nil {
			return nil, err
		}
	}
	return
}
func (s *BucketStore) Close() {}
func (s *BucketStore) GetPartionIDs() (parids []string) {
	s.managersLock.RLock()
	for id, _ := range s.bucketManagers {
		parids = append(parids, id.String())
	}
	s.managersLock.RUnlock()
	return
}
func (s *BucketStore) OutgoingAccSync(id string, parNum int, dests map[string][]string) {
	pID, err := NewPartitionId(id)
	if err != nil {
		glog.V(0).Infoln("OutSync:", BADPARIDERR(id).Error())
	}
	buk, ok := s.GetBucketManager(pID)
	if ok == false {
		glog.V(0).Infoln("OutSync:", "No Partition Found")
		return
	}
	buk.OutgoingAccSync(parNum, dests)
	return
}
func (s *BucketStore) Set(id, account, bucket, key, value string) (err error) {
	pid, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if bukMgr, ok := s.GetBucketManager(pid); ok {
		return bukMgr.Set(account, bucket, key, value)
	}
	return fmt.Errorf("NotBucket")
}
func (s *BucketStore) FindMaster() string {
	if master, err := s.masterNodes.FindMaster(); err == nil {
		return master
	}
	return ""
}
func (s *BucketStore) BatchTrx(id, dst string) (err error) {
	pid, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if bukMgr, ok := s.GetBucketManager(pid); ok {
		if err = bukMgr.BatchTrx(dst); err == nil {
			s.DeletePartition(id)
			s.Join()
		}
		return
	}
	return errors.New("NoBucketPartitionAvailable")
}
func (s *BucketStore) BatchRcv(id string, data []byte) (err error) {
	pId, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	db, prefix, err := s.GetPublicMysqlDB()
	if err != nil {
		return err
	}
	defer db.Close()
	dsn := prefix + "/" + "bucket" + "_" + pId.String()
	if db, err = sql.Open("mysql", dsn); err != nil {
		return
	}
	defer db.Close()
	bukList := &BucketList{}
	if err = json.Unmarshal(data, bukList); err != nil {
		glog.V(0).Infoln("Moving bucket.bucket:Failed to decode data:", err)
		return err
	}
	if err = s.batchInsBucket(bukList.Buks, db); err != nil {
		glog.V(0).Infoln("Moving bucket.bucket:Failed to insert row:", err)
		return err
	}
	return
}

func (s *BucketStore) batchInsBucket(list []Bucket, db *sql.DB) (err error) {
	insStmt := "INSERT INTO bucket(" +
		"name," +
		"account," +
		"created_time," +
		"put_time," +
		"object_count," +
		"byte_used," +
		"acl," +
		"logprefix," +
		"staticnotfound," +
		"refers," +
		"refersallowempty," +
		"lifecycle," +
		"collection," +
		"access_type," +
		"sys_define," +
		"enable," +
		"location," +
		"is_deleted," +
		"support_append," +
		"image_service," +
		"max_down_tps," +
		"max_up_tps," +
		"max_file_size," +
		"sync," +
		"cdn_life_cycle," +
		"reserver1," +
		"reserver2," +
		"reserver3," +
		"reserver4," +
		"reserver5," +
		"resource)" +
		"VALUES" +
		"(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
		"ON DUPLICATE KEY UPDATE " +
		"name=?," +
		"account=?," +
		"created_time=?," +
		"put_time=?," +
		"object_count=?," +
		"byte_used=?," +
		"acl=?," +
		"logprefix=?," +
		"staticnotfound=?," +
		"refers=?," +
		"refersallowempty=?," +
		"lifecycle=?," +
		"collection=?," +
		"access_type=?," +
		"sys_define=?," +
		"enable=?," +
		"location=?," +
		"is_deleted=?," +
		"support_append=?," +
		"image_service=?," +
		"max_down_tps=?," +
		"max_up_tps=?," +
		"max_file_size=?," +
		"sync=?," +
		"cdn_life_cycle=?," +
		"reserver1=?," +
		"reserver2=?," +
		"reserver3=?," +
		"reserver4=?," +
		"reserver5=?," +
		"resource=?"
	count := 0
	defer func() {
		glog.V(0).Infoln("======> add", count, "records to table bucket")
	}()
	_, err = db.Exec(bucketTableCreateScript)
	if err != nil {
		return
	}
	tx, err := db.Begin()
	if err != nil {
		return
	}

	for _, b := range list {
		resource := ""
		if b.MRuleSets != nil {
			tempdata, _ := json.Marshal(b.MRuleSets)
			resource = string(tempdata)
		}

		_, err = tx.Exec(insStmt, b.Name, b.Account, b.CreateTime, b.PutTime, b.ObjectCount, b.ByteUsed, b.Acl, b.LogPrefix, b.StaticNotFound, b.Refers, b.RefersAllowEmpty, b.LifeCycle, b.Collection, b.AccessType, b.SysDefine, b.Enable, b.Location, b.IsDeleted, b.SupportAppend, b.ImageService, b.MaxDonwTps, b.MaxUpTps, b.MaxFileSize, b.SyncType, b.MaxAgeDay, b.Reserver1, b.Reserver2, b.Reserver3, b.Reserver4, b.Reserver5, resource, b.Name, b.Account, b.CreateTime, b.PutTime, b.ObjectCount, b.ByteUsed, b.Acl, b.LogPrefix, b.StaticNotFound, b.Refers, b.RefersAllowEmpty, b.LifeCycle, b.Collection, b.AccessType, b.SysDefine, b.Enable, b.Location, b.IsDeleted, b.SupportAppend, b.ImageService, b.MaxDonwTps, b.MaxUpTps, b.MaxFileSize, b.SyncType, b.MaxAgeDay, b.Reserver1, b.Reserver2, b.Reserver3, b.Reserver4, b.Reserver5, resource)
		if err != nil {
			tx.Rollback()
			return
		}
		count++
	}
	tx.Commit()
	return
}
func (s *BucketStore) DeletePartition(id string) (err error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	storage.DelDsnFromConf(s.GetConfDir(), id, "", "")
	s.managersLock.Lock()
	delete(s.bucketManagers, pID)
	s.managersLock.Unlock()
	s.Join()
	return
}
func (s *BucketStore) MaskPartition(id string) (err error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if _, ok := s.GetBucketManager(pID); ok {
		s.managersLock.Lock()
		delete(s.bucketManagers, pID)
		s.managersLock.Unlock()
		s.Join()
		return
	}
	return errors.New("NoAccountPartitionAvailable")
}
func (s *BucketStore) UnMaskPartition(id string) (err error) {
	s.loadExsitingBucketPartition()
	s.Join()
	return nil
}
