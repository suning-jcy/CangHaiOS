package account

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"

	proto "code.google.com/p/goprotobuf/proto"
	"code.google.com/p/weed-fs/go/bucket"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/util"
)

type AccountStore struct {
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
	accountManagers  map[PartitionId]AccountManager
	managersLock     sync.RWMutex

	accessLock sync.Mutex
	util.DBStoreCommon
}

func NewAccountStore(port int, ip string, rack string, publicUrl string, storedir string, storetype string, replication string) (s *AccountStore, err error) {
	s = &AccountStore{Port: port, Ip: ip, rack: rack, PublicUrl: publicUrl, storetype: storetype, createIfNotExist: true}
	s.accountManagers = make(map[PartitionId]AccountManager)
	s.ReplicaPlacement, err = storage.NewReplicaPlacementFromString(replication)
	if err != nil {
		return
	}
	s.InitStoreCom(storedir)
	s.loadExsitingAccountPartition()
	return
}

func (s *AccountStore) AddAccountPartition(id string, needCreate, insTopo bool) (err error) {
	s.accessLock.Lock()
	defer s.accessLock.Unlock()
	parId, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if s.HasAccountManager(parId) {
		return nil
	}
	glog.V(0).Infoln("adds AccountPartition =", id,needCreate,insTopo)
	dbname := "account" + "_" + parId.String()
	publicdb, dbprefix, err := s.GetPublicMysqlDB()
	if err != nil {
		glog.V(0).Infoln("get public dsn err", err.Error())
		return err
	}
	defer publicdb.Close()
	if needCreate {
		_, err = publicdb.Exec("create database if not exists " + dbname)
		if err != nil {
			glog.V(0).Infoln("create database err", dbname, err.Error())
			return err
		}
	}
	if insTopo {
		dsn := dbprefix + "/" + dbname
		if accountMngr, err := NewMysqlAccountManager(dsn, parId.String(), needCreate); err == nil {
			s.SetPartition(parId, accountMngr)
			s.Join()
			storage.WriteNewParToConf(s.GetConfDir(), dsn, parId.String(), "")
			return nil
		}
	}
	return
}

func (s *AccountStore) HasAccountManager(id PartitionId) bool {
	s.managersLock.RLock()
	_, ok := s.accountManagers[id]
	s.managersLock.RUnlock()
	return ok
}

//load existing bucket from bukcet dir
func (s *AccountStore) loadExsitingAccountPartition() {
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
				if !s.HasAccountManager(parID) {
					if accountMngr, e := NewMysqlAccountManager(dsn, parID.String(), false); e == nil {
						glog.V(0).Infoln("Add account partition:", dsn, parID.String())
						s.SetPartition(parID, accountMngr)
					}
				}
			}
		default:
			continue
		}

	}
	glog.V(0).Infoln("AccountPartitionStore started on dir:", s.GetConfDir())
}

//SetPartition
func (s *AccountStore) SetPartition(id PartitionId, accountMngr AccountManager) {
	s.managersLock.Lock()
	s.accountManagers[id] = accountMngr
	s.managersLock.Unlock()
}

func (s *AccountStore) SetDataCenter(dataCenter string) {
	s.dataCenter = dataCenter
}
func (s *AccountStore) SetRack(rack string) {
	s.rack = rack
}

func (s *AccountStore) SetBootstrapMaster(bootstrapMaster string) {
	s.masterNodes = storage.NewMasterNodes(bootstrapMaster)
}
func (s *AccountStore) Join() (masterNode string, e error) {
	masterNode, e = s.masterNodes.FindMaster()
	if e != nil {
		return
	}
	var AccountMessages []*operation.PartitionInformationMessage
	s.managersLock.RLock()
	for k, _ := range s.accountManagers {
		AccountMessage := &operation.PartitionInformationMessage{
			Id:               proto.Uint32(uint32(k)),
			ReplicaPlacement: proto.Uint32(uint32(s.ReplicaPlacement.Byte())),
			Collection:       proto.String(public.ACCOUNT),
		}
		AccountMessages = append(AccountMessages, AccountMessage)
	}
	s.managersLock.RUnlock()
	joinMessage := &operation.PartitionJoinMessage{
		IsInit:     proto.Bool(!s.connected),
		Ip:         proto.String(s.Ip),
		Port:       proto.Uint32(uint32(s.Port)),
		PublicUrl:  proto.String(s.PublicUrl),
		DataCenter: proto.String(s.dataCenter),
		Rack:       proto.String(s.rack),
		Partitions: AccountMessages,
		Collection: proto.String(public.ACCOUNT),
	}

	data, err := proto.Marshal(joinMessage)
	if err != nil {
		return "", err
	}
	jsonBlob, err := util.PostBytes_timeout("http://"+masterNode+"/accountpartition/join", data)
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
	s.connected = true
	return
}
func (s *AccountStore) GetAccountManager(id PartitionId) (AccountManager, bool) {
	s.managersLock.RLock()
	v, ok := s.accountManagers[id]
	s.managersLock.RUnlock()
	return v, ok
}
func (s *AccountStore) Create(id string, name string, password,creatTime string) (string, error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return "", BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.Create(name, password,creatTime)
	}
	return "", errors.New("NoAccountPartitionAvailable")

}
func (s *AccountStore) Delete(id string, name string) (string, error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return "", BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.Delete(name)
	}
	return "", errors.New("NoAccountPartitionAvailable")
}
func (s *AccountStore) MarkDelete(id string, name string) error {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.MarkDelete(name)
	}
	return errors.New("NoAccountPartitionAvailable")
}
func (s *AccountStore) List(id string, marker, prefix string, maxNum int) ([]*Account, error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return nil, BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.List(marker, prefix, maxNum)
	}
	return nil, errors.New("NoAccountPartitionAvailable")
}
func (s *AccountStore) Get(id string, name string) ([]*Account, error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return nil, BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.Get(name)
	}
	return nil, errors.New("NoAccountPartitionAvailable")
}
func (s *AccountStore) IncomingBukSync(id string, buk *bucket.Bucket) error {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.IncomingBukSync(buk)
	}
	return errors.New("NoAccountPartitionAvailable")
}
func (s *AccountStore) Close() {
}
func (s *AccountStore) Status() {

}
func (s *AccountStore) GetPartionIDs() (parids []string) {
	s.managersLock.RLock()
	for id, _ := range s.accountManagers {
		parids = append(parids, id.String())
	}
	s.managersLock.RUnlock()
	return parids
}
func (s *AccountStore) EnableAccount(id string, account string) (err error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.Enable(account)
	}
	return errors.New("NoAccountPartitionAvailable")
}
func (s *AccountStore) DisableAccount(id string, account string) (err error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.Disable(account)
	}
	return errors.New("NoAccountPartitionAvailable")
}
func (s *AccountStore) DelAccessKey(id, account, accId string) (err error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.DelAccessKey(account, accId)
	}
	return errors.New("NoAccountPartitionAvailable")
}
func (s *AccountStore) AddAccessKey(id, account, accId, accKey string) (err error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.AddAccessKey(account, accId, accKey)
	}
	return errors.New("NoAccountPartitionAvailable")
}
func (s *AccountStore) BatchRcv(id string, data []byte) (err error) {
	pId, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	db, prefix, err := s.GetPublicMysqlDB()
	if err != nil {
		return err
	}
	defer db.Close()
	dsn := prefix + "/" + "account" + "_" + pId.String()
	if db, err = sql.Open("mysql", dsn); err != nil {
		return
	}
	defer db.Close()
	glog.V(0).Infoln("id:",id)
	accData := &AccountData{}
	if err = json.Unmarshal(data, accData); err != nil {
		return
	}
	if accData.Table == "account" {
		list := []AccountStat{}
		if accData.Data != nil{
			if err = json.Unmarshal(accData.Data, &list); err != nil {
				return
			}
		}
		glog.V(0).Infoln(" add account records:",len(list))
		return s.batchInsAccount(list, db)
	} else if accData.Table == "bucket" {
		list := []AccountBucket{}
		if accData.Data != nil {
			if err = json.Unmarshal(accData.Data, &list); err != nil {
				return
			}
		}
		glog.V(0).Infoln(" add bucket records:",len(list))
		return s.batchInsBucket(list, db)
	}
	return
}
func (s *AccountStore) batchInsAccount(list []AccountStat, db *sql.DB) (err error) {
	count:=0
	insStmt := "INSERT INTO account(" +
		"name," +
		"created_time," +
		"auth_type," +
		"bucket_count," +
		"object_count," +
		"byte_used," +
		"deleted," +
		"access_user," +
		"status," +
		"max_bucket," +
		"descrip," +
		"access_user_ex," +
		"reserver1," +
		"reserver2," +
		"reserver3," +
		"reserver4," +
		"reserver5) " +
		"VALUES" +
		"(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
		"ON DUPLICATE KEY UPDATE " +
		"name=?," +
		"created_time=?," +
		"auth_type=?," +
		"bucket_count=?," +
		"object_count=?," +
		"byte_used=?," +
		"deleted=?," +
		"access_user=?," +
		"status=?," +
		"max_bucket=?," +
		"descrip=?," +
		"access_user_ex=?," +
		"reserver1=?," +
		"reserver2=?," +
		"reserver3=?," +
		"reserver4=?," +
		"reserver5=?"
	defer func() {
		glog.V(0).Infoln("======> add",count,"records to table account")
	}()
	_, err = db.Exec(accountTableCreateScript)
	if err != nil {
		return
	}
	tx, err := db.Begin()
	if err != nil {
		return
	}
	for _, i := range list {
		_, err = tx.Exec(insStmt, i.Name, i.CreateTime, i.AuthType, i.BucketUsed, i.ObjectCount, i.ByteUsed, i.IsDeleted, i.AccUser, i.Enable, i.MaxBucket, i.Descrip, i.AccessEx, i.RSV1, i.RSV2, i.RSV3, i.RSV4, i.RSV5, i.Name, i.CreateTime, i.AuthType, i.BucketUsed, i.ObjectCount, i.ByteUsed, i.IsDeleted, i.AccUser, i.Enable, i.MaxBucket, i.Descrip, i.AccessEx, i.RSV1, i.RSV2, i.RSV3, i.RSV4, i.RSV5)
		if err != nil {
			tx.Rollback()
			return
		}
		count++
	}
	tx.Commit()
	return
}
func (s *AccountStore) batchInsBucket(list []AccountBucket, db *sql.DB) (err error) {
	insStmt := "INSERT INTO bucket(" +
		"name," +
		"account," +
		"created_time," +
		"put_time," +
		"object_count," +
		"byte_used," +
		"is_deleted)" +
		"VALUES" +
		"(?,?,?,?,?,?,?) " +
		"ON DUPLICATE KEY UPDATE " +
		"name=?," +
		"account=?," +
		"created_time=?," +
		"put_time=?," +
		"object_count=?," +
		"byte_used=?," +
		"is_deleted=?"
	count:=0
	defer func() {
		glog.V(0).Infoln("======> add",count,"records to table bucket")
	}()
	_, err = db.Exec(bucketTableCreateScript)
	if err != nil {
		return
	}
	tx, err := db.Begin()
	if err != nil {
		return
	}
	for _, i := range list {
		_, err = tx.Exec(insStmt, i.Name, i.Account, i.CreatedTime, i.PutTime, i.ObjectCnt, i.ByteUsed, i.IsDeleted, i.Name, i.Account, i.CreatedTime, i.PutTime, i.ObjectCnt, i.ByteUsed, i.IsDeleted)
		if err != nil {
			tx.Rollback()
			return
		}
		count++
	}
	tx.Commit()
	return
}
func (s *AccountStore) BatchTrx(id, dst string) (err error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		if err = accMgr.BatchTrx(dst); err == nil {
			s.DeletePartition(id)
			s.Join()
		}
		return
	}
	return errors.New("NoAccountPartitionAvailable")
}
func (s *AccountStore) DeletePartition(id string) (err error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	storage.DelDsnFromConf(s.GetConfDir(), id, "", "")
	s.managersLock.Lock()
	delete(s.accountManagers, pID)
	s.managersLock.Unlock()
	s.Join()
	return
}
func (s *AccountStore) MaskPartition(id string) (err error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if _, ok := s.GetAccountManager(pID); ok {
		s.managersLock.Lock()
		delete(s.accountManagers, pID)
		s.managersLock.Unlock()
		s.Join()
		return
	}
	return errors.New("NoAccountPartitionAvailable")
}
func (s *AccountStore) UnMaskPartition(id string) (err error) {
	s.loadExsitingAccountPartition()
	s.Join()
	return nil
}

func (s *AccountStore) GetDomainResolution(id string, domainname string) (string, error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return "", BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.GetDomainResolution(domainname)
	}
	return "", errors.New("NoAccountPartitionAvailable")
}

func (s *AccountStore) GetBucketDomain(id string, bucketname string) ([]*BucketDomain, error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return nil, BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.GetBucketDomain(bucketname)
	}
	return nil, errors.New("NoAccountPartitionAvailable")
}

func (s *AccountStore) DeleteDomainResolution(id string, domainname string, bucketname string) (error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.DeleteDomainResolution(domainname, bucketname)
	}
	return errors.New("NoAccountPartitionAvailable")
}

func (s *AccountStore) SetDomainResolution(id, domainname, bucketname string,enable,isdefault bool) (error) {
	pID, err := NewPartitionId(id)
	if err != nil {
		return BADPARIDERR(id)
	}
	if accMgr, ok := s.GetAccountManager(pID); ok {
		return accMgr.SetDomainResolution(domainname, bucketname,enable,isdefault)
	}
	return errors.New("NoAccountPartitionAvailable")
}
