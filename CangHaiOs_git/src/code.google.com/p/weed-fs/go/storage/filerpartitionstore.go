package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	_ "math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	proto "code.google.com/p/goprotobuf/proto"
	"code.google.com/p/weed-fs/go/filemap"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/util"
)

type PartitionCollection struct {
	partitions map[PartitionId]Partition
	sync.RWMutex
}
type FilerPartitionStore struct {
	Port               int
	Ip                 string
	PublicUrl          string
	parCollection      map[string]*PartitionCollection
	dataCenter         string //optional informaton, overwriting master setting if exists
	rack               string //optional information, overwriting master setting if exists
	connected          bool
	masterNodes        *MasterNodes
	storetype          string
	Collection         string
	parRebalanceStatus ParRebalanceStatus
	parRepRepairStatus ParRepRepairStatus
	accessLock         sync.Mutex
	joinMask           map[string]string
	util.DBStoreCommon
}

func NewFilerPartitionStore(port int, ip string, publicUrl string, storedir string, storetype, collection string) (s *FilerPartitionStore) {
	s = &FilerPartitionStore{Port: port, Ip: ip, PublicUrl: publicUrl, storetype: storetype, Collection: collection}
	s.parCollection = make(map[string]*PartitionCollection)
	s.joinMask = make(map[string]string)
	s.InitStoreCom(storedir)
	s.loadExistingPartitions(storetype)
	return
}
func (s *FilerPartitionStore) AddFilerPartition(parListString string, collection string, replicaPlacement string, needCreate bool) error {
	rt, e := NewReplicaPlacementFromString(replicaPlacement)
	if e != nil {
		return e
	}
	for _, range_string := range strings.Split(parListString, ",") {
		if strings.Index(range_string, "-") < 0 {
			id_string := range_string
			id, err := NewPartitionId(id_string)
			if err != nil {
				return fmt.Errorf("FilerPartition Id %s is not a valid unsigned integer!", id_string)
			}
			e = s.addFilerPartition(PartitionId(id), collection, rt, needCreate)
		} else {
			pair := strings.Split(range_string, "-")
			start, start_err := strconv.ParseUint(pair[0], 10, 64)
			if start_err != nil {
				return fmt.Errorf("FilerPartition Start Id %s is not a valid unsigned integer!", pair[0])
			}
			end, end_err := strconv.ParseUint(pair[1], 10, 64)
			if end_err != nil {
				return fmt.Errorf("FilerPartition End Id %s is not a valid unsigned integer!", pair[1])
			}
			for id := start; id <= end; id++ {
				if err := s.addFilerPartition(PartitionId(id), collection, rt, needCreate); err != nil {
					e = err
				}
			}
		}
	}
	return e
}
func (s *FilerPartitionStore) addFilerPartition(parId PartitionId, collection string, replicaPlacement *ReplicaPlacement, needCreate bool) (err error) {

	if s.HasPartition(collection, parId) {
		return nil
	}

	glog.V(0).Infoln("adds FilerPartition =", parId, ", collection =", collection, ", replicaPlacement =", replicaPlacement)
	if s.storetype == "leveldb" {
		FilerPartition, err := NewFilerPartition(s.GetConfDir(), parId, collection, replicaPlacement)
		if err == nil {
			s.SetPartition(collection, parId, FilerPartition)
			return nil
		}
	} else {
		publicdb, dbprefix, err := s.GetPublicMysqlDB()
		if err != nil {
			glog.V(0).Infoln("get public dsn err", err.Error())
			return err
		}
		defer publicdb.Close()
		dbname := genDbName(collection, parId.String())
		if needCreate {
			_, err = publicdb.Exec("create database " + dbname)
			if err != nil {
				glog.V(0).Infoln("create database err", dbname, err.Error())
				return err
			}
		}
		dsn := dbprefix + "/" + dbname
		if par, err := NewMysqlPartition(dsn, parId, collection, replicaPlacement, needCreate); err == nil {
			s.SetPartition(collection, parId, par)
			s.Join()
			WriteNewParToConf(s.GetConfDir(), dsn, parId.String(), collection)
			return nil
		}

	}

	return err
}

func (s *FilerPartitionStore) loadExistingPartitions(storetype string) {
	if storetype == "leveldb" {
		s.loadExistingFilerPartitions()
	} else {
		s.loadExistingMysqlPartitions()
	}

}
func (s *FilerPartitionStore) loadExistingFilerPartitions() {
	if dirs, err := ioutil.ReadDir(s.GetConfDir()); err == nil {
		for _, dir := range dirs {
			name := dir.Name()
			if dir.IsDir() && strings.HasPrefix(name, "par_") {
				collection := ""
				rpstr := "000"
				base := name[len("par_"):]
				parts := strings.Split(base, "_")
				lens := len(parts)
				if lens < 2 || lens > 4 {
					glog.V(0).Infoln("dir in not a file map partition", name)
					continue
				}
				if lens == 2 {
					base, rpstr = parts[0], parts[1]
				} else {
					collection, base, rpstr = parts[0], parts[1], parts[2]
				}
				if PartitionId, err := NewPartitionId(base); err == nil {
					if !s.HasPartition(collection, PartitionId) {
						rt, e := NewReplicaPlacementFromString(rpstr)
						if e != nil {
							glog.V(0).Infoln("dir in not a file map partition", name)
							continue
						}
						if FilerPartition, e := NewFilerPartition(s.GetConfDir(), PartitionId, collection, rt); e == nil {
							s.SetPartition(collection, PartitionId, FilerPartition)
							glog.V(0).Infoln("load file map partition", name)
						}
					}
				}
			}
		}
	}
	glog.V(0).Infoln("FilerPartitionStore started on dir:", s.GetConfDir())
}

func (s *FilerPartitionStore) loadExistingMysqlPartitions() {
	confFile, err := os.OpenFile(filepath.Join(s.GetConfDir(), MYSQL_CONF), os.O_RDONLY, 0644)
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
			collection := ""
			rpstr := "000"
			dsn := ""
			base := ""
			parts := strings.Split(parstr, ",")
			lens := len(parts)
			if lens < 2 || lens > 3 {
				glog.V(0).Infoln("not partition", parstr)
				continue
			}
			if lens == 2 {
				base, dsn = parts[0], parts[1]
			} else {
				collection, base, dsn = parts[0], parts[1], parts[2]
			}
			if PartitionId, err := NewPartitionId(base); err == nil {
				if !s.HasPartition(collection, PartitionId) {
					rt, e := NewReplicaPlacementFromString(rpstr)
					if e != nil {
						glog.V(0).Infoln("not partition", parstr)
						continue
					}
					//if FilerPartition, e := NewMysqlPartition(dsn, PartitionId, collection, rt, false); e == nil {
					//2018-2-5  为了能够新增filetag，
					if FilerPartition, e := NewMysqlPartition(dsn, PartitionId, collection, rt, true); e == nil {
						s.SetPartition(collection, PartitionId, FilerPartition)
						glog.V(0).Infoln("load file map partition", parstr)
					}
				}
			}
		default:
			fmt.Printf("line %s has %s!\n", line, parts[0])
			continue
		}

	}

	glog.V(0).Infoln("FilerPartitionStore started on dir:", s.GetConfDir())
}

func (s *FilerPartitionStore) Status() []*PartitionInfo {
	var stats []*PartitionInfo
	s.accessLock.Lock()
	defer s.accessLock.Unlock()
	for _, parColl := range s.parCollection {
		parColl.RLock()
		for k, v := range parColl.partitions {
			s := &PartitionInfo{Id: PartitionId(k),
				Collection:       v.Collection(),
				ReplicaPlacement: v.Rp()}
			stats = append(stats, s)
		}
		parColl.RUnlock()
	}
	return stats
}

func (s *FilerPartitionStore) SetDataCenter(dataCenter string) {
	s.dataCenter = dataCenter
}
func (s *FilerPartitionStore) SetRack(rack string) {
	s.rack = rack
}

func (s *FilerPartitionStore) SetBootstrapMaster(bootstrapMaster string) {
	s.masterNodes = NewMasterNodes(bootstrapMaster)
}
func (s *FilerPartitionStore) Join() (masterNode string, e error) {
	s.accessLock.Lock()
	defer s.accessLock.Unlock()
	masterNode, e = s.masterNodes.FindMaster()
	if e != nil {
		return
	}
	var partitionMessages []*operation.PartitionInformationMessage
	for _, parColl := range s.parCollection {
		parColl.RLock()
		for k, v := range parColl.partitions {
			if _, ok := s.joinMask[k.String()]; ok {
				continue
			}
			partitionMessage := &operation.PartitionInformationMessage{
				Id:               proto.Uint32(uint32(k)),
				Collection:       proto.String(v.Collection()),
				ReplicaPlacement: proto.Uint32(uint32(v.Rp().Byte())),
			}
			partitionMessages = append(partitionMessages, partitionMessage)

		}
		parColl.RUnlock()
	}

	joinMessage := &operation.PartitionJoinMessage{
		IsInit:     proto.Bool(!s.connected),
		Ip:         proto.String(s.Ip),
		Port:       proto.Uint32(uint32(s.Port)),
		PublicUrl:  proto.String(s.PublicUrl),
		DataCenter: proto.String(s.dataCenter),
		Rack:       proto.String(s.rack),
		Partitions: partitionMessages,
		Collection: proto.String(s.Collection),
	}

	data, err := proto.Marshal(joinMessage)
	if err != nil {
		return "", err
	}

	jsonBlob, err := util.PostBytes_timeout("http://"+masterNode+"/partition/join", data)
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
func (s *FilerPartitionStore) Close() {
	s.accessLock.Lock()
	defer s.accessLock.Unlock()
	for _, parColl := range s.parCollection {
		parColl.RLock()
		for _, v := range parColl.partitions {
			v.Close()
		}
		parColl.RUnlock()
	}
}
func (s *FilerPartitionStore) GetOrCreatePartitionCollection(collection string) *PartitionCollection {
	s.accessLock.Lock()
	defer s.accessLock.Unlock()
	if v, found := s.parCollection[collection]; found {
		return v
	} else {
		parColl := &PartitionCollection{}
		parColl.partitions = make(map[PartitionId]Partition)
		s.parCollection[collection] = parColl
	}
	return s.parCollection[collection]
}

func (s *FilerPartitionStore) SetPartition(collection string, parId PartitionId, partition Partition) {
	parcoll := s.GetOrCreatePartitionCollection(collection)
	parcoll.Lock()
	parcoll.partitions[parId] = partition
	parcoll.Unlock()
}
func (s *FilerPartitionStore) GetPartition(collection string, parId PartitionId) Partition {
	parcoll := s.GetOrCreatePartitionCollection(collection)
	parcoll.RLock()
	if v, found := parcoll.partitions[parId]; found {
		parcoll.RUnlock()
		return v
	}
	parcoll.RUnlock()
	return nil
}

func (s *FilerPartitionStore) HasPartition(collection string, parId PartitionId) bool {
	parcoll := s.GetOrCreatePartitionCollection(collection)

	parcoll.RLock()
	_, found := parcoll.partitions[parId]
	parcoll.RUnlock()
	return found
}

func (s *FilerPartitionStore) Write(collection string, parId PartitionId, key string, value string, expireat string, length int64, mtime,disposition,mimeType,etag string) (oldvalue string, olen int64, oldMTime string, err error) {
	if filerPartition := s.GetPartition(collection, parId); filerPartition != nil {
		oldvalue, olen, oldMTime, err = filerPartition.CreateFile(key, value, expireat, length, mtime,disposition,mimeType,etag)
	}
	return
}

func (s *FilerPartitionStore) Delete(collection string, parId PartitionId, key string) (oldvalue string, olen int64, oldMTime string, err error) {
	if filerPartition := s.GetPartition(collection, parId); filerPartition != nil {
		oldvalue, olen, oldMTime, err = filerPartition.DeleteFile(key)
	}
	return
}

func (s *FilerPartitionStore) PartitionBatchUpdateFilemaps(parIdString string, collection string, rp string, data []byte) (error, bool) {
	parId, err := NewPartitionId(parIdString)
	if err != nil {
		return fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdString), false
	}

	if par := s.GetPartition(collection, parId); par != nil {
		if parRecover, ok := par.(PartitionRecover); ok {
			err = parRecover.batchUpdateFilemaps(data)
			if err == nil {
				return nil, true
			} else {
				return fmt.Errorf("partition update filemaps failed,%s", parIdString), false
			}
		}
	}
	return fmt.Errorf("par id %d is not found during update filemaps!", parId), false
}

//hujf 150409
func (s *FilerPartitionStore) DeleteCollection(collection string) (e error) {
	s.accessLock.Lock()
	defer s.accessLock.Unlock()
	if parColl, ok := s.parCollection[collection]; ok {
		parColl.Lock()
		for parId, partition := range parColl.partitions {
			if e = partition.Destroy(); e != nil {
				parColl.Unlock()
				return
			}
			delete(parColl.partitions, parId)
		}
		parColl.Unlock()
	}
	delete(s.parCollection, collection)
	return
}

func (s *FilerPartitionStore) DirFileCount(dir, reg string) (count int, err error) {
	return
}
func (s *FilerPartitionStore) List(parIdString string, account string, bucket string, marker string, prefix string, max int) (objList *filemap.ObjectList, err error) {
	parId, err := NewPartitionId(parIdString)
	if err != nil {
		return nil, fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdString)
	}

	if par := s.GetPartition(s.Collection, parId); par != nil {
		return par.List(account, bucket, marker, prefix, max)
	}
	return nil, fmt.Errorf("par id %d is not found during update filemaps!", parId)
}

func (s *FilerPartitionStore) GetFileCount(parIdString string, account string, bucket string) (count int64, size int64, err error) {
	parId, err := NewPartitionId(parIdString)
	if err != nil {
		return 0, 0, fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdString)
	}

	if par := s.GetPartition(s.Collection, parId); par != nil {
		return par.GetFileCount(account, bucket)
	}
	return 0, 0, fmt.Errorf("par id %d is not found during update filemaps!", parId)
}

//func (sfp *MysqlPartition) ListFiles(lastFileName string, dir string, limit int) (objList *filemap.ObjectListHissync, err error)
func (s *FilerPartitionStore) ListFiles(parIdString string, lastFileName string, dir string, limit int) (objList *filemap.ObjectListHissync, err error) {
	parId, err := NewPartitionId(parIdString)
	if err != nil {
		return nil, fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdString)
	}

	if par := s.GetPartition(s.Collection, parId); par != nil {
		return par.ListFilesFromPar(lastFileName, dir, limit)
	}
	return nil, fmt.Errorf("par id %d is not found during update filemaps!", parId)
}

//20200819zqx,删除partition侧扫完过期文件立即删除的操作，改为扫分片上指定范围的文件。
func (s *FilerPartitionStore) DeleteExpired(parIdString string, startlimit string, limitcnt, prefix string) (objList *filemap.ObjectList, err error) {
	parId, err := NewPartitionId(parIdString)
	if err != nil {
		return nil, fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdString)
	}

	if par := s.GetPartition(s.Collection, parId); par != nil {
		return par.DeleteExpired(startlimit, limitcnt, prefix)
	}
	return nil, fmt.Errorf("par id %d is not found during update filemaps!", parId)
}
