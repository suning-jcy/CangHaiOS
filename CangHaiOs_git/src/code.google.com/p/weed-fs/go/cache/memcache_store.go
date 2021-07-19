package cache

import (
	"bufio"
	proto "code.google.com/p/goprotobuf/proto"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/util"
	"github.com/hoisie/redis"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)
type MemcacheManager struct {
     url string
     client *redis.Client
}
type MemcacheStore struct {
	Port             int
	Ip               string
	PublicUrl        string
	dir              string
	dataCenter       string //optional informaton, overwriting master setting if exists
	rack             string //optional information, overwriting master setting if exists
	connected        bool
	masterNodes      *storage.MasterNodes
	storetype        string
	createIfNotExist bool
	memcacheManagers  map[storage.PartitionId] *MemcacheManager
	accessLock       sync.Mutex
}

func NewMemcacheStore(port int, ip string, rack string, publicUrl string, storedir string, storetype string) (s *MemcacheStore, err error) {
	s = &MemcacheStore{Port: port, Ip: ip, rack: rack, PublicUrl: publicUrl, dir: storedir, storetype: storetype}
	s.memcacheManagers = make(map[storage.PartitionId]*MemcacheManager)
	s.loadExsitingMemcachePartition()
	return
}

func NewMemcacheManager(url string) (mcm *MemcacheManager, err error) {
	mcm = &MemcacheManager{url:url}
	
	client := &redis.Client{}
	client.Addr = url
	client.Db   = 0
	client.MaxPoolSize = 100
	client.Password = "go-redis"
	mcm.client = client
	return
}


func (s *MemcacheStore) HasMemcacheManager(id storage.PartitionId) bool {
	_, ok := s.memcacheManagers[id]
	return ok
}

//load existing bucket from bukcet dir
func (s *MemcacheStore) loadExsitingMemcachePartition() {
	confFile, err := os.OpenFile(filepath.Join(s.dir, "memcache.conf"), os.O_RDONLY, 0644)
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

			parts := strings.Split(parstr, ",")
			lens := len(parts)
			if lens != 2 {
				continue
			}
			base, url := parts[0], parts[1]
			if parID, err := storage.NewPartitionId(base); err == nil {
				if !s.HasMemcacheManager(parID) {
		
					if mcMngr, e := NewMemcacheManager(url); e == nil {
						glog.V(0).Infoln("Add memcache partition:", url, parID.String())
						s.SetPartition(parID, mcMngr)
					}else{
					   glog.V(0).Infoln("Add memcache partition err:", url, parID.String(),e)
					}
				}
			}
		default:
			continue
		}

	}
	glog.V(0).Infoln("MemcacheStore started on dir:", s.dir)
}

//SetPartition
func (s *MemcacheStore) SetPartition(id storage.PartitionId, mcMngr *MemcacheManager) {
	s.memcacheManagers[id] = mcMngr
}

func (s *MemcacheStore) SetDataCenter(dataCenter string) {
	s.dataCenter = dataCenter
}
func (s *MemcacheStore) SetRack(rack string) {
	s.rack = rack
}

func (s *MemcacheStore) SetBootstrapMaster(bootstrapMaster string) {
	s.masterNodes = storage.NewMasterNodes(bootstrapMaster)
}
func (s *MemcacheStore) Join() (masterNode string, e error) {
	masterNode, e = s.masterNodes.FindMaster()
	if e != nil {
		return
	}
	rp, e := storage.NewReplicaPlacementFromString("001")
	if e != nil {
		return
	}
	var MemcacheMessages []*operation.PartitionInformationMessage
	for k, v := range s.memcacheManagers {
		MemcacheMessage := &operation.PartitionInformationMessage{
			Id:               proto.Uint32(uint32(k)),
			ReplicaPlacement: proto.Uint32(uint32(rp.Byte())),
			Collection:       proto.String(public.MEMCACHE),
			AccessUrl:              proto.String(v.url),
		}
		MemcacheMessages = append(MemcacheMessages, MemcacheMessage)
	}

	joinMessage := &operation.PartitionJoinMessage{
		IsInit:     proto.Bool(!s.connected),
		Ip:         proto.String(s.Ip),
		Port:       proto.Uint32(uint32(s.Port)),
		PublicUrl:  proto.String(s.PublicUrl),
		DataCenter: proto.String(s.dataCenter),
		Rack:       proto.String(s.rack),
		Partitions: MemcacheMessages,
		Collection: proto.String(public.MEMCACHE),
	}

	data, err := proto.Marshal(joinMessage)
	if err != nil {
		return "", err
	}
	jsonBlob, err := util.PostBytes("http://"+masterNode+"/partition/join", data)
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
func (s *MemcacheStore) GetMemcacheManager(id storage.PartitionId) (*MemcacheManager, bool) {
	v, ok := s.memcacheManagers[id]
	return v, ok
}


func (s *MemcacheStore) Close() {
}
func (s *MemcacheStore) Status() map[string]interface{} {
	m := make(map[string] interface{})
	m["partition_total"] = len(s.memcacheManagers)
	mcmangers :=make(map[string]interface{})
	for parId, mcm := range s.memcacheManagers {
	      mcmangers[parId.String()] = mcm.url

		}
    m["partition"] = mcmangers
	return m
}
func (s *MemcacheStore) GetPartionIDs() (parids []string) {
	for id, _ := range s.memcacheManagers {
		parids = append(parids, id.String())
	}
	return parids
}

