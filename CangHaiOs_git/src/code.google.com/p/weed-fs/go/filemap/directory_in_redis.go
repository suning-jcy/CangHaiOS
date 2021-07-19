package filemap

import (
	"github.com/hoisie/redis"
	"strconv"
	"sync"
	"strings"
	//"fmt"
	//"time"
)

const (
    DirIdSeq = "dirIdSeq"
   )


type DirManagerInRedis struct {
    client *redis.Client
    slaveClient *redis.Client
	max       DirectoryId
	accessLock sync.Mutex
}
func NewDirManagerInRedis(db *DbManager) (dm *DirManagerInRedis, err error) {
   /*
    parts := strings.Split(db.dirmapconf, ":")
    ports, _ := strconv.Atoi(parts[1])
	dm = &DirManagerInRedis{}
	spec := redis.DefaultSpec().Host(parts[0]).Port(ports).Db(0).Password("go-redis")
	connTimeout := time.Duration(db.connTimeout) * time.Second
	spec = spec.ConnTimeout(connTimeout)
	if dm.client, err = redis.NewSynchClientWithSpec(spec); err != nil {
		return
	}   
	*/  
	   parts := strings.Split(db.dirmapconf, ",")
	    dm = &DirManagerInRedis{}
	    dm.client = &redis.Client{}
		dm.client.Addr = parts[0]
		dm.client.Db   = 0
		dm.client.Password = "go-redis"
	   if len(parts) == 2 {
	   
	    dm.slaveClient = &redis.Client{}
		dm.slaveClient.Addr = parts[1]
		dm.slaveClient.Db   = 0
		dm.slaveClient.Password = "go-redis"
	   
	   }
		
	dm.max, _ = dm.FindDirectory(DirIdSeq)
	return
}

func (dm *DirManagerInRedis) FindDirectory(dirPath string) (DirectoryId, error) {
   	data, e := dm.client.Get(dirPath)
	if e !=nil  && dm.slaveClient != nil {
	  // fmt.Println("finddir err,%s",e.Error())
	   data, e = dm.slaveClient.Get(dirPath)   
	}
    if e != nil  {
	    return 0,e
	 }
	str := string(data)
	v, pe := strconv.Atoi(str)
	if pe != nil {
	    return 0, pe
	 }
	return DirectoryId(v), nil
}

func (dm *DirManagerInRedis) MakeDirectory(dirPath string) (DirectoryId, error) {
    dm.accessLock.Lock()
	defer dm.accessLock.Unlock()
	dirId, e := dm.FindDirectory(dirPath)
	if e == nil {
	    return dirId,nil
	 }
	dm.max++
	dirId = dm.max
	e = dm.client.Set(dirPath, []byte(strconv.Itoa(int(dirId))))
	if e == nil {
		e = dm.client.Set(DirIdSeq, []byte(strconv.Itoa(int(dm.max))))
	}
	return dirId, e
}

func (dm *DirManagerInRedis) MoveUnderDirectory(oldDirPath string, newParentDirPath string, newName string) error {

	return nil
}

func (dm *DirManagerInRedis) ListDirectories(dirPath string) (dirNames []DirectoryEntry, err error) {
    subdirs, e := dm.client.Keys(dirPath + "*")
    if e != nil {
		return dirNames, e
	}
	for _, subdir := range subdirs {
	    id, e := dm.FindDirectory(subdir)
	    if e == nil {
		    dirNames = append(dirNames, DirectoryEntry{Name: subdir, Id: id})
		    }
	}
	return dirNames, nil
}
func (dm *DirManagerInRedis) DeleteDirectory(dirPath string) error {
	_, e := dm.client.Del(dirPath)
	return e
	
}
