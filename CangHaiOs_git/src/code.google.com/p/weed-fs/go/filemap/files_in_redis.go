package filemap

import (
	"code.google.com/p/weed-fs/go/glog"
	"context"
	"github.com/hoisie/redis"
	"strings"
	//"fmt"
	//"strconv"
	"hash/crc32"
	//"time"
)

/*
The entry in level db has this format:
  key: genKey(dirId, fileName)
  value: []byte(fid)
And genKey(dirId, fileName) use first 4 bytes to store dirId, and rest for fileName
*/

type FileListInRedis struct {
	clients      []*redis.Client
	slaveClients []*redis.Client
}

func NewFileListInRedis(db *DbManager) (fl *FileListInRedis, err error) {
	fl = &FileListInRedis{}
	for i := 0; i < len(db.filemapconf); i++ {
		/*
		    	parts := strings.Split(db.filemapconf[i], ":")
		    	ports, _ := strconv.Atoi(parts[1])
				spec := redis.DefaultSpec().Host(parts[0]).Port(ports).Db(13).Password("go-redis")
				connTimeout := time.Duration(db.connTimeout) * time.Second
			    spec = spec.ConnTimeout(connTimeout)
				client, err := redis.NewSynchClientWithSpec(spec)
				 if err != nil {
					return fl,err
				}
		*/
		parts := strings.Split(db.filemapconf[i], ",")
		client := &redis.Client{}
		client.Addr = parts[0]
		client.Db = 13
		client.Password = "go-redis"
		fl.clients = append(fl.clients, client)
		if len(parts) == 2 {
			client := &redis.Client{}
			client.Addr = parts[1]
			client.Db = 13
			client.Password = "go-redis"
			fl.slaveClients = append(fl.slaveClients, client)
		}
	}
	return
}

func genKey1(dirId DirectoryId, fileName string) (key string, hash uint32) {
	ret := make([]byte, 0, 4+len(fileName))
	for i := 3; i >= 0; i-- {
		ret = append(ret, byte(dirId>>(uint(i)*8)))
	}
	ret = append(ret, []byte(fileName)...)
	key = string(ret)
	hash = crc32.ChecksumIEEE(ret)
	//fmt.Println("in genkey hash,",hash)
	return
}

func (fl *FileListInRedis) CreateFile(dirId DirectoryId, fileName string, fid string) (err error) {
	glog.V(4).Infoln("directory", dirId, "fileName", fileName, "fid", fid)
	key, hash := genKey1(dirId, fileName)
	//fmt.Println("out genkey hash,",hash)
	pos := hash % uint32(len(fl.clients))
	//fmt.Println("pos,clients",pos,uint32(len(fl.clients)))
	return fl.clients[pos].Set(key, []byte(fid))
}
func (fl *FileListInRedis) DeleteFile(dirId DirectoryId, fileName string) (fid string, err error) {
	if fid, err = fl.FindFile(dirId, fileName); err != nil {
		return
	}
	key, hash := genKey1(dirId, fileName)
	pos := hash % uint32(len(fl.clients))
	_, err = fl.clients[pos].Del(key)
	return fid, err
}
func (fl *FileListInRedis) FindFile2(filePath string, collection string, needtag bool, ctx *context.Context) (url1, expireat string, fid string, olen int64, oldMTime, tag string, err1 error, url2, expireat2 string, fid2 string, olen2 int64, oldMTime2, tag2 string, err2, err error) {
	return "", "", "", 0, "", "", nil, "", "", "", 0, "", "", nil, err
}
func (fl *FileListInRedis) FindFile(dirId DirectoryId, fileName string) (fid string, err error) {
	key, hash := genKey1(dirId, fileName)
	pos := hash % uint32(len(fl.clients))
	data, e := fl.clients[pos].Get(key)
	if e != nil && len(fl.clients) == len(fl.slaveClients) {
		//  fmt.Println("findfile err,%s",e.Error())
		data, e = fl.slaveClients[pos].Get(key)
	}
	if e != nil {
		return "", e
	}
	return string(data), nil
}
func (fl *FileListInRedis) ListFiles(dirId DirectoryId, lastFileName string, cursor string, limit int) (files []FileEntry) {
	glog.V(4).Infoln("directory", dirId, "lastFileName", lastFileName, "limit", limit)
	pos := uint32(0)
	if lastFileName != "" {
		_, hash := genKey1(dirId, lastFileName)
		pos = hash % uint32(len(fl.clients))
	}
	ret := make([]byte, 0, 4)
	for i := 3; i >= 0; i-- {
		ret = append(ret, byte(dirId>>(uint(i)*8)))
	}
	key := string(ret) + "*"
	if cursor == "" {
		cursor = "0"
	}
	limitCounter := 0
	for {
		res, e := fl.clients[pos].Scan(cursor, key, "500")
		if e != nil {
			break
		}

		data := res.([]interface{})
		if len(data) != 2 {
			break
		}
		cursor = string(data[0].([]byte))
		tmp := data[1].([]interface{})
		for i := 0; i < len(tmp); i++ {
			filename := string(tmp[i].([]byte))
			fid, e := fl.clients[pos].Get(filename)
			if e == nil {
				files = append(files, FileEntry{Name: filename[4:], Id: FileId(fid)})
				limitCounter++
			}
		}
		if limitCounter >= limit {
			break
		}
		if cursor == "0" {
			pos++
		}
		if pos >= uint32(len(fl.clients)) {
			break
		}
	}
	files = append(files, FileEntry{Name: "cursor", Id: FileId(cursor)})
	return
	/*
		dirKey := genKey(dirId, "")
		iter := nil
		limitCounter := 0
		for iter.Next() {
			key := iter.Key()
			if !bytes.HasPrefix(key, dirKey) {
				break
			}
			fileName := string(key[len(dirKey):])
			if fileName == lastFileName {
				continue
			}
			limitCounter++
			if limit > 0 {
				if limitCounter > limit {
					break
				}
			}
			files = append(files, FileEntry{Name: fileName, Id: FileId(string(iter.Value()))})
		}
		iter.Release()
	*/
	return
}

//hujf 20141118
func (fl *FileListInRedis) GetClients() []*redis.Client {
	return fl.clients
}

func (fl *FileListInRedis) GetSlaveClients() []*redis.Client {
	return fl.slaveClients
}
