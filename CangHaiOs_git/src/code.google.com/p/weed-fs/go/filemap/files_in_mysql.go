package filemap

import (
	"database/sql"
	"strings"
)

type FileListInMysql struct {
	clients      []*sql.DB
	slaveClients []*sql.DB
}

func NewFileListInMysql(db *DbManager) (fl *FileListInMysql, err error) {
	fl = &FileListInMysql{}
	for i := 0; i < len(db.filemapconf); i++ {
		parts := strings.Split(db.filemapconf[i], ",")
		client, err := sql.Open("mysql", parts[0])
		if err == nil {
			fl.clients = append(fl.clients, client)
		}
		if len(parts) == 2 {
			client, err := sql.Open("mysql", parts[1])
			if err == nil {
				fl.slaveClients = append(fl.slaveClients, client)
			}

		}
	}
	return
}

func (fl *FileListInMysql) GetClients() []*sql.DB {
	return fl.clients
}

func (fl *FileListInMysql) GetSlaveClients() []*sql.DB {
	return fl.slaveClients
}

type Object struct {
	Key          string `json:"Key,omitempty"`
	LastModified string `json:"Last-Modified,omitempty"`
	Size         int64  `json:"Size"`
	ObjectAcl    string `json:"ObjectAcl,omitempty"`
	Etag         string `json:"Etag,omitempty"`
	Fid          string `json:"Fid,omitempty"`
	Expire       string `json:"Expire,omitempty"`
	Type         string `json:"Object-Type,omitempty"`
	AccessTime   string `json:"Access_Time,omitempty"`
}

type ObjectHissync struct {
	Name         string `json:"Filename,omitempty"`
	Path         string `json:"Key,omitempty"`
	LastModified string `json:"Last-Modified,omitempty"`
	Size         int64  `json:"Size,omitempty"`
	ObjectAcl    string `json:"ObjectAcl,omitempty"`
	Etag         string `json:"Etag,omitempty"`
	Fid          string `json:"Fid,omitempty"`
	Expire       string `json:"Expire,omitempty"`
	Type         string `json:"Object-Type,omitempty"`
	Pos          int
}

func (obj *Object) GetAccountBucket() (a string, b string) {
	parts := strings.SplitN(obj.Key, "/", 4)
	if len(parts) != 4 {
		return
	} else {
		return parts[1], parts[2]
	}
}

func (obj *Object)GetAccountBucketObj() (account string, bucket string, object string) {
	parts := strings.SplitN(obj.Key, "/", 4)
	switch len(parts) {
	case 2:
		return parts[1], "", ""
	case 3:
		return parts[1], parts[2], ""
	case 4:
		return parts[1], parts[2], parts[3]
	}
	return "", "", ""
}

type ObjectList struct {
	List []Object
}
type ObjectListHissync struct {
	List []ObjectHissync
}
