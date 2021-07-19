package account

import (
	"fmt"

	"code.google.com/p/weed-fs/go/bucket"
	"code.google.com/p/weed-fs/go/util"
)

type AccountManager interface {
	Create(name string, password string,createtime string) (string, error)
	Delete(name string) (string, error)
	MarkDelete(name string) error
	List(marker, prefix string, maxNum int) ([]*Account, error)
	Get(name string) ([]*Account, error)
	IncomingBukSync(*bucket.Bucket) error
	Enable(name string) error
	Disable(name string) error
	DelAccessKey(name, id string) error
	AddAccessKey(name, id, key string) error
	BatchTrx(dst string) error

	//获取域名对应的bucekt
	GetDomainResolution(domain string) (string, error)
	//获取指定bucket对应的域名
	GetBucketDomain(bucketname string) ([]*BucketDomain, error)
	//删除指定的域名
	DeleteDomainResolution(domainname string, bucketname string) error
	//创建、变更指定的域名-bucket关系
	SetDomainResolution(domainname string, bucketname string, enable, isdefault bool) error
}

type Account struct {
	Name        string
	createTime  string
	authType    int
	bucketUsed  int
	objectCount int64
	byteUsed    int64
	enable      string
	accUser     string
	isDeleted   bool
	maxBucket   int
}
type AccountBucket struct {
	Name        string
	Account     string
	CreatedTime string
	PutTime     string
	ObjectCnt   int64
	ByteUsed    int64
	IsDeleted   int
}
type AccountStat struct {
	Name         string                   `json:"Account-Name,omitempty"`
	CreateTime   string                   `json:"Created-Time,omitempty"`
	AuthType     string                   `json:"Auth-Type,omitempty"`
	BucketUsed   int                      `json:"Bucket-Used,omitempty"`
	ObjectCount  int64                    `json:"Object-Count,omitempty"`
	ByteUsed     int64                    `json:"Byte-Used,omitempty"`
	IsDeleted    string                   `json:"Is-Deleted,omitempty"`
	Enable       string                   `json:"Account-Status,omitempty"`
	AccUser      string                   `json:"Accuser,omitempty"`
	AccessID     string                   `json:"Access-ID,omitempty"`
	AccessSecret string                   `json:"Access-Secret,omitempty"`
	MaxBucket    int                      `json:"Max-Bucket,omitempty"`
	Descrip      string                   `json:"Descrip,omitempty"`
	AccessEx     string                   `json:"AccessEx,omitempty"`
	RSV1         string                   `json:"RSV1,omitempty"`
	RSV2         string                   `json:"RSV2,omitempty"`
	RSV3         string                   `json:"RSV3,omitempty"`
	RSV4         int64                    `json:"RSV4,omitempty"`
	RSV5         int64                    `json:"RSV5,omitempty"`
	AccKeys      []map[string]interface{} `json:"AccessKeys,omitempty"`
}
type AccountData struct {
	Table string
	Data  []byte
}
type AccountResult struct {
	Name  string `json:"account,omitempty"`
	Error string `json:"info,omitempty"`
}

type BucketInfo struct {
	Name        string
	BucketName  string
	ObjectName  string
	Method      string
	ByteUsed    int64
	ObjectCount int
	BucketCount int
	TimeStamp   string
	IsSync      string
	AccessType  string
}

func (acc *Account) GetAccountStat() (accStat AccountStat) {
	accStat.Name = acc.Name
	accStat.AuthType = fmt.Sprint(acc.authType)
	accStat.CreateTime = util.ParseUnixNano(acc.createTime)
	accStat.BucketUsed = acc.bucketUsed
	accStat.ObjectCount = acc.objectCount
	accStat.ByteUsed = acc.byteUsed
	accStat.IsDeleted = fmt.Sprint(acc.isDeleted)
	accStat.Enable = acc.enable
	accStat.AccUser = acc.accUser
	accStat.MaxBucket = acc.maxBucket
	return accStat
}

type BucketDomainColl struct{
	Domains []*BucketDomain
}

type BucketDomain struct {
	Domain    string
	Enable    bool
	DefaultSeq int64
	Isdefault bool
}
