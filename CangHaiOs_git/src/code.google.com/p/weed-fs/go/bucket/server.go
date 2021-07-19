package bucket

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"strconv"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
)

const (
	SYNC_BUCKET_STATIC  = "static"
	SYNC_BUCKET_DESCRIP = "descrip"
)
const (
	STARSUBS = "[A-Za-z0-9_.~!*'();:@&=+$,/?\\]#-\\[]*"
)

const (
	MAXUPTPS    = 1
	MINUPTPS    = 2
	MAXUPRATE   = 4
	MINUPRATE   = 8
	MAXDOWNTPS  = 16
	MINDOWNTPS  = 32
	MAXDOWNRATE = 64
	MINDOWNRATE = 128
	MAXPICTPS   = 256
	MINPICTPS   = 512
	MAXPICRATE  = 1024
	MINPICRATE  = 2048
	MAXDELTPS   = 4096
	MINDELTPS   = 8192
	MAXDELRATE  = 16384
	MINDELRATE  = 32768
	MAXLISTTPS  = 65536
	MINLISTTPS  = 131072
	MAXLISTRATE = 262144
	MINLISTRATE = 524288
)

const (
	QOSLEN = 20
)

var Level uint32
var Precision uint32

type SyncInfoSt struct {
	AccountName string
	ObjectName  string
	BucketName  string
	ByteUsed    int64
	IsSyncInfo  string
	TimeStamp   string
	Collection  string
	AccessType  string
	SysDefine   string
	Method      string
}
type BucketManager interface {
	Create(buk Bucket) error
	Delete(account, bucket string) error
	List(account, marker, prefix string, maxNum int) (*BucketList, error)
	ListAll() (bukList *BucketList, err error)
	Get(account, bucket string) (*Bucket, error)
	ModifyAuth(account string, bucket string, accessType string, timeStamp string, isSync bool) (err error)
	Update(account, bucket string, num int, size int64) error
	Set(account, bucket, key, value string) error
	OutgoingAccSync(parNum int, accList map[string][]string)
	BatchTrx(dst string) error
}

type Bucket struct {
	Name             string               `json:"Bucket-Name,omitempty"`
	CreateTime       string               `json:"Created-Time,omitempty"`
	PutTime          string               `json:"Last-Put-Time,omitempty"`
	Account          string               `json:"Account-Name,omitempty"`
	ObjectCount      int64                `json:"Object-Count,omitempty"`
	ByteUsed         int64                `json:"Byte-Used,omitempty"`
	Location         string               `json:"Location"`
	Acl              string               `json:"Acl,omitempty"`
	LogPrefix        string               `json:"Log-Prefix,omitempty"`
	StaticIndex      string               `json:"Static-Index,omitempty"`
	StaticNotFound   string               `json:"Static-Not-Found,omitempty"`
	Refers           string               `json:"Refers,omitempty"`
	RefersAllowEmpty string               `json:"Refers-All-Empty,omitempty"`
	LifeCycle        string               `json:"Life-Cycle,omitempty"`
	Collection       string               `json:"Collection,omitempty"`
	AccessType       string               `json:"Access-Type,omitempty"`
	SysDefine        string               `json:"File-Name-Defined-Mode,omitempty"`
	SyncType         string               `json:"sync-type,omitempty"`
	IsDeleted        int                  `json:"is-deleted,omitempty"`
	MaxUpTps         int                  `json:"Max-Up-Tps,omitempty"`
	MaxDonwTps       int                  `json:"Max-Down-Tps,omitempty"`
	MaxFileSize      int                  `json:"Max-File-Size,omitempty"`
	Enable           int                  `json:"Enable"`
	SupportAppend    string               `json:"Support-Append,omitempty"`
	ImageService     string               `json:"Image-Service,omitempty"`
	CORS             string               `json:"CORS,omitempty"`
	MaxAgeDay        int                  `json:"max-age,omitempty"`
	Reserver1        string               `json:"Need-Backup,omitempty"`
	Reserver2        string               `json:"Merge,omitempty"`
	Reserver3        string               `json:"reserver3,omitempty"`
	Reserver4        int64                `json:"Reserver4,omitempty"` //此处用位来标记开关
	Reserver5        int64                `json:"reserver5,omitempty"`
	RefererConfig    RefererConfiguration `json:"Referer-Configuration,omitempty"`
	MRuleSets        *MigrationRuleSets   `json:"MigrationRuleSets,omitempty"`
	LRuleSets        *LifeCycleRuleSets   `json:"LifeCycleRuleSets,omitempty"`
	Thresholds       *BucketThresholds    `json:"Thresholds,omitempty"`
}

const (
	SUPPORTED_EMPTY_LOCATION           = iota //bucket属性中reserver4字段的第0位来标记该bucket是否支持空文件,其他的位用于将来的拓展jjj
	SUPPORTED_FILE_HEAD_CHECK_LOCATION        //bucket属性中reserver4字段的第1位来标记该bucket是否支持图片文件头校验
	SUPPORTED_BUCKET_DEGRADE_LOCATION         //bucket属性中reserver4字段的第2位来标记该bucket是否支持降级
)

type BucketList struct {
	Buks []Bucket
}
type BucketResult struct {
	Name  string `json:"fid,omitempty"`
	Error string `json:"error,omitempty"`
}
type RefererConfiguration struct {
	AllowEmpty bool    `json:"Allow-Empty-Referer,omitempty"`
	Referers   []Refer `json:"Referer-List,omitempty"`
}
type Refer struct {
	Refer        string `json:"Referer,omitempty"`
	reg          *regexp.Regexp
	compileError error
}

//noinspection GoNameStartsWithPackageName
type BucketDegradeLifeCycle struct {
	Prefix string `json:"prefix,omitempty"`
	Period int    `json:"period,omitempty"`
}

//bucket的限速属性
/*
type BucketThresholds struct {

	//上传-----------------------------------------------
	//最大上传TPS，能够达到的最高值 最大65536
	MaxUpTps uint32 `json:"MaxUpTps,omitempty"`
	//最小上传TPS,指的是，提供的最低保留 最大65536
	MinUpTps uint32 `json:"MinUpTps,omitempty"`
	//网络流量
	//最大上传网速，能够达到的最高值,MB计算 最大65536
	MaxUpRate uint32 `json:"MaxUpRate,omitempty"`
	//最小上传网速,指的是，提供的最低保留,MB计算 最大65536
	MinUpRate uint32 `json:"MinUpRate,omitempty"`

	//下载-----------------------------------------------
	//最高下载TPS，能够达到的最高值 最大65536
	MaxDownTps uint32 `json:"MaxDownTps,omitempty"`
	//最低下载TPS，指的是提供的，最低的保留的能力 最大65536
	MinDownTps uint32 `json:"MinDownTps,omitempty"`
	//最高下载网速，能够达到的最高值,MB计算 最大65536
	MaxDownRate uint32 `json:"MaxDownRate,omitempty"`
	//最低下载网速，指的是提供的，最低的保留的能力,MB计算 最大65536
	MinDownRate uint32 `json:"MinDownRate,omitempty"`

	//图片处理--------------------------------------------
	//最大tps 最大65536
	MaxPicDealTps uint32 `json:"MaxPicDealTps,omitempty"`
	//最小保留tps 最大65536
	MinPicDealTps uint32 `json:"MinPicDealTps,omitempty"`
	//最大处理大小，单位时间内图片处理的图片总大小 最大65536
	MaxPicDealRate uint32 `json:"MaxPicDealRate,omitempty"`
	//最小保留大小 最大65536
	MinPicDealRate uint32 `json:"MinPicDealRate,omitempty"`

	//删除处理--------------------------------------------
	//最高删除TPS，能够达到的最高值 最大65536
	MaxDelTps uint32 `json:"MaxDelTps,omitempty"`
	//最低删除TPS，指的是提供的，最低的保留的能力 最大65536
	MinDelTps uint32 `json:"MinDelTps,omitempty"`
	//最高删除网速，能够达到的最高值,MB计算 最大65536
	MaxDelRate uint32 `json:"MaxDelRate,omitempty"`
	//最低删除网速，指的是提供的，最低的保留的能力,MB计算 最大65536
	MinDelRate uint32 `json:"MinDelRate,omitempty"`

	//列bucket处理-----------------------------------------
	//最高列bucket TPS，能够达到的最高值 最大65536
	MaxListTps uint32 `json:"MaxListTps,omitempty"`
	//最低列bucket TPS，指的是提供的，最低的保留的能力 最大65536
	MinListTps uint32 `json:"MinListTps,omitempty"`
	//最高列bucket 网速，能够达到的最高值,MB计算 最大65536
	MaxListRate uint32 `json:"MaxListRate,omitempty"`
	//最低列bucket 网速，指的是提供的，最低的保留的能力,MB计算 最大65536
	MinListRate uint32 `json:"MinListRate,omitempty"`

	//最大空间使用量GB计
	MaxSpace uint32 `json:"MaxSpace,omitempty"`
	//使用优先级 最大256
	Priority uint8 `json:"Priority,omitempty"`
}
*/
//bucket的限速属性 新版的QOS
type BucketThresholds struct {

	//上传-----------------------------------------------
	//最大上传TPS，能够达到的最高值 最大65536
	MaxUpTps string `json:"MaxUpTps,omitempty"`
	//最小上传TPS,指的是，提供的最低保留 最大65536
	MinUpTps string `json:"MinUpTps,omitempty"`
	//网络流量
	//最大上传网速，能够达到的最高值,MB计算 最大65536
	MaxUpRate string `json:"MaxUpRate,omitempty"`

	//最小上传网速,指的是，提供的最低保留,MB计算 最大65536
	MinUpRate string `json:"MinUpRate,omitempty"`

	//下载-----------------------------------------------
	//最高下载TPS，能够达到的最高值 最大65536
	MaxDownTps string `json:"MaxDownTps,omitempty"`
	//最低下载TPS，指的是提供的，最低的保留的能力 最大65536
	MinDownTps string `json:"MinDownTps,omitempty"`
	//最高下载网速，能够达到的最高值,MB计算 最大65536
	MaxDownRate string `json:"MaxDownRate,omitempty"`
	//最低下载网速，指的是提供的，最低的保留的能力,MB计算 最大65536
	MinDownRate string `json:"MinDownRate,omitempty"`

	//图片处理--------------------------------------------
	//最大tps 最大65536
	MaxPicDealTps string `json:"MaxPicDealTps,omitempty`
	//最小保留tps 最大65536
	MinPicDealTps string `json:"MinPicDealTps,omitempty"`
	//最大处理大小，单位时间内图片处理的图片总大小 最大65536
	MaxPicDealRate string `json:"MaxPicDealRate,omitempty`
	//最小保留大小 最大65536
	MinPicDealRate string `json:"MinPicDealRate,omitempty"`

	//删除处理--------------------------------------------
	//最高删除TPS，能够达到的最高值 最大65536
	MaxDelTps string `json:"MaxDelTps,omitempty"`
	//最低删除TPS，指的是提供的，最低的保留的能力 最大65536
	MinDelTps string `json:"MinDelTps,omitempty"`
	//最高删除网速，能够达到的最高值,MB计算 最大65536
	MaxDelRate string `json:"MaxDelRate,omitempty"`
	//最低删除网速，指的是提供的，最低的保留的能力,MB计算 最大65536
	MinDelRate string `json:"MinDelRate,omitempty"`

	//列bucket处理-----------------------------------------
	//最高列bucket TPS，能够达到的最高值 最大65536
	MaxListTps string `json:"MaxListTps,omitempty"`
	//最低列bucket TPS，指的是提供的，最低的保留的能力 最大65536
	MinListTps string `json:"MinListTps,omitempty"`
	//最高列bucket 网速，能够达到的最高值,MB计算 最大65536
	MaxListRate string `json:"MaxListRate,omitempty"`
	//最低列bucket 网速，指的是提供的，最低的保留的能力,MB计算 最大65536
	MinListRate string `json:"MinListRate,omitempty"`

	//最大空间使用量GB计
	MaxSpace uint32 `json:"MaxSpace,omitempty"`
	//使用优先级 最大256
	Priority uint8 `json:"Priority,omitempty"`
}

func FormatIdentify(input string) int {
	match, _ := regexp.MatchString("^0.[0-9]{1,2}$", input)
	if match {
		return 1
	}
	match2, _ := regexp.MatchString("^[0-9]{1,5}$", input)
	if match2 {
		return 2
	}
	if input == "unlimited" {
		return 3
	}
	return 0
}

func Decode(raw []byte) []byte {
	var buf bytes.Buffer
	decoded := make([]byte, 58)
	buf.Write(raw)
	decoder := base64.NewDecoder(base64.StdEncoding, &buf)
	decoder.Read(decoded)
	return decoded
}

func Encode(raw []byte) []byte {
	var encoded bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &encoded)
	encoder.Write(raw)
	encoder.Close()
	return encoded.Bytes()
}

//新版QOS，不符合格式的数据一律按默认值处理
func (bsl *BucketThresholds) Tobyte() []byte {
	glog.V(4).Infoln("BucketQosConfig to byte")
	var Level uint32 = 0
	var Decimal uint32 = 0

	glog.V(4).Infoln("level:", fmt.Sprintf("%b", Level), ",decimal:", fmt.Sprintf("%b", Decimal))

	var iFormatType int = 0

	var valueint int

	data := make([]byte, 54)
	temp := make([]byte, 4)
	//首位是版本标识
	util.Uint8toBytes(temp, uint8(2))
	copy(data[0:1], temp[0:1])
	//------------------------------------------------------上传
	iFormatType = FormatIdentify(bsl.MaxUpTps)
	if iFormatType == 1 { //小数
		Decimal = Decimal | MAXUPTPS
		Level = Level | MAXUPTPS
		valueint, _ = strconv.Atoi(bsl.MaxUpTps[2:])
		if len(bsl.MaxUpTps) == 3 { //像0.1这样的
			valueint = valueint * 10
		}
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxUpTps  decimal:", valueint)
	} else if iFormatType == 2 { //整数
		Level = Level | MAXUPTPS
		valueint, _ = strconv.Atoi(bsl.MaxUpTps)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxUpTps", valueint)
	} else { //不限制或者不符合要求的格式按不限制处理
		glog.V(4).Infoln("MaxUpTps unlimited")
	}
	copy(data[1:3], temp[2:4])
	iFormatType = FormatIdentify(bsl.MinUpTps)
	if iFormatType == 1 { //小数
		Decimal = Decimal | MINUPTPS
		Level = Level | MINUPTPS
		valueint, _ = strconv.Atoi(bsl.MinUpTps[2:])
		if len(bsl.MinUpTps) == 3 { //像0.1这样的
			valueint = valueint * 10
		}
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinUpTps  decimal:", valueint)
	} else if iFormatType == 2 { //整数
		Level = Level | MINUPTPS
		valueint, _ = strconv.Atoi(bsl.MinUpTps)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinUpTps:", valueint)
	} else { //不限制或者不符合要求的格式按不限制处理
		glog.V(4).Infoln("MaxUpTps unlimited")
	}
	copy(data[3:5], temp[2:4])
	if bsl.MaxUpRate != "unlimited" {
		Level = Level | MAXUPRATE
		valueint, _ = strconv.Atoi(bsl.MaxUpRate)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxUpRate:", valueint)
	}
	copy(data[5:7], temp[2:4])
	if bsl.MinUpRate != "unlimited" {
		Level = Level | MINUPRATE
		valueint, _ = strconv.Atoi(bsl.MinUpRate)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinUpRate:", valueint)
	}
	copy(data[7:9], temp[2:4])
	glog.V(4).Infoln("level:", fmt.Sprintf("%b", Level), ",decimal:", fmt.Sprintf("%b", Decimal))
	//---------------------------------------------------------------下载
	iFormatType = FormatIdentify(bsl.MaxDownTps)
	if iFormatType == 1 { //小数
		Decimal = Decimal | MAXDOWNTPS
		Level = Level | MAXDOWNTPS
		valueint, _ = strconv.Atoi(bsl.MaxDownTps[2:])
		if len(bsl.MaxDownTps) == 3 { //像0.1这样的
			valueint = valueint * 10
		}
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxDownTps  decimal:", valueint)
	} else if iFormatType == 2 { //整数
		Level = Level | MAXDOWNTPS
		valueint, _ = strconv.Atoi(bsl.MaxDownTps)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxDownTps:", valueint)
	} else { //不限制或者不符合要求的格式按不限制处理
		glog.V(4).Infoln("MaxDownTps unlimited")
	}
	copy(data[9:11], temp[2:4])
	iFormatType = FormatIdentify(bsl.MinDownTps)
	if iFormatType == 1 { //小数
		Decimal = Decimal | MINDOWNTPS
		Level = Level | MINDOWNTPS
		valueint, _ = strconv.Atoi(bsl.MinDownTps[2:])
		if len(bsl.MinDownTps) == 3 { //像0.1这样的
			valueint = valueint * 10
		}
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinDownTps  decimal:", valueint)
	} else if iFormatType == 2 { //整数
		Level = Level | MINDOWNTPS
		valueint, _ = strconv.Atoi(bsl.MinDownTps)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinDownTps:", valueint)
	} else { //不限制或者不符合要求的格式按不限制处理
		glog.V(4).Infoln("MinDownTps unlimited")
	}
	copy(data[11:13], temp[2:4])
	if bsl.MaxDownRate != "unlimited" {
		Level = Level | MAXDOWNRATE
		valueint, _ = strconv.Atoi(bsl.MaxDownRate)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxDownRate:", valueint)
	}
	copy(data[13:15], temp[2:4])
	if bsl.MinDownRate != "unlimited" {
		Level = Level | MINDOWNRATE
		valueint, _ = strconv.Atoi(bsl.MinDownRate)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinDownRate:", valueint)
	}
	copy(data[15:17], temp[2:4])
	glog.V(4).Infoln("level:", fmt.Sprintf("%b", Level), ",decimal:", fmt.Sprintf("%b", Decimal))
	//----------------------------------------------------------------------图片处理
	iFormatType = FormatIdentify(bsl.MaxPicDealTps)
	if iFormatType == 1 { //小数
		Decimal = Decimal | MAXPICTPS
		Level = Level | MAXPICTPS
		valueint, _ = strconv.Atoi(bsl.MaxPicDealTps[2:])
		if len(bsl.MaxPicDealTps) == 3 { //像0.1这样的
			valueint = valueint * 10
		}
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxPicDealTps  decimal:", valueint)
	} else if iFormatType == 2 { //整数
		Level = Level | MAXPICTPS
		valueint, _ = strconv.Atoi(bsl.MaxPicDealTps)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxPicDealTps:", valueint)
	} else { //不限制或者不符合要求的格式按不限制处理
		glog.V(4).Infoln("MaxPicDealTps unlimited")
	}
	copy(data[17:19], temp[2:4])
	iFormatType = FormatIdentify(bsl.MinPicDealTps)
	if iFormatType == 1 { //小数
		Decimal = Decimal | MINPICTPS
		Level = Level | MINPICTPS
		valueint, _ = strconv.Atoi(bsl.MinPicDealTps[2:])
		if len(bsl.MinPicDealTps) == 3 { //像0.1这样的
			valueint = valueint * 10
		}
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinPicDealTps decimal:", valueint)
	} else if iFormatType == 2 { //整数
		Level = Level | MINPICTPS
		valueint, _ = strconv.Atoi(bsl.MinPicDealTps)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinPicDealTps:", valueint)
	} else { //不限制或者不符合要求的格式按不限制处理
		glog.V(4).Infoln("MinPicDealTps unlimited")
	}
	copy(data[19:21], temp[2:4])
	if bsl.MaxPicDealRate != "unlimited" {
		Level = Level | MAXPICRATE
		valueint, _ = strconv.Atoi(bsl.MaxPicDealRate)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxPicDealRate:", valueint)
	}
	copy(data[21:23], temp[2:4])
	if bsl.MinPicDealRate != "unlimited" {
		Level = Level | MINPICRATE
		valueint, _ = strconv.Atoi(bsl.MinPicDealRate)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinPicDealRate:", valueint)
	}
	copy(data[23:25], temp[2:4])
	glog.V(4).Infoln("level:", fmt.Sprintf("%b", Level), ",decimal:", fmt.Sprintf("%b", Decimal))
	//-------------------------------------------------------------------------------删除
	iFormatType = FormatIdentify(bsl.MaxDelTps)
	if iFormatType == 1 { //小数
		Decimal = Decimal | MAXDELTPS
		Level = Level | MAXDELTPS
		valueint, _ = strconv.Atoi(bsl.MaxDelTps[2:])
		if len(bsl.MaxDelTps) == 3 { //像0.1这样的
			valueint = valueint * 10
		}
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxDelTps  decimal:", valueint)
	} else if iFormatType == 2 { //整数
		Level = Level | MAXDELTPS
		valueint, _ = strconv.Atoi(bsl.MaxDelTps)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxDelTps:", valueint)
	} else { //不限制或者不符合要求的格式按不限制处理
		glog.V(4).Infoln("MaxDelTps unlimited")
	}
	copy(data[25:27], temp[2:4])
	iFormatType = FormatIdentify(bsl.MinDelTps)
	if iFormatType == 1 { //小数
		Decimal = Decimal | MINDELTPS
		Level = Level | MINDELTPS
		valueint, _ = strconv.Atoi(bsl.MinDelTps[2:])
		if len(bsl.MinDelTps) == 3 { //像0.1这样的
			valueint = valueint * 10
		}
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinDelTps  decimal:", valueint)
	} else if iFormatType == 2 { //整数
		Level = Level | MINDELTPS
		valueint, _ = strconv.Atoi(bsl.MinDelTps)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinDelTps :", valueint)
	} else { //不限制或者不符合要求的格式按不限制处理
		glog.V(4).Infoln("MinDelTps unlimited")
	}
	copy(data[27:29], temp[2:4])
	if bsl.MaxDelRate != "unlimited" {
		Level = Level | MAXDELRATE
		valueint, _ = strconv.Atoi(bsl.MaxDelRate)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxDelRate:", valueint)
	}
	copy(data[29:31], temp[2:4])
	if bsl.MinDelRate != "unlimited" {
		Level = Level | MINDELRATE
		valueint, _ = strconv.Atoi(bsl.MinDelRate)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinDelRate:", valueint)
	}
	copy(data[31:33], temp[2:4])
	glog.V(4).Infoln("level:", fmt.Sprintf("%b", Level), ",decimal:", fmt.Sprintf("%b", Decimal))
	//------------------------------------------------------------------------列bucket
	iFormatType = FormatIdentify(bsl.MaxListTps)
	if iFormatType == 1 { //小数
		Decimal = Decimal | MAXLISTTPS
		Level = Level | MAXLISTTPS
		valueint, _ = strconv.Atoi(bsl.MaxListTps[2:])
		if len(bsl.MaxListTps) == 3 { //像0.1这样的
			valueint = valueint * 10
		}
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxListTps  decimal:", valueint)
	} else if iFormatType == 2 { //整数
		Level = Level | MAXLISTTPS
		valueint, _ = strconv.Atoi(bsl.MaxListTps)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxListTps :", valueint)
	} else if iFormatType == 0 { //不符合要求的格式按默认值处理
		Level = Level | MAXLISTTPS
		util.Uint32toBytes(temp, uint32(1))
		glog.V(4).Infoln("MaxListTps:1")
	} else { //不限制
		glog.V(4).Infoln("MaxListTps unlimited")
	}
	copy(data[33:35], temp[2:4])
	iFormatType = FormatIdentify(bsl.MinListTps)
	if iFormatType == 1 { //小数
		Decimal = Decimal | MINLISTTPS
		Level = Level | MINLISTTPS
		valueint, _ = strconv.Atoi(bsl.MinListTps[2:])
		if len(bsl.MinListTps) == 3 { //像0.1这样的
			valueint = valueint * 10
		}
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinListTps  decimal:", valueint)
	} else if iFormatType == 2 { //整数
		Level = Level | MINLISTTPS
		valueint, _ = strconv.Atoi(bsl.MinListTps)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinListTps:", valueint)
	} else if iFormatType == 0 { //不符合要求的格式按默认值处理
		Level = Level | MINLISTTPS
		util.Uint32toBytes(temp, uint32(1))
		glog.V(4).Infoln("MinListTps:1")
	} else { //不限制或者不符合要求的格式按不限制处理
		glog.V(4).Infoln("MinListTps unlimited")
	}
	copy(data[35:37], temp[2:4])
	if bsl.MaxListRate != "unlimited" {
		Level = Level | MAXLISTRATE
		valueint, _ = strconv.Atoi(bsl.MaxListRate)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MaxListRate:", valueint)
	}
	copy(data[37:39], temp[2:4])
	if bsl.MinListRate != "unlimited" {
		Level = Level | MINLISTRATE
		valueint, _ = strconv.Atoi(bsl.MinListRate)
		util.Uint32toBytes(temp, uint32(valueint))
		glog.V(4).Infoln("MinListRate:", valueint)
	}
	copy(data[39:41], temp[2:4])
	glog.V(4).Infoln("level:", fmt.Sprintf("%b", Level), ",decimal:", fmt.Sprintf("%b", Decimal))

	util.Uint32toBytes(temp, bsl.MaxSpace)
	copy(data[41:45], temp[0:4])
	util.Uint8toBytes(temp, bsl.Priority)
	copy(data[45:46], temp[0:1])

	util.Uint32toBytes(temp, Level)
	copy(data[46:49], temp[1:4])
	util.Uint32toBytes(temp, Decimal)
	copy(data[49:52], temp[1:4])

	return Encode(data)
}

//为了节省存储的字段空间，同时也根据实际的业务上需求，BucketSpeedLimit 的内部字段基本都是占4个字节就够了
/*
func (bsl *BucketThresholds) Tobyte() []byte {

	data := make([]byte, 48)
	temp := make([]byte, 4)
	//首位是版本标识
	util.Uint8toBytes(temp, uint8(1))
	copy(data[0:1], temp[0:1])

	util.Uint32toBytes(temp, uint32(bsl.MaxUpTps))
	copy(data[1:3], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MinUpTps))
	copy(data[3:5], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MaxUpRate))
	copy(data[5:7], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MinUpRate))
	copy(data[7:9], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MaxDownTps))
	copy(data[9:11], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MinDownTps))
	copy(data[11:13], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MaxDownRate))
	copy(data[13:15], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MinDownRate))
	copy(data[15:17], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MaxPicDealTps))
	copy(data[17:19], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MinPicDealTps))
	copy(data[19:21], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MaxPicDealRate))
	copy(data[21:23], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MinPicDealRate))
	copy(data[23:25], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MaxDelTps))
	copy(data[25:27], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MinDelTps))
	copy(data[27:29], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MaxDelRate))
	copy(data[29:31], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MinDelRate))
	copy(data[31:33], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MaxListTps))
	copy(data[33:35], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MinListTps))
	copy(data[35:37], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MaxListRate))
	copy(data[37:39], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MinListRate))
	copy(data[39:41], temp[2:4])
	util.Uint32toBytes(temp, uint32(bsl.MaxSpace))
	copy(data[41:45], temp[0:4])
	util.Uint8toBytes(temp, bsl.Priority)
	copy(data[45:46], temp[0:1])

	return Encode(data)
}
*/
//从data解析
func (bsl *BucketThresholds) Parse(data []byte) {
	glog.V(4).Infoln(data)
	data = Decode(data)
	if util.BytesToUint8(data[0:1]) == 2 && len(data) >= 52 {
		var Level uint32 = 0
		var Decimal uint32 = 0
		Level = util.BytesToUint32(data[46:49])
		Decimal = util.BytesToUint32(data[49:52])
		glog.V(4).Infoln("level:", fmt.Sprintf("%b", Level), ",decimal:", fmt.Sprintf("%b", Decimal))
		MaxUpTps := util.BytesToUint32(data[1:3])
		glog.V(4).Infoln("MaxUpTps:", MaxUpTps)
		if Level&MAXUPTPS == MAXUPTPS {
			if Decimal&MAXUPTPS == MAXUPTPS { //小数
				glog.V(4).Infoln(" decimal number")
				bsl.MaxUpTps = fmt.Sprintf("0.%02d", MaxUpTps)
			} else { //整数
				glog.V(4).Infoln("number")
				bsl.MaxUpTps = strconv.Itoa(int(MaxUpTps))
			}
		} else {
			bsl.MaxUpTps = "unlimited"
		}
		glog.V(4).Infoln("bsl.MaxUpTps:", bsl.MaxUpTps)
		MinUpTps := util.BytesToUint32(data[3:5])
		if Level&MINUPTPS == MINUPTPS {
			if Decimal&MINUPTPS == MINUPTPS { //小数
				bsl.MinUpTps = fmt.Sprintf("0.%02d", MinUpTps)
			} else { //整数
				bsl.MinUpTps = strconv.Itoa(int(MinUpTps))
			}
		} else {
			bsl.MinUpTps = "unlimited"
		}
		if Level&MAXUPRATE == MAXUPRATE {
			bsl.MaxUpRate = strconv.Itoa(int(util.BytesToUint32(data[5:7])))
		} else {
			bsl.MaxUpRate = "unlimited"
		}
		if Level&MINUPRATE == MINUPRATE {
			bsl.MinUpRate = strconv.Itoa(int(util.BytesToUint32(data[7:9])))
		} else {
			bsl.MinUpRate = "unlimited"
		}

		MaxDownTps := util.BytesToUint32(data[9:11])
		if Level&MAXDOWNTPS == MAXDOWNTPS {
			if Decimal&MAXDOWNTPS == MAXDOWNTPS { //小数
				bsl.MaxDownTps = fmt.Sprintf("0.%02d", MaxDownTps)
			} else { //整数
				bsl.MaxDownTps = strconv.Itoa(int(MaxDownTps))
			}
		} else {
			bsl.MaxDownTps = "unlimited"
		}
		MinDownTps := util.BytesToUint32(data[11:13])
		if Level&MINDOWNTPS == MINDOWNTPS {
			if Decimal&MINDOWNTPS == MINDOWNTPS { //小数
				bsl.MinDownTps = fmt.Sprintf("0.%02d", MinDownTps)
			} else { //整数
				bsl.MinDownTps = strconv.Itoa(int(MinDownTps))
			}
		} else {
			bsl.MinDownTps = "unlimited"
		}
		if Level&MAXDOWNRATE == MAXDOWNRATE {
			bsl.MaxDownRate = strconv.Itoa(int(util.BytesToUint32(data[13:15])))
		} else {
			bsl.MaxDownRate = "unlimited"
		}
		if Level&MINDOWNRATE == MINDOWNRATE {
			bsl.MinDownRate = strconv.Itoa(int(util.BytesToUint32(data[15:17])))
		} else {
			bsl.MinDownRate = "unlimited"
		}

		MaxPicDealTps := util.BytesToUint32(data[17:19])
		if Level&MAXPICTPS == MAXPICTPS {
			if Decimal&MAXPICTPS == MAXPICTPS { //小数
				bsl.MaxPicDealTps = fmt.Sprintf("0.%02d", MaxPicDealTps)
			} else { //整数
				bsl.MaxPicDealTps = strconv.Itoa(int(MaxPicDealTps))
			}
		} else {
			bsl.MaxPicDealTps = "unlimited"
		}
		MinPicDealTps := util.BytesToUint32(data[19:21])
		if Level&MINPICTPS == MINPICTPS {
			if Decimal&MINPICTPS == MINPICTPS { //小数
				bsl.MinPicDealTps = fmt.Sprintf("0.%02d", MinPicDealTps)
			} else { //整数
				bsl.MinPicDealTps = strconv.Itoa(int(MinPicDealTps))
			}
		} else {
			bsl.MinPicDealTps = "unlimited"
		}
		if Level&MAXPICRATE == MAXPICRATE {
			bsl.MaxPicDealRate = strconv.Itoa(int(util.BytesToUint32(data[21:23])))
		} else {
			bsl.MaxPicDealRate = "unlimited"
		}
		if Level&MINPICRATE == MINPICRATE {
			bsl.MinPicDealRate = strconv.Itoa(int(util.BytesToUint32(data[23:25])))
		} else {
			bsl.MinPicDealRate = "unlimited"
		}

		MaxDelTps := util.BytesToUint32(data[25:27])
		if Level&MAXDELTPS == MAXDELTPS {
			if Decimal&MAXDELTPS == MAXDELTPS { //小数
				bsl.MaxDelTps = fmt.Sprintf("0.%02d", MaxDelTps)
			} else { //整数
				bsl.MaxDelTps = strconv.Itoa(int(MaxDelTps))
			}
		} else {
			bsl.MaxDelTps = "unlimited"
		}
		MinDelTps := util.BytesToUint32(data[27:29])
		if Level&MINDELTPS == MINDELTPS {
			if Decimal&MINDELTPS == MINDELTPS { //小数
				bsl.MinDelTps = fmt.Sprintf("0.%02d", MinDelTps)
			} else { //整数
				bsl.MinDelTps = strconv.Itoa(int(MinDelTps))
			}
		} else {
			bsl.MinDelTps = "unlimited"
		}
		if Level&MAXDELRATE == MAXDELRATE {
			bsl.MaxDelRate = strconv.Itoa(int(util.BytesToUint32(data[29:31])))
		} else {
			bsl.MaxDelRate = "unlimited"
		}
		if Level&MINDELRATE == MINDELRATE {
			bsl.MinDelRate = strconv.Itoa(int(util.BytesToUint32(data[31:33])))
		} else {
			bsl.MinDelRate = "unlimited"
		}

		MaxListTps := util.BytesToUint32(data[33:35])
		if Level&MAXLISTTPS == MAXLISTTPS {
			if Decimal&MAXLISTTPS == MAXLISTTPS { //小数
				bsl.MaxListTps = fmt.Sprintf("0.%02d", MaxListTps)
			} else { //整数
				bsl.MaxListTps = strconv.Itoa(int(MaxListTps))
			}
		} else {
			bsl.MaxListTps = "unlimited"
		}
		MinListTps := util.BytesToUint32(data[35:37])
		if Level&MINLISTTPS == MINLISTTPS {
			if Decimal&MINLISTTPS == MINLISTTPS { //小数
				bsl.MinListTps = fmt.Sprintf("0.%02d", MinListTps)
			} else { //整数
				bsl.MinListTps = strconv.Itoa(int(MinListTps))
			}
		} else {
			bsl.MinListTps = "unlimited"
		}
		if Level&MAXLISTRATE == MAXLISTRATE {
			bsl.MaxListRate = strconv.Itoa(int(util.BytesToUint32(data[37:39])))
		} else {
			bsl.MaxListRate = "unlimited"
		}
		if Level&MINLISTRATE == MINLISTRATE {
			bsl.MinListRate = strconv.Itoa(int(util.BytesToUint32(data[39:41])))
		} else {
			bsl.MinListRate = "unlimited"
		}

		bsl.MaxSpace = util.BytesToUint32(data[41:45])
		bsl.Priority = util.BytesToUint8(data[45:46])
	} else if util.BytesToUint8(data[0:1]) == 1 && len(data) >= 46 { //新版本要支持解析老版本数据
		bsl.MaxUpTps = strconv.Itoa(int(util.BytesToUint32(data[1:3])))
		if bsl.MaxUpTps == "0" {
			bsl.MaxUpTps = "unlimited"
		}
		bsl.MinUpTps = strconv.Itoa(int(util.BytesToUint32(data[3:5])))
		if bsl.MinUpTps == "0" {
			bsl.MinUpTps = "unlimited"
		}
		bsl.MaxUpRate = strconv.Itoa(int(util.BytesToUint32(data[5:7])))
		if bsl.MaxUpRate == "0" {
			bsl.MaxUpRate = "unlimited"
		}
		bsl.MinUpRate = strconv.Itoa(int(util.BytesToUint32(data[7:9])))
		if bsl.MinUpRate == "0" {
			bsl.MinUpRate = "unlimited"
		}
		bsl.MaxDownTps = strconv.Itoa(int(util.BytesToUint32(data[9:11])))
		if bsl.MaxDownTps == "0" {
			bsl.MaxDownTps = "unlimited"
		}
		bsl.MinDownTps = strconv.Itoa(int(util.BytesToUint32(data[11:13])))
		if bsl.MinDownTps == "0" {
			bsl.MinDownTps = "unlimited"
		}
		bsl.MaxDownRate = strconv.Itoa(int(util.BytesToUint32(data[13:15])))
		if bsl.MaxDownRate == "0" {
			bsl.MaxDownRate = "unlimited"
		}
		bsl.MinDownRate = strconv.Itoa(int(util.BytesToUint32(data[15:17])))
		if bsl.MinDownRate == "0" {
			bsl.MinDownRate = "unlimited"
		}
		bsl.MaxPicDealTps = strconv.Itoa(int(util.BytesToUint32(data[17:19])))
		if bsl.MaxPicDealTps == "0" {
			bsl.MaxPicDealTps = "unlimited"
		}
		bsl.MinPicDealTps = strconv.Itoa(int(util.BytesToUint32(data[19:21])))
		if bsl.MinPicDealTps == "0" {
			bsl.MinPicDealTps = "unlimited"
		}
		bsl.MaxPicDealRate = strconv.Itoa(int(util.BytesToUint32(data[21:23])))
		if bsl.MaxPicDealRate == "0" {
			bsl.MaxPicDealRate = "unlimited"
		}
		bsl.MinPicDealRate = strconv.Itoa(int(util.BytesToUint32(data[23:25])))
		if bsl.MinPicDealRate == "0" {
			bsl.MinPicDealRate = "unlimited"
		}

		bsl.MaxDelTps = strconv.Itoa(int(util.BytesToUint32(data[25:27])))
		if bsl.MaxDelTps == "0" {
			bsl.MaxDelTps = "unlimited"
		}
		bsl.MinDelTps = strconv.Itoa(int(util.BytesToUint32(data[27:29])))
		if bsl.MinDelTps == "0" {
			bsl.MinDelTps = "unlimited"
		}
		bsl.MaxDelRate = strconv.Itoa(int(util.BytesToUint32(data[29:31])))
		if bsl.MaxDelRate == "0" {
			bsl.MaxDelRate = "unlimited"
		}
		bsl.MinDelRate = strconv.Itoa(int(util.BytesToUint32(data[31:33])))
		if bsl.MinDelRate == "0" {
			bsl.MinDelRate = "unlimited"
		}
		bsl.MaxListTps = strconv.Itoa(int(util.BytesToUint32(data[33:35])))
		if bsl.MaxListTps == "0" {
			bsl.MaxListTps = "unlimited"
		}
		bsl.MinListTps = strconv.Itoa(int(util.BytesToUint32(data[35:37])))
		if bsl.MinListTps == "0" {
			bsl.MinListTps = "unlimited"
		}
		bsl.MaxListRate = strconv.Itoa(int(util.BytesToUint32(data[37:39])))
		if bsl.MaxListRate == "0" {
			bsl.MaxListRate = "unlimited"
		}
		bsl.MinListRate = strconv.Itoa(int(util.BytesToUint32(data[39:41])))
		if bsl.MinListRate == "0" {
			bsl.MinListRate = "unlimited"
		}

		bsl.MaxSpace = util.BytesToUint32(data[41:45])
		bsl.Priority = util.BytesToUint8(data[45:46])
	}
	return
}

//从data解析
/*
func (bsl *BucketThresholds) Parse(data []byte) {
	glog.V(4).Infoln(data)
	data = Decode(data)
	if util.BytesToUint8(data[0:1]) == 1 && len(data) >= 46 {
		bsl.MaxUpTps = util.BytesToUint32(data[1:3])
		bsl.MinUpTps = util.BytesToUint32(data[3:5])
		bsl.MaxUpRate = util.BytesToUint32(data[5:7])
		bsl.MinUpRate = util.BytesToUint32(data[7:9])

		bsl.MaxDownTps = util.BytesToUint32(data[9:11])
		bsl.MinDownTps = util.BytesToUint32(data[11:13])
		bsl.MaxDownRate = util.BytesToUint32(data[13:15])
		bsl.MinDownRate = util.BytesToUint32(data[15:17])

		bsl.MaxPicDealTps = util.BytesToUint32(data[17:19])
		bsl.MinPicDealTps = util.BytesToUint32(data[19:21])
		bsl.MaxPicDealRate = util.BytesToUint32(data[21:23])
		bsl.MinPicDealRate = util.BytesToUint32(data[23:25])

		bsl.MaxDelTps = util.BytesToUint32(data[25:27])
		bsl.MinDelTps = util.BytesToUint32(data[27:29])
		bsl.MaxDelRate = util.BytesToUint32(data[29:31])
		bsl.MinDelRate = util.BytesToUint32(data[31:33])

		bsl.MaxListTps = util.BytesToUint32(data[33:35])
		bsl.MinListTps = util.BytesToUint32(data[35:37])
		bsl.MaxListRate = util.BytesToUint32(data[37:39])
		bsl.MinListRate = util.BytesToUint32(data[39:41])

		bsl.MaxSpace = util.BytesToUint32(data[41:45])
		bsl.Priority = util.BytesToUint8(data[45:46])
	}
	return
}
*/
func (refer *Refer) GenRegexp() {
	if !strings.HasSuffix(refer.Refer, "*") {
		refer.Refer += "$"
	}
	refer.Refer = strings.Replace(refer.Refer, "*", STARSUBS, -1)
	refer.reg, refer.compileError = regexp.CompilePOSIX(refer.Refer)
}
func (buk *Bucket) ParseRefererConfig() {
	if buk.RefersAllowEmpty == "" {
		buk.RefererConfig.AllowEmpty = true
	}
	buk.Refers = strings.Trim(buk.Refers, " ")
	parts := strings.Split(buk.Refers, ";")
	for _, part := range parts {
		if part != "" {
			buk.RefererConfig.Referers = append(buk.RefererConfig.Referers, Refer{Refer: part})
		}
	}
	buk.RefersAllowEmpty = ""
	buk.Refers = ""
}
func (config *RefererConfiguration) AuthReferer(r *http.Request) bool {
	s := r.Header.Get("Referer")
	if len(config.Referers) == 0 {
		return true
	}
	if config.AllowEmpty && s == "" {
		return true
	}
	for _, referer := range config.Referers {
		if referer.compileError != nil {
			continue
		}
		if referer.reg == nil {
			if referer.GenRegexp(); referer.compileError != nil || referer.reg == nil {
				continue
			}
		}
		if referer.reg.MatchString(s) {
			return true
		}
	}
	return false
}
func (config *RefererConfiguration) ToString() (ret string) {
	for _, referer := range config.Referers {
		ret = ret + ";" + referer.Refer
	}
	ret = strings.TrimPrefix(ret, ";")
	return ret
}
func (buk *Bucket) AuthReferer(r *http.Request) bool {
	return buk.RefererConfig.AuthReferer(r)
}

/*
1 必须是用户自定义bucket
2 符合规则
*/
func (buk *Bucket) NeedMigration(objectname string) bool {
	if buk.SysDefine != public.USER_DEFINED_OBJECT {
		glog.V(4).Infoln(buk.Name, "is not user defined, need not to migration")
		return false
	}
	//假如MigrationRule 是空，需要看看是不是没有初始化这个属性。
	if buk.MRuleSets == nil {
		glog.V(4).Infoln(buk.Name, " need not to migration")
		return false
	}
	return buk.MRuleSets.NeedMigration(objectname)
}

func (buk *Bucket) GetBucketName() string {
	if len(buk.Name) > len(buk.Account)+1 {
		return buk.Name[len(buk.Account)+1:]
	}
	return ""
}

/*
bucket内部配置的远端回源信息
MirrorURL+Dir+objectname 是回源地址
*/
type MigrationRule struct {
	MirrorURL string `json:"Url"`
	Dir       string `json:"Dir"`
	//本地的，需要回源的前缀匹配
	Prefix string `json:"Prefix"`
}

/*
bucket的
*/
type MigrationRuleSets struct {
	//远端url, 是配置的域名。
	Rules []*MigrationRule `json:"Rules"`
}

/*
对照bucket的规则，验证指定的object是不是需要进行迁移动作
没开启热迁移不迁
不符合热迁移规则不迁
*/
func (mrs *MigrationRuleSets) NeedMigration(objectname string) bool {
	if !public.LiveMigrationTask {
		glog.V(4).Infoln("public.LiveMigrationTask is false")
		return false
	}
	if mrs.Rules == nil || len(mrs.Rules) == 0 {
		glog.V(4).Infoln("Remotrurls is nil")
		return false
	}

	for i, _ := range mrs.Rules {
		if strings.HasPrefix(objectname, mrs.Rules[i].Prefix) {
			return true
		}
	}
	return false
}

/*
具体的规则
*/
type LifeCycleRule struct {
	//是否开启
	Enable bool `json:"Enable"`
	//规则的匹配生命周期时间
	LifeCycle int `json:"LifeCycle"`
	//前缀匹配
	//为空表示全bucket适用
	Prefix string `json:"Prefix"`
}
type LifeCycleRuleSets struct {
	//远端url, 是配置的域名。
	Rules []*LifeCycleRule `json:"Rules"`
}

//获取对象是不是需要自动设置生命周期
func (lcrs *LifeCycleRuleSets) GetLifeCycle(objectname string) int {
	if lcrs.Rules == nil || len(lcrs.Rules) == 0 {
		return 0
	}
	for i, _ := range lcrs.Rules {
		v := lcrs.Rules[i]
		if !v.Enable {
			continue
		}
		if v.Prefix == "" || strings.HasPrefix(objectname, v.Prefix) {
			return v.LifeCycle
		}
	}
	return 0
}

func (lcrs *LifeCycleRuleSets) RulesCheck() error {
	if lcrs.Rules == nil || len(lcrs.Rules) == 0 {
		return nil
	}
	Overall := 0
	pres := make(map[string]struct{})
	for i, _ := range lcrs.Rules {
		v := lcrs.Rules[i]
		if v.Prefix == "" {
			Overall++
		}
		if len(v.Prefix) > 255 {
			return errors.New("bad rules:too long Prefix")
		}
		if v.LifeCycle > public.MAXlIFECYCLE {
			return errors.New("bad rules:too larger lifecycle")
		}
		if _, ok := pres[v.Prefix]; ok {
			return errors.New("bad rules: same Prefix exists")
		}
		pres[v.Prefix] = struct{}{}
	}
	if Overall != 0 && Overall != len(lcrs.Rules) {
		return errors.New("bad rules!")
	}

	return nil
}
