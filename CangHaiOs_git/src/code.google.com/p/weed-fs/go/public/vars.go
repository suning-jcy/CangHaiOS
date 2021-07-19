package public

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
)

var (
	APPEND_BLOCK_MAX_SIZE = int64(4 * 1024 * 1024)
)

var (
	FrameLimt  int = 0
	GifProduct int = 0
)

type InitorFuncType func(parm interface{})

var (
	InitFuncMap     = make(map[string]InitorFuncType)
	AccountCheckMap = make(map[uint8]struct{})
	BucketCheckMap  = make(map[uint8]struct{})
)

var (
	MAX_MAX_FILE_SIZE     = 500
	MIN_MAX_FILE_SIZE     = 1
	DEFAULT_MAX_FILE_SIZE = 500
	MAX_FILENAME_LENGTH   = 255
	Imgser_Maxfilesize    = 5
	Imgser_Cdnlife        = 864000
	MaxSubFileSum         = 40000
	Default_Cdnlife       = 3600 // 1小时
)

func init() {
	//init Account/Bucket Check map
	for i := uint8(45); i <= 122; i++ {
		if i == 45 || (i >= 48 && i <= 57) || i == 95 || (i >= 97 && i <= 122) {
			if i != 95 && i != 45 {
				AccountCheckMap[i] = struct{}{}
			}
			BucketCheckMap[i] = struct{}{}
		}
	}
	//Register global var's initlizer
	InitFuncMap["maxAppendBlockSize"] = func(param interface{}) {
		if reflect.TypeOf(param) == reflect.TypeOf(APPEND_BLOCK_MAX_SIZE) {
			value := param.(int64)
			if value > 0 {
				APPEND_BLOCK_MAX_SIZE = value * 1024 * 1024
			}
		}
	}
}
func InitGlobalVar(name string, param interface{}) {
	if initFunc, ok := InitFuncMap[name]; ok {
		initFunc(param)
	}
	return
}

//parition的请求合并
var (
	PartitionAsyncTask            = false
	PartitionAsyncQuery_QueueLen  = 32
	PartitionAsyncQuery_ConNum    = 16
	PartitionAsyncQuery_CacheTime = 0
)

func SetPartitionAsyncTaskParameter(str string) error {
	paras := strings.Split(str, ",")
	if len(paras) != 3 {
		return errors.New("parameter number must be 3.")
	}
	value := make([]int, 3)
	for i, para := range paras {
		val, e := strconv.Atoi(para)
		if e != nil {
			return errors.New("parameter  must be number.")
		}
		value[i] = val
	}
	PartitionAsyncQuery_QueueLen = value[0]
	PartitionAsyncQuery_ConNum = value[1]
	PartitionAsyncQuery_CacheTime = value[2]
	return nil
}

//volume的请求合并
var (
	VolumeAsyncTask               = false
	VolumeAsyncRead_QueueLen      = 32
	VolumeAsyncRead_ConNum        = 16
	VolumeAsyncRead_CacheTime     = 1
	VolumeAsyncPicTrans_QueueLen  = 32
	VolumeAsyncPicTrans_ConNum    = 16
	VolumeAsyncPicTrans_CacheTime = 1
	VolumeProxyRead_QueueLen      = 32
	VolumeProxyRead_ConNum        = 16
	VolumeProxyRead_CacheTime     = 1
)

func SetVolumeAsyncTaskParameter(str string) error {
	paras := strings.Split(str, ",")
	if len(paras) != 6 {
		return errors.New("parameter number must be 6.")
	}
	value := make([]int, 6)
	for i, para := range paras {
		val, e := strconv.Atoi(para)

		if e != nil {
			return errors.New("parameter  must be number.")
		}
		value[i] = val
	}
	VolumeAsyncRead_QueueLen = value[0]
	VolumeAsyncRead_ConNum = value[1]
	VolumeAsyncRead_CacheTime = value[2]
	VolumeAsyncPicTrans_QueueLen = value[3]
	VolumeAsyncPicTrans_ConNum = value[4]
	VolumeAsyncPicTrans_CacheTime = value[5]
	return nil

}

//
var (
	FilerLoadBalance = false
)

var (
	VolumeAutoRepair = false
)

//是不是需要热迁移的开关和参数
var (
	//开不开启
	LiveMigrationTask = false
	//热迁移等待队列长度
	LiveMigration_QueueLen = 32
	//热迁移总的执行线程数量
	LiveMigration_ThrNum = 16
)

func SetLiveMigrationParameter(str string) error {
	paras := strings.Split(str, ",")
	if len(paras) != 2 {
		return errors.New("parameter number must be 2.")
	}
	value := make([]int, 2)
	for i, para := range paras {
		val, e := strconv.Atoi(para)
		if e != nil {
			return errors.New("parameter must be number.")
		}
		value[i] = val
	}
	LiveMigration_QueueLen = value[0]
	LiveMigration_ThrNum = value[1]
	return nil
}

func SetIntValue(Max int, Min int, value int) int {
	temp := value
	if temp > Max {
		temp = Max
	}
	if temp < Min {
		temp = Min
	}
	return temp
}

//volume 日志记录中用到，超过此值会在info日志中有相应打印
var (
	WriteDiskTimeout = 2000
	ReadDiskTimeout  = 2000
)

//最大生命周期
const MAXlIFECYCLE = 1825

//热迁移的时延，距离现在的时间比这个时间小的，不用热迁移
var (
	MigrationDelay = 0
)

const BucketStaticThread = 100

//recycle中请求partition失败次数，失败超过3次跳到下一个分片
const RecycleFailCount = 3
