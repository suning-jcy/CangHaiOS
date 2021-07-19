package public

const (
	HTTP_PREFIX = "http://"
)

const (
	ACCOUNT  = "account"
	BUCKET   = "bucket"
	MEMCACHE = "memcache"
	FILEMAP  = "filemap"
	FOLDER   = "folder"
	DEFAULT  = "default"
	EC       = "ec"
	ECTemp   = "ectemp"
)

const (
	MASTER_PARTITION_LOOKUP         = "/partition/lookup"
	MASTER_SEPARATION_LEADER_LOOKUP = "/sdoss/separationserver/status"
)

const (
	MAX_LIST_COUNT_ONCE     = 1000
	DEFAULT_LIST_COUNT_ONCE = 100
)
const (
	MAX_TPS = 1000
)

const (
	BUCKET_PRIVATE   = "private"
	BUCKET_PUBLIC_RO = "public-read"
	BUCKET_PUBLIC_WR = "public-read-write"
)

var BucketAccessMap = map[string]int{
	BUCKET_PRIVATE:   1,
	BUCKET_PUBLIC_RO: 2,
	BUCKET_PUBLIC_WR: 3,
}

const (
	USER_DEFINED_OBJECT   = "user-define"
	SYSTEM_DEFINED_OBJECT = "sys-define"
)

const (
	SDOSS_AUTH_PREFIX = "SDOSS"
)

const (
	UPLOAD = iota
	DOWNLOAD
	DELETE
	PICDEAL
	LISTBUCKET
	SYNCPLATFORM
)

const (
	clockAccuracy       = 1000
	maxSleepTimeSeconds = 60
	logSleepTimeSeconds = 0
	RATE_BUFFER_SECONDS = 5
	accountRatelimit    = 0
)
const (
	SYNC_B2A_CREATE = 1
	SYNC_B2A_UPDATE = 2
	SYNC_B2A_DELETE = 3
)

const (
	MEMCACHE_DIRTREE_REALTIME_DIR_PREFIX            = "/SDOSS_REALTIME_DIR_V"
	MEMCACHE_DIRTREE_REALTIME_FILE_PREFIX           = "/SDOSS_REALTIME_FILE_V"
	MEMCACHE_DIRTREE_PREFIX                         = "/SDOSS_DIR"
	MEMCACHE_DIRTREE_LOCK_PREFIX                    = "/SDOSS_DIR_LOCK"
	MEMCACHE_QOS_LOCK_PREFIX                        = "/SDOSS_QOS_LOCK"
	MEMCACHE_APPEND_PREFIX                          = "/SDOSS_APPEND_PREFIX"
	MEMCACHE_MULTIPART_PREFIX                       = "/SDOSS_MULTIPART_PREFIX/"
	MEMCACHE_MULTIPART_LOCK_PREFIX                  = "/SDOSS_MULTIPART_PREFIX_LOCK/"
	MEMCACHE_TPS_PREFIX                             = "/SDOSS_TPS_PREFIX"
	MEMCACHE_RATE_PREFIX                            = "/SDOSS_RATE_PREFIX"
	MEMCACHE_TPS_LOCK_PREFIX                        = "/SDOSS_TPS_PREFIX_LOCK"
	MEMCACHE_RATE_LOCK_PREFIX                       = "/SDOSS_RATE_PREFIX_LOCK"
	MEMCACHE_DIRTREE_CONFIG_PREFIX                  = "/SDOSS_DIRTREE_CONFIG"
	MEMCACHE_DIRTREE_CONFIG_MAXFILENUM              = MEMCACHE_DIRTREE_CONFIG_PREFIX+"/MAXFILENUM"
	MEMCACHE_DIRTREE_BUCKET_STATISTIC_PREFIX        = "/SDOSS_BUCKET_STATISTIC_PREFIX/"
	MEMCACHE_DIRTREE_BUCKET_STATISTIC_LOCK_PREFIX   = "/SDOSS_BUCKET_STATISTIC_PREFIX_LOCK/"
	MEMCACHE_TOKENID_PREFIX                         = "/SDOSS_TOKENID_PREFIX/"

	SYNC_TPS_PREFIX          = "/SYNC_TPS_PREFIX"
	SYNC_RATE_PREFIX         = "/SYNC_RATE_PREFIX"
	SYNC_TPS_LOCK_PREFIX     = "/SYNC_TPS_PREFIX_LOCK"
	SYNC_RATE_LOCK_PREFIX    = "/SYNC_RATE_PREFIX_LOCK"
)

//dirtree.dirtreemanager
const (
	//max thread num for batch deal
	ThreadNum = 12
	//
	ThreadDeathTime = 5
	//
	WorkerQueueMaxSum = 1000
	//
	RefullThreadNum = 2
	//
	RefullQueueMaxSum = 200
)

//dirtree.dirtree
const (
	DirtreeRefullThreshold = 6000
	DISTRIBUTED_LOCK_TIME  = 5000
)

//ftp.driver
const (
	MaxShowFileSum = 3000
)

const (
	STORAGE_MIN_COMPRESS = int64(1024)
)

//x-sdoss-header
const (
	SDOSS_COMPRESS_LEVEL = "x-sdoss-compress-level"
)

//LocalLogScanner
const (
	READ_BINGLOG_TIMEOUT = 3
)

//AppendFile memcache max life-cycle
const (
	APPEND_FILE_CACHE_TIMEOUT = int64(30) //seconds
)

//Max num of List all multipart
const (
	MAX_LIST_ALL_MULTIPART = 1000
)

//Max bucket per account
const (
	MAX_BUCKET_NUM = 10
)

//Max filer server binlog channel size
const (
	MAX_BINLOG_CH_SIZE = 10000
)

//Max append size in byte
const (
	MAX_APPEND_SLICE_SIZE = 100 * 1024 * 1024
	MAX_APPEND_FILE_SIZE  = int64(5 * 1024 * 1024 * 1024)
)

//Max white list hosts
const (
	MAX_WHITE_LIST = 100
)

//bad file lock time ,
const (
	FTP_BAD_FILE_LOCK_TIME = 5
)

const (
	FUSE_User_Agent      = "OSS_FUSE"
	FTP_User_Agent       = "OSS_FTP"
	SFTP_User_Agent      = "OSS_SFTP"
	MIGRATION_User_Agent = "OSS_MIGRATION"
)
const (
	SDOSS_MERGE = "Sdoss_merge"
)
const (
	//requestid
	SDOSS_REQUEST_ID = "x-sdoss-request-id"
	//requestid
	SDOSS_SESSION_ID = "x-sdoss-sessionid"
	//函数分段时间
	SDOSS_SPLIT_TIME = "SdossSplitTime"

	SDOSS_ERROR_MESSAGE = "SdossErrMess"

	CONTENT_ENCODING = "Content-Encoding"
)

const (
	MAX_LIST_SEARCH      = 1
	FILER_LIST_THREADNUM = 128
)

const (
	MAX_TAG_LENGTH = 255
)

//merge
const (
	MAX_MERGE_LENGTH     = 10240
	MAX_MERGECONF_LENGTH = 64
)

//单个目录下最多支持写多少个文件数到目录缓存(redis)里,做一个极限限制
const (
	MAX_MAX_SUB_FILE_SUM = 5000000
	MIN_MAX_SUB_FILE_SUM = 0
)

//http status used for file header check
const (
	StatusInvalidFileHeader = 452
	StatusHighCpu = 453
)

//file header check map
const (
	JPGHEAD  = "FFD8FF"
	JPEGHEAD = "FFD8FF"
	PNGHEAD  = "89504E47"
)

//bucket store location
const (
	STORE_BUCKET_REPLICA = "rep_store"
	STORE_BUCKET_EC      = "ec_store"
)

//volume service status
const (
	SERVICE_START = "start"
	SERVICE_STOP = "stop"
	SERVICE_READONLY = "readonly"
)

const (
	NEEDFRESHACCESSTIME = "freshAccessTime"
	UPDATECYCLE         = "updateCycle"
)
const (
	REDISSCANPERCOUNT = 1000
	REDISSCANMAXCOUNT = 10000
)
//tokenID memcache max life-cycle
const (
	MAX_TOKEN_ID_CACHE_TIMEOUT = int64(60*60*12) //max:12 hour
	TOKEN_ID_CACHE_TIMEOUT = int64(60*60) //defaulst: 1 hour
)