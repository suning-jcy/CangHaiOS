package util

/*
const (
	VERSION = "0.63 beta"
)
*/

var VERSION = "0.63 beta"

const (
	MAX_NODES             = 500
	TIME_OUT_SEC_INTERVAL = 1
)
const (
	FILER         = "filer"
	VOLUME        = "volume"
	ECVOLUME        = "ecVolume"
	FOLDER        = "folder"
	MASTER        = "master"
	ACCOUNT       = "account"
	BACKUP        = "sync"
	DIRTREE       = "dirTreeSync"
	BUCKET        = "bucket"
	HISSYNC       = "hissync"
	SUBFILER      = "FilerPartition"
	MEMCACHE      = "memcache"
	RECYCLE       = "recycle"
	FTP           = "ftp"
	SFTP          = "sftp"
	BACKUPSERVER  = "backupserver"
	BINLOGSERVER  = "binlogserver"
	COMPARESERVER = "compareserver"
	DISKCHECK     = "Diskcheck"
	REGION        = "region"
	SYNCINTERFACE = "syncinterface"
	SYNCDATAHANDLER = "syncdatahandler"
	SEPARATION = "separation"
)
const (
	UPGRADE  = "upgrade"
	MODIFY   = "modify"
	ROLLBACK = "recover"
)

const (
	MASTERPORT = "9333"
	VOLUMEPORT = "8080"
	ECVOLUMEPORT = "8282"
	FILERPROT  = "8888"
	PARTIPORT  = "9090"
	ACCNTPORT  = "9191"
	BUCKTPORT  = "9292"
	MEMCHPORT  = "9393"
	REGIONPORT = "9888"
)
const (
	UPLOAD   = "UPLOAD"
	DOWNLOAD = "DOWNLOAD"
	DELETE   = "DELETE"
	LOOKUP   = "LOOKUP"
	VACUUM   = "VACUUM"
	FIX      = "FIX"
	DEFAULT  = "DEFAULT"
)

const (
	F_FILESIZEOVER_ERR         = "1000"
	F_FILEEMPTY_ERR            = "1001"
	F_URLPRASE_ERR             = "1002"
	F_FILENONAME_ERR           = "1003"
	F_FILEASSIGN_ERR           = "1004"
	F_UPLOADTOVOLUME_ERR       = "1005"
	F_WRITEDB_ERR              = "1006"
	F_LOCATIONLOOKUP_ERR       = "1100"
	F_DOWNLOAD_FIDNOTFOUND_ERR = "1101"
	F_FIDFORMAT_ERR            = "1102"
	F_GETDATA_ERR              = "1103"
	F_DELETE_ERR               = "1200"
	F_DELETE_FIDNOTFOUND_ERR   = "1201"
	//2000
	//2001
	//2100
	V_FORMPRASE_ERR               = "3000"
	V_NEEDLEALLOCATE_ERR          = "3001"
	V_NEWVOLUMEID_ERR             = "3002"
	V_REPLICATEWRITE_ERR          = "3003"
	V_READ_ERR                    = "3100"
	V_LOCATIONLOOKUP_ERR          = "3101"
	V_RESPONSEWRITE_ERR           = "3102"
	V_DOWNLOAD_COOKIEUNMATCH_ERR  = "3103"
	V_URLPRASE_ERR                = "3104"
	V_REPLICATEDELETE_ERR         = "3200"
	V_DELETE_COOKIEUNMATCH_ERR    = "3201"
	V_FIX_LOADDATA_ERR            = "3300"
	V_FIX_READSB_ERR              = "3301"
	V_FIX_READNEEDLEHEADER_ERR    = "3302"
	V_FIX_READNEEDLEBODY_ERR      = "3303"
	V_CREATEINDEX_ERR             = "3304"
	V_CREATEDATA_ERR              = "3400"
	V_VACUUM_LOADDATA_ERR         = "3401"
	V_VACUUM_READSB_ERR           = "3402"
	V_VACUUM_READNEEDLEHEADER_ERR = "3403"
	V_VACUUM_READNEEDLEBODY_ERR   = "3404"
	V_NEEDLEMAPERR_ERR            = "3405"
	V_NEEDLEWRITE_ERR             = "3406"
	V_VACCUM_ERR                  = "3405"
	//3406
	//3407
)

const (
	HTTP_GET    = "GET"
	HTTP_HEADER = "HEADER"
	HTTP_DELETE = "DELETE"
	HTTP_PUT    = "PUT"
	HTTP_POST   = "POST"
)

const (
	NormalFile Filetype = iota
	BlockedFile
	ObjectFile
	AppndFile

	NormalFile_Ec
	BlockedFile_Ec

	UnknownFile
)