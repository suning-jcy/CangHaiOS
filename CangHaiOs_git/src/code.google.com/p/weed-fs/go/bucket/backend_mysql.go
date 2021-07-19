package bucket

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
)

var BucketAlreadyExistedErr = errors.New("BucketAlreadyExisted")
var BucketNotFoundErr = errors.New("BucketNotFound")
var BucketNotEmptyErr = errors.New("BucketNotEmpty")
var BucketBadReqErr = errors.New("BadRequest")
var BucketMovedErr = errors.New("BucketMoved")
var SyncMaxRecordOnce = 20
var MAXSYNCROUND = 1
var SECSOFYEAR = 365 * 24 * 3600

type MysqlBucketManager struct {
	dsn        string
	db         *sql.DB
	table      string
	accessLock sync.Mutex
	id         string
	isLoading  bool
	isMoved    bool
	sync       bool
	pushTime   string
}

func (bm *MysqlBucketManager) MarkSync() {
	bm.sync = true
}

const bucketTableCreateScript = "create table if not exists bucket (" +
	"name varchar(64) BINARY Not NULL," +
	"account varchar(64) BINARY  Not NULL," +
	"created_time varchar(64) NOT NULL," +
	"put_time varchar(64) NOT NULL," +
	"object_count BIGINT(20) Not NULL DEFAULT 0," +
	"byte_used BIGINT(20) Not NULL DEFAULT 0," +
	"acl varchar(64) NOT NULL DEFAULT ''," +
	"logprefix varchar(64) Not NULL DEFAULT ''," +
	"staticnotfound varchar(64) Not NULL DEFAULT ''," +
	"refers varchar(64) Not NULL DEFAULT ''," +
	"refersallowempty varchar(64) Not NULL DEFAULT ''," +
	"refersext varchar(1024) Not NULL DEFAULT ''," +
	"lifecycle varchar(1024) Not NULL DEFAULT ''," +
	"collection varchar(64) Not NULL," +
	"access_type varchar(64) Not NULL DEFAULT '0'," +
	"sys_define varchar(32) Not NULL ," +
	"enable tinyint(1) Not NULL DEFAULT 1," +
	"location varchar(64) Not NULL DEFAULT ''," +
	"is_deleted tinyint(1) Not NULL DEFAULT 0," +
	"support_append varchar(8) Not NULL DEFAULT 'yes'," +
	"image_service varchar(8) Not NULL DEFAULT 'yes'," +
	"max_down_tps INT(64) Not NULL DEFAULT 0," +
	"max_up_tps INT(64) Not NULL DEFAULT 0," +
	"max_file_size INT(64) Not NULL DEFAULT 500," +
	"sync tinyint(1) Not NULL DEFAULT 0," +
	"cdn_life_cycle  INT DEFAULT 0," +
	"reserver1 varchar(64) NOT NULL DEFAULT ''," +
	"reserver2 varchar(64) NOT NULL DEFAULT ''," +
	"reserver3 varchar(72) NOT NULL DEFAULT ''," +
	"reserver4 INT(64) NOT NULL DEFAULT 0," +
	"reserver5 INT(64) NOT NULL DEFAULT 0," +
	"resource varchar(1024) NOT NULL DEFAULT ''," +
	"PRIMARY KEY (name));"

var req2dbMap = map[string]string{
	"acl":                     "acl",
	"log-prefix":              "logprefix",
	"static-index":            "static_index",
	"refersallowempty":        "refersallowempty",
	"life-cycle":              "lifecycle",
	"access-type":             "access_type",
	"max-up-tps":              "max_up_tps",
	"max-down-tps":            "max_down_tps",
	"max-file-size":           "max_file_size",
	"enable":                  "enable",
	"cors":                    "refers",
	"max-age":                 "cdn_life_cycle",
	"referersext":             "refersext",
	"support-append":          "support_append",
	"backup":                  "reserver1",
	"merge":                   "reserver2",
	"resource":                "resource",
	"thresholds":              "reserver3",
	"qosconfig":               "reserver3",
	"support-empty":           "reserver4",
	"support-file-head-check": "reserver4",
	"support-degrade":         "reserver4",
}
var dbValueTypeMap = map[string]string{
	"acl":              "string",
	"logprefix":        "string",
	"static_index":     "string",
	"refers":           "string",
	"lifecycle":        "string",
	"access_type":      "string",
	"support_append":   "string",
	"max_up_tps":       "int",
	"max_down_tps":     "int",
	"max_file_size":    "int",
	"enable":           "int",
	"cdn_life_cycle":   "int",
	"refersallowempty": "string",
	"refersext":        "string",
	"reserver1":        "string",
	"reserver2":        "string",
	"reserver3":        "string",
	"resource":         "string",
	"reserver4":        "int",
}

func NewMysqlBucketManager(dsn string, partitionID string, createTable bool) (bm *MysqlBucketManager, err error) {
	bm = &MysqlBucketManager{dsn: dsn, id: partitionID}

	bm.db, err = sql.Open("mysql", dsn)
	if err != nil {
		panic(err.Error())
	}
	bm.table = "bucket"
	bm.pushTime = fmt.Sprint(time.Now().UnixNano())
	if createTable {
		_, err = bm.db.Exec(bucketTableCreateScript)
		if err != nil {
			glog.V(0).Infoln(err.Error())
			return nil, err
		}
		glog.V(0).Infoln("create bucket ok", partitionID, createTable, dsn)
	}
	return bm, nil
}

func (bm *MysqlBucketManager) Create(buk Bucket) (err error) {
	bm.accessLock.Lock()
	defer bm.accessLock.Unlock()
	if bm.isMoved {
		return BucketMovedErr
	}
	defer bm.MarkSync()
	if buk.AccessType == "" {
		buk.AccessType = public.BUCKET_PRIVATE
	}
	if buk.SysDefine == "" {
		buk.SysDefine = public.USER_DEFINED_OBJECT
	}
	if buk.SupportAppend == "" {
		buk.SupportAppend = "no"
	}
	if buk.ImageService == "" {
		buk.ImageService = "yes"
	}
	if buk.MaxFileSize < 0 {
		buk.MaxFileSize = public.DEFAULT_MAX_FILE_SIZE
	}
	if buk.MaxFileSize < public.MIN_MAX_FILE_SIZE {
		buk.MaxFileSize = public.MIN_MAX_FILE_SIZE
	}
	if buk.MaxFileSize > public.MAX_MAX_FILE_SIZE {
		buk.MaxFileSize = public.MAX_MAX_FILE_SIZE
	}
	stmtIns, err := bm.db.Prepare("INSERT INTO bucket (name,location,account,created_time,collection,access_type,put_time,sys_define,support_append,image_service,max_down_tps,max_up_tps,max_file_size,sync,reserver1,cdn_life_cycle,reserver4,logprefix) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		return err
	}
	defer stmtIns.Close()
	name := buk.Account + "/" + buk.Name
	bucketName := ""
	bucketIsDel := 0
	tx, err := bm.db.Begin()
	if err != nil {
		return err
	}
	err = tx.QueryRow("SELECT name,is_deleted from bucket WHERE name=?", name).Scan(&bucketName, &bucketIsDel)
	if err != nil && err != sql.ErrNoRows {
		tx.Rollback()
		return err
	}
	if bucketName != "" {
		if bucketIsDel == 0 {
			tx.Rollback()
			return BucketAlreadyExistedErr
		} else {
			if _, err = tx.Exec("DELETE FROM bucket WHERE name=?", bucketName); err != nil {
				tx.Rollback()
				return err
			}
		}
	}
	txs := util.StmtAdd(tx, stmtIns) //tx.Stmt(stmtIns)
	defer txs.Close()
	date := ""
	if buk.CreateTime != "" {
		oldTime := buk.CreateTime
		newTime, err := time.Parse(util.LocalTimeFormat, oldTime)
		if err != nil {
			glog.V(0).Infoln("parse time err:", err, oldTime)
			date = ""
		} else {
			date = fmt.Sprint(util.ParseUnixSec(fmt.Sprint(newTime.Unix()))) + "000000000"
		}
	}
	if date == "" {
		date = fmt.Sprint(time.Now().UnixNano())
	}
	_, err = txs.Exec(name, buk.Location, buk.Account, date, buk.Collection, buk.AccessType, date, buk.SysDefine, buk.SupportAppend, buk.ImageService, buk.MaxDonwTps, buk.MaxUpTps, buk.MaxFileSize, public.SYNC_B2A_CREATE, buk.Reserver1, buk.MaxAgeDay, buk.Reserver4, buk.LogPrefix)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	return
}
func (bm *MysqlBucketManager) Delete(account, bucket string) error {
	bm.accessLock.Lock()
	defer bm.accessLock.Unlock()
	if bm.isMoved {
		return BucketMovedErr
	}
	defer bm.MarkSync()
	stmtSel, err := bm.db.Prepare("SELECT name,account,created_time,put_time,object_count,byte_used from bucket WHERE name=?")
	if err != nil {
		return err
	}
	defer stmtSel.Close()
	buk := &Bucket{}
	name := account + "/" + bucket
	err = stmtSel.QueryRow(name).Scan(&buk.Name, &buk.Account, &buk.CreateTime, &buk.PutTime, &buk.ObjectCount, &buk.ByteUsed)
	if err != nil {
		if err == sql.ErrNoRows {
			return BucketNotFoundErr
		}
		return err
	}
	if buk.ObjectCount > 0 {
		return BucketNotEmptyErr
	}
	date := fmt.Sprint(time.Now().UnixNano())
	stmtDel, err := bm.db.Prepare("UPDATE bucket set sync=?,is_deleted=?,put_time=? WHERE name=?")
	if err != nil {
		return err
	}
	defer stmtDel.Close()
	tx, err := bm.db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return err
	}
	txs := util.StmtAdd(tx, stmtDel) //tx.Stmt(stmtDel)
	defer txs.Close()
	_, err = txs.Exec(public.SYNC_B2A_DELETE, 1, date, name)
	if err != nil {
		glog.V(0).Infoln(err.Error())
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	return err
}
func (bm *MysqlBucketManager) Get(account, bucket string) (buk *Bucket, err error) {
	if bm.isMoved {
		return nil, BucketMovedErr
	}
	stmt, err := bm.db.Prepare("SELECT name,created_time,account,object_count,byte_used,collection,sys_define,access_type,support_append,image_service,max_down_tps,max_up_tps,max_file_size,enable,refers,cdn_life_cycle,refersallowempty,refersext,reserver1,reserver2,resource,lifecycle,reserver3,reserver4,location,logprefix from bucket WHERE name=? and is_deleted=0")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	buk = &Bucket{}
	name := account + "/" + bucket
	tx, err := bm.db.Begin()
	if err != nil {
		tx.Rollback()
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return nil, err
	}
	ReSource := ""
	thresholdsstr := ""
	txs := util.StmtAdd(tx, stmt) // tx.Stmt(stmt)
	defer txs.Close()
	err = txs.QueryRow(name).Scan(
		&buk.Name,
		&buk.CreateTime,
		&buk.Account,
		&buk.ObjectCount,
		&buk.ByteUsed,
		&buk.Collection,
		&buk.SysDefine,
		&buk.AccessType,
		&buk.SupportAppend,
		&buk.ImageService,
		&buk.MaxDonwTps,
		&buk.MaxUpTps,
		&buk.MaxFileSize,
		&buk.Enable,
		&buk.CORS,
		&buk.MaxAgeDay,
		&buk.RefersAllowEmpty,
		&buk.Refers,
		&buk.Reserver1,
		&buk.Reserver2,
		&ReSource,
		&buk.LifeCycle,
		&thresholdsstr,
		&buk.Reserver4,
		&buk.Location,
		&buk.LogPrefix)
	if err != nil {
		glog.V(0).Infoln("failing to QueryRow ", err.Error())
		tx.Rollback()
		if err == sql.ErrNoRows {
			return nil, BucketNotFoundErr
		}
		return nil, err
	}
	tx.Commit()
	mr := &MigrationRuleSets{}
	if ReSource != "" {
		err = json.Unmarshal([]byte(ReSource), mr)
		if err != nil {
			glog.V(0).Infoln("bad Return Source of:", buk.Name, "error:", err)
			err = nil
		} else {
			buk.MRuleSets = mr
		}
	}
	lr := &LifeCycleRuleSets{}
	if buk.LifeCycle != "" {
		err = json.Unmarshal([]byte(buk.LifeCycle), lr)
		if err != nil {
			glog.V(0).Infoln("bad Return Source of:", buk.Name, "error:", err)
			err = nil
		} else {
			buk.LRuleSets = lr
		}
	}
	buk.CreateTime = util.ParseUnixNano(buk.CreateTime)
	buk.Thresholds = &BucketThresholds{
		MaxUpTps:       "unlimited",
		MinUpTps:       "0",
		MaxUpRate:      "unlimited",
		MinUpRate:      "0",
		MaxDownTps:     "unlimited",
		MinDownTps:     "0",
		MaxDownRate:    "unlimited",
		MinDownRate:    "0",
		MaxPicDealTps:  "unlimited",
		MinPicDealTps:  "0",
		MaxPicDealRate: "unlimited",
		MinPicDealRate: "0",
		MaxDelTps:      "unlimited",
		MinDelTps:      "0",
		MaxDelRate:     "unlimited",
		MinDelRate:     "0",
		MaxListTps:     "1",
		MinListTps:     "0",
		MaxListRate:    "unlimited",
		MinListRate:    "0",
	}
	buk.Thresholds.Parse([]byte(thresholdsstr))

	return
}
func (bm *MysqlBucketManager) List(account, marker, prefix string, maxNum int) (bukList *BucketList, err error) {
	bm.accessLock.Lock()
	defer bm.accessLock.Unlock()
	if bm.isMoved {
		return nil, BucketMovedErr
	}
	prefix = account + "/" + prefix
	tx, err := bm.db.Begin()
	if err != nil {
		glog.V(0).Infoln("Fail to begin transiction:", err.Error())
		return
	}
	bukList = &BucketList{}
	stmtSel, err := bm.db.Prepare("select name,account,created_time,put_time,object_count,byte_used,acl,logprefix,staticnotfound,refers,refersallowempty,lifecycle,collection,access_type,sys_define,is_deleted,enable,location from bucket where name like CONCAT(?, '%') and name > ? and is_deleted=0 limit ?")
	if err != nil {
		glog.V(0).Infoln("failing to Prepare ", err.Error())
		tx.Rollback()
		return nil, err
	}
	defer stmtSel.Close()
	rows, err := stmtSel.Query(prefix, marker, maxNum)
	if err != nil {
		glog.V(0).Infoln(err.Error())
		tx.Rollback()
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		buk := Bucket{}
		err = rows.Scan(&buk.Name, &buk.Account, &buk.CreateTime, &buk.PutTime, &buk.ObjectCount, &buk.ByteUsed, &buk.Acl, &buk.LogPrefix, &buk.StaticNotFound, &buk.Refers, &buk.RefersAllowEmpty, &buk.LifeCycle, &buk.Collection, &buk.AccessType, &buk.SysDefine, &buk.IsDeleted, &buk.Enable, &buk.Location)
		if err != nil {
			tx.Rollback()
			glog.V(0).Infoln(err.Error())
			return
		}
		bukList.Buks = append(bukList.Buks, buk)
	}
	tx.Commit()
	return
}
func (bm *MysqlBucketManager) ListAll() (bukList *BucketList, err error) {
	bm.accessLock.Lock()
	defer bm.accessLock.Unlock()
	if bm.isMoved {
		return nil, BucketMovedErr
	}
	tx, err := bm.db.Begin()
	if err != nil {
		glog.V(0).Infoln("Fail to begin transiction:", err.Error())
		return
	}
	bukList = &BucketList{}
	stmtSel, err := bm.db.Prepare("select name,account,created_time,put_time,object_count,byte_used,acl,logprefix,staticnotfound,refers,refersallowempty,lifecycle,collection,access_type,sys_define,is_deleted,enable,location from bucket")
	if err != nil {
		glog.V(0).Infoln("failing to Prepare ", err.Error())
		tx.Rollback()
		return nil, err
	}
	defer stmtSel.Close()
	rows, err := stmtSel.Query()
	if err != nil {
		glog.V(0).Infoln(err.Error())
		tx.Rollback()
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		buk := Bucket{}
		err = rows.Scan(&buk.Name, &buk.Account, &buk.CreateTime, &buk.PutTime, &buk.ObjectCount, &buk.ByteUsed, &buk.Acl, &buk.LogPrefix, &buk.StaticNotFound, &buk.Refers, &buk.RefersAllowEmpty, &buk.LifeCycle, &buk.Collection, &buk.AccessType, &buk.SysDefine, &buk.IsDeleted, &buk.Enable, &buk.Location)
		if err != nil {
			tx.Rollback()
			glog.V(0).Infoln(err.Error())
			return
		}
		bukList.Buks = append(bukList.Buks, buk)
	}
	tx.Commit()
	return
}
func (bm *MysqlBucketManager) ModifyAuth(account string, bucket string, accessType string, timeStamp string, isSync bool) (err error) {
	bm.accessLock.Lock()
	defer bm.accessLock.Unlock()
	if bm.isMoved {
		return BucketMovedErr
	}
	defer bm.MarkSync()
	if accessType == "" {
		return errors.New("NoAccessTypeSpecified!")
	}
	name := account + "/" + bucket
	//检查数据库是否已经有bucket存在
	stmtSel, err := bm.db.Prepare("Select access_type,put_time from bucket where name=?")
	if err != nil {
		return err
	}
	defer stmtSel.Close()
	oldType := ""
	putTime := ""
	err = stmtSel.QueryRow(name).Scan(&oldType, &putTime)
	if err == sql.ErrNoRows {
		return err
	}
	if oldType == accessType {
		return nil
	}
	putTimeNum := util.ParseInt64(putTime, 0)
	timeStampNum := util.ParseInt64(timeStamp, 0)
	if timeStampNum <= putTimeNum {
		return nil
	}
	stmtUpt, err := bm.db.Prepare("Update bucket set access_type=?,put_time=? where name=?")
	if err != nil {
		return err
	}
	defer stmtUpt.Close()
	tx, err := bm.db.Begin()
	if err != nil {
		return err
	}
	ret, err := stmtUpt.Exec(accessType, timeStamp, name)
	if err != nil {
		tx.Rollback()
		return err
	}
	rowsEffected, err := ret.RowsAffected()
	if err != nil {
		tx.Rollback()
		return err
	}
	//更新失败，无需回滚，直接退出
	if rowsEffected == 0 {
		return sql.ErrNoRows
	}
	//如果插入同步表失败，回滚整个数据库，包括上面的更新
	if isSync == false {
		glog.V(0).Infoln("Insert into sync table")
		stmtIns, err := bm.db.Prepare("Insert into sync (account,bucket,access_type,sync_flag,sync_method,put_time) values (?,?,?,?,?,?)")
		if err != nil {
			return err
		}
		defer stmtIns.Close()
		_, err = stmtIns.Exec(account, bucket, accessType, 0, "PUT", fmt.Sprint(time.Now().UnixNano()))
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	//提交数据库
	err = tx.Commit()
	return
}
func (bm *MysqlBucketManager) Update(account, bucket string, num int, size int64) (err error) {
	bm.accessLock.Lock()
	defer bm.accessLock.Unlock()
	if bm.isMoved {
		return BucketMovedErr
	}
	defer bm.MarkSync()
	name := account + "/" + bucket
	var putTime string
	var updateRslt sql.Result
	stmtSel, err := bm.db.Prepare("SELECT put_time from bucket WHERE name=?")
	if err != nil {
		return err
	}
	defer stmtSel.Close()
	stmtUpt, err := bm.db.Prepare("UPDATE bucket SET object_count=object_count+?,byte_used=byte_used+?,put_time=?,sync=?  WHERE name=? and put_time=?")
	if err != nil {
		return err
	}
	defer stmtUpt.Close()
	tx, err := bm.db.Begin()
	if err != nil {
		glog.V(0).Infoln("failed to begin transacation", err.Error())
		tx.Rollback()
		return err
	}
	txUpt := util.StmtAdd(tx, stmtUpt) //tx.Stmt(stmtUpt)
	defer txUpt.Close()
	for {
		if err = stmtSel.QueryRow(name).Scan(&putTime); err != nil {
			if err == sql.ErrNoRows {
				return BucketNotFoundErr
			} else {
				return err
			}
		}
		if updateRslt, err = txUpt.Exec(num, size, fmt.Sprint(time.Now().UnixNano()), public.SYNC_B2A_UPDATE, name, putTime); err != nil {
			tx.Rollback()
			glog.V(0).Infoln("Failed to update bucket", err.Error())
			return err
		}
		if rc, _ := updateRslt.RowsAffected(); rc <= 0 {
			time.Sleep(time.Second / 10)
			continue
		} else {
			break
		}
	}
	tx.Commit()
	return
}
func (bm *MysqlBucketManager) OutgoingAccSync(parNum int, accList map[string][]string) {
	if parNum <= 0 {
		return
	}
	if !bm.sync {
		return
	}
	bm.accessLock.Lock()
	defer bm.accessLock.Unlock()
	defer func() {
		bm.pushTime = fmt.Sprint(time.Now().UnixNano())
	}()
	safe := true
	bm.sync = false
	stmtSel, err := bm.db.Prepare("SELECT name,account,created_time,put_time,object_count,byte_used,is_deleted FROM bucket WHERE put_time>? LIMIT ?")
	if err != nil {
		return
	}
	defer stmtSel.Close()
	stmtSelTime, err := bm.db.Prepare("SELECT put_time from bucket WHERE name=?")
	if err != nil {
		glog.V(0).Infoln("mysql failed", err)
		return
	}
	defer stmtSelTime.Close()
	for syncRound := 0; syncRound < MAXSYNCROUND; syncRound++ {
		rows, err := stmtSel.Query(SyncMaxRecordOnce, bm.pushTime)
		if err != nil {
			if err == sql.ErrNoRows {
				glog.V(4).Infoln("No sync task ready!")
				return
			} else {
				glog.V(4).Infoln("Fail to select record from db:", err.Error())
				return
			}
		}
		defer rows.Close()
		for rows.Next() {
			buk := &Bucket{}
			err = rows.Scan(&buk.Name, &buk.Account, &buk.CreateTime, &buk.PutTime, &buk.ObjectCount, &buk.ByteUsed, &buk.IsDeleted)
			if err != nil {
				glog.V(4).Infoln("Failed to get data from db:", err.Error())
				return
			}
			//sync
			pos := util.HashPos(buk.Account, parNum)
			err = bm.syncOutBuk(accList[strconv.Itoa(pos)], buk)
			if err == nil && !safe {
				tx, err := bm.db.Begin()
				if err != nil {
					glog.V(4).Infoln("Fail to begin transiction:", err.Error())
					return
				}
				stmtDel, err := bm.db.Prepare("UPDATE bucket SET sync=0 WHERE name=? and put_time=?")
				if err != nil {
					glog.V(4).Infoln("Fail to prepare db:", err.Error())
					return
				}
				defer stmtDel.Close()
				txdel := util.StmtAdd(tx, stmtDel) //tx.Stmt(stmtDel)
				defer txdel.Close()
				var putTime string
				var reslt sql.Result
				for {
					if err = stmtSelTime.QueryRow(buk.Name).Scan(&putTime); err != nil {
						glog.V(0).Infoln("failed mysql", err)
						return
					}
					if reslt, err = txdel.Exec(buk.Name, putTime); err != nil {
						glog.V(0).Infoln("Failed to update db:", err.Error())
						tx.Rollback()
						return
					}
					if rc, _ := reslt.RowsAffected(); rc > 0 {
						break
					} else {
						glog.V(0).Infoln("mysql,retry update row...", buk.Name)
					}
				}
				if err = tx.Commit(); err != nil {
					glog.V(4).Infoln("Fail to commit db transiction:", err.Error())
					return
				}
			} else {
				continue
			}
		}
	}
	return
}
func (bm *MysqlBucketManager) syncOutBuk(dests []string, buk *Bucket) error {
	data, err := json.Marshal(*buk)
	if err != nil {
		return err
	}
	suc_num := 0
	for _, dest := range dests {
		if dest != "" {
			resp, err := util.MakeRequest_timeout(dest, "/bukSync", "PUT", data, nil)
			if err != nil {
				glog.V(4).Infoln("Request to:", dest, " failed!")
				continue
			} else if resp.StatusCode == http.StatusOK {
				suc_num = suc_num + 1
				break
			}
		}
	}
	if suc_num < 1 {
		return errors.New("NotSynced")
	}
	return nil
}
func (bm *MysqlBucketManager) Set(acc, buk, key, value string) (err error) {
	dbKey := req2dbMap[key]
	if dbKey == "" {
		return fmt.Errorf("DB key not specified!")
	}
	keyType := dbValueTypeMap[dbKey]
	switch keyType {
	case "string":
		return bm.setString(acc, buk, dbKey, value)
	case "int":
		return bm.setInt(acc, buk, dbKey, value)
	case "int64":
		return bm.setInt64(acc, buk, dbKey, value)
	default:
		return fmt.Errorf("Not supported key type!")
	}
}
func (bm *MysqlBucketManager) setInt(acc, buk, dbKey, value string) (err error) {
	bm.accessLock.Lock()
	defer bm.accessLock.Unlock()
	if bm.isMoved {
		return BucketMovedErr
	}
	defer bm.MarkSync()
	oldValue := 0
	isDeleted := 1
	name := acc + "/" + buk
	stmtSel, errSel := bm.db.Prepare("SELECT " + dbKey + ",is_deleted FROM bucket WHERE name=?")
	if errSel != nil {
		err = errSel
		return
	}
	defer stmtSel.Close()
	err = stmtSel.QueryRow(name).Scan(&oldValue, &isDeleted)
	if err != nil {
		if err == sql.ErrNoRows {
			return BucketNotFoundErr
		} else {
			return err
		}
	}
	if isDeleted == 1 {
		return BucketNotFoundErr
	}
	v, err := strconv.Atoi(value)
	if err != nil {
		return BucketBadReqErr
	}

	if dbKey == "max_file_size" {
		if v < 0 {
			v = public.DEFAULT_MAX_FILE_SIZE
		}
		if v < public.MIN_MAX_FILE_SIZE && v >= 0 {
			v = public.MIN_MAX_FILE_SIZE
		}
		if v > public.MAX_MAX_FILE_SIZE {
			v = public.MAX_MAX_FILE_SIZE
		}
	}
	if "max_up_tps" == dbKey || "max_down_tps" == dbKey {
		if v > public.MAX_TPS {
			v = public.MAX_TPS
		}
		if v < 0 {
			v = 0
		}
	}
	if "enable" == dbKey {
		if v != 1 && v != 0 {
			return fmt.Errorf("NotSet!")
		}
	}
	if "cdn_life_cycle" == dbKey {
		if v < 0 || v > SECSOFYEAR {
			return BucketBadReqErr
		}
	}
	sqtStr := "update bucket set " + dbKey + "=? where name=?"
	stmtUpt, err := bm.db.Prepare(sqtStr)
	if err != nil {
		return err
	}
	defer stmtUpt.Close()
	tx, err := bm.db.Begin()
	if err != nil {
		glog.V(0).Infoln("failed to begin transacation", err.Error())
		tx.Commit()
		return err
	}
	if _, err = stmtUpt.Exec(v, name); err != nil {
		if err == sql.ErrNoRows {
			tx.Commit()
			return
		}
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}
func (bm *MysqlBucketManager) setString(acc, buk, dbKey, value string) (err error) {
	bm.accessLock.Lock()
	defer bm.accessLock.Unlock()
	if bm.isMoved {
		return BucketMovedErr
	}
	defer bm.MarkSync()
	oldValue := ""
	isDeleted := 1
	name := acc + "/" + buk
	stmtSel, errSel := bm.db.Prepare("SELECT " + dbKey + ",is_deleted FROM bucket WHERE name=?")
	if errSel != nil {
		err = errSel
		return
	}
	defer stmtSel.Close()
	err = stmtSel.QueryRow(name).Scan(&oldValue, &isDeleted)
	if err != nil {
		if err == sql.ErrNoRows {
			return BucketNotFoundErr
		} else {
			return err
		}
	}
	if isDeleted == 1 {
		return BucketNotFoundErr
	}
	if dbKey == "access_type" {
		if value != public.BUCKET_PRIVATE && value != public.BUCKET_PUBLIC_RO && value != public.BUCKET_PUBLIC_WR {
			return fmt.Errorf("Not surpported dbKey value type!")
		}
	}
	sqtStr := "update bucket set " + dbKey + "=? where name=?"
	stmtUpt, err := bm.db.Prepare(sqtStr)
	if err != nil {
		return err
	}
	defer stmtUpt.Close()
	tx, err := bm.db.Begin()
	if err != nil {
		glog.V(0).Infoln("failed to begin transacation", err.Error())
		tx.Commit()
		return err
	}
	if _, err = stmtUpt.Exec(value, name); err != nil {
		if err == sql.ErrNoRows {
			tx.Commit()
			return
		}
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}
func (bm *MysqlBucketManager) setInt64(acc, buk, dbKey, value string) (err error) {
	bm.accessLock.Lock()
	defer bm.accessLock.Unlock()
	defer bm.MarkSync()
	return
}

const bucketFields = "name," +
	"account," +
	"created_time," +
	"put_time," +
	"object_count," +
	"byte_used," +
	"acl," +
	"logprefix," +
	"staticnotfound," +
	"refers," +
	"refersallowempty," +
	"lifecycle," +
	"collection," +
	"access_type," +
	"sys_define," +
	"enable," +
	"location," +
	"is_deleted," +
	"support_append," +
	"image_service," +
	"max_down_tps," +
	"max_up_tps," +
	"max_file_size," +
	"sync," +
	"cdn_life_cycle," +
	"reserver1," +
	"reserver2," +
	"reserver3," +
	"reserver4," +
	"reserver5," +
	"resource"

func (bm *MysqlBucketManager) BatchTrx(dst string) (err error) {
	bm.accessLock.Lock()
	defer bm.accessLock.Unlock()
	if err = bm.transBucket(dst); err == nil {
		bm.isMoved = true
	}
	return
}

func (bm *MysqlBucketManager) transBucket(dst string) (err error) {
	var (
		path        = "/sys/partitionRcv?id=" + bm.id
		bukList     = BucketList{}
		maxTxBufLen = 5000
	)
	rows, err := bm.db.Query("SELECT " + bucketFields + " from bucket")
	if err != nil {
		glog.V(0).Infoln("Moving bucket.bucket:Failed to get data from db:", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		buk := Bucket{}
		tempresource := ""
		err = rows.Scan(&buk.Name, &buk.Account, &buk.CreateTime, &buk.PutTime, &buk.ObjectCount, &buk.ByteUsed, &buk.Acl, &buk.LogPrefix, &buk.StaticNotFound, &buk.Refers, &buk.RefersAllowEmpty, &buk.LifeCycle, &buk.Collection, &buk.AccessType, &buk.SysDefine, &buk.Enable, &buk.Location, &buk.IsDeleted, &buk.SupportAppend, &buk.ImageService, &buk.MaxDonwTps, &buk.MaxUpTps, &buk.MaxFileSize, &buk.SyncType, &buk.MaxAgeDay, &buk.Reserver1, &buk.Reserver2, &buk.Reserver3, &buk.Reserver4, &buk.Reserver5, &tempresource)
		if err != nil {
			glog.V(0).Infoln("Moving bucket.bucket:Failed to scan db row:", err)
			return err
		}
		mr := &MigrationRuleSets{}
		if tempresource != "" {
			err = json.Unmarshal([]byte(tempresource), mr)
			if err != nil {
				glog.V(0).Infoln("bad Return Source of:", buk.Name, "error:", err)
				err = nil
			} else {
				buk.MRuleSets = mr
			}
		}
		bukList.Buks = append(bukList.Buks, buk)
		if len(bukList.Buks) >= maxTxBufLen {
			if err = bm.trans(dst, path, bukList); err != nil {
				return err
			}
			bukList.Buks = []Bucket{}
		}
	}
	if len(bukList.Buks) >= 0 {
		glog.V(0).Infoln("transBucket", len(bukList.Buks), "records")
		err = bm.trans(dst, path, bukList)
	}
	return
}
func (bm *MysqlBucketManager) trans(dst, path string, bukList BucketList) (err error) {
	if len(bukList.Buks) < 0 {
		return nil
	}
	data, err := json.Marshal(bukList)
	if err != nil {
		glog.V(0).Infoln("Moving bucket.bucket:Failed to json encode:", err)
		return err
	}
	rsp, err := util.MakeRequest_timeout(dst, path, "PUT", data, nil)
	if err != nil {
		glog.V(0).Infoln("Moving bucket.bucket:Failed to request data:", err)
		return err
	}
	defer rsp.Body.Close()
	if rsp.StatusCode != http.StatusOK {
		glog.V(0).Infoln("Moving bucket.bucket:Data not processed successfully")
		return errors.New("Moving bucket.bucket:Data not processed successfully " + rsp.Status)
	}
	return
}
