package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.google.com/p/weed-fs/go/filemap"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
	_ "github.com/go-sql-driver/mysql"
)

type MysqlPartition struct {
	Id           PartitionId
	dsn          string
	collection   string
	db           *sql.DB
	table        string
	rp           *ReplicaPlacement
	rebal        bool
	rebalstatus  int
	repairstatus int
	accessLock   sync.Mutex
	pathLock     *PathLock
}

const filemapColumnDeclaration = "(" +
	"filepath varchar(255) BINARY  NOT NULL," +
	"fid varchar(32) NOT NULL," +
	"expireat varchar(64) NOT NULL DEFAULT ''," +
	"length BIGINT(20) NOT NULL DEFAULT 0," +
	"last_modified varchar(64) NOT NULL DEFAULT ''," +
	"content_type varchar(255) NOT NULL DEFAULT ''," +
	"description varchar(1024) NOT NULL DEFAULT ''," +
	"disposition varchar(255) NOT NULL DEFAULT ''," +
	"tag varchar(64) NOT NULL DEFAULT ''," +
	"link varchar(1024) NOT NULL DEFAULT ''," +
	"crc varchar(64) NOT NULL DEFAULT ''," +
	"acl varchar(64) NOT NULL DEFAULT ''," +
	"reserve1 varchar(64) NOT NULL DEFAULT ''," +
	"reserve2 varchar(64) NOT NULL DEFAULT ''," +
	"reserve3 INT NOT NULL DEFAULT 0," +
	"reserve4 INT NOT NULL DEFAULT 0," +
	"primary key (filepath))"

const filetagColumnDeclaration = "(" +
	"`fid` varchar(32) NOT NULL," +
	"`tag` varchar(255) NOT NULL DEFAULT ''," +
	"`reserve1` varchar(64) NOT NULL DEFAULT ''," +
	"`reserve2` varchar(64) NOT NULL DEFAULT ''," +
	"`reserve3` INT NOT NULL DEFAULT 0," +
	"`reserve4` INT NOT NULL DEFAULT 0," +
	"PRIMARY KEY (`fid`))"

func NewMysqlPartition(dsn string, id PartitionId, collection string, replacement *ReplicaPlacement, createtable bool) (sfp *MysqlPartition, err error) {
	sfp = &MysqlPartition{dsn: dsn, Id: id, collection: collection, rp: replacement, rebal: false}
	sfp.pathLock = NewPathLock(1000)
	if nil == sfp.pathLock {
		err = errors.New("PathLock not init!")
		return
	}
	sfp.db, err = sql.Open("mysql", dsn)
	if err != nil {
		panic(err.Error())
	}
	sfp.table = "filemap"
	if createtable {
		_, err = sfp.db.Exec("create table if not exists " + sfp.table + filemapColumnDeclaration)
		if err != nil {
			glog.V(0).Infoln(err.Error())
		}
		_, err = sfp.db.Exec("create table if not exists   filetag  " + filetagColumnDeclaration)
		if err != nil {
			glog.V(0).Infoln(err.Error())
		}
	}
	return
}
func (sfp *MysqlPartition) createFile_nolock(filePath string, fid string, expireat string, length int64) (oldfid string, olen int64, err error) {
	glog.V(4).Infoln("create file", filePath, fid)
	stmtIns, err := sfp.db.Prepare("INSERT INTO " + sfp.table + "(filepath,fid,expireat,length) " + " VALUES(?,?,?,?)")
	if err != nil {
		glog.V(0).Infoln("failed to create file:", err)
		return "", 0, err
	}
	defer stmtIns.Close()
	stmtUpd, err := sfp.db.Prepare("update " + sfp.table + " set fid =?,expireat=?,length=? WHERE filepath=?")
	if err != nil {
		glog.V(0).Infoln("failed to create file:", err)
		return "", 0, err
	}
	defer stmtUpd.Close()
	tx, err := sfp.db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return "", 0, err
	}
	err = tx.QueryRow("SELECT fid,length from "+sfp.table+" WHERE filepath=?", filePath).Scan(&oldfid, &olen)
	glog.V(4).Infoln("create file", oldfid, err)
	if err != nil && err != sql.ErrNoRows {
		glog.V(0).Infoln("failed to create file:", err)
		tx.Rollback()
		return "", 0, err
	}
	var txs *sql.Stmt
	if oldfid == "" {
		txs = util.StmtAdd(tx, stmtIns) //tx.Stmt(stmtIns)
	} else {
		txs = util.StmtAdd(tx, stmtUpd) //tx.Stmt(stmtUpd)
	}
	defer txs.Close()
	if oldfid == "" {
		_, err = txs.Exec(filePath, fid, expireat, length)
	} else {
		_, err = txs.Exec(fid, expireat, length, filePath)
	}
	if err != nil {
		glog.V(0).Infoln("failed to create file:", err)
		tx.Rollback()
		return "", 0, err
	}
	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln("failed to create file:", err)
		return "", 0, err
	}

	return
}

//mtime是纳秒
func (sfp *MysqlPartition) CreateFile(filePath string, fid string, lifeCycle string, length int64, mtime,disposition,mimeType,etag string) (oldfid string, olen int64, oldMTime string, err error) {
	sfp.pathLock.Lock(filePath)
	defer sfp.pathLock.Unlock(filePath)
	if sfp.table != "filemap" && sfp.rebal == false {
		err = errors.New("Parition has been moved away!")
		return
	}
	_, oldfid, olen, oldMTime, err = sfp.findFromTable(filePath, sfp.table)
	if err != nil && err != sql.ErrNoRows {
		glog.V(0).Infoln("failed to write db:", err)
		return
	}
	if err == sql.ErrNoRows && sfp.rebal {
		_, oldfid, olen, oldMTime, err = sfp.findFromTable(filePath, "filemap")
		if err != nil && err != sql.ErrNoRows {
			glog.V(0).Infoln("failed to write db:", err)
			return
		}
	}
	if len(mimeType)>255{
		mimeType=""
	}
	expireAt := "0"
	if lifeCycle != "" {
		day := util.ParseInt(lifeCycle, -1)
		if day == -1 || day < 0 {
			return "", 0, "", fmt.Errorf("Bad life-cycle specified!")
		}
		if day > public.MAXlIFECYCLE {
			day = public.MAXlIFECYCLE
		}
		if day != 0 {
			//2017-11-1 zxw 变更为以last_modified为锚的
			lmtime := util.ParseInt64(mtime, time.Now().UnixNano()) / 1e9
			lmtime += int64(day * 24 * 60 * 60)
			expireAt = fmt.Sprint(lmtime)
			//expireAt = strconv.FormatInt(time.Now().Add(time.Duration(day)*24*time.Hour).Unix(), 10)
		}
	}
	//zqx 增加tag字段设置为空，即覆盖掉tagging信息
	stmtUptIns, err := sfp.db.Prepare("INSERT INTO " + sfp.table + "(filepath,fid,expireat,length,last_modified,content_type,disposition,crc,reserve2)values(?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE fid=?,expireat=?,length=?,last_modified=?,content_type=?,disposition=?,crc=?,tag=?,reserve2=?")
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		return "", 0, "", err
	}
	defer stmtUptIns.Close()

	stmtDelTag, err := sfp.db.Prepare("DELETE FROM filetag  where  fid=?")
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		return "", 0, "", err
	}
	defer stmtDelTag.Close()

	tx, err := sfp.db.Begin()
	if err != nil {
		tx.Rollback()
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return "", 0, "", err
	}
	err = tx.QueryRow("SELECT fid,length,last_modified from "+sfp.table+" WHERE filepath=? LIMIT 1", filePath).Scan(&oldfid, &olen, &oldMTime)
	if err != nil && err != sql.ErrNoRows {
		glog.V(0).Infoln("failed to write db:", err)
		tx.Rollback()
		return "", 0, "", err
	}
	txs := util.StmtAdd(tx, stmtUptIns) //tx.Stmt(stmtUptIns)
	defer txs.Close()

	_, err = txs.Exec(filePath, fid, expireAt, length, mtime,mimeType,disposition,etag ,mtime, fid, expireAt, length, mtime,mimeType,disposition,etag,"",mtime)
	if err != nil {
		glog.V(0).Infoln("failed to write db", err)
		tx.Rollback()
		return "", 0, "", err
	}
	if oldfid != "" {
		txsd := util.StmtAdd(tx, stmtDelTag) //tx.Stmt(stmtDelTag)
		defer txsd.Close()
		_, err = txsd.Exec(oldfid)
		if err != nil {
			glog.V(0).Infoln("failed to write db", err)
			tx.Rollback()
			return "", 0, "", err
		}
	}

	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln("failed to write db", err)
		return "", 0, "", err
	}

	return
}

func (sfp *MysqlPartition) CreateFile3(filePath string, fid string, lifeCycle string, length int64, mtime,disposition,mimeType,etag string) (oldfid string, olen int64, oldMTime string, err error) {
	sfp.pathLock.Lock(filePath)
	defer sfp.pathLock.Unlock(filePath)
	if sfp.table != "filemap" && sfp.rebal == false {
		err = errors.New("Parition has been moved away!")
		return
	}
	_, oldfid, olen, oldMTime, err = sfp.findFromTable(filePath, sfp.table)
	if err != nil && err != sql.ErrNoRows {
		glog.V(0).Infoln("failed to write db:", err)
		return
	}
	if err == sql.ErrNoRows && sfp.rebal {
		_, oldfid, olen, oldMTime, err = sfp.findFromTable(filePath, "filemap")
		if err != nil && err != sql.ErrNoRows {
			glog.V(0).Infoln("failed to write db:", err)
			return
		}
	}
	if len(mimeType)>255{
		mimeType=""
	}
	expireAt := lifeCycle
	stmtUptIns, err := sfp.db.Prepare("INSERT INTO " + sfp.table + "(filepath,fid,expireat,length,last_modified,content_type,disposition,crc)values(?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE fid=?,expireat=?,length=?,last_modified=?,content_type=?,disposition=?,crc=?,tag=?")
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		return "", 0, "", err
	}
	defer stmtUptIns.Close()

	stmtDelTag, err := sfp.db.Prepare("DELETE FROM filetag  where  fid=?")
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		return "", 0, "", err
	}
	defer stmtDelTag.Close()

	tx, err := sfp.db.Begin()
	if err != nil {
		tx.Rollback()
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return "", 0, "", err
	}
	err = tx.QueryRow("SELECT fid,length,last_modified from "+sfp.table+" WHERE filepath=? LIMIT 1", filePath).Scan(&oldfid, &olen, &oldMTime)
	if err != nil && err != sql.ErrNoRows {
		glog.V(0).Infoln("failed to write db:", err)
		tx.Rollback()
		return "", 0, "", err
	}
	txs := util.StmtAdd(tx, stmtUptIns) //tx.Stmt(stmtUptIns)
	defer txs.Close()

	_, err = txs.Exec(filePath, fid, expireAt, length, mtime,mimeType,disposition,etag, fid, expireAt, length, mtime,mimeType,disposition,etag, "")
	if err != nil {
		glog.V(0).Infoln("failed to write db", err)
		tx.Rollback()
		return "", 0, "", err
	}

	if oldfid != "" {
		txsd := util.StmtAdd(tx, stmtDelTag) //tx.Stmt(stmtDelTag)
		defer txsd.Close()
		_, err = txsd.Exec(oldfid)
		if err != nil {
			glog.V(0).Infoln("failed to write db", err)
			tx.Rollback()
			return "", 0, "", err
		}
	}
	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln("failed to write db", err)
		return "", 0, "", err
	}

	return
}

func (sfp *MysqlPartition) DeleteFile(filePath string) (oldfid string, length int64, oldMTime string, err error) {
	sfp.pathLock.Lock(filePath)
	defer sfp.pathLock.Unlock(filePath)
	if sfp.table != "filemap" && sfp.rebal == false {
		err = errors.New("Parition has been moved away!")
		return
	}
	stmtDel, err := sfp.db.Prepare("DELETE from " + sfp.table + " WHERE filepath=?")
	if err != nil {
		glog.V(0).Infoln("failed to delete from db:", err)
		return "", 0, "", err
	}
	defer stmtDel.Close()

	stmtDelTag, err := sfp.db.Prepare("DELETE FROM filetag  where  fid=?")
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		return "", 0, "", err
	}
	defer stmtDelTag.Close()

	tx, err := sfp.db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return "", 0, "", err
	}
	err = tx.QueryRow("SELECT fid,length,last_modified from "+sfp.table+" WHERE filepath=?", filePath).Scan(&oldfid, &length, &oldMTime)
	if err != nil && err != sql.ErrNoRows {
		glog.V(0).Infoln("failed to delete from db:", err)
		return "", 0, "", err
	}
	isLookupInFilemap := false
	if err == sql.ErrNoRows {
		if sfp.rebal {
			isLookupInFilemap = true
			_, oldfid, length, oldMTime, err = sfp.findFromTable(filePath, "filemap")
		}
		if err == sql.ErrNoRows || oldfid == "negative" {
			err = fmt.Errorf("NotFound")
		}
	}
	if err != nil {
		glog.V(0).Infoln("failed to delete from db:", err)
		tx.Rollback()
		return "", 0, "", err
	}
	var txs *sql.Stmt
	if oldfid != "" {
		if isLookupInFilemap {
			sfp.createFile_nolock(filePath, "negative", "", 0)
		} else {
			txs = util.StmtAdd(tx, stmtDel) //tx.Stmt(stmtDel)
			defer txs.Close()
			if _, err = txs.Exec(filePath); err != nil {
				glog.V(0).Infoln("Delete file:", err.Error())
				tx.Rollback()
				return "", 0, "", err
			}

			txsd := util.StmtAdd(tx, stmtDelTag) //tx.Stmt(stmtDelTag)
			defer txsd.Close()
			_, err = txsd.Exec(oldfid)
			if err != nil {
				glog.V(0).Infoln("failed to write db", err)
				tx.Rollback()
				return "", 0, "", err
			}

		}
	}
	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln("failed to delete from db:", err)
		return "", 0, "", err
	}
	return oldfid, length, oldMTime, nil
}

//给重命名用，判定多个最后修改时间
func (sfp *MysqlPartition) DeleteFile2(filePath, fid string) (err error) {
	sfp.pathLock.Lock(filePath)
	defer sfp.pathLock.Unlock(filePath)
	if sfp.table != "filemap" && sfp.rebal == false {
		err = errors.New("Parition has been moved away!")
		return
	}
	stmtDel, err := sfp.db.Prepare("DELETE from " + sfp.table + " WHERE filepath=? and fid=?")
	if err != nil {
		glog.V(0).Infoln("failed to delete from db:", err)
		return err
	}
	defer stmtDel.Close()
	tx, err := sfp.db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return err
	}

	var txs *sql.Stmt

	txs = util.StmtAdd(tx, stmtDel) //tx.Stmt(stmtDel)
	defer txs.Close()
	if _, err = txs.Exec(filePath, fid); err != nil {
		glog.V(0).Infoln("Delete file:", err.Error())
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln("failed to delete from db:", err)
		return err
	}
	return nil
}

//重命名使用，存在旧的直接返回，不覆盖
func (sfp *MysqlPartition) CreateFile2(filePath string, fid string, expireat string, length int64, mtime string) (err error) {
	sfp.pathLock.Lock(filePath)
	defer sfp.pathLock.Unlock(filePath)
	var oldfid string
	glog.V(4).Infoln("create file", filePath, fid)
	stmtIns, err := sfp.db.Prepare("INSERT INTO " + sfp.table + "(filepath,fid,expireat,length,last_modified) " + " VALUES(?,?,?,?,?)")
	if err != nil {
		glog.V(0).Infoln("failed to create file:", err)
		return err
	}
	defer stmtIns.Close()

	tx, err := sfp.db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return err
	}
	err = tx.QueryRow("SELECT fid from "+sfp.table+" WHERE filepath=?", filePath).Scan(&oldfid)
	glog.V(4).Infoln("create file", oldfid, err)
	if err != nil && err != sql.ErrNoRows {
		glog.V(0).Infoln("failed to create file:", err)
		tx.Rollback()
		return err
	}
	err = nil
	if oldfid != "" {
		err = errors.New("file existed")
		glog.V(0).Infoln("failed to create file:", err)
		tx.Rollback()
		return err
	}

	txs := util.StmtAdd(tx, stmtIns) //tx.Stmt(stmtIns)

	defer txs.Close()

	_, err = txs.Exec(filePath, fid, expireat, length, mtime)

	if err != nil {
		glog.V(0).Infoln("failed to create file:", err)
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln("failed to create file:", err)
		return err
	}

	return
}

func (sfp *MysqlPartition) FindFile(filePath string, freshAccessTime bool, updateCycle int) (expireat string, fid string, length int64, MTime,Mtype,Disposition,Etag string, err error) {
	stmt, err := sfp.db.Prepare("SELECT fid,expireat,length,last_modified,content_type,disposition,crc from " + sfp.table + " WHERE filepath=?")
	if err != nil {
		glog.V(0).Infoln("failed to read db:", err)
		return "", "", 0, "","","","", err
	}
	defer stmt.Close()
	err = stmt.QueryRow(filePath).Scan(&fid, &expireat, &length, &MTime,&Mtype,&Disposition,&Etag)
	if err != nil {
		if err == sql.ErrNoRows {
			if sfp.rebal {
				stmtRetry, err := sfp.db.Prepare("SELECT fid,expireat,length,last_modified,content_type,disposition,crc from " + "filemap" + " WHERE filepath=?")
				defer stmtRetry.Close()
				if err == nil {
					if err = stmtRetry.QueryRow(filePath).Scan(&fid, &expireat, &length, &MTime,&Mtype,&Disposition,&Etag); err == nil {
						expireDate := util.ParseInt64(expireat, -1)
						if expireDate > 0 && expireDate < time.Now().Unix() {
							return "", "", 0, "","","","", filemap.NotFound
						}
						return expireat, fid, length, MTime, Mtype,Disposition,Etag,nil
					}
				}
			}
			err = filemap.NotFound
		}
		glog.V(0).Infoln("failed to read db:", err, " filePath", filePath)
		return "", "", 0, "","","","", err
	}
	if fid == "negative" {
		return "", "", 0, "","","","", filemap.NotFound
	}
	expireDate := util.ParseInt64(expireat, -1)
	if expireDate > 0 && expireDate < time.Now().Unix() {
		return "", "", 0, "","","","", filemap.NotFound
	}
	if freshAccessTime {
		_, err = sfp.UpdateAccessTime(filePath, updateCycle)
		if err != nil{
			glog.V(0).Infoln("fresh access time err:", err)
		}else {
			glog.V(4).Infoln("fresh access time success",filePath)
		}
	}
	return expireat, fid, length, MTime, Mtype,Disposition,Etag,nil
}

func (sfp *MysqlPartition) GetFileTag(fid string) (tag string, err error) {
	err = sfp.db.QueryRow("SELECT tag from  filetag WHERE fid=?", fid).Scan(&tag)
	return
}

func (sfp *MysqlPartition) SetFileExpire(filepath string, lifeCycle int, mtime string) (oldMTime string, err error) {
	expireAt := "0"
	if lifeCycle > 0 {
		//mtime是纳秒
		lmtime := util.ParseInt64(mtime, time.Now().UnixNano()) / 1e9
		lmtime += int64(lifeCycle * 24 * 60 * 60)
		expireAt = fmt.Sprint(lmtime)
		//expireAt = strconv.FormatInt(time.Now().Add(time.Duration(lifeCycle)*24*time.Hour).Unix(), 10)
	}

	sfp.pathLock.Lock(filepath)
	defer sfp.pathLock.Unlock(filepath)
	oldfid := ""
	smtUpt, errUpt := sfp.db.Prepare("UPDATE " + sfp.table + " set expireat=? , last_modified=?  WHERE filepath=?")
	if errUpt != nil {
		return "", errUpt
	}
	defer smtUpt.Close()
	tx, errDb := sfp.db.Begin()
	if errDb != nil {
		tx.Rollback()
		return "", errDb
	}
	err = tx.QueryRow("SELECT fid,last_modified from "+sfp.table+" WHERE filepath=?", filepath).Scan(&oldfid, &oldMTime)
	if err != nil && err != sql.ErrNoRows {
		glog.V(0).Infoln("failed to set db", err)
		return "", err
	}
	isLookupInFilemap := false
	if err == sql.ErrNoRows {
		if sfp.rebal {
			isLookupInFilemap = true
			_, oldfid, _, oldMTime, err = sfp.findFromTable(filepath, "filemap")
		}
		if err == sql.ErrNoRows || oldfid == "negative" {
			//err = fmt.Errorf("NotFound")
			err = filemap.NotFound
		}
	}
	if err != nil {
		glog.V(0).Infoln("failed to set db:", err)
		tx.Rollback()
		return "", err
	}
	var txs *sql.Stmt
	if oldfid != "" {
		if isLookupInFilemap {
			sfp.createFile_nolock(filepath, "negative", "", 0)
		} else {
			txs = util.StmtAdd(tx, smtUpt) // tx.Stmt(smtUpt)
			defer txs.Close()
			if _, err = txs.Exec(expireAt, mtime, filepath); err != nil {
				tx.Rollback()
				if err == sql.ErrNoRows {
					return "", filemap.NotFound
				} else {
					glog.V(0).Infoln("failed to set db:", err)
					return "", err
				}
			}
		}
	}
	err = tx.Commit()
	return oldMTime, err
}

func (sfp *MysqlPartition) SetFileTagging(filePath, tagging string) (oldtagging string, err error) {
	//oldtagging表示原来的tagging的fid
	sfp.pathLock.Lock(filePath)
	defer sfp.pathLock.Unlock(filePath)
	if sfp.table != "filemap" && sfp.rebal == false {
		err = errors.New("Parition has been moved away!")
		return
	}

	stmtUptIns, err := sfp.db.Prepare("UPDATE " + sfp.table + " set tag=?  WHERE filepath=?")
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		return "", err
	}
	defer stmtUptIns.Close()
	tx, err := sfp.db.Begin()
	if err != nil {
		tx.Rollback()
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return "", err
	}
	err = tx.QueryRow("SELECT tag  from "+sfp.table+" WHERE filepath=? LIMIT 1", filePath).Scan(&oldtagging)
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		tx.Rollback()
		return "", err
	}

	txs := util.StmtAdd(tx, stmtUptIns) //tx.Stmt(stmtUptIns)
	defer txs.Close()

	_, err = txs.Exec(tagging, filePath)
	if err != nil {
		glog.V(0).Infoln("failed to write db", err)
		tx.Rollback()
		return "", err
	}
	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln("failed to write db", err)
		return "", err
	}
	return
}

func (sfp *MysqlPartition) GetFileTagging(filepath string) (tagging string, err error) {
	err = sfp.db.QueryRow("SELECT tag from "+sfp.table+" WHERE filepath=?", filepath).Scan(&tagging)
	return
	return
}

func (sfp *MysqlPartition) SetFileTagByPath(filePath, tag string) (err error) {

	sfp.pathLock.Lock(filePath)
	defer sfp.pathLock.Unlock(filePath)
	if sfp.table != "filemap" && sfp.rebal == false {
		err = errors.New("Parition has been moved away!")
		return
	}
	fid := ""

	stmtUptIns, err := sfp.db.Prepare("INSERT INTO filetag( fid, tag)values(?,?) ON DUPLICATE KEY UPDATE tag=?")
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		return err
	}
	defer stmtUptIns.Close()
	tx, err := sfp.db.Begin()
	if err != nil {
		tx.Rollback()
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return err
	}
	err = tx.QueryRow("SELECT fid  from "+sfp.table+" WHERE filepath=? LIMIT 1", filePath).Scan(&fid)
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		tx.Rollback()
		return err
	}

	txs := util.StmtAdd(tx, stmtUptIns) //tx.Stmt(stmtUptIns)
	defer txs.Close()

	_, err = txs.Exec(fid, tag, tag)
	if err != nil {
		glog.V(0).Infoln("failed to write db", err)
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln("failed to write db", err)
		return err
	}
	return
}

func (sfp *MysqlPartition) SetFileTagByFid(fid, tag string) (err error) {

	stmtUptIns, err := sfp.db.Prepare("INSERT INTO filetag( fid, tag)values(?,?) ON DUPLICATE KEY UPDATE tag=?")
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		return err
	}
	defer stmtUptIns.Close()
	tx, err := sfp.db.Begin()
	if err != nil {
		tx.Rollback()
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return err
	}

	txs := util.StmtAdd(tx, stmtUptIns) //tx.Stmt(stmtUptIns)
	defer txs.Close()

	_, err = txs.Exec(fid, tag, tag)
	if err != nil {
		glog.V(0).Infoln("failed to write db", err)
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln("failed to write db", err)
		return err
	}
	return
}

func (sfp *MysqlPartition) NeedToReplicate() bool {
	return sfp.rp.GetCopyCount() > 1
}

func (sfp *MysqlPartition) Close() {
	sfp.db.Close()
}

func (sfp *MysqlPartition) Collection() (collection string) {
	return sfp.collection
}

func (sfp *MysqlPartition) Rp() (rp *ReplicaPlacement) {
	return sfp.rp
}

func printErr(err *error) {
	if *err != nil && *err != filemap.NotFound {
		glog.V(0).Infoln("MYSQL ERROR:", *err)
	}
}

func getDatabase(dsn string) string {
	rs := []rune(dsn)
	lth := len(rs)
	lastN := strings.LastIndex(dsn, "/")
	return string(rs[lastN+1 : lth])
}

func (sfp *MysqlPartition) Destroy() (err error) {
	database := getDatabase(sfp.dsn)
	_, err = sfp.db.Exec("drop database " + database)
	if err != nil {
		glog.V(0).Infoln("Drop database ", database, " error:", err)
	} else {
		glog.V(0).Infoln("Drop database ", database, " success")
	}
	return
}
func (sfp *MysqlPartition) DeleteExpired(startlimit string, limitcnt, prefix string) (files *filemap.ObjectList, err error) {
	startLmtInt := util.ParseInt(startlimit, -1)
	limitInt := util.ParseInt(limitcnt, -1)
	sqlStmt := "select filepath,fid,length,expireat,last_modified from filemap"
	if prefix != "" {
		sqlStmt += " where filepath like '" + prefix + "%'"
	}
	sqlStmt += " limit ?,? "
	stmtSel, err := sfp.db.Prepare(sqlStmt)
	if err != nil {
		glog.V(0).Infoln("failed to scan recycle:", err)
		return nil, err
	}
	defer stmtSel.Close()
	rows, err := stmtSel.Query(startLmtInt, limitInt)
	if err != nil {
		glog.V(0).Infoln("failed to scan recycle:", err)
		return nil, err
	}
	defer rows.Close()
	files = &filemap.ObjectList{}
	for rows.Next() {
		file := filemap.Object{}
		if err = rows.Scan(&file.Key, &file.Fid, &file.Size, &file.Expire, &file.LastModified); err != nil {
			glog.V(0).Infoln("failed to scan recycle:", err)
			if len(files.List) > 0 {
				return files, nil
			}
			return nil, err
		}
		glog.V(2).Infoln("delete file from mysql", file.Key, file.Fid, file.Size, file.Expire)
		files.List = append(files.List, file)
	}
	return
}

//原DeleteExpired
func (sfp *MysqlPartition) DeleteExpired2(delay string, limitcnt, prefix string) (files *filemap.ObjectList, err error) {
	delayDay := util.ParseInt(delay, -1)
	if delayDay < 3 {
		delayDay = 3
	}
	limitInt := util.ParseInt(limitcnt, -1)
	deadLine := time.Now().Add(time.Duration(-delayDay) * 24 * time.Hour).Unix()
	expiredBefore := strconv.FormatInt(deadLine, 10)
	sqlStmt := "select filepath,fid,length,expireat from filemap where expireat < ? and expireat !='0' "
	if prefix != "" {
		sqlStmt += " and filepath like '" + prefix + "%'"
	}
	sqlStmt += " limit ? "
	stmtSel, err := sfp.db.Prepare(sqlStmt)
	if err != nil {
		glog.V(0).Infoln("failed to delete expired:", err)
		return nil, err
	}
	defer stmtSel.Close()
	rows, err := stmtSel.Query(expiredBefore, limitInt)
	if err != nil {
		glog.V(0).Infoln("failed to delete expired:", err)
		return nil, err
	}
	defer rows.Close()
	files = &filemap.ObjectList{}
	for rows.Next() {
		file := filemap.Object{}
		if err = rows.Scan(&file.Key, &file.Fid, &file.Size, &file.Expire); err != nil {
			glog.V(0).Infoln("failed to delete expired:", err)

			if len(files.List) > 0 {
				return files, nil
			}
			return nil, err
		}
		if _, _, _, err = sfp.DeleteFile(file.Key); err != nil {
			glog.V(0).Infoln("failed to delete expired:", err)
			if len(files.List) > 0 {
				return files, nil
			}
			return nil, err
		}
		//glog.V(2).Infoln("delete file from mysql", file.Key,file.Fid,file.Size,file.Expire)
		files.List = append(files.List, file)
	}
	return
}
func (sfp *MysqlPartition) List(account string, bucket string, marker string, prefix string, max int) (objList *filemap.ObjectList, err error) {
	marker = strings.Replace(marker, "%20", " ", -1)
	marker = "/" + account + "/" + bucket + "/" + marker
	var rows *sql.Rows = nil
	var stmtSel *sql.Stmt = nil
	if prefix == "*" {
		fix := "/" + account + "/" + bucket + "/%"
		stmtSel, err = sfp.db.Prepare("select filepath,fid,length,last_modified,acl,expireat,reserve2 from filemap where  filepath like ? and filepath > ? limit ?")
		if err != nil {
			glog.V(0).Infoln("failed to list:", err)
			return nil, err
		}
		defer stmtSel.Close()
		rows, err = stmtSel.Query(fix, marker, strconv.Itoa(max))
		if err != nil {
			glog.V(0).Infoln("failed to list:", err)
			return nil, err
		}
	} else {
		fix := "/" + account + "/" + bucket + "/" + prefix + "%"
		date := strconv.FormatInt(time.Now().Unix(), 10)
		stmtSel, err = sfp.db.Prepare("select filepath,fid,length,last_modified,acl,expireat,reserve2 from filemap where (expireat> ? or expireat='0') and filepath like ? and filepath > ? limit ?")
		if err != nil {
			return nil, err
		}
		defer stmtSel.Close()
		rows, err = stmtSel.Query(date, fix, marker, strconv.Itoa(max))
		if err != nil {
			glog.V(0).Infoln("failed to list:", err)
			return nil, err
		}
	}
	defer rows.Close()
	objList = &filemap.ObjectList{}
	for rows.Next() {
		obj := filemap.Object{}
		err = rows.Scan(&obj.Key, &obj.Fid, &obj.Size, &obj.LastModified, &obj.ObjectAcl, &obj.Expire, &obj.AccessTime)
		if err != nil {
			glog.V(0).Infoln("failed to list:", err)
			return
		}
		parts := strings.SplitN(obj.Key, "/", 4)
		if len(parts) > 3 {
			obj.Key = parts[3]
		}
		if err != nil {
			glog.V(0).Infoln("failed to list:", err)
			return
		}
		objList.List = append(objList.List, obj)
	}
	return
}

func (sfp *MysqlPartition) GetFileCount(account string, bucket string) (countSum int64, sizeSum int64, err error) {
	var num int = 100000
	var startNum = 0
	var now = time.Now()
	var timeinterval = 10
	defer func() {
		glog.V(0).Infoln("Get file count:", countSum, "sizeSum:", sizeSum, "pid:", sfp.Id, "account:", account, "bucket:", bucket, "timecost:", time.Since(now), "err:", err)
	}()

	for {
		var rows *sql.Rows = nil
		var stmtSel *sql.Stmt = nil
		fix := "/" + account + "/" + bucket + "/%"
		date := strconv.FormatInt(time.Now().Unix(), 10)
		stmtSel, err = sfp.db.Prepare("select filepath,length,expireat from filemap where (expireat> ? or expireat='0') and filepath like ? limit ?,?")
		if err != nil {
			glog.V(0).Infoln("failed to Query:", err, fix)
			return 0, 0, err
		}
		defer stmtSel.Close()
		rows, err = stmtSel.Query(date,fix, strconv.Itoa(startNum), strconv.Itoa(num))
		if err != nil {
			glog.V(0).Infoln("failed to Query:", err, fix)
			return 0, 0, err
		}
		glog.V(1).Infoln("Query:", fix, "from", startNum, "to", num, "pid:", sfp.Id)
		startNum += num
		defer rows.Close()
		scanNum := 0
		for rows.Next() {
			var key string
			var size int64
			var expire string
			err = rows.Scan(&key, &size, &expire)
			if err != nil {
				glog.V(0).Infoln("failed to Query:", err, fix)
				return
			}
			scanNum++
			countSum += 1
			sizeSum += size
		}
		if timeinterval != 0 {
			time.Sleep(time.Millisecond * time.Duration(timeinterval))
		}
		if scanNum == 0 {
			break
		}
	}
	return
}

func (sfp *MysqlPartition) ListFilesFromPar(lastFileName string, dir string, limit int) (objList *filemap.ObjectListHissync, err error) {
	lastFileName = strings.Replace(lastFileName, "%20", " ", -1)
	//marker = "/" + account + "/" + bucket + "/" + marker
	var rows *sql.Rows = nil
	var stmtSel *sql.Stmt = nil
	fix := dir + "%"
	date := strconv.FormatInt(time.Now().Unix(), 10)
	stmtSel, err = sfp.db.Prepare("select filepath,fid,length,last_modified,acl,expireat from filemap where (expireat> ? or expireat='0') and filepath like ? and filepath > ? limit ?")
	if err != nil {
		return nil, err
	}
	defer stmtSel.Close()
	rows, err = stmtSel.Query(date, fix, lastFileName, limit)
	if err != nil {
		glog.V(0).Infoln("failed to list:", err)
		return nil, err
	}
	defer rows.Close()
	objList = &filemap.ObjectListHissync{}
	for rows.Next() {
		obj := filemap.ObjectHissync{}
		err = rows.Scan(&obj.Path, &obj.Fid, &obj.Size, &obj.LastModified, &obj.ObjectAcl, &obj.Expire)
		if err != nil {
			glog.V(0).Infoln("failed to list:", err)
			return
		}
		parts := strings.SplitN(obj.Path, "/", 4)
		if len(parts) < 4 {
			continue
		}
		objList.List = append(objList.List, obj)
	}
	return
}

func (sfp *MysqlPartition) findFromTable(filePath, table string) (expireat string, fid string, length int64, mTime string, err error) {
	err = sfp.db.QueryRow("SELECT expireat,fid,length,last_modified from "+table+" WHERE filepath=?", filePath).Scan(&expireat, &fid, &length, &mTime)
	return
}

func (sfp *MysqlPartition) UpdateFid(filepath string, oldFid ,newFid string) (err error) {
	if oldFid == "" ||  newFid == ""{
		return errors.New("invalid fid")
	}
	sfp.pathLock.Lock(filepath)
	defer sfp.pathLock.Unlock(filepath)
	oldfid := ""
	smtUpt, errUpt := sfp.db.Prepare("UPDATE " + sfp.table + " set fid=? WHERE filepath=?")
	if errUpt != nil {
		return errUpt
	}
	defer smtUpt.Close()
	tx, errDb := sfp.db.Begin()
	if errDb != nil {
		tx.Rollback()
		return errDb
	}
	err = tx.QueryRow("SELECT fid from "+sfp.table+" WHERE filepath=?", filepath).Scan(&oldfid)
	if err != nil && err != sql.ErrNoRows {
		glog.V(0).Infoln("failed to set db", err)
		return err
	}
	isLookupInFilemap := false
	if err == sql.ErrNoRows {
		if sfp.rebal {
			isLookupInFilemap = true
			_, oldfid, _, _, err = sfp.findFromTable(filepath, "filemap")
		}
		if err == sql.ErrNoRows || oldfid == "negative" {
			//err = fmt.Errorf("NotFound")
			err = filemap.NotFound
		}
	}
	if err != nil {
		glog.V(0).Infoln("failed to set db:", err)
		tx.Rollback()
		return err
	}
	if oldfid != oldFid {
		err = errors.New("fid changed")
		glog.V(0).Infoln("fid not match", oldfid, oldFid)
		return err
	}
	var txs *sql.Stmt
	if oldfid != "" {
		if isLookupInFilemap {
			sfp.createFile_nolock(filepath, "negative", "", 0)
		} else {
			txs = util.StmtAdd(tx, smtUpt) // tx.Stmt(smtUpt)
			defer txs.Close()
			if _, err = txs.Exec(newFid, filepath); err != nil {
				tx.Rollback()
				if err == sql.ErrNoRows {
					return filemap.NotFound
				} else {
					glog.V(0).Infoln("failed to set db:", err)
					return err
				}
			}
		}
	}
	err = tx.Commit()
	return err
}
func (sfp *MysqlPartition) UpdateAccessTime(filePath string, updateCycle int) (oldAccessTime string, err error) {
	sfp.pathLock.Lock(filePath)
	defer sfp.pathLock.Unlock(filePath)

	stmtUptIns, err := sfp.db.Prepare("UPDATE " + sfp.table + " set reserve2=?  WHERE filepath=?")
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		return "", err
	}
	defer stmtUptIns.Close()
	tx, err := sfp.db.Begin()
	if err != nil {
		tx.Rollback()
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return "", err
	}
	err = tx.QueryRow("SELECT reserve2  from "+sfp.table+" WHERE filepath=? LIMIT 1", filePath).Scan(&oldAccessTime)
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		tx.Rollback()
		return "", err
	}

	//根据oldAccessTime判断是否需要更新
	if oldAccessTime != "" {
		oldAT := util.ParseUnixNanoToTime(oldAccessTime)
		if oldAT.IsZero() {
			glog.V(0).Infoln("bad old access time:", oldAccessTime)
			tx.Rollback()
			return "", nil
		}
		halfSur := time.Duration(updateCycle) * time.Hour * 24 //updateCycle为天数，如果当前时间与上次访问时间相隔少于updateCycle的天数，则不更新
		if time.Now().Sub(oldAT) < halfSur {
			glog.V(4).Infoln("no need to fresh access time")
			tx.Rollback()
			return "", nil
		}
	}
	txs := util.StmtAdd(tx, stmtUptIns) //tx.Stmt(stmtUptIns)
	defer txs.Close()

	_, err = txs.Exec(strconv.FormatInt(time.Now().UnixNano(), 10), filePath)
	if err != nil {
		glog.V(0).Infoln("failed to write db", err)
		tx.Rollback()
		return "", err
	}
	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln("failed to write db", err)
		return "", err
	}
	return
}
