package account

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"code.google.com/p/weed-fs/go/bucket"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
	"strings"
)

var AccountExistedErr = errors.New("AccountAlreadyExisted")
var AccountNotFoundErr = errors.New("AccountNotFound")
var AccountDBMovedErr = errors.New("AccountDBMovedErr")

var DomainExistedErr = errors.New("DomainAlreadyExisted")
var DomainNotFoundErr = errors.New("DomainNotFoundErr")
var DomainNotSupportErr = errors.New("DomainNotSupportErr")

var SyncMaxRecordOnce = 100
var MAXSYNCROUND = 1

type MysqlAccountManager struct {
	dsn        string
	table      string
	synctable  string
	id         string
	db         *sql.DB
	accessLock sync.Mutex
	isLoading  bool
	isMoved    bool
}

const accountTableCreateScript = "create table if not exists account (" +
	"name varchar(64) BINARY Not NULL," +
	"created_time varchar(64) NOT NULL," +
	"auth_type  INT DEFAULT 0," +
	"bucket_count BIGINT(20) DEFAULT 0," +
	"object_count BIGINT(20) DEFAULT 0," +
	"byte_used BIGINT(20) Not NULL DEFAULT 0," +
	"deleted  BOOL DEFAULT FALSE," +
	"access_user varchar(128) DEFAULT ''," +
	"status varchar(8) DEFAULT 'enable'," +
	"max_bucket INT DEFAULT 10," +
	"descrip varchar(128) NOT NULL DEFAULT ''," +
	"access_user_ex varchar(500) NOT NULL DEFAULT ''," +
	"reserver1 varchar(64) NOT NULL DEFAULT ''," +
	"reserver2 varchar(64) NOT NULL DEFAULT ''," +
	"reserver3 varchar(64) NOT NULL DEFAULT ''," +
	"reserver4 INT(64) NOT NULL DEFAULT 0," +
	"reserver5 INT(64) NOT NULL DEFAULT 0," +
	"PRIMARY KEY (name))"

const bucketTableCreateScript = "create table if not exists bucket (" +
	"name varchar(64) BINARY Not NULL," +
	"account varchar(64) BINARY Not NULL," +
	"created_time varchar(64) NOT NULL," +
	"put_time varchar(64) NOT NULL," +
	"object_count INT(64) Not NULL DEFAULT 0," +
	"byte_used BIGINT(20) Not NULL DEFAULT 0," +
	"is_deleted tinyint(1) Not NULL DEFAULT 0," +
	"PRIMARY KEY (name));"
const accountFields = "name," +
	"created_time," +
	"auth_type," +
	"bucket_count," +
	"object_count," +
	"byte_used," +
	"deleted," +
	"access_user," +
	"status," +
	"max_bucket," +
	"descrip," +
	"access_user_ex," +
	"reserver1," +
	"reserver2," +
	"reserver3," +
	"reserver4," +
	"reserver5"
const bucketFields = "name,account,created_time,put_time,object_count,byte_used,is_deleted"

func NewMysqlAccountManager(dsn string, partitionID string, createTable bool) (am *MysqlAccountManager, err error) {
	am = &MysqlAccountManager{dsn: dsn, id: partitionID}

	am.db, err = sql.Open("mysql", am.dsn)
	if err != nil {
		panic(err.Error())
	}
	am.table = "account"
	am.synctable = "sync"
	if createTable {
		_, err = am.db.Exec(accountTableCreateScript)
		if err != nil {
			glog.V(0).Infoln(err.Error())
		}
		_, err = am.db.Exec(bucketTableCreateScript)
		if err != nil {
			glog.V(0).Infoln(err.Error())
		}
	}
	return am, nil
}

func (am *MysqlAccountManager) Create(name string, password string,created_time string) (string, error) {
	am.accessLock.Lock()
	defer am.accessLock.Unlock()
	if am.isMoved {
		return "", AccountDBMovedErr
	}
	stmtIns, err := am.db.Prepare("INSERT INTO account (name,access_user,created_time) VALUES(?,?,?)")
	//created_time := time.Now().UnixNano()
	if err != nil {
		return "", err
	}
	defer stmtIns.Close()

	tx, err := am.db.Begin()
	if err != nil {
		return "", err
	}
	var accountName string
	var isDeleted bool
	err = tx.QueryRow("SELECT name,deleted from account WHERE name=? ", name).Scan(&accountName, &isDeleted)
	if err != nil && err != sql.ErrNoRows {
		tx.Rollback()
		return "", err
	}
	if accountName != "" {
		if !isDeleted {
			tx.Rollback()
			return "", AccountExistedErr
		} else {
			stmtUpt, err := am.db.Prepare("update account set deleted=FALSE ,access_user=?  where name=?")
			if err != nil {
				tx.Rollback()
				glog.V(0).Infoln(err.Error())
				return "", err
			}
			defer stmtUpt.Close()
			txs := util.StmtAdd(tx, stmtUpt) //tx.Stmt(stmtIns)
			defer txs.Close()
			_, err = txs.Exec(password, name)
			if err != nil {
				tx.Rollback()
				return "", err
			}
			err = tx.Commit()
			if err != nil {
				return "", err
			}
			return "", nil
		}

	}

	txs := util.StmtAdd(tx, stmtIns) //tx.Stmt(stmtIns)

	defer txs.Close()

	_, err = txs.Exec(name, password, fmt.Sprint(created_time))

	if err != nil {
		tx.Rollback()
		return "", err
	}
	err = tx.Commit()
	if err != nil {
		return "", err
	}

	return "", nil

}
func (am *MysqlAccountManager) Delete(name string) (string, error) {
	am.accessLock.Lock()
	defer am.accessLock.Unlock()
	if am.isMoved {
		return "", AccountDBMovedErr
	}
	glog.V(4).Infoln("delete account", name)

	stmtDel, err := am.db.Prepare("DELETE from account WHERE name=?")
	if err != nil {
		return "", err
	}
	defer stmtDel.Close()
	tx, err := am.db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return "", err
	}

	txs := util.StmtAdd(tx, stmtDel) // tx.Stmt(stmtDel)
	defer txs.Close()
	_, err = txs.Exec(name)
	if err != nil {
		glog.V(0).Infoln(err.Error())
		tx.Rollback()
		return "", err
	}
	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return "", err
	}
	return "", nil
}
func (am *MysqlAccountManager) MarkDelete(name string) error {
	am.accessLock.Lock()
	defer am.accessLock.Unlock()
	if am.isMoved {
		return AccountDBMovedErr
	}
	glog.V(4).Infoln("delete account", name)
	stmtSel, err := am.db.Prepare("SELECT name from account WHERE name=?")
	if err != nil {
		return err
	}
	defer stmtSel.Close()
	oldName := ""
	err = stmtSel.QueryRow(name).Scan(&oldName)
	if err != nil {
		if err == sql.ErrNoRows {
			return AccountNotFoundErr
		}
		return err
	}
	stmtUpt, err := am.db.Prepare("update account set deleted=TRUE WHERE name=?")
	if err != nil {

		return err
	}
	defer stmtUpt.Close()
	tx, err := am.db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return err
	}
	txs := util.StmtAdd(tx, stmtUpt) // tx.Stmt(stmtUpt)
	defer txs.Close()
	_, err = txs.Exec(name)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		glog.V(0).Infoln(err.Error())
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	return nil
}
func (am *MysqlAccountManager) List(marker, prefix string, maxNum int) ([]*Account, error) {
	am.accessLock.Lock()
	defer am.accessLock.Unlock()
	if am.isMoved {
		return nil, AccountDBMovedErr
	}
	var accList []*Account
	stmt, err := am.db.Prepare("SELECT name,created_time,auth_type,bucket_count,object_count,byte_used,deleted,status,access_user,max_bucket from account where name like CONCAT(?, '%') and name>? and deleted=FALSE limit ?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(prefix, marker, maxNum)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, AccountNotFoundErr
		} else {
			return nil, err
		}
	}
	defer rows.Close()
	for rows.Next() {
		account := &Account{}
		err = rows.Scan(&account.Name, &account.createTime, &account.authType, &account.bucketUsed, &account.objectCount, &account.byteUsed, &account.isDeleted, &account.enable, &account.accUser, &account.maxBucket)
		if err != nil {
			return nil, err
		}
		accList = append(accList, account)
	}
	return accList, nil
}
func (am *MysqlAccountManager) Get(name string) ([]*Account, error) {
	if am.isMoved {
		return nil, AccountDBMovedErr
	}
	var accList []*Account

	{
		stmt, err := am.db.Prepare("SELECT name,created_time,auth_type,bucket_count,object_count,byte_used,deleted,status,access_user,max_bucket from account WHERE name=? and deleted=0")
		if err != nil {
			return nil, err
		}
		defer stmt.Close()
		account := &Account{}
		err = stmt.QueryRow(name).Scan(&account.Name, &account.createTime, &account.authType, &account.bucketUsed, &account.objectCount, &account.byteUsed, &account.isDeleted, &account.enable, &account.accUser, &account.maxBucket)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, AccountNotFoundErr
			}
			return nil, err
		}
		accList = append(accList, account)
	}
	return accList, nil
}
func (am *MysqlAccountManager) IncomingBukSync(buk *bucket.Bucket) (err error) {
	am.accessLock.Lock()
	defer am.accessLock.Unlock()
	if am.isMoved {
		return AccountDBMovedErr
	}
	localBuk := &bucket.Bucket{}
	isBukExisted := false
	stmtSel, err := am.db.Prepare("SELECT name,account,created_time,put_time,object_count,byte_used,is_deleted FROM bucket WHERE name=?")
	if err != nil {
		glog.V(0).Infoln("Fail to prepare sql statement:", err.Error())
		return err
	}
	defer stmtSel.Close()
	err = stmtSel.QueryRow(buk.Name).Scan(&localBuk.Name, &localBuk.Account, &localBuk.CreateTime, &localBuk.PutTime, &localBuk.ObjectCount, &localBuk.ByteUsed, &localBuk.IsDeleted)
	if err != nil && err != sql.ErrNoRows {
		glog.V(0).Infoln("Fail to select record from db:", err.Error())
		return
	}
	if err != sql.ErrNoRows {
		isBukExisted = true
	}
	stmtUptAcc, err := am.db.Prepare("UPDATE account SET object_count=object_count+?,byte_used=byte_used+?,bucket_count=bucket_count+? WHERE name=?")
	if err != nil {
		glog.V(0).Infoln("Fail to prepare sql statement:", err.Error())
		return err
	}
	defer stmtUptAcc.Close()
	if buk.IsDeleted == 1 {
		if isBukExisted == false {
			return nil
		} else {
			if util.CompareStringInt64(buk.PutTime, localBuk.PutTime) == false {
				return nil
			}
			stmtDelBuk, err := am.db.Prepare("DELETE FROM bucket WHERE name=?")
			if err != nil {
				glog.V(0).Infoln("Fail to prepare sql statement:", err.Error())
				return err
			}
			defer stmtDelBuk.Close()
			tx, err := am.db.Begin()
			if err != nil {
				glog.V(0).Infoln("Fail to begin db transiction:", err.Error())
				return err
			}
			txsUptAcc := util.StmtAdd(tx, stmtUptAcc) // tx.Stmt(stmtUptAcc)
			defer txsUptAcc.Close()
			if _, err = txsUptAcc.Exec(-localBuk.ObjectCount, -localBuk.ByteUsed, -1, buk.Account); err != nil {
				glog.V(0).Infoln("Fail to update db:", err.Error())
				tx.Rollback()
				return err
			}
			txsDelBuk := util.StmtAdd(tx, stmtDelBuk) // tx.Stmt(stmtDelBuk)
			defer txsDelBuk.Close()
			if _, err = txsDelBuk.Exec(localBuk.Name); err != nil {
				glog.V(0).Infoln("Fail to delete db record:", err.Error())
				tx.Rollback()
				return err
			}
			err = tx.Commit()
		}
	} else {
		if isBukExisted == false {
			stmtInsBuk, err := am.db.Prepare("Insert into bucket (name,account,created_time,put_time,object_count,byte_used) values(?,?,?,?,?,?)")
			if err != nil {
				glog.V(0).Infoln("Fail to prepare sql statement:", err.Error())
				return err
			}
			defer stmtInsBuk.Close()
			tx, err := am.db.Begin()
			if err != nil {
				glog.V(0).Infoln("Fail to begin db transiction:", err.Error())
				return err
			}
			txsUptAcc := util.StmtAdd(tx, stmtUptAcc) //tx.Stmt(stmtUptAcc)
			defer txsUptAcc.Close()
			txsInsBuk := util.StmtAdd(tx, stmtInsBuk) //tx.Stmt(stmtInsBuk)
			defer txsInsBuk.Close()
			_, err = txsInsBuk.Exec(buk.Name, buk.Account, buk.CreateTime, buk.PutTime, buk.ObjectCount, buk.ByteUsed)
			if err != nil {
				glog.V(0).Infoln("Fail to insert db record:", err.Error())
				tx.Rollback()
				return err
			}
			if _, err = txsUptAcc.Exec(buk.ObjectCount, buk.ByteUsed, 1, buk.Account); err != nil {
				glog.V(0).Infoln("Fail to update db:", err.Error())
				tx.Rollback()
				return err
			}
			err = tx.Commit()
		} else {
			if util.CompareStringInt64(buk.PutTime, localBuk.PutTime) == false {
				return nil
			}
			stmtUptBuk, err := am.db.Prepare("UPDATE bucket SET put_time=?,object_count=?,byte_used=? WHERE name=?")
			if err != nil {
				glog.V(0).Infoln("Fail to prepare db stmt:", err.Error())
				return err
			}
			defer stmtUptBuk.Close()
			tx, err := am.db.Begin()
			if err != nil {
				glog.V(0).Infoln("Fail to begin db transiction:", err.Error())
				return err
			}
			txsUptAcc := util.StmtAdd(tx, stmtUptAcc) //tx.Stmt(stmtUptAcc)
			defer txsUptAcc.Close()
			txsUptBuk := util.StmtAdd(tx, stmtUptBuk) //tx.Stmt(stmtUptBuk)
			defer txsUptBuk.Close()
			_, err = txsUptAcc.Exec(buk.ObjectCount-localBuk.ObjectCount, buk.ByteUsed-localBuk.ByteUsed, 0, localBuk.Account)
			if err != nil {
				glog.V(0).Infoln("Fail to update db:", err.Error())
				tx.Rollback()
				return err
			}
			_, err = txsUptBuk.Exec(buk.PutTime, buk.ObjectCount, buk.ByteUsed, buk.Name)
			if err != nil {
				glog.V(0).Infoln("Fail to update db:", err.Error())
				tx.Rollback()
				return err
			}
			err = tx.Commit()
		}
	}
	return err
}
func (am *MysqlAccountManager) Enable(name string) (err error) {
	am.accessLock.Lock()
	defer am.accessLock.Unlock()
	if am.isMoved {
		return AccountDBMovedErr
	}
	stmtSel, err := am.db.Prepare("SELECT name from account WHERE name=?")
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	defer stmtSel.Close()
	oldName := name
	err = stmtSel.QueryRow(name).Scan(&oldName)
	if err != nil {
		if err == sql.ErrNoRows {
			return AccountNotFoundErr
		} else {
			return err
		}
	}
	stmtUpt, err := am.db.Prepare("update account set status=? where name=?")
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	defer stmtUpt.Close()
	tx, err := am.db.Begin()
	if err != nil {
		return err
	}
	txs := util.StmtAdd(tx, stmtUpt) // tx.Stmt(stmtUpt)
	defer txs.Close()
	if _, err = txs.Exec("enable", name); err != nil {
		if err == sql.ErrNoRows {
			err = AccountNotFoundErr
		}
		tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return
}
func (am *MysqlAccountManager) Disable(name string) (err error) {
	am.accessLock.Lock()
	defer am.accessLock.Unlock()
	if am.isMoved {
		return AccountDBMovedErr
	}
	stmtSel, err := am.db.Prepare("SELECT name from account WHERE name=?")
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	defer stmtSel.Close()
	oldName := name
	err = stmtSel.QueryRow(name).Scan(&oldName)
	if err != nil {
		if err == sql.ErrNoRows {
			return AccountNotFoundErr
		} else {
			return err
		}
	}
	stmtUpt, err := am.db.Prepare("update account set status=? where name=?")
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	defer stmtUpt.Close()
	tx, errB := am.db.Begin()
	err = errB
	if err != nil {
		return err
	}
	txs := util.StmtAdd(tx, stmtUpt) //tx.Stmt(stmtUpt)
	defer txs.Close()
	if _, err = txs.Exec("disable", name); err != nil {
		glog.V(0).Infoln(err.Error())
		if err == sql.ErrNoRows {
			err = AccountNotFoundErr
		}
		tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return

}
func (am *MysqlAccountManager) DelAccessKey(name, id string) (err error) {
	am.accessLock.Lock()
	defer am.accessLock.Unlock()
	if am.isMoved {
		return AccountDBMovedErr
	}
	var (
		accessUsers string
	)
	stmt, err := am.db.Prepare("SELECT access_user from account WHERE name=? and deleted=0")
	if err != nil {
		return err
	}
	defer stmt.Close()
	err = stmt.QueryRow(name).Scan(&accessUsers)
	if err != nil {
		if err == sql.ErrNoRows {
			return AccountNotFoundErr
		}
		return err
	}
	accKeyMgt := public.NewAccessKeyMgt(accessUsers)
	have := accKeyMgt.DelUsr(id)
	if have == false {
		return public.ErrUsrNotFound
	}
	accusrs := accKeyMgt.ToString()
	stmtUpt, err := am.db.Prepare("update account set access_user=? where name=? and deleted=0")
	if err != nil {
		glog.V(0).Infoln("failed to prepare statement:", err)
		return err
	}
	defer stmtUpt.Close()
	tx, err := am.db.Begin()
	if err != nil {
		return err
	}
	txs := util.StmtAdd(tx, stmtUpt) //tx.Stmt(stmtUpt)
	defer txs.Close()
	if _, err = txs.Exec(accusrs, name); err != nil {
		if err == sql.ErrNoRows {
			err = AccountNotFoundErr
		}
		tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}
func (am *MysqlAccountManager) AddAccessKey(name, id, key string) (err error) {
	am.accessLock.Lock()
	defer am.accessLock.Unlock()
	if am.isMoved {
		return AccountDBMovedErr
	}
	var (
		accessUsers string
	)
	stmt, err := am.db.Prepare("SELECT access_user from account WHERE name=? and deleted=0")
	if err != nil {
		return err
	}
	defer stmt.Close()
	err = stmt.QueryRow(name).Scan(&accessUsers)
	if err != nil {
		if err == sql.ErrNoRows {
			return AccountNotFoundErr
		}
		return err
	}
	accKeyMgt := public.NewAccessKeyMgt(accessUsers)
	if err = accKeyMgt.AddUsr(id, key); err != nil {
		glog.V(0).Infoln("failed to add access usr:", err)
		return
	}
	accusrs := accKeyMgt.ToString()
	stmtUpt, err := am.db.Prepare("update account set access_user=? where name=? and deleted=0")
	if err != nil {
		glog.V(0).Infoln("failed to prepare statement:", err)
		return err
	}
	defer stmtUpt.Close()
	tx, err := am.db.Begin()
	if err != nil {
		return err
	}
	txs := util.StmtAdd(tx, stmtUpt) //tx.Stmt(stmtUpt)
	defer txs.Close()
	if _, err = txs.Exec(accusrs, name); err != nil {
		if err == sql.ErrNoRows {
			err = AccountNotFoundErr
		}
		tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (am *MysqlAccountManager) BatchTrx(dst string) (err error) {
	am.accessLock.Lock()
	defer am.accessLock.Unlock()
	if am.isMoved {
		return AccountDBMovedErr
	}
	if err = am.transAccount(dst); err != nil {
		glog.V(0).Infoln("failed to transfer account table", err)
		return err
	}
	if err = am.transBucket(dst); err != nil {
		glog.V(0).Infoln("failed to transfer bucket table", err)
		return err
	}
	am.isMoved = true
	return
}
func (am *MysqlAccountManager) transAccount(dst string) (err error) {
	var (
		path        = "/sys/partitionRcv?id=" + am.id
		txBuf       = []AccountStat{}
		maxTxBufLen = 5000
		data        = []byte{}
		tdata       = []byte{}
	)
	rows, err := am.db.Query("SELECT " + accountFields + " from account")
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		acc := AccountStat{}
		err = rows.Scan(&acc.Name, &acc.CreateTime, &acc.AuthType, &acc.BucketUsed, &acc.ObjectCount, &acc.ByteUsed, &acc.IsDeleted, &acc.AccUser, &acc.Enable, &acc.MaxBucket, &acc.Descrip, &acc.AccessEx, &acc.RSV1, &acc.RSV2, &acc.RSV3, &acc.RSV4, &acc.RSV5)
		if err != nil {
			return err
		}
		txBuf = append(txBuf, acc)
		if len(txBuf) >= maxTxBufLen {
			accTxData := AccountData{}
			accTxData.Table = "account"
			if tdata, err = json.Marshal(txBuf); err != nil {
				return err
			}
			accTxData.Data = tdata
			if data, err = json.Marshal(accTxData); err != nil {
				return err
			}
			rsp, err := util.MakeRequest_timeout(dst, path, "PUT", data, nil)
			if err != nil {
				return err
			}
			defer rsp.Body.Close()
			if rsp.StatusCode != http.StatusOK {
				glog.V(0).Infoln("partition data was not processed successfully")
				return errors.New("bad http ret code: " + rsp.Status)
			}
			txBuf = []AccountStat{}
		}
	}
	if len(txBuf) >= 0 {
		glog.V(0).Infoln("transAccount",len(txBuf),"records")
		accTxData := AccountData{}
		accTxData.Table = "account"
		if len(txBuf) > 0 {
			if tdata, err = json.Marshal(txBuf); err != nil {
				return err
			}
			accTxData.Data = tdata
		}
		if data, err = json.Marshal(accTxData); err != nil {
			return err
		}
		rsp, err := util.MakeRequest_timeout(dst, path, "PUT", data, nil)
		if err != nil {
			return err
		}
		defer rsp.Body.Close()
		if rsp.StatusCode != http.StatusOK {
			glog.V(0).Infoln("partition data was not processed successfully.ret:",rsp.StatusCode)
			return errors.New("bad http ret code: " + rsp.Status)
		}
	}
	return
}
func (am *MysqlAccountManager) transBucket(dst string) (err error) {
	var (
		path        = "/sys/partitionRcv?id=" + am.id
		txBuf       = []AccountBucket{}
		maxTxBufLen = 5000
		data        = []byte{}
		tdata       = []byte{}
	)
	rows, err := am.db.Query("SELECT " + bucketFields + " from bucket")
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		buk := AccountBucket{}
		err = rows.Scan(&buk.Name, &buk.Account, &buk.CreatedTime, &buk.PutTime, &buk.ObjectCnt, &buk.ByteUsed, &buk.IsDeleted)
		if err != nil {
			return err
		}
		txBuf = append(txBuf, buk)
		if len(txBuf) >= maxTxBufLen {
			accTxData := AccountData{}
			accTxData.Table = "bucket"
			if tdata, err = json.Marshal(txBuf); err != nil {
				return err
			}
			accTxData.Data = tdata
			if data, err = json.Marshal(accTxData); err != nil {
				return err
			}
			rsp, err := util.MakeRequest_timeout(dst, path, "PUT", data, nil)
			if err != nil {
				return err
			}
			defer rsp.Body.Close()
			if rsp.StatusCode != http.StatusOK {
				glog.V(0).Infoln("partition data was not processed successfully")
				return errors.New("bad http ret code: " + rsp.Status)
			}
			txBuf = []AccountBucket{}
		}
	}
	if len(txBuf) >= 0 {
		glog.V(0).Infoln("transBucket",len(txBuf),"records")
		accTxData := AccountData{}
		accTxData.Table = "bucket"
		if len(txBuf) > 0 {
			if tdata, err = json.Marshal(txBuf); err != nil {
				return err
			}
			accTxData.Data = tdata
		}
		if data, err = json.Marshal(accTxData); err != nil {
			return err
		}
		rsp, err := util.MakeRequest_timeout(dst, path, "PUT", data, nil)
		if err != nil {
			return err
		}
		defer rsp.Body.Close()
		if rsp.StatusCode != http.StatusOK {
			glog.V(0).Infoln("partition data was not processed successfully")
			return errors.New("bad http ret code: " + rsp.Status)
		}
	}
	return
}

//获取域名对应的account/bucket
//一个域名只可以对应1个bucket。返回值就是account/bucket
//domain 是主键，不能重复，一个domain对应一个bucket
//必须是可用的
func (am *MysqlAccountManager) GetDomainResolution(domain string) (string, error) {
	if am.isMoved {
		return "", AccountDBMovedErr
	}
	var res string
	stmt, err := am.db.Prepare("SELECT bucketname  from domain WHERE domainname=? and enable=?")
	if err != nil {
		return "", err
	}
	defer stmt.Close()
	err = stmt.QueryRow(domain, true).Scan(&res)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", DomainNotFoundErr
		}
		if strings.Contains(err.Error(), "1146") {
			return "", DomainNotSupportErr
		}
		return "", err
	}
	return res, nil
}

//指定的bucket下的domain
func (am *MysqlAccountManager) GetBucketDomain(bucketname string) ([]*BucketDomain, error) {
	if am.isMoved {
		return nil, AccountDBMovedErr
	}
	res := []*BucketDomain{}
	stmt, err := am.db.Prepare("SELECT domainname,enable,isdefault  from domain WHERE bucketname=?")
	if err != nil {
		if strings.Contains(err.Error(), "1146") {
			return nil, DomainNotSupportErr
		}
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(bucketname)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, DomainNotFoundErr
		}
		if strings.Contains(err.Error(), "1146") {
			return nil, DomainNotSupportErr
		}
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		tempres := ""
		tempenable := false
		tempisdefault := int64(0)
		err = rows.Scan(&tempres, &tempenable, &tempisdefault)
		if err != nil {
			return nil, err
		}
		temp := &BucketDomain{
			Domain:     tempres,
			Enable:     tempenable,
			DefaultSeq: tempisdefault,
		}
		res = append(res, temp)
	}
	return res, nil
}

//删除domain记录
func (am *MysqlAccountManager) DeleteDomainResolution(domainname string, bucketname string) error {
	am.accessLock.Lock()
	defer am.accessLock.Unlock()
	if am.isMoved {
		return AccountDBMovedErr
	}
	glog.V(4).Infoln("delete domain", domainname)

	stmtDel, err := am.db.Prepare("DELETE from domain WHERE domainname=? AND bucketname=?")
	if err != nil {
		return err
	}
	defer stmtDel.Close()
	tx, err := am.db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return err
	}
	txs := util.StmtAdd(tx, stmtDel) // tx.Stmt(stmtDel)
	defer txs.Close()
	_, err = txs.Exec(domainname, bucketname)
	if err != nil {
		glog.V(0).Infoln(err.Error())
		if strings.Contains(err.Error(), "1146") {
			err = DomainNotSupportErr
		}
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	return nil
}

//在表中添加一条记录
func (am *MysqlAccountManager) SetDomainResolution(domainname string, bucketname string, enable, isdefault bool) error {
	am.accessLock.Lock()
	defer am.accessLock.Unlock()
	if am.isMoved {
		return AccountDBMovedErr
	}
	isdefaultseq := int64(0)
	if isdefault {
		isdefaultseq = time.Now().Unix()
	} else {
		isdefaultseq = 0
	}

	//联合主键
	stmtUptIns, err := am.db.Prepare("INSERT INTO domain(domainname,bucketname,enable,isdefault)values(?,?,?,?) ON DUPLICATE KEY UPDATE enable=?,isdefault=?")
	if err != nil {
		glog.V(0).Infoln("failed to write db:", err)
		return err
	}

	//stmtIns, err := am.db.Prepare("INSERT INTO domain (domainname,bucketname,enable,isdefault) VALUES(?,?,?,?)")

	defer stmtUptIns.Close()

	tx, err := am.db.Begin()
	if err != nil {
		return err
	}
	var oldbucketname string
	err = tx.QueryRow("SELECT bucketname from domain WHERE domainname=?", domainname).Scan(&oldbucketname)
	if err != nil && err != sql.ErrNoRows {
		if strings.Contains(err.Error(), "1146") {
			err = DomainNotSupportErr
		}
		tx.Rollback()
		return err
	}
	if err != sql.ErrNoRows && oldbucketname != bucketname {
		tx.Rollback()
		return DomainExistedErr
	}

	txs := util.StmtAdd(tx, stmtUptIns) //tx.Stmt(stmtIns)

	defer txs.Close()

	_, err = txs.Exec(domainname, bucketname, enable, isdefaultseq, enable, isdefaultseq)

	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
