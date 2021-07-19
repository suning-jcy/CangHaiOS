package util

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"code.google.com/p/weed-fs/go/glog"
	_ "github.com/go-sql-driver/mysql"
	"sync"
)

const (
	MYSQL_CONF = "mysql.conf"
)

var StmtAdd_Lock sync.Mutex

type DBStoreCommon struct {
	confDir string
	MySqlMonitor
}

func (this *DBStoreCommon) GetConfDir() string {
	return this.confDir
}
func (this *DBStoreCommon) GetPublicMysqlDB() (db *sql.DB, dbprefix string, err error) {
	confFile, err := os.OpenFile(filepath.Join(this.confDir, MYSQL_CONF), os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	lines := bufio.NewReader(confFile)
	for {
		line, err := Readln(lines)
		if err != nil && err != io.EOF {
			return nil, "", err
		}
		if err == io.EOF {
			break
		}
		str := string(line)
		if strings.HasPrefix(str, "#") {
			continue
		}
		if str == "" {
			continue
		}
		parts := strings.Split(str, "=")
		if len(parts) == 0 {
			continue
		}
		if parts[0] == "publicdsn" {
			db, err = sql.Open("mysql", parts[1])
			dsnparts := strings.Split(parts[1], "/")
			dbprefix = dsnparts[0]
			return db, dbprefix, err
		}
	}
	return nil, "", fmt.Errorf("public dsn not found")
}
func (this *DBStoreCommon) getPublicMysqlRpluserDB() (db *sql.DB, dbprefix string, err error) {
	confFile, err := os.OpenFile(filepath.Join(this.confDir, "mysql.conf"), os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	lines := bufio.NewReader(confFile)
	for {
		line, err := Readln(lines)
		if err != nil && err != io.EOF {
			return nil, "", err
		}
		if err == io.EOF {
			break
		}
		str := string(line)
		if strings.HasPrefix(str, "#") {
			continue
		}
		if str == "" {
			continue
		}
		parts := strings.Split(str, "=")
		if len(parts) == 0 {
			continue
		}
		if parts[0] == "publicdsn" {
			sparts := strings.SplitN(parts[1], ":", 2)
			if len(sparts) == 0 {
				return nil, "", fmt.Errorf("Not proper dsn:", str)
			}
			db, err = sql.Open("mysql", "rpluser:"+sparts[1])
			dsnparts := strings.Split(parts[1], "/")
			dbprefix = dsnparts[0]
			return db, dbprefix, err
		}
	}
	return nil, "", fmt.Errorf("public dsn not found")
}
func (this *DBStoreCommon) InitStoreCom(dir string) error {
	this.confDir = dir
	this.SlvStatus = &SlaveStatusSt{}
	if db, _, err := this.GetPublicMysqlDB(); err == nil {
		this.DB = db
	} else {
		return err
	}
	if db, _, err := this.getPublicMysqlRpluserDB(); err == nil {
		this.rplDB = db
	} else {
		return err
	}
	return nil
}

var (
	KEYS = []string{"Slave_IO_State","Slave_IO_Running","Slave_SQL_Running","Seconds_Behind_Master", "Last_IO_Error", "Last_SQL_Error", "Rpl_semi_sync_master_status"}
)

type MySqlMonitor struct {
	DB        *sql.DB
	rplDB     *sql.DB
	SlvStatus *SlaveStatusSt
}

var (
	ErrNotSlaveStatus = errors.New("not slave status")
)

func (this *MySqlMonitor) querySlaveStatusGetValue(key string) (err error, status string) {
	immutable := reflect.ValueOf(*this.SlvStatus)
	val := immutable.FieldByName(key)
	if !val.IsValid() {
		return ErrNotSlaveStatus, ""
	}
	rows, err := this.rplDB.Query("show slave status")
	if err != nil {
		glog.V(4).Infoln("show slave status error :", err)
		return err, ""
	}
	defer rows.Close()
	slv := this.SlvStatus
	for rows.Next() {
		glog.V(4).Infoln("get slave status")
		var columns []string
		columns, err = rows.Columns()
		if len(columns) == 40 {
			err = rows.Scan(&slv.Slave_IO_State, &slv.Master_Host, &slv.Master_User, &slv.Master_Port, &slv.Connect_Retry, &slv.Master_Log_File, &slv.Read_Master_Log_Pos, &slv.Relay_Log_File, &slv.Relay_Log_Pos, &slv.Relay_Master_Log_File, &slv.Slave_IO_Running, &slv.Slave_SQL_Running, &slv.Replicate_Do_DB, &slv.Replicate_Ignore_DB, &slv.Replicate_Do_Table, &slv.Replicate_Ignore_Table, &slv.Replicate_Wild_Do_Table, &slv.Replicate_Wild_Ignore_Table, &slv.Last_Errno, &slv.Last_Error, &slv.Skip_Counter, &slv.Exec_Master_Log_Pos, &slv.Relay_Log_Space, &slv.Until_Condition, &slv.Until_Log_File, &slv.Until_Log_Pos, &slv.Master_SSL_Allowed, &slv.Master_SSL_CA_File, &slv.Master_SSL_CA_Path, &slv.Master_SSL_Cert, &slv.Master_SSL_Cipher, &slv.Master_SSL_Key, &slv.Seconds_Behind_Master, &slv.Master_SSL_Verify_Server_Cert, &slv.Last_IO_Errno, &slv.Last_IO_Error, &slv.Last_SQL_Errno, &slv.Last_SQL_Error, &slv.Replicate_Ignore_Server_Ids, &slv.Master_Server_Id)
		} else {
			err = rows.Scan(&slv.Slave_IO_State, &slv.Master_Host, &slv.Master_User, &slv.Master_Port, &slv.Connect_Retry, &slv.Master_Log_File, &slv.Read_Master_Log_Pos, &slv.Relay_Log_File, &slv.Relay_Log_Pos, &slv.Relay_Master_Log_File, &slv.Slave_IO_Running, &slv.Slave_SQL_Running, &slv.Replicate_Do_DB, &slv.Replicate_Ignore_DB, &slv.Replicate_Do_Table, &slv.Replicate_Ignore_Table, &slv.Replicate_Wild_Do_Table, &slv.Replicate_Wild_Ignore_Table, &slv.Last_Errno, &slv.Last_Error, &slv.Skip_Counter, &slv.Exec_Master_Log_Pos, &slv.Relay_Log_Space, &slv.Until_Condition, &slv.Until_Log_File, &slv.Until_Log_Pos, &slv.Master_SSL_Allowed, &slv.Master_SSL_CA_File, &slv.Master_SSL_CA_Path, &slv.Master_SSL_Cert, &slv.Master_SSL_Cipher, &slv.Master_SSL_Key, &slv.Seconds_Behind_Master, &slv.Master_SSL_Verify_Server_Cert, &slv.Last_IO_Errno, &slv.Last_IO_Error, &slv.Last_SQL_Errno, &slv.Last_SQL_Error, &slv.Replicate_Ignore_Server_Ids, &slv.Master_Server_Id, &slv.Master_UUID, &slv.Master_Info_File, &slv.SQL_Delay, &slv.SQL_Remaining_Delay, &slv.Slave_SQL_Running_State, &slv.Master_Retry_Count, &slv.Master_Bind, &slv.Last_IO_Error_Timestamp, &slv.Last_SQL_Error_Timestamp, &slv.Master_SSL_Crl, &slv.Master_SSL_Crlpath, &slv.Retrieved_Gtid_Set, &slv.Executed_Gtid_Set, &slv.Auto_Position, &slv.Replicate_Rewrite_DB, &slv.Channel_Name, &slv.Master_TLS_Version)
		}
		//mysql 5.7
		//mysql 5.5
		if err != nil {
			return err, ""
		}
	}
	immutable = reflect.ValueOf(*this.SlvStatus)
	val = immutable.FieldByName(key)
	if val.IsValid() {
		iv := val.Interface()
		if nullstr, ok := iv.(sql.NullString); ok {
			if nullstr.Valid {
				return nil, nullstr.String
			}
		}
	}
	return nil, ""
}
func (this *MySqlMonitor) QueryStatusGetValue(key string) (err error, status string) {
	if err, status = this.querySlaveStatusGetValue(key); err != nil {
		if err != ErrNotSlaveStatus {
			return err, ""
		}
	}
	rows, err := this.DB.Query(fmt.Sprintf("show status like '%s'", key))
	if err != nil {
		return err, ""
	}
	defer rows.Close()
	var _key string
	for rows.Next() {
		err = rows.Scan(&_key, &status)
	}
	return
}

func (this *MySqlMonitor) GetDbStatus(p string) map[string]string {
	keys := strings.Split(p, "|")
	ret := make(map[string]string)
	for _, key := range KEYS {
		if err, value := this.QueryStatusGetValue(key); err == nil {
			ret[key] = value
		} else {
			glog.V(0).Infoln("Query error:", err)
		}
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		if _, ok := ret[key]; !ok {
			if err, value := this.QueryStatusGetValue(key); err == nil {
				ret[key] = value
			}
		}
	}
	return ret
}

type SlaveStatusSt struct {
	Slave_IO_State                sql.NullString
	Master_Host                   sql.NullString
	Master_User                   sql.NullString
	Master_Port                   sql.NullString
	Connect_Retry                 sql.NullString
	Master_Log_File               sql.NullString
	Read_Master_Log_Pos           sql.NullString
	Relay_Log_File                sql.NullString
	Relay_Log_Pos                 sql.NullString
	Relay_Master_Log_File         sql.NullString
	Slave_IO_Running              sql.NullString
	Slave_SQL_Running             sql.NullString
	Replicate_Do_DB               sql.NullString
	Replicate_Ignore_DB           sql.NullString
	Replicate_Do_Table            sql.NullString
	Replicate_Ignore_Table        sql.NullString
	Replicate_Wild_Do_Table       sql.NullString
	Replicate_Wild_Ignore_Table   sql.NullString
	Last_Errno                    sql.NullString
	Last_Error                    sql.NullString
	Skip_Counter                  sql.NullString
	Exec_Master_Log_Pos           sql.NullString
	Relay_Log_Space               sql.NullString
	Until_Condition               sql.NullString
	Until_Log_File                sql.NullString
	Until_Log_Pos                 sql.NullString
	Master_SSL_Allowed            sql.NullString
	Master_SSL_CA_File            sql.NullString
	Master_SSL_CA_Path            sql.NullString
	Master_SSL_Cert               sql.NullString
	Master_SSL_Cipher             sql.NullString
	Master_SSL_Key                sql.NullString
	Seconds_Behind_Master         sql.NullString
	Master_SSL_Verify_Server_Cert sql.NullString
	Last_IO_Errno                 sql.NullString
	Last_IO_Error                 sql.NullString
	Last_SQL_Errno                sql.NullString
	Last_SQL_Error                sql.NullString
	Replicate_Ignore_Server_Ids   sql.NullString
	Master_Server_Id              sql.NullString
	Master_UUID                   sql.NullString
	Master_Info_File              sql.NullString
	SQL_Delay                     sql.NullString
	SQL_Remaining_Delay           sql.NullString
	Slave_SQL_Running_State       sql.NullString
	Master_Retry_Count            sql.NullString
	Master_Bind                   sql.NullString
	Last_IO_Error_Timestamp       sql.NullString
	Last_SQL_Error_Timestamp      sql.NullString
	Master_SSL_Crl                sql.NullString
	Master_SSL_Crlpath            sql.NullString
	Retrieved_Gtid_Set            sql.NullString
	Executed_Gtid_Set             sql.NullString
	Auto_Position                 sql.NullString
	Replicate_Rewrite_DB          sql.NullString
	Channel_Name                  sql.NullString
	Master_TLS_Version            sql.NullString
}

// go 1.9.2 直接Stmt会报错
func StmtAdd(tx *sql.Tx, stmt *sql.Stmt) *sql.Stmt {
	StmtAdd_Lock.Lock()
	res := tx.Stmt(stmt)
	StmtAdd_Lock.Unlock()
	return res
}
