package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
	_ "github.com/go-sql-driver/mysql"
)

const (
	PARREBALANCE_INIT          = iota
	PARREBALANCE_PREPARED
	PARREBALANCE_TRANSFER
	PARREBALANCE_TRANSFEREXTRA
)

type ParRebalanceStatus struct {
	parId      PartitionId
	collection string
	db         *sql.DB
	dsn        string
	rp         *ReplicaPlacement
	status     int
	expDate    time.Time
}

func (parRebSts *ParRebalanceStatus) checkExpire() bool {
	if time.Now().After(parRebSts.expDate.Add(15 * time.Minute)) {
		parRebSts.expDate = time.Now()
		return true
	}
	return false
}
func (parRebSts *ParRebalanceStatus) updateExpire() {
	parRebSts.expDate = time.Now()
	return
}

func (s *FilerPartitionStore) DisablePartitionCache()(error){
	//get filers ip
	var filers []string
	masterNode, e := s.masterNodes.FindMaster()
	if e != nil {
		glog.V(0).Infoln("find master node err:",e)
		return errors.New("find master node err")
	}
	url := "/sdoss/filer/status"
	rsp, e := util.MakeRequest(masterNode, url, "GET", nil, nil)
	if e != nil {
		glog.V(0).Infoln("Can not get filers status. err:", e,masterNode)
		return e
	}
	if rsp.StatusCode != 200 {
		glog.V(0).Infoln("Can not get filers status",rsp.StatusCode)
		return errors.New("Can not get filers status")
	}else{
		data,e:=ioutil.ReadAll(rsp.Body)
		if e != nil {
			glog.V(0).Infoln("read body err",e)
			return errors.New("read body err")
		}
		var msg map[string]interface{}
		e = json.Unmarshal(data,&msg)
		if e != nil {
			glog.V(0).Infoln("Unmarshal data err",e)
			return errors.New("Unmarshal data err")
		}
		if _,ok:=msg["Filers Available"];ok{
			filersIP := msg["Filers Available"].([]interface{})
			filers = make([]string,len(filersIP))
			for k,v:=range filersIP {
				filers[k] = v.(string)
			}
		}else{
			return errors.New("find no filers")
		}
	}
	//disable filers' partitionCache
	for _,v:=range filers {
		urlTail := "?node=" + s.Ip + ":"+fmt.Sprintf("%d",s.Port)
		urlTail = "/sdoss/filer/delPC" + urlTail
		url := "http://" + v + urlTail
		b,e:=util.PostBytes_timeout(url, nil)
		if e != nil {
			glog.V(0).Infoln("disable filer cache node:",url," failed:",e)
			return errors.New("failed to send req")
		}
		var msg string
		e = json.Unmarshal(b,&msg)
		if e != nil {
			glog.V(0).Infoln("disable filer cache node:",url," failed:",e)
			return errors.New("failed to unmarshal data")
		}
		if msg != "success" {
			e = errors.New("failed to disable cache node")
			glog.V(0).Infoln("disable filer cache node:",url," failed:",msg)
			return e
		}
		glog.V(0).Infoln("disable filer cache node:",url," ok")
	}
	return nil
}

func (s *FilerPartitionStore) PartitionRebalance(parIdString string, collection string, rp string, dstAddr string) (error, bool) {
	parId, err := NewPartitionId(parIdString)
	if err != nil {
		return fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdString), false
	}
	glog.V(0).Infoln("PartitionRebalance check point 1 partition="+parIdString,collection,dstAddr)
	if par := s.GetPartition(collection, parId); par != nil {
		glog.V(0).Infoln("PartitionRebalance check point 2. partition="+parIdString,collection,dstAddr)
		if parRebal, ok := par.(PartitionRebalance); ok {
			glog.V(0).Infoln("PartitionRebalance check point 3. partition="+parIdString,collection,dstAddr)
			if err = parRebal.ParRebalance(dstAddr); err == nil {
				glog.V(0).Infoln("PartitionRebalance check point 4. partition="+parIdString,collection,dstAddr)
				s.ParRebalanceSetMask(parIdString, collection)
				s.Join()
				//失效所有的filer上本partition的PID.在临时表迁移完成前，不再接受新的请求
				err = s.DisablePartitionCache()
				if err != nil {
					glog.V(0).Infoln("failed to disable filers' partition cache. partition="+parIdString,collection,dstAddr,err)
					parRebal.RebalanceRollBack() //如果失败了，要将临时表的数据再倒回到主表中
					s.ParRebalanceResetMask(parIdString, collection)
					return err,false
				}
				glog.V(0).Infoln("PartitionRebalance check point 5. partition="+parIdString,collection,dstAddr,err)
				time.Sleep(5 * time.Second)
				//把filemap_rebalance 也同步过去
				if err = parRebal.ParRebalanceExt(dstAddr); err != nil {
					glog.V(0).Infoln("PartitionRebalance check point 6. partition="+parIdString,collection,dstAddr,err)
					s.ParRebalanceResetMask(parIdString, collection)
				}
			}
			return err, false
		}
	}
	glog.V(0).Infoln("PartitionRebalance check point 6. partition="+parIdString,collection,dstAddr)
	return fmt.Errorf("par id %d is not found during partition rebalance!", parId), false
}
func (s *FilerPartitionStore) PartitionRebalanceEnd(parIdString string, collection string, rp string, dstAddr string) (errs error, statusCode int) {
	statusCode = http.StatusOK
	parId, err := NewPartitionId(parIdString)
	if err != nil {
		return fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdString), 500
	}
	glog.V(0).Infoln("PartitionRebalance End check point 1.partition="+parIdString,collection,dstAddr)
	if par := s.GetPartition(collection, parId); par != nil {
		glog.V(0).Infoln("PartitionRebalance End check point 2.partition="+parIdString,collection,dstAddr)
		if parRebal, ok := par.(PartitionRebalance); ok {
			if err = parRebal.ParRebalanceCommit(dstAddr); err == nil {
				glog.V(0).Infoln("PartitionRebalance End check point 3.partition="+parIdString,collection,dstAddr)
				err = s.DeletePartition(parIdString, collection)
				if err != nil {
					glog.V(0).Infoln("delete partition id failed.partition="+parIdString,collection)
					err = errors.New("Delete Partition ID Failed")
					statusCode = 501  //500 需要回滚到原rack，  501不需要回滚到原rack，使用新的rack，手动干预删除原rack
				}
			}else {
				glog.V(0).Infoln("assign to master failed.partition="+parIdString,collection,dstAddr)
				err = errors.New("Assign DstAddr To Master Failed")
				statusCode = 500
			}
			return err, statusCode
		}
	}
	glog.V(0).Infoln("PartitionRebalance End check point 4.partition="+parIdString,collection,dstAddr)
	return fmt.Errorf("par id %d is not found!", parId), 500
}
func (s *FilerPartitionStore) ParRebalancePrepare(parIdString string, collection string, rp string) error {
	if !s.parRebalanceStatus.checkExpire() && s.parRebalanceStatus.status != PARREBALANCE_INIT {
		return fmt.Errorf("partition %d rebalance have started,%d", s.parRebalanceStatus.parId, s.parRebalanceStatus.status)
	}
	parId, err := NewPartitionId(parIdString)
	if err != nil {
		return fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdString)
	}
	if s.HasPartition(collection, parId) {
		return fmt.Errorf("Partition Id %d already exists!", parId)
	}
	replicaPlacement, e := NewReplicaPlacementFromString(rp)
	if e != nil {
		return e
	}
	if s.storetype == "mysql" {
		publicdb, dbprefix, err := s.GetPublicMysqlDB()
		if err != nil {
			glog.V(0).Infoln("get public dsn err", err.Error())
			return err
		}
		defer publicdb.Close()
		dbname := "sdoss" + "_" + parIdString
		if collection != "" {
			dbname = "sdoss" + "_" + collection + "_" + parIdString
		}

		_, err = publicdb.Exec("create database " + dbname)
		if err != nil {
			glog.V(0).Infoln("create database err", dbname, err.Error())
			return err
		}
		s.parRebalanceStatus.dsn = dbprefix + "/" + dbname

		s.parRebalanceStatus.db, err = sql.Open("mysql", s.parRebalanceStatus.dsn)
		if err != nil {
			glog.V(0).Infoln("open database err", s.parRebalanceStatus.dsn, err.Error())
			return err
		}
		tableCreateSql := "create table if not exists filemap" + filemapColumnDeclaration
		if _, err = s.parRebalanceStatus.db.Exec(tableCreateSql); err != nil {
			glog.V(0).Infoln("Par Reba filemap prepare:", err)
		}

		tableCreateSql2 := "create table if not exists filetag " + filetagColumnDeclaration
		if _, err = s.parRebalanceStatus.db.Exec(tableCreateSql2); err != nil {
			glog.V(0).Infoln("Par Reba filetag prepare:", err)
		}
		if err == nil {
			s.parRebalanceStatus.status = PARREBALANCE_PREPARED
			s.parRebalanceStatus.parId = parId
			s.parRebalanceStatus.collection = collection
			s.parRebalanceStatus.rp = replicaPlacement
		}
		return err
	}
	return fmt.Errorf("current store type do not support partition rebalance.")
}
func (s *FilerPartitionStore) ParRebalanceSetMask(parIdString string, collection string) error {
	parId, err := NewPartitionId(parIdString)
	if err != nil {
		return fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdString)
	}
	if !s.HasPartition(collection, parId) {
		return fmt.Errorf("Partition Id %d does not exist!", parId)
	}
	s.accessLock.Lock()
	s.joinMask[parIdString] = parIdString
	s.accessLock.Unlock()
	s.Join()
	return nil
}
func (s *FilerPartitionStore) ParRebalanceResetMask(parIdString string, collection string) error {
	s.accessLock.Lock()
	delete(s.joinMask, parIdString)
	s.accessLock.Unlock()
	s.Join()
	return nil
}
func (s *FilerPartitionStore) DeletePartition(parIdString string, collection string) error {
	parId, err := NewPartitionId(parIdString)
	if err != nil {
		return fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdString)
	}
	if !s.HasPartition(collection, parId) {
		return fmt.Errorf("Partition Id %d does not exist!", parId)
	}
	DelDsnFromConf(s.GetConfDir(), parIdString, collection, "")
	parcoll := s.GetOrCreatePartitionCollection(collection)
	if parcoll != nil {
		parcoll.Lock()
		delete(parcoll.partitions, parId)
		parcoll.Unlock()
	}
	s.Join()
	s.ParRebalanceResetMask(parIdString, collection)
	return nil
}
func createFile(db *sql.DB, filePath string, fid string) (oldfid string, err error) {

	glog.V(4).Infoln("create file", filePath, fid)
	stmtIns, err := db.Prepare("INSERT INTO filemap VALUES(?,?)")
	if err != nil {
		return "", err
	}
	defer stmtIns.Close()
	/*
	   stmtSel, err := db.Prepare("SELECT fid from filemap WHERE filepath=?")
	       if err != nil {
	       return "", err
	   }
	*/
	stmtUpd, err := db.Prepare("update filemap set fid =? WHERE filepath=?")
	if err != nil {
		return "", err
	}
	defer stmtUpd.Close()
	tx, err := db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return "", err
	}
	err = tx.QueryRow("SELECT fid from filemap WHERE filepath=?", filePath).Scan(&oldfid)
	glog.V(4).Infoln("create file", oldfid, err)
	if err != nil && err != sql.ErrNoRows {
		glog.V(0).Infoln(err.Error())
		tx.Rollback()
		return "", err
	}
	var txs *sql.Stmt
	if oldfid == "" {
		txs = tx.Stmt(stmtIns)
	} else {
		txs = tx.Stmt(stmtUpd)
	}
	defer txs.Close()
	if oldfid == "" {
		_, err = txs.Exec(filePath, fid)
	} else {
		_, err = txs.Exec(fid, filePath)
	}
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

	return oldfid, nil

}

func batchCreateFile(db *sql.DB, data []byte,pid string) (err error) {
	count1:=0
	count2:=0
	defer func() {
		glog.V(0).Infoln("======> add",count1,"records to table filemap,",count2,"records to table filetag.partition="+pid)
	}()
	stmtIns, err := db.Prepare("INSERT INTO filemap(filepath,fid,expireat,length,last_modified)values(?,?,?,?,?)")
	if err != nil {
		return err
	}
	defer stmtIns.Close()

	stmtInstag, err := db.Prepare("INSERT INTO filetag( fid, tag)values(?,?) ON DUPLICATE KEY UPDATE tag=?")
	if err != nil {
		return err
	}
	defer stmtIns.Close()

	tx, err := db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return err
	}

	var txs *sql.Stmt

	txs = tx.Stmt(stmtIns)

	defer txs.Close()

	var txstag *sql.Stmt

	txstag = tx.Stmt(stmtInstag)

	defer txstag.Close()

	keyvalues := make(map[string][]string)
	err = json.Unmarshal(data, &keyvalues)
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	for filepath, fe := range keyvalues {
		if len(fe) != 5 {
			glog.V(0).Infoln("FilerPartition rebalance: Bad data count, skip item!",len(fe))
			continue
		}
		length := util.ParseInt64(fe[2], 0)
		_, err = txs.Exec(filepath, fe[0], fe[1], length, fe[3])
		if err != nil {
			glog.V(0).Infoln(err.Error())
			tx.Rollback()
			return err
		}
		count1++
		if (fe[4] != "") {
			_, err = txstag.Exec(fe[0], fe[4], fe[4])
			if err != nil {
				glog.V(0).Infoln(err.Error())
				tx.Rollback()
				return err
			}
			count2++
		}

	}
	return tx.Commit()
}
func (s *FilerPartitionStore) ParRebalanceReceive(parIdStr string, data []byte) error {
	glog.V(0).Infoln("partition upload start. partition="+parIdStr, ", data len =", len(data))
	s.parRebalanceStatus.updateExpire()
	if s.parRebalanceStatus.status != PARREBALANCE_PREPARED {
		return fmt.Errorf("partition  %d rebalance have not started,now status %d", s.parRebalanceStatus.parId, s.parRebalanceStatus.status)
	}

	if s.parRebalanceStatus.parId.String() != parIdStr {
		return fmt.Errorf("Partition Id %s is not as same as store repairing %d", parIdStr, s.parRebalanceStatus.parId)
	}
	err := batchCreateFile(s.parRebalanceStatus.db, data,parIdStr)
	if err != nil {
		s.parRebalanceStatus.status = PARREBALANCE_INIT
		return err
	}
	glog.V(0).Infoln("partition upload end.partition="+parIdStr, ", data len =", len(data))
	return nil
}

func batchCreateFileExtra2(db *sql.DB, data []byte) (err error) {
	countA:=0
	countD:=0
	countU:=0
	defer func() {
		glog.V(0).Infoln("======> add",countA,"records,"," update ",countU,"records,"," del ",countD,"records to table filemap")
	}()
	stmtIns, err := db.Prepare("INSERT INTO filemap(filepath,fid,expireat,length,last_modified)values(?,?,?,?,?)")
	if err != nil {
		return err
	}
	defer stmtIns.Close()
	stmtDel, err := db.Prepare("DELETE from filemap WHERE filepath=?")
	if err != nil {
		return err
	}
	defer stmtDel.Close()

	stmtUpd, err := db.Prepare("update filemap set fid =?,expireat=?,length=?,last_modified=? WHERE filepath=?")
	if err != nil {
		return err
	}
	defer stmtUpd.Close()
	tx, err := db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return err
	}

	var txs, txd, txu *sql.Stmt

	txs = tx.Stmt(stmtIns)
	txu = tx.Stmt(stmtUpd)
	txd = tx.Stmt(stmtDel)

	defer txs.Close()
	defer txd.Close()
	defer txu.Close()
	keyvalues := make(map[string][]string)
	err = json.Unmarshal(data, &keyvalues)
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	var oldfid string
	for filepath, fe := range keyvalues {
		if len(fe) != 4 {
			glog.V(0).Infoln("FilerPartition rebalnace, bad data, skip!")
			continue
		}
		glog.V(2).Infoln("exta:", filepath, fe)
		if fe[0] == "negative" {
			_, err = txd.Exec(filepath)
			if err == nil {
				countD++
			}
		} else {
			err = tx.QueryRow("SELECT fid from filemap WHERE filepath=?", filepath).Scan(&oldfid)
			if err != nil && err != sql.ErrNoRows {
				glog.V(0).Infoln(err.Error())
				tx.Rollback()
				return err
			}
			length := util.ParseInt64(fe[2], 0)
			if oldfid == "" {
				_, err = txs.Exec(filepath, fe[0], fe[1], length, fe[3])
				if err == nil {
					countA++
				}
			} else {
				_, err = txu.Exec(fe[0], fe[1], length, fe[3], filepath)
				if err == nil {
					countU++
				}
			}
		}
		if err != nil {
			glog.V(0).Infoln(err.Error())
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func batchCreateFileExtra(db *sql.DB, data []byte) (err error) {
	countA:=0
	countD:=0
	countU:=0
	countAT:=0
	countDT:=0
	countUT:=0
	defer func() {
		glog.V(0).Infoln("======> add",countA,",",countAT,"records,"," update ",countU,",",countUT,"records,"," del ",countD,",",countDT,"records to table filemap from filemap_rebalance")
	}()
	stmtIns, err := db.Prepare("INSERT INTO filemap(filepath,fid,expireat,length,last_modified)values(?,?,?,?,?)")
	if err != nil {
		return err
	}
	defer stmtIns.Close()

	stmtInstag, err := db.Prepare("INSERT INTO filetag( fid, tag)values(?,?) ON DUPLICATE KEY UPDATE tag=?")
	if err != nil {
		return err
	}
	defer stmtInstag.Close()


	stmtDel, err := db.Prepare("DELETE from filemap WHERE filepath=?")
	if err != nil {
		return err
	}
	defer stmtDel.Close()

	stmtDeltag, err := db.Prepare("DELETE from filetag WHERE fid=?")
	if err != nil {
		return err
	}
	defer stmtDeltag.Close()

	stmtUpd, err := db.Prepare("update filemap set fid =?,expireat=?,length=?,last_modified=? WHERE filepath=?")
	if err != nil {
		return err
	}
	defer stmtUpd.Close()


	stmtUpdtag, err := db.Prepare("update filetag set tag=? WHERE fid=?")
	if err != nil {
		return err
	}
	defer stmtUpdtag.Close()


	tx, err := db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return err
	}

	var txs, txd, txu,txstag,txutag,txdtag *sql.Stmt

	txs = tx.Stmt(stmtIns)
	txu = tx.Stmt(stmtUpd)
	txd = tx.Stmt(stmtDel)

	txstag = tx.Stmt(stmtInstag)
	txutag = tx.Stmt(stmtUpdtag)
	txdtag = tx.Stmt(stmtDeltag)


	defer txs.Close()
	defer txd.Close()
	defer txu.Close()

	defer txstag.Close()
	defer txutag.Close()
	defer txdtag.Close()

	keyvalues := make(map[string][]string)
	err = json.Unmarshal(data, &keyvalues)
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	var oldfid string
	for filepath, fe := range keyvalues {
		if len(fe) != 5 {
			glog.V(0).Infoln("FilerPartition rebalnace, bad data, skip!")
			continue
		}
		glog.V(1).Infoln("filepath:", filepath, fe[0],fe[1],fe[2],fe[3],fe[4])
		if fe[0] == "negative" {
			_, err = txd.Exec(filepath)
			if err != nil {
				glog.V(0).Infoln(" filepath:",filepath,err.Error())
				tx.Rollback()
				return err
			}else{
				countD++
				if fe[4] != ""{
					_, err = txdtag.Exec(fe[0])
					if err != nil {
						glog.V(0).Infoln("tag filepath:",filepath,err.Error())
						tx.Rollback()
						return err
					}
					countDT++
				}
			}
		} else {
			oldfid=""
			err = tx.QueryRow("SELECT fid from filemap WHERE filepath=?", filepath).Scan(&oldfid)
			if err != nil && err != sql.ErrNoRows {
				glog.V(0).Infoln(err.Error())
				tx.Rollback()
				return err
			}
			glog.V(1).Infoln("filepath:",filepath,"oldfid:",oldfid, fe[0],fe[1],fe[2],fe[3],fe[4])
			length := util.ParseInt64(fe[2], 0)
			if oldfid == "" {
				_, err = txs.Exec(filepath, fe[0], fe[1], length, fe[3])
				if err != nil {
					glog.V(0).Infoln(" filepath:",filepath,err.Error())
					tx.Rollback()
					return err
				}else{
					countA++
					if fe[4] != ""{
						_, err = txstag.Exec(fe[0], fe[4], fe[4])
						if err != nil {
							glog.V(0).Infoln("tag filepath:",filepath,err.Error())
							tx.Rollback()
							return err
						}
						countAT++
					}
				}
			} else {
				_, err = txu.Exec(fe[0], fe[1], length, fe[3], filepath)
				if err != nil {
					glog.V(0).Infoln(" filepath:",filepath,err.Error())
					tx.Rollback()
					return err
				}else{
					countU++
					if fe[4] != ""{
						_, err = txutag.Exec(fe[0], fe[4])
						if err != nil {
							glog.V(0).Infoln("tag filepath:",filepath,err.Error())
							tx.Rollback()
							return err
						}
						countUT++
					}
				}
			}
		}
		if err != nil {
			glog.V(0).Infoln(err.Error())
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *FilerPartitionStore) ParRebalanceExtraReceive(parIdStr string, data []byte) (err error) {
	s.parRebalanceStatus.updateExpire()
	if s.parRebalanceStatus.status != PARREBALANCE_PREPARED {
		return fmt.Errorf("partition %d rebalance have not started,%d", s.parRebalanceStatus.parId, s.parRebalanceStatus.status)
	}
	if s.parRebalanceStatus.parId.String() != parIdStr {
		return fmt.Errorf("Partition Id %s is not as same as store repairing %d", parIdStr, s.parRebalanceStatus.parId)
	}
	if err = batchCreateFileExtra(s.parRebalanceStatus.db, data); err != nil {
		glog.V(0).Infoln("par rebal extra data:", parIdStr, len(data), err)
	}
	if err != nil {
		s.parRebalanceStatus.status = PARREBALANCE_INIT
	}
	return
}
func (s *FilerPartitionStore) ParRebalanceCommit(parIdStr string) error {
	if s.parRebalanceStatus.status != PARREBALANCE_PREPARED {
		return fmt.Errorf("partition %d rebalance have not started,%d", s.parRebalanceStatus.parId, s.parRebalanceStatus.status)
	}
	defer func() {
		s.parRebalanceStatus.status = PARREBALANCE_INIT
	}()
	if s.parRebalanceStatus.parId.String() != parIdStr {
		return fmt.Errorf("Partition Id %s is not as same as store repairing %d", parIdStr, s.parRebalanceStatus.parId)
	}
	prs := &s.parRebalanceStatus
	//2018-2-5 createtable变更为true，为了增加filetag表
	if par, err := NewMysqlPartition(prs.dsn, prs.parId, prs.collection, prs.rp, true); err == nil {
		s.accessLock.Lock()
		delete(s.joinMask, parIdStr)
		s.accessLock.Unlock()
		s.SetPartition(prs.collection, prs.parId, par)
		s.Join()
		WriteNewParToConf(s.GetConfDir(), prs.dsn, prs.parId.String(), prs.collection)

		return nil
	} else {
		return err
	}
}
