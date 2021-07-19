package storage

import (
	_ "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "fmt"
	"net/url"
	"strconv"
	"time"
	_ "time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
	_ "github.com/go-sql-driver/mysql"
)

type ParRebalanceResult struct {
	Result  bool
	dstAddr string
	Error   string
}

func (p *MysqlPartition) prepareParRebal(dstAddr string) (err error) {
	values := make(url.Values)
	values.Add("partition", p.Id.String())
	values.Add("collection", p.Collection())
	values.Add("replacement", p.Rp().String())
	jsonBlob, err := util.Post("http://"+dstAddr+"/partition/prepare_rebalance", values)
	if err != nil {
		glog.V(0).Infoln("parameters:", values, dstAddr, err)
		return err
	}
	var ret ParRebalanceResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
func (p *MysqlPartition) transferPartition2(dstAddr string, inf string) (err error) {
	buf := make(map[string][]string)
	url := "http://" + dstAddr + inf + "?partition=" + p.Id.String()

	rows, err := p.db.Query("SELECT t1.filepath,t1.fid,t1.expireat,t1.length,t1.last_modified,ifnull(t2.tag,'') tag from filemap t1 left join filetag t2 on t1.fid=t2.fid")
	//rows, err := p.db.Query("SELECT filepath,filemap.fid,expireat,length,last_modified,filetag.tag from filemap,filetag where filemap.fid=filetag.fid")
	if err != nil {
		glog.V(0).Infoln("DB error:", err.Error())
		return
	}
	count := 0
	sum:=0
	defer func() {
		glog.V(0).Infoln(" transfer ",sum,"records."," url:",url," err:",err)
	}()
	for rows.Next() {
		var filepath, fid, expireat, MTime,tag string
		var length int64
		err = rows.Scan(&filepath, &fid, &expireat, &length, &MTime ,&tag)
		if err != nil {
			glog.V(0).Infoln(" scan err",err)
			return err
		}
		buf[filepath] = []string{fid, expireat, strconv.FormatInt(length, 10), MTime,tag}
		count++
		sum++
		if count >= 5000 {
			b, err := json.Marshal(buf)
			if err != nil {
				//   fmt.Println("error:", err)
				glog.V(0).Infoln(" Marshal err",err)
				return err
			}
			jsonBlob, err := util.PostBytes(url, b)
			if err != nil {
				glog.V(0).Infoln(" PostBytes err",err)
				return err
			}
			var ret ParRebalanceResult
			if err = json.Unmarshal(jsonBlob, &ret); err != nil {
				glog.V(0).Infoln(" Unmarshal err",err)
				return err
			}
			if ret.Error != "" {
				glog.V(0).Infoln("Transfer error:", ret.Error)
				return errors.New(ret.Error)
			}
			count = 0
			buf = make(map[string][]string)
		}
	}
	err = rows.Err()
	if err != nil {
		glog.V(0).Infoln(" rows err",err)
		return err
	}

	if len(buf) > 0 {
		b, err := json.Marshal(buf)
		if err != nil {
			//  fmt.Println("error:", err)
			glog.V(0).Infoln(" Marshal err",err)
			return err
		}
		jsonBlob, err := util.PostBytes(url+"&eof=true", b)
		if err != nil {
			glog.V(0).Infoln(" PostBytes err",url,err)
			return err
		}
		var ret ParRebalanceResult
		if err = json.Unmarshal(jsonBlob, &ret); err != nil {
			glog.V(0).Infoln(" Unmarshal err",err)
			return err
		}
		if ret.Error != "" {
			glog.V(0).Infoln(" ret err",ret.Error)
			return errors.New(ret.Error)
		}

	}

	return
}
func (p *MysqlPartition) transferPartition(dstAddr string, inf string) (err error) {
	url := "http://" + dstAddr + inf + "?partition=" + p.Id.String()
	var count int64
	sum:=0
	starttime:=time.Now()
	defer func() {
		glog.V(0).Infoln(" transfer ",sum,"records."," url:",url," err:",err," timecost:",time.Since(starttime))
	}()

	var start int64
	var num int64 = 100000

	start = 0
	var scannum  = 0
	for {
		starttime1:=time.Now()
		buf := make(map[string][]string)
		queryString:="SELECT t1.filepath,t1.fid,t1.expireat,t1.length,t1.last_modified,ifnull(t2.tag,'') tag from filemap t1 left join filetag t2 on t1.fid=t2.fid limit "+fmt.Sprintf("%d",start)+" , "+fmt.Sprintf("%d",num)
		rows, err := p.db.Query(queryString)
		//rows, err := p.db.Query("SELECT filepath,filemap.fid,expireat,length,last_modified,filetag.tag from filemap,filetag where filemap.fid=filetag.fid")
		if err != nil {
			glog.V(0).Infoln("DB error:", err.Error())
			return err
		}
		glog.V(0).Infoln("scaning  from  filemap",start," to ",start+num," partition="+p.Id.String()," timecost:",time.Since(starttime1))
		start += num
		scannum = 0
		for rows.Next() {
			var filepath, fid, expireat, MTime,tag string
			var length int64
			err = rows.Scan(&filepath, &fid, &expireat, &length, &MTime ,&tag)
			if err != nil {
				glog.V(0).Infoln(" scan err",err)
				return err
			}
			scannum++
			buf[filepath] = []string{fid, expireat, strconv.FormatInt(length, 10), MTime,tag}
			count++
			sum++
			if count >= 10000 {
				b, err := json.Marshal(buf)
				if err != nil {
					//   fmt.Println("error:", err)
					glog.V(0).Infoln(" Marshal err",err)
					return err
				}
				jsonBlob, err := util.PostBytes(url, b)
				if err != nil {
					glog.V(0).Infoln(" PostBytes err",err)
					return err
				}
				var ret ParRebalanceResult
				if err = json.Unmarshal(jsonBlob, &ret); err != nil {
					glog.V(0).Infoln(" Unmarshal err",err)
					return err
				}
				if ret.Error != "" {
					glog.V(0).Infoln("Transfer error:", ret.Error)
					return errors.New(ret.Error)
				}
				glog.V(0).Infoln("transfer ",len(buf)," records,partition="+p.Id.String()," timecost:",time.Since(starttime1))
				count = 0
				buf = make(map[string][]string)
			}
		}
		err = rows.Err()
		if err != nil {
			glog.V(0).Infoln(" rows err",err)
			return err
		}

		if len(buf) > 0 {
			b, err := json.Marshal(buf)
			if err != nil {
				//  fmt.Println("error:", err)
				glog.V(0).Infoln(" Marshal err",err)
				return err
			}
			jsonBlob, err := util.PostBytes(url+"&eof=true", b)
			if err != nil {
				glog.V(0).Infoln(" PostBytes err",url,err)
				return err
			}
			var ret ParRebalanceResult
			if err = json.Unmarshal(jsonBlob, &ret); err != nil {
				glog.V(0).Infoln(" Unmarshal err",err)
				return err
			}
			if ret.Error != "" {
				glog.V(0).Infoln(" ret err",ret.Error)
				return errors.New(ret.Error)
			}
			glog.V(0).Infoln("transfer ",len(buf)," records,partition="+p.Id.String()," records",time.Since(starttime1))
		}else if scannum == 0{
			break
		}
	}
	return
}

func (p *MysqlPartition) transferExtraPartition2(dstAddr string, inf string) (err error) {
	glog.V(0).Infoln("begin to transfer exta db data", dstAddr)
	buf := make(map[string][]string)
	url := "http://" + dstAddr + inf + "?partition=" + p.Id.String()

	//rows, err := p.db.Query("SELECT filepath,filemap_rebalance.fid,expireat,length,last_modified,filetag.tag from filemap_rebalance,filetag where filemap_rebalance.fid=filetag.fid")
	rows, err := p.db.Query("SELECT t1.filepath,t1.fid,t1.expireat,t1.length,t1.last_modified,ifnull(t2.tag,'') tag from filemap_rebalance t1 left join filetag t2 on t1.fid=t2.fid")
	if err != nil {
		glog.V(0).Infoln(err)
		return
	}
	count := 0
	sum:=0
	defer func() {
		glog.V(0).Infoln(" transfer ",sum,"records."," url:",url," err:",err)
	}()
	for rows.Next() {
		var filepath, fid, expireat, MTime,tag string
		var length int64
		err = rows.Scan(&filepath, &fid, &expireat, &length, &MTime,&tag)
		if err != nil {
			glog.V(0).Infoln(err)
			return err
		}
		buf[filepath] = []string{fid, expireat, strconv.FormatInt(length, 10), MTime,tag}
		count++
		sum++
		if count >= 5000 {
			b, err := json.Marshal(buf)
			if err != nil {
				glog.V(0).Infoln(err)
				return err
			}
			jsonBlob, err := util.PostBytes(url, b)
			if err != nil {
				glog.V(0).Infoln(err)
				return err
			}
			var ret ParRebalanceResult
			if err = json.Unmarshal(jsonBlob, &ret); err != nil {
				glog.V(0).Infoln(err)
				return err
			}
			if ret.Error != "" {
				return errors.New(ret.Error)
			}
			count = 0
			buf = make(map[string][]string)
		}
	}
	err = rows.Err()
	if err != nil {
		glog.V(0).Infoln(err)
		return err
	}

	if len(buf) > 0 {
		b, err := json.Marshal(buf)
		if err != nil {
			glog.V(0).Infoln(err)
			return err
		}
		jsonBlob, err := util.PostBytes(url+"&eof=true", b)
		if err != nil {
			glog.V(0).Infoln(err)
			return err
		}
		var ret ParRebalanceResult
		if err = json.Unmarshal(jsonBlob, &ret); err != nil {
			glog.V(0).Infoln(err)
			return err
		}
		if ret.Error != "" {
			return errors.New(ret.Error)
		}

	}

	return
}
func (p *MysqlPartition) transferExtraPartition(dstAddr string, inf string) (err error) {
	glog.V(0).Infoln("begin to transfer exta db data", dstAddr," partition="+p.Id.String())
	url := "http://" + dstAddr + inf + "?partition=" + p.Id.String()
	var count int64
	sum:=0
	starttime:=time.Now()
	defer func() {
		glog.V(0).Infoln(" transfer ",sum,"records."," url:",url," err:",err," timecost:",time.Since(starttime))
	}()

	var start int64
	var num int64 = 100000

	start = 0
	var buf	map[string][]string
	var scannum  = 0
	for {
		starttime1:=time.Now()
		buf = make(map[string][]string)
		queryString:="SELECT t1.filepath,t1.fid,t1.expireat,t1.length,t1.last_modified,ifnull(t2.tag,'') tag from filemap_rebalance t1 left join filetag t2 on t1.fid=t2.fid limit "+fmt.Sprintf("%d",start)+" , "+fmt.Sprintf("%d",num)
		rows, err := p.db.Query(queryString)
		//rows, err := p.db.Query("SELECT filepath,filemap_rebalance.fid,expireat,length,last_modified,filetag.tag from filemap_rebalance,filetag where filemap_rebalance.fid=filetag.fid")
		//rows, err := p.db.Query("SELECT t1.filepath,t1.fid,t1.expireat,t1.length,t1.last_modified,ifnull(t2.tag,'') tag from filemap_rebalance t1 left join filetag t2 on t1.fid=t2.fid")
		if err != nil {
			glog.V(0).Infoln(err)
			return err
		}
		glog.V(0).Infoln(" scaning  from  filemap_rebalance",start," to ",start + num,"partition="+p.Id.String(),"timecost:",time.Since(starttime1))
		start += num
		scannum = 0
		for rows.Next() {
			var filepath, fid, expireat, MTime,tag string
			var length int64
			err = rows.Scan(&filepath, &fid, &expireat, &length, &MTime,&tag)
			if err != nil {
				glog.V(0).Infoln(err)
				return err
			}
			buf[filepath] = []string{fid, expireat, strconv.FormatInt(length, 10), MTime,tag}
			count++
			sum++
			scannum++
			if count >= 10000 {
				b, err := json.Marshal(buf)
				if err != nil {
					glog.V(0).Infoln(err)
					return err
				}
				jsonBlob, err := util.PostBytes(url, b)
				if err != nil {
					glog.V(0).Infoln(err)
					return err
				}
				var ret ParRebalanceResult
				if err = json.Unmarshal(jsonBlob, &ret); err != nil {
					glog.V(0).Infoln(err)
					return err
				}
				if ret.Error != "" {
					return errors.New(ret.Error)
				}
				glog.V(0).Infoln("transfer ",len(buf)," records,partition="+p.Id.String()," timecost:",time.Since(starttime1))
				count = 0
				buf = make(map[string][]string)
			}
		}
		err = rows.Err()
		if err != nil {
			glog.V(0).Infoln(err)
			return err
		}

		if len(buf) > 0 {
			b, err := json.Marshal(buf)
			if err != nil {
				glog.V(0).Infoln(err)
				return err
			}
			jsonBlob, err := util.PostBytes(url+"&eof=true", b)
			if err != nil {
				glog.V(0).Infoln(err)
				return err
			}
			var ret ParRebalanceResult
			if err = json.Unmarshal(jsonBlob, &ret); err != nil {
				glog.V(0).Infoln(err)
				return err
			}
			if ret.Error != "" {
				return errors.New(ret.Error)
			}
			glog.V(0).Infoln("transfer ",len(buf)," records,partition="+p.Id.String()," timecost:",time.Since(starttime1))
		}else if scannum == 0{
			break
		}
	}
	return
}

func (p *MysqlPartition) commitPartition(dstAddr string) (err error) {
	values := make(url.Values)
	values.Add("partition", p.Id.String())
	jsonBlob, err := util.Post("http://"+dstAddr+"/partition/commit_rebalance", values)
	if err != nil {
		glog.V(0).Infoln("parameters:", values)
		return err
	}
	var ret ParRebalanceResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
func (p *MysqlPartition) restoreExtraPartition() (err error) {
	countAddUpdate:=0 //新增和更新的记录合在一起
	countDelete:=0
	defer func() {
		glog.V(0).Infoln("restore filemap!", p.table,"partition="+p.Id.String()," countAddUpdate:",countAddUpdate," countDelete:",countDelete," err:",err)
	}()

	rows, err := p.db.Query("SELECT filepath,fid,expireat,length,last_modified from filemap_rebalance")
	if err != nil {
		return
	}
	stmtUptIns, err := p.db.Prepare("INSERT INTO " + "filemap" + "(filepath,fid,expireat,length,last_modified)values(?,?,?,?,?) ON DUPLICATE KEY UPDATE fid=?,expireat=?,length=?,last_modified=?")
	if err != nil {
		return err
	}
	defer stmtUptIns.Close()
	stmtDel, err := p.db.Prepare("DELETE from filemap WHERE filepath=?")
	if err != nil {
		return err
	}
	defer stmtDel.Close()
	tx, err := p.db.Begin()
	if err != nil {
		glog.V(0).Infoln("failing to begin transacation", err.Error())
		return err
	}

	txs := tx.Stmt(stmtUptIns)
	txd := tx.Stmt(stmtDel)
	defer txs.Close()
	defer txd.Close()
	for rows.Next() {
		var filepath, fid, expireat, MTime string
		var length int64
		if err = rows.Scan(&filepath, &fid, &expireat, &length, &MTime); err != nil {
			tx.Rollback()
			return err
		}
		if fid == "negative" {
			_, err = txd.Exec(filepath)
			if err == nil {
				countDelete++
			}
		} else {
			_, err = txs.Exec(filepath, fid, expireat, length, MTime, fid, expireat, length, MTime)
			if err == nil {
				countAddUpdate++
			}
		}
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	err = rows.Err()
	if err != nil {
		glog.V(0).Infoln(err.Error())
	}

	err = tx.Commit()
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	//清空表
	_, err = p.db.Exec("delete from filemap_rebalance")
	if err != nil {
		glog.V(0).Infoln("Delete from filemap_rebalance", err.Error())
	}
	p.table = "filemap"
	return
}
func (p *MysqlPartition) ParRebalance(dstAddr string) (err error) {
	glog.V(0).Infoln("**********Copy db to*********", dstAddr," partition=" + p.Id.String())
	defer func() {
		if err != nil {
			p.pathLock.LockAll()
			defer p.pathLock.UnLockAll()
			//出现问题了，需要把filemap_rebalance中的数据也合并进来
			p.restoreExtraPartition()
			p.rebal = false
		}
	}()
	//源端准备工作，创建一个filemap_rebalance
	err = p.srcPrepareParRepairOrRebalance()
	if err != nil {
		glog.V(0).Infoln("Failed to prepare local:", err.Error())
		return err
	}
	//远端准备动作，建库建表
	err = p.prepareParRebal(dstAddr)
	if err != nil {
		glog.V(0).Infoln("Failed to prepare dest rebalancing:", err.Error())
		return err
	}
	//进行数据的交互。把本地的filemap中的数据都发送到远端的filemap
	err = p.transferPartition(dstAddr, "/partition/transfer_rebalance")
	if err != nil {
		glog.V(0).Infoln("Failed to transfer data:", err.Error())
		return err
	}
	return
}
func (p *MysqlPartition) ParRebalanceExt(dstAddr string) (err error) {
	p.pathLock.LockAll()
	defer p.pathLock.UnLockAll()
	defer func() {
		//if err != nil {
		p.restoreExtraPartition()  //不管是否成功，最后都要把记录从filemap_rebalance导入到filemap中，并把table指向filemap，防止后面的步骤失败
		//}
		p.rebal = false
	}()
	err = p.transferExtraPartition(dstAddr, "/partition/transfer_extra_rebalance")
	if err != nil {
		glog.V(0).Infoln("Failed to transfer data:", err.Error())
	}
	glog.V(0).Infoln("ParRebalanceExt:",err,"partition="+p.Id.String())
	return
}
func (p *MysqlPartition) ParRebalanceCommit(dstAddr string) (err error) {
	defer func() {
		if err != nil {
			p.pathLock.LockAll()
			defer p.pathLock.UnLockAll()
			p.restoreExtraPartition()
		} else {
			p.db.Exec("delete from filemap_rebalance")
		}
		p.rebal = false
	}()
	err = p.commitPartition(dstAddr)
	return err
}

func (p *MysqlPartition) RebalanceRollBack(){
	p.pathLock.LockAll()
	defer p.pathLock.UnLockAll()
	p.restoreExtraPartition()
	p.rebal = false
	return
}