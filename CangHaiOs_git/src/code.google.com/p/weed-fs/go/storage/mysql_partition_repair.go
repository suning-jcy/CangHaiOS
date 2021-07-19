package storage

import (
	"fmt"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
	//	"database/sql"
	"encoding/json"
	"errors"
	_ "fmt"
	"net/url"
	"strings"
	_ "time"

	_ "github.com/go-sql-driver/mysql"
)

type ParRePairResult struct {
	Result bool
	Error  string
}

func (p *MysqlPartition) ParRepRepair(dstAddr string) error {
	err := p.srcPrepareParRepairOrRebalance()
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	//request repair
	if err := p.prepareParRepair(dstAddr); err != nil {
		p.accessLock.Lock()
		p.endParRepair()
		if err := p.commitParRepair(dstAddr); err != nil {
			glog.V(0).Infoln(err.Error())
		}
		p.accessLock.Unlock()
		return fmt.Errorf("Partition Replicationg Repair: request prepareing for repair,%s", err.Error())
	}
	//transfer
	dsts := strings.Split(dstAddr, ",")
	for _, dst := range dsts {
		if dst != "" {
			glog.V(0).Infoln("Partition Replicationg Repair:Transfering data to ", dst)
			if err := p.transferPartition(dst, "/partition/transfer_repair"); err != nil {
				glog.V(0).Infoln("Partition Replicationg Repair:Failed to transfer data to ", dst, err.Error())
				p.accessLock.Lock()
				glog.V(0).Infoln("Partition Replicationg Repair:Failed to transfer data to ", dst, err.Error(), "End up with failur!")
				p.endParRepair()
				if err := p.commitParRepair(dstAddr); err != nil {
					glog.V(0).Infoln(err.Error())
				}
				p.accessLock.Unlock()
				return fmt.Errorf("Partition Replicationg Repair:transfer error:%s", err.Error())
			}
		}
	}
	glog.V(0).Infoln("Partition Replicationg Repairing:Finished transfering  data to ", dsts)
	//transfer extra data and complete repair
	p.accessLock.Lock()
	glog.V(0).Infoln("Partition Replicationg Repair:Transfering extra repair data...")
	if err := p.applyExtraUpdate(dstAddr); err != nil {
		glog.V(0).Infoln(err.Error())
	}
	if err := p.commitParRepair(dstAddr); err != nil {
		glog.V(0).Infoln(err.Error())
	}
	p.endParRepair()
	p.accessLock.Unlock()
	return nil
}
func (p *MysqlPartition) srcPrepareParRepairOrRebalance() error {
	p.pathLock.LockAll()
	defer p.pathLock.UnLockAll()
	if p.rebal == true {
		glog.V(0).Infoln("partition rebalance or repair already started on", p.Id)
		return errors.New("partition rebalance or repair already started.")
	}
	_, err := p.db.Exec("create table if not exists filemap_rebalance " + filemapColumnDeclaration)
	if err != nil {
		glog.V(0).Infoln("Create filemap_rebalance:", err.Error())
		return err
	}
	if _, err = p.db.Exec("delete from filemap_rebalance"); err != nil {
		glog.V(0).Infoln("Clean table:", err.Error())
		return err
	}
	p.table = "filemap_rebalance"
	p.rebal = true
	return nil
}
func (p *MysqlPartition) prepareParRepair(dstAddr string) (err error) {
	dsts := strings.Split(dstAddr, ",")
	for _, dst := range dsts {
		values := make(url.Values)
		values.Add("collection", p.Collection())
		values.Add("partition", p.Id.String())
		if dst != "" {
			if b, err := util.PostOnce(dst, "/partition/repair_prepare", values); err != nil {
				glog.V(0).Infoln("Failed to request dst node to prepare for repair!", dst)
			} else {
				var ret ParRePairResult
				if err := json.Unmarshal(b, &ret); err != nil {
					return err
				}
				if ret.Error != "" {
					return errors.New(ret.Error)
				}
			}

		}
	}
	glog.V(0).Infoln("Dsts prepare okay!")
	return nil
}

func (p *MysqlPartition) commitParRepair(dstAddr string) (err error) {
	dsts := strings.Split(dstAddr, ",")
	for _, dst := range dsts {
		if dst != "" {
			url := "http://" + dst + "/partition/repair_commit?partition=" + p.Id.String()
			if _, err := util.PostBytes(url, nil); err != nil {
				return fmt.Errorf("Partition Rep Repair, commit error:%s", err.Error())
			}
		}
	}
	return nil
}

func (p *MysqlPartition) applyExtraUpdate(dstAddr string) (err error) {
	dsts := strings.Split(dstAddr, ",")
	for _, dst := range dsts {
		if dst != "" {
			if err := p.transferExtraPartition(dst, "/partition/transfer_extra_repair"); err != nil {
				return fmt.Errorf("Partition Rep Repair, transfer extra error:%s", err.Error())
			}
		}
	}
	return nil
}
func (p *MysqlPartition) endParRepair() error {
	p.restoreExtraPartition()
	_, err := p.db.Exec("drop table filemap_rebalance")
	if err != nil {
		glog.V(0).Infoln(err.Error())
	}
	p.rebal = false
	p.table = "filemap"
	return err

}
