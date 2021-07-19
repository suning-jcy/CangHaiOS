package storage

import (
	//"bufio"
	"code.google.com/p/weed-fs/go/glog"
	//"code.google.com/p/weed-fs/go/util"
	"database/sql"
	//"encoding/json"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	//"io"
	//"os"
	//"path/filepath"
	//"strings"
)

const (
	PARREPREPAIR_NOTREPAIR = iota
	PARREPREPAIR_INIT
	PARREPREPAIR_PREPARED
	PARREPREPAIR_TRANSFER
	PARREPREPAIR_TRANSFEREXTRA
	PARREPREPAIR_COMMIT
	PARREPREPAIR_AS_CLIENT
	PARREPREPAIR_AS_SERVER
)

type ParRepRepairStatus struct {
	parId      PartitionId
	collection string
	db         *sql.DB
	dsn        string
	rp         *ReplicaPlacement
	status     int
}

func (s *FilerPartitionStore) PartitionRepair(parIdString, collection, dstAddr string) (error, bool) {
	s.accessLock.Lock()
	s.parRepRepairStatus.status = PARREPREPAIR_INIT
	s.accessLock.Unlock()
	defer func() {
		s.parRepRepairStatus.status = PARREPREPAIR_NOTREPAIR
	}()
	parId, err := NewPartitionId(parIdString)
	if err != nil {
		return fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdString), false
	}
	if par := s.GetPartition(collection, parId); par != nil {
		if parRepRepair, ok := par.(PartitionRepair); ok {
			if err = parRepRepair.ParRepRepair(dstAddr); err != nil {
				return fmt.Errorf("Partition Replication Repair failed%s %s", parIdString, err.Error()), false
			} else {
				glog.V(0).Infoln("Partition Replication Repair Sueccess!", parIdString, collection)
				s.Join()
				return nil, true
			}
		} else {
			return fmt.Errorf("A runtime error!"), false
		}
	}

	return fmt.Errorf("No local partition found%s %s", parIdString, collection), false
}
func (s *FilerPartitionStore) GetPartitionRepairStatus() (error, bool) {
	s.accessLock.Lock()
	defer s.accessLock.Unlock()
	return nil, s.parRepRepairStatus.status == PARREPREPAIR_NOTREPAIR
}
func (s *FilerPartitionStore) PartitionRepairPrepare(parIdString, collection string) error {
	s.accessLock.Lock()
	if s.parRepRepairStatus.status != PARREPREPAIR_NOTREPAIR {
		s.accessLock.Unlock()
		return fmt.Errorf("A partition repair is under processing now!")

	} else {
		s.parRepRepairStatus.status = PARREPREPAIR_INIT
		s.accessLock.Unlock()
	}
	parId, err := NewPartitionId(parIdString)
	if err != nil {
		return fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdString)
	}
	s.parRepRepairStatus.parId = parId
	s.parRepRepairStatus.collection = collection
	if par := s.GetPartition(collection, parId); par != nil {
		if parRepRepair, ok := par.(PartitionRepair); ok {
			parRepRepair.parRepairSetRepStat()
		} else {
			return fmt.Errorf("A runtime error")
		}
		replicaPlacement := par.Rp()
		rp := replicaPlacement.String()
		if s.storetype == "mysql" {
			publicdb, dbprefix, err := s.GetPublicMysqlDB()
			if err != nil {
				glog.V(0).Infoln("get public dsn err", err.Error())
				return err
			}
			defer publicdb.Close()
			dbname := "sdoss" + "_" + parIdString + "_" + rp
			if collection != "" {
				dbname = "sdoss" + "_" + collection + "_" + parIdString + "_" + rp
			}

			_, err = publicdb.Exec("create database if not exists " + dbname)
			if err != nil {
				glog.V(0).Infoln("create database err", dbname, err.Error())
				return err
			}
			s.parRepRepairStatus.dsn = dbprefix + "/" + dbname

			s.parRepRepairStatus.db, err = sql.Open("mysql", s.parRepRepairStatus.dsn)
			if err != nil {
				glog.V(0).Infoln("open database err", s.parRepRepairStatus.dsn, err.Error())
				return err
			}
			isExist := 0
			err = s.parRepRepairStatus.db.QueryRow("select count(*) from information_schema.tables  where table_schema=? and table_name='filemap'", dbname).Scan(&isExist)
			switch {
			case err == sql.ErrNoRows:
				break
			case err != nil:
				glog.V(0).Infoln("DB error", err.Error())
				return err
			case isExist > 0:
				return fmt.Errorf("partition already exists in db")
			}
			_, err = s.parRepRepairStatus.db.Exec("create table if not exists filemap (filepath varchar(255) NOT NULL,fid varchar(20) NOT NULL,primary key (filepath))")
			if err != nil {
				glog.V(0).Infoln(err.Error())
			}
			_, err = s.parRepRepairStatus.db.Exec("delete from filemap")
			if err != nil {
				glog.V(0).Infoln(err.Error())
			}
			s.parRepRepairStatus.status = PARREPREPAIR_PREPARED
			s.parRepRepairStatus.rp = replicaPlacement
			return err
		} else {
			fmt.Errorf("Store type not suported!")
		}

	}
	return fmt.Errorf("No local partition found with id:%s %s", parIdString, collection)
}
func (s *FilerPartitionStore) ParRepairReceive(parIdStr string, isEof string, data []byte) error {
	if s.parRepRepairStatus.status != PARREPREPAIR_PREPARED {
		return fmt.Errorf("partition rebalance have not started %d, %d", s.parRepRepairStatus.parId, s.parRepRepairStatus.status)
	}
	if isEof == "true" {
		s.parRepRepairStatus.status = PARREPREPAIR_TRANSFER
	}
	if s.parRepRepairStatus.parId.String() != parIdStr {
		return fmt.Errorf("Partition Id %s is not as same as store repairing %d", parIdStr, s.parRepRepairStatus.parId)
	}
	err := batchCreateFile(s.parRepRepairStatus.db, data,parIdStr)
	if err != nil {
		return err
	}
	glog.V(4).Infoln("partition upload success", parIdStr, ", data len =", len(data))
	return nil
}
func (s *FilerPartitionStore) ParRepairExtraReceive(parIdStr string, isEof string, data []byte) error {
	if s.parRepRepairStatus.status != PARREPREPAIR_TRANSFER {
		return fmt.Errorf("partition %d rebalance have not started,%d", s.parRepRepairStatus.parId, s.parRepRepairStatus.status)
	}
	if isEof == "true" {
		s.parRepRepairStatus.status = PARREPREPAIR_TRANSFEREXTRA
	}
	if s.parRepRepairStatus.parId.String() != parIdStr {
		return fmt.Errorf("Partition Id %s is not as same as store repairing %d", parIdStr, s.parRepRepairStatus.parId)
	}
	err := batchCreateFileExtra(s.parRepRepairStatus.db, data)
	if err != nil {
		s.parRepRepairStatus.status = PARREPREPAIR_NOTREPAIR
		return err
	}

	glog.V(4).Infoln("partition extra upload success", parIdStr, ", data len =", len(data))
	return nil
}
func (s *FilerPartitionStore) ParRepairCommit(parIdStr string) error {
	s.accessLock.Lock()
	defer s.accessLock.Unlock()
	defer func() {
		s.parRepRepairStatus.status = PARREPREPAIR_NOTREPAIR
		s.Join()
	}()
	if s.parRepRepairStatus.parId.String() != parIdStr {
		return fmt.Errorf("Partition Id %s is not as same as store repairing %d", parIdStr, s.parRebalanceStatus.parId)
	}
	pId, err := NewPartitionId(parIdStr)
	if err != nil {
		return fmt.Errorf("Partition Id %s is not a valid unsigned integer!", parIdStr)
	}
	if par := s.GetPartition(s.parRepRepairStatus.collection, pId); par != nil {
		if parRepRepair, ok := par.(PartitionRepair); ok {
			parRepRepair.parRepairResetStat()
		} else {
			return fmt.Errorf("A runtime error")
		}
	}
	return nil
}
