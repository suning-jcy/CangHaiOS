package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	_ "time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
)

//此文件中函数，基本没有使用了

type RepRepairResult struct {
	Result bool
	Error  string
}

const (
	TRANSFER_DAT = iota
	TRANSFER_IDX
)

func (v *Volume) prepareRepFile(dstAddr string) (err error) {
	values := make(url.Values)
	values.Add("volumeId", v.Id.String())
	values.Add("collection", v.Collection)
	values.Add("replacement", v.ReplicaPlacement.String())
	jsonBlob, err := util.Post("http://"+dstAddr+"/admin/prepare_repfile", values)
	if err != nil {
		glog.V(0).Infoln("parameters:", values, dstAddr)
		return err
	}
	var ret RepRepairResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
func (v *Volume) uploadRepFile(file *os.File, id VolumeId, filename string, dstAddr string) (err error) {
	buf := make([]byte, 1024*1024)
	url := "http://" + dstAddr + "/admin/upload_repfile?volumeId=" + id.String() + "&filename=" + filename
	offset := int64(0)
	var jsonBlob []byte
	var count int
	for {
		count, err = file.ReadAt(buf, offset)
		if err != nil {
			break
		}
		jsonBlob, err = util.PostBytes(url, buf)
		if err != nil {
			return err
		}
		var ret RepRepairResult
		if err := json.Unmarshal(jsonBlob, &ret); err != nil {
			return err
		}
		if ret.Error != "" {
			return errors.New(ret.Error)
		}
		offset += int64(count)
	}
	if err == io.EOF {
		jsonBlob, err = util.PostBytes(url+"&eof=true", buf[:count])
	}
	if err != nil {
		return err
	}
	var ret RepRepairResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return
}

func (v *Volume) commitRepFile(vid VolumeId, dstAddr string) (err error) {
	values := make(url.Values)
	values.Add("volumeId", vid.String())
	jsonBlob, err := util.Post("http://"+dstAddr+"/admin/commit_repfile", values)
	if err != nil {
		glog.V(0).Infoln("parameters:", values)
		return err
	}
	var ret RepRepairResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}

func (v *Volume) RepairRep(dstAddr string) error {
	glog.V(3).Infof("repairReping ...")
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	glog.V(3).Infof("Got RepairRep lock...")

	err := v.prepareRepFile(dstAddr)
	if err != nil {
		return err
	}
	filePath := v.FileName()
	glog.V(3).Infof("creating copies for volume %d ...", v.Id)
	srcdatafile, err := os.Open(filePath + ".dat")
	if err != nil {
		return err
	}
	err = v.uploadRepFile(srcdatafile, v.Id, ".dat", dstAddr)
	srcidxfile, err := os.Open(filePath + ".idx")
	if err != nil {
		return err
	}
	err = v.uploadRepFile(srcidxfile, v.Id, ".idx", dstAddr)
	if err != nil {
		return err
	}
	err = v.commitRepFile(v.Id, dstAddr)

	return err
}

func (v *Volume) RepairByNodeVidRep(dstNodeList []string) error {
	//prepare
	var offsetDat int64 = int64(0)
	var offsetIdx int64 = int64(0)
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	if err := v.volSysOpStatus.trySetREP(); err != nil {
		glog.V(0).Infoln("v.volStatus", v.volSysOpStatus.getStatus())
		return err
	}
	defer v.volSysOpStatus.reset()

	/*
		if v.RepairStatus != VOL_REPAIR_NOT_REPAIR || v.CompactStatus != NotCompact || v.RebalanceStatus != NotRebalance {
			v.accessLock.Unlock()
			glog.V(0).Infoln("v.RepairStatus", v.RepairStatus)
			return errors.New("Can not repair now!")
		}
	*/

	for _, dstAddr := range dstNodeList { //request rep srv to prepare for recieve rep file
		err := v.prepareRepFile(dstAddr)
		if err != nil {
			return errors.New("Fail to request " + dstAddr + "to prepare for replication repair!" + err.Error())
		}
	}

	//transfer data to all rep srv
	offsetDat, errdat := v.transFileWithCRC(TRANSFER_DAT, 5, dstNodeList, false, 0)
	if errdat != nil {
		glog.V(0).Infoln(errdat.Error())
		//v.RepairStatus = VOL_REPAIR_TRANSFER_DAT_FAIL
	}
	offsetIdx, erridx := v.transFileWithCRC(TRANSFER_IDX, 8, dstNodeList, false, 0)
	if erridx != nil {
		glog.V(0).Infoln(erridx.Error())
		//v.RepairStatus = VOL_REPAIR_TRANSFER_IDX_FAIL
	}

	if errdat == nil && erridx == nil {
		if fi, err := v.dataFile.Stat(); err == nil {
			if fi.Size() > offsetDat {
				if _, err := v.transFileWithCRC(TRANSFER_DAT, 5, dstNodeList, false, offsetDat); err != nil {
					glog.V(0).Infoln(err.Error())
					errdat = err
					//v.RepairStatus = VOL_REPAIR_TRANSFER_DAT_FAIL
				}
				if _, err := v.transFileWithCRC(TRANSFER_IDX, 8, dstNodeList, false, offsetIdx); err != nil {
					glog.V(0).Infoln(err.Error())
					erridx = err
					//v.RepairStatus = VOL_REPAIR_TRANSFER_IDX_FAIL
				}
			}
		}
	}
	glog.V(2).Infoln("repair:", v.Id, "offsetDat", offsetDat, "offsetIdx", offsetIdx)
	//commit repair

	if err := v.commitRepairByNodeVid(dstNodeList, errdat != nil || erridx != nil); err != nil {
		glog.V(0).Infoln("Fail when commit repair:", err.Error())
		return errors.New("Fail when commit repair:" + err.Error())
	}
	glog.V(0).Infoln("Finish replication repair!")

	return nil
}
func (v *Volume) commitRepairByNodeVid(dstNodeList []string, rollback bool) error {
	var postRslt RepRepairResult
	//var jsonBlob []byte
	var urlbase string = "http://"
	commitOpType := "1"
	commitInterface := "/admin/commit_repfile"
	if rollback {
		commitOpType = "0"
	}

	//defer v.volStatus.reset()

	glog.V(0).Infoln(" Start commit, repair  ", commitOpType, dstNodeList)
	values := make(url.Values)
	values.Add("rslt", commitOpType)
	values.Add("volumeId", v.Id.String())
	for _, dst := range dstNodeList {
		glog.V(0).Infoln(" Start commit, repair status", dst)
		if jsonBlob, err := util.Post_timeout(urlbase+dst+commitInterface, values); err != nil {
			if err := json.Unmarshal(jsonBlob, &postRslt); err != nil {
				glog.V(0).Infoln(" Start commit, repair status", dst, 2)
				if postRslt.Result == false {
					glog.V(0).Infoln("Commit repair fail:", dst, postRslt.Error)
					return errors.New("Commit repair fail:" + dst + postRslt.Error)
				} else {
					glog.V(0).Infoln("Commit reapair fail:", dst, err.Error())
					return errors.New("Commit repair fail:" + dst + err.Error())
				}
			} else {
				glog.V(0).Infoln("Commit repair fail:", dst, err.Error())
				return errors.New("Commit repair fail:" + dst + err.Error())
			}

		}
	}
	glog.V(0).Infoln("All Reqlication repair have been committed  successfully!")
	return nil
}
func (v *Volume) transFileWithCRC(transType int, try int, dstNodeList []string, isExtraData bool, lastPos int64) (int64, error) {
	var postRslt RepRepairResult
	var file *os.File
	//var jsonBlob []byte
	//var count int = 0
	var offset int64 = int64(0)
	var buf []byte = make([]byte, 1024*1024)
	var urlbase string = "http://"
	var filePath string = v.FileName()
	var isEOF bool = false
	var fext string = ".dat.rep"
	transInterface := "/admin/upload_repfile?volumeId=" + v.Id.String()
	if transType == TRANSFER_DAT {
		file = v.dataFile
	} else if transType == TRANSFER_IDX {
		file = v.nm.(*NeedleMap).indexFile
		fext = ".idx.rep"
	} else {
		return 0, errors.New("Unknown file type to transfer!")
	}
	transInterface += "&filename=" + fext
	if isExtraData {
		offset = lastPos
		file.Seek(offset, 0)
	}
	for {
		count, err := file.ReadAt(buf, offset)
		if err != nil {
			if err == io.EOF {
				isEOF = true
			} else {
				return 0, errors.New("Fail when read file" + filePath + err.Error())
			}
		}
		crcBuf := NewCRC(buf[:count])
		crcValue := crcBuf.Value()
		crcStrCode := fmt.Sprintf("%d", crcValue)
		urlPosfix := "&crc=" + crcStrCode
		if isEOF {
			glog.V(0).Infoln("Set eof!")
			urlPosfix += "&eof=true"
		}
		for _, dst := range dstNodeList {
			url := urlbase + dst + transInterface + urlPosfix
			for retry := 0; retry < try; retry++ {
				glog.V(3).Infoln("Send data to", dst, "datalen:", count)
				jsonBlob, err := util.PostBytes(url, buf[:count])
				if err != nil && retry < try {
					glog.V(0).Infoln("Try to send data for", retry, "times!")
					continue
				} else if err != nil && retry >= try {
					return 0, errors.New("Fail to send data to " + url + ":" + err.Error())
				}
				json.Unmarshal(jsonBlob, &postRslt)
				if postRslt.Result == true {
					break
				} else if postRslt.Result == false && retry < try {
					if postRslt.Error == "Need Retransfer" {
						glog.V(0).Infoln("Need Retransfer ", url, retry)
						continue
					} else {
						return 0, errors.New("Fail to send data to " + url + ":" + postRslt.Error)
					}
				} else if postRslt.Result == false && retry >= try {
					return 0, errors.New("Fail to send data to " + url + ":" + postRslt.Error)
				}
			}
		}
		offset += int64(count)
		if isEOF == true {
			break
		}
	}
	return offset, nil
}
