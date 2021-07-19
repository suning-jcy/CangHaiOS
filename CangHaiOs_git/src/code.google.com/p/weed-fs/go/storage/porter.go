package storage

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
)

const (
	TX_BUF_SIZE = 1024 * 1024
)

func (v *Volume) Backup(delVol bool) (err error) {
	v.dataFile.Close()
	v.nm.Close()
	if delVol {
		if err = os.Remove(v.FileName() + ".idx"); err == nil {
			err = os.Remove(v.FileName() + ".dat")
		}
	} else {
		if err = os.Rename(v.FileName()+".idx", v.FileName()+".idx.bak"); err == nil {
			err = os.Rename(v.FileName()+".dat", v.FileName()+".dat.bak")
		}
	}
	return err
}

//用于卷的文件传输
type PorterSt struct {
	uuid      string
	baseName  string
	suffix    string
	volId     string
	dat       string
	idx       string
	dst       []*Factor
	peers     []string
	dataBase  int64
	idxBase   int64
	datOffset int64
	idxOffset int64
	crypto    hash.Hash
	optype    int
}

func NewPorter(baseName, volId string, dst []*Factor, peers []string, uuid string, optype int) *PorterSt {
	p := &PorterSt{baseName: baseName,
		volId: volId,
		dst: dst,
		peers: peers,
		dat: baseName + ".dat",
		idx: baseName + ".idx",
		dataBase: int64(0),
		idxBase: int64(0),
		datOffset: int64(0),
		idxOffset: int64(0),
		uuid: uuid,
		optype: optype,
	}
	file, err := os.Open(p.dat)
	if err != nil {
		glog.V(0).Infoln("failed to open file", p.dat, err)
		return nil
	}
	defer file.Close()
	if fi, err := file.Stat(); err == nil {
		p.dataBase = fi.Size()
	}
	file, err = os.Open(p.idx)
	if err != nil {
		glog.V(0).Infoln("failed to open file", p.idx, err)
		return nil
	}
	defer file.Close()
	if fi, err := file.Stat(); err == nil {
		p.idxBase = fi.Size()
	}
	p.crypto = md5.New()
	date := time.Now().String()
	p.crypto.Reset()
	p.crypto.Write([]byte(date))
	p.suffix = hex.EncodeToString(p.crypto.Sum(nil))
	return p
}

func (p *PorterSt) transferVolBase(serialNum string) error {
	if err := p.transferVolDat(true,serialNum); err != nil {
		return err
	}
	if err := p.transferVolIdx(true,serialNum); err != nil {
		return err
	}
	return nil
}
func (p *PorterSt) transferVolExt(serialNum string) error {
	if err := p.transferVolDat(false,serialNum); err != nil {
		return err
	}
	if err := p.transferVolIdx(false,serialNum); err != nil {
		return err
	}
	return nil
}

//数据传输完了，commit
//成功了带有rslt=1
func (p *PorterSt) finMigration(success bool,serialNum string) (err error) {
	url := p.txFinUrl()
	if success {
		url = url + "&rslt=1"
	}
	for _, dst := range p.dst {
		if dst.Dest == "" {
			continue
		}
		finalUrl := url+"&serialNum="+serialNum
		glog.V(4).Infoln("finMigration:",dst.Dest, finalUrl, "PUT", nil, nil)
		rsp, err := util.MakeRequest(dst.Dest, finalUrl, "PUT", nil, nil)
		if err != nil {
			return errors.New("storage: not finish migration as " + err.Error())
		}
		if rsp.StatusCode != http.StatusOK {
			return errors.New("storage:Can not complete migration")
		}
	}
	return nil
}
func (p *PorterSt) transferBase(name, baseurl string,sn string, start int64, limit int64) (offset int64, err error) {
	glog.V(0).Infoln("transfering:", name, baseurl, start)
	file, err := os.Open(name)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	txSize := int64(0)
	if fi, err := file.Stat(); err == nil {
		txSize = fi.Size()
	} else {
		return txSize, err
	}
	offset, rdlen := start, 0
	buf := make([]byte, TX_BUF_SIZE)
	limitRdr := io.LimitReader(file, limit)
	for {
		if VolBackOpEnable == false {
			glog.V(0).Infoln("background task is not permitted now,vol:",name)
			err = errors.New("Background task is disabled")
			return
		}
		if VolBackOpInternal != 0 {
			time.Sleep(time.Millisecond * time.Duration(VolBackOpInternal))
		}
		if rdlen, err = limitRdr.Read(buf); err != nil {
			if err == io.EOF && rdlen <= 0 {
				return offset, nil
			} else if err != io.EOF {
				return
			}
		}
		p.crypto.Reset()
		p.crypto.Write(buf[:rdlen])
		url := baseurl + "&sum="
		url = url + hex.EncodeToString(p.crypto.Sum(nil))

		for _, dst := range p.dst {
			if dst.Dest == "" {
				continue
			}
			finalUrl := url+"&serialNum="+sn
			glog.V(4).Infoln("transferBase:",dst.Dest, finalUrl, "POST")
			rsp, err := util.MakeRequest_timeout(dst.Dest, finalUrl, "POST", buf[:rdlen], nil)
			if err != nil {
				return 0, err
			}
			if rsp.StatusCode != http.StatusOK {
				rspData, _ := ioutil.ReadAll(rsp.Body)
				defer rsp.Body.Close()
				m := make(map[string]string)
				json.Unmarshal(rspData, &m)
				return 0, errors.New("storage:Can not transfer vol data" + m["error"] + rsp.Status)
			}
		}
		offset += int64(rdlen)
	}
	return
}
func (p *PorterSt) transferExt(name, baseurl,sn string, start int64) (offset int64, err error) {
	glog.V(0).Infoln("transfering:", name, baseurl, start)
	file, err := os.Open(name)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	txSize := int64(0)
	if fi, err := file.Stat(); err == nil {
		txSize = fi.Size()
	} else {
		return txSize, err
	}
	offset, rdlen := start, 0
	buf := make([]byte, TX_BUF_SIZE)
	for {
		if VolBackOpEnable == false {
			glog.V(0).Infoln("background task is not permitted now,vol:",name)
			err = errors.New("Background task is disabled")
			return
		}
		if VolBackOpInternal != 0 {
			time.Sleep(time.Millisecond * time.Duration(VolBackOpInternal))
		}
		if rdlen, err = file.ReadAt(buf, offset); err != nil {
			if err == io.EOF && rdlen <= 0 {
				return offset, nil
			} else if err != io.EOF {
				return
			}
		}
		p.crypto.Reset()
		p.crypto.Write(buf[:rdlen])
		url := baseurl + "&sum="
		url = url + hex.EncodeToString(p.crypto.Sum(nil))

		for _, dst := range p.dst {
			if dst.Dest == "" {
				continue
			}
			finalUrl := url+"&serialNum="+sn
			glog.V(4).Infoln("transferExt:",dst.Dest, finalUrl, "POST")
			rsp, err := util.MakeRequest_timeout(dst.Dest, finalUrl, "POST", buf[:rdlen], nil)
			if err != nil {
				return 0, err
			}
			if rsp.StatusCode != http.StatusOK {
				rspData, _ := ioutil.ReadAll(rsp.Body)
				defer rsp.Body.Close()
				m := make(map[string]string)
				json.Unmarshal(rspData, &m)
				return 0, errors.New("storage:Can not transfer vol data" + m["error"] + rsp.Status)
			}
		}
		offset += int64(rdlen)
	}
	return
}
func (p *PorterSt) transferVolDat(isBase bool,serialNum string) (err error) {
	if isBase {
		p.datOffset, err = p.transferBase(p.dat, p.txDatUrl(),serialNum, p.datOffset, p.dataBase)
	} else {
		p.datOffset, err = p.transferExt(p.dat, p.txDatUrl(),serialNum, p.datOffset)
	}
	return
}
func (p *PorterSt) transferVolIdx(isBase bool,serialNum string) (err error) {
	if isBase {
		p.idxOffset, err = p.transferBase(p.idx, p.txIdxUrl(),serialNum, p.idxOffset, p.idxBase)
	} else {
		p.idxOffset, err = p.transferExt(p.idx, p.txIdxUrl(),serialNum, p.idxOffset)
	}
	return
}

func (p *PorterSt) txDatUrl() string {
	url := ""
	if p.optype == VOL_REB {
		url = "/admin/upload_rebfile"
	} else if p.optype == VOL_REP {
		url = "/admin/upload_repfile"
	}
	url += "?volumeId=" + p.volId
	url = url + "&ext=" + "dat"
	url += "&uuid=" + p.uuid
	return url
}

func (p *PorterSt) txIdxUrl() string {
	url := ""
	if p.optype == VOL_REB {
		url = "/admin/upload_rebfile"
	} else if p.optype == VOL_REP {
		url = "/admin/upload_repfile"
	}
	url += "?volumeId=" + p.volId
	url = url + "&ext=" + "idx"
	url += "&uuid=" + p.uuid
	return url
}

func (p *PorterSt) txFinUrl() string {
	url := ""
	if p.optype == VOL_REB {
		url = "/admin/commit_rebfile"
	} else if p.optype == VOL_REP {
		url = "/admin/commit_repfile"
	}
	url += "?volumeId=" + p.volId
	url += "&uuid=" + p.uuid
	return url
}

//只有迁移的使用，把自身的移除掉
func (p *PorterSt) backupPeers(delVol bool,serialNum string) (err error) {
	url := "/admin/vol/vol2bak?id=" + p.volId
	if delVol {
		url = url + "&delVol=true"
	}
	for _, peer := range p.peers {
		if peer != "" {
			finalUrl := url+"&serialNum="+serialNum
			glog.V(0).Infoln("storage:back up volume file", peer, finalUrl)
			rsp, err := util.MakeRequest(peer, finalUrl, "", nil, nil)
			if err != nil {
				return err
			}
			if rsp.StatusCode != http.StatusOK {
				return errors.New("storage: can not backup vol " + p.volId + "on" + peer)
			}
		}
	}
	return nil
}
