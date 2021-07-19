package storage

import (
	"encoding/json"
	"errors"
	_ "fmt"
	"io"
	"net/url"
	"os"
	_ "time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
)

type ReblanceResult struct {
	Result bool
	Error  string
}

func (v *Volume) prepareRebFile(dstAddr string) (err error) {
	values := make(url.Values)
	values.Add("volumeId", v.Id.String())
	values.Add("collection", v.Collection)
	values.Add("replacement", v.ReplicaPlacement.String())
	jsonBlob, err := util.Post("http://"+dstAddr+"/admin/prepare_rebfile", values)
	if err != nil {
		glog.V(0).Infoln("parameters:", values, dstAddr)
		return err
	}
	var ret ReblanceResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
func (v *Volume) uploadRebFile(file *os.File, id VolumeId, filename string, dstAddr string) (err error) {
	buf := make([]byte, 1024*1024)
	url := "http://" + dstAddr + "/admin/upload_rebfile?volumeId=" + id.String() + "&filename=" + filename
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
		var ret ReblanceResult
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
	var ret ReblanceResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return
}
func (v *Volume) commitRebFile(vid VolumeId, dstAddr string) (err error) {
	values := make(url.Values)
	values.Add("volumeId", vid.String())
	jsonBlob, err := util.Post("http://"+dstAddr+"/admin/commit_rebfile", values)
	if err != nil {
		glog.V(0).Infoln("parameters:", values)
		return err
	}
	var ret ReblanceResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}

//main control
func (v *Volume) Rebalance(dstAddr string) error {
	glog.V(3).Infof("rebalancing ...")
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	glog.V(3).Infof("Got rebalance lock...")

	err := v.prepareRebFile(dstAddr)
	if err != nil {
		return err
	}
	filePath := v.FileName()
	glog.V(3).Infof("creating copies for volume %d ...", v.Id)
	srcdatafile, err := os.Open(filePath + ".dat")
	if err != nil {
		return err
	}
	defer srcdatafile.Close()
	glog.V(3).Infoln("uploading dat", v.Id)
	err = v.uploadRebFile(srcdatafile, v.Id, ".dat", dstAddr)
	srcidxfile, err := os.Open(filePath + ".idx")
	if err != nil {
		return err
	}
	defer srcidxfile.Close()
	glog.V(3).Infoln("uploading idx", v.Id)
	err = v.uploadRebFile(srcidxfile, v.Id, ".idx", dstAddr)
	if err != nil {
		return err
	}
	glog.V(3).Infoln("committing", v.Id)
	err = v.commitRebFile(v.Id, dstAddr)
	//if err != nil {
	//   return err
	//}
	glog.V(3).Infoln("destroying", v.Id)
	//err = v.Destroy()
	//glog.V(3).Infoln("destroyed", v.Id)
	return err
}
