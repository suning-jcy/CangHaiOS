package operation

import (
	"encoding/hex"
	"encoding/json"
	"errors"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
)

type SyskeyResult struct {
	Syskey    string `json:"syskey,omitempty"`
	OldSyskey string `json:"oldsyskey,omitempty"`
	Error     string `json:"error,omitempty"`
}

func GetSysKey(server string) (string, error) {
	jsonBlob, err := util.Get("http://" + server + "/sys/syskey")
	glog.V(2).Info("get syskey result :", server, string(jsonBlob))
	if err != nil {
		return "", err
	}
	var ret SyskeyResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return "", err
	}
	if ret.Error != "" {
		return "", errors.New(ret.Error)
	}
	bsyskey, err := hex.DecodeString(ret.Syskey)
	return string(bsyskey), err
}
