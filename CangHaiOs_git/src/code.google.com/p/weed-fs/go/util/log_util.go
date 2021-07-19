package util

import (
	"code.google.com/p/weed-fs/go/glog"
	"encoding/json"
	"net/http"
	"strconv"
	"fmt"
)

func LogHandler(w http.ResponseWriter, r *http.Request)  {
    tmpV := r.FormValue("v")
    //judge the formValue
    v , err := strconv.Atoi(tmpV)
    if err != nil {
       w.WriteHeader(http.StatusInternalServerError)
       writeJson(w, r, err.Error())
       return 
    }
    glog.SetVerbosity(v)
    w.WriteHeader(http.StatusOK)
    return
}

func writeJson(w http.ResponseWriter, r *http.Request, obj interface{}) (err error) {
	var bytes []byte
	if r.FormValue("pretty") != "" {
		bytes, err = json.MarshalIndent(obj, "", "  ")
	} else {
		bytes, err = json.Marshal(obj)
	}
	if err != nil {
		return
	}
	callback := r.FormValue("callback")
	if callback == "" {
		w.Header().Set("Content-Type", "application/json")
		_, err = w.Write(bytes)
	} else {
		w.Header().Set("Content-Type", "application/javascript")
		if _, err = w.Write([]uint8(callback)); err != nil {
			return
		}
		if _, err = w.Write([]uint8("(")); err != nil {
			return
		}
		fmt.Fprint(w, string(bytes))
		if _, err = w.Write([]uint8(")")); err != nil {
			return
		}
	}
	return
}



