package storage

import (
	"bytes"
	"code.google.com/p/weed-fs/go/glog"
	"compress/flate"
	"compress/gzip"
	"io/ioutil"
	"strings"
	"strconv"
)

/*
* Default more not to gzip since gzip can be done on client side.
 */
func IsGzippable(ext, mtype string) bool {
	if strings.HasPrefix(mtype, "text/") {
		return true
	}
	switch ext {
	case ".zip", ".rar", ".gz", ".bz2", ".xz":
		return false
	case ".pdf", ".txt", ".html", ".htm", ".css", ".js", ".json":
		return true
	}
	if strings.HasPrefix(mtype, "application/") {
		if strings.HasSuffix(mtype, "xml") {
			return true
		}
		if strings.HasSuffix(mtype, "script") {
			return true
		}
	}
	return false
}

func GzipData(input []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	w, _ := gzip.NewWriterLevel(buf, flate.BestCompression)
	if _, err := w.Write(input); err != nil {
		glog.V(2).Infoln("error compressing data:", err)
		return nil, err
	}
	if err := w.Close(); err != nil {
		glog.V(2).Infoln("error closing compressed data:", err)
		return nil, err
	}
	return buf.Bytes(), nil
}
func UnGzipData(input []byte) ([]byte, error) {
	buf := bytes.NewBuffer(input)
	r, err:= gzip.NewReader(buf)
	if err != nil {
		glog.V(0).Infoln("ungzip data err:",err,len(input))
		return nil,err
	}
	defer r.Close()
	output, err := ioutil.ReadAll(r)
	if err != nil {
		glog.V(2).Infoln("error uncompressing data:", err)
	}
	return output, err
}

func GzipDataLevel(input []byte, level string) ([]byte,bool, error) {
	clevel := flate.NoCompression
	l, _ := strconv.Atoi(level)
	switch l {
		case 1:
			clevel = flate.BestSpeed
		case 2:
			clevel = flate.BestCompression
		case 3:
			clevel = flate.DefaultCompression
		default:
			clevel = flate.NoCompression
	}
	if clevel==flate.NoCompression{
		return input,false,nil
	}
	buf := new(bytes.Buffer)
	w, _ := gzip.NewWriterLevel(buf, clevel)
	 _, err := w.Write(input)
	if  err != nil {
		glog.V(2).Infoln("error compressing data:", err)
		return nil,false, err
	}
	if err := w.Close(); err != nil {
		glog.V(2).Infoln("error closing compressed data:", err)
		return nil,false, err
	}
	return buf.Bytes(), true, nil
}

