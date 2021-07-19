package storage

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
)

const (
	MYSQL_CONF = "mysql.conf"
)

func DelDsnFromConf(dir, parIdString string, collection string, rp string) (err error) {
	glog.V(0).Infoln("partition delete Dsn From mysql Conf.partition="+parIdString, ", collection =", collection, ", replicaPlacement =", rp)
	confFile, err := os.OpenFile(filepath.Join(dir, MYSQL_CONF), os.O_RDONLY, 0644)
	if err != nil {

		return
	}
	var outbuf []string
	var exist bool
	lines := bufio.NewReader(confFile)
	for {
		line, err := util.Readln(lines)
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}
		str := string(line)
		if strings.HasPrefix(str, "#") {
			outbuf = append(outbuf, str)
			continue
		}
		if str == "" {
			outbuf = append(outbuf, str)
			continue
		}

		if collection != "" {
			exist = strings.Contains(str, "partition="+collection+","+parIdString+",")
		} else {
			exist = strings.Contains(str, "partition="+parIdString+",")
		}
		if exist {
			continue
		} else {
			outbuf = append(outbuf, str)
		}

	}
	confFile.Close()

	confFile, err = os.OpenFile(filepath.Join(dir, MYSQL_CONF), os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(confFile)
	for _, line := range outbuf {
		fmt.Fprintln(w, line)
	}
	w.Flush()
	confFile.Close()

	return nil

}

func genDbName(collection, parId string) string {
	dbname := "sdoss" + "_" + parId
	if collection != "" {
		dbname = "sdoss" + "_" + collection + "_" + parId
	}
	return dbname
}

func WriteNewParToConf(dir, dsn string, parIdStr string, collection string) error {
	fd, err := os.OpenFile(filepath.Join(dir, MYSQL_CONF), os.O_RDWR, 0644)
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	defer fd.Close()
	_, err = fd.Seek(0, 2)
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return err
	}
	w := bufio.NewWriter(fd)
	if collection == "" {
		fmt.Fprintln(w, "partition="+parIdStr+","+dsn)
	} else {
		fmt.Fprintln(w, "partition="+collection+","+parIdStr+","+dsn)
	}
	w.Flush()

	return err
}
