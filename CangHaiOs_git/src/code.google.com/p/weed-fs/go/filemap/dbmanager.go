package filemap

import (
	"bufio"
	"code.google.com/p/weed-fs/go/util"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"strconv"
)

type DbManager struct {
	dirmapconf    string
	filemapconf   []string
	connTimeout   int
	confFile   *os.File
	isLoading bool
	accessLock sync.Mutex
}

func NewDbManager(confFile string) (dm *DbManager, err error) {
	dm = &DbManager{}
	if dm.confFile, err = os.OpenFile(confFile, os.O_RDONLY,0644); err != nil {
		return nil, fmt.Errorf("cannot load conf file %s: %s", confFile, err.Error())
	}
	return dm, dm.load()
}

func (dm *DbManager) processEachLine(line string) error {
	if strings.HasPrefix(line, "#") {
		return nil
	}
	if line == "" {
		return nil
	}
	parts := strings.Split(line, "\t")
	if len(parts) == 0 {
		return nil
	}
	switch parts[0] {
	case "dirdb":
	   dm.dirmapconf = parts[1]
	case "filedb":
     dm.filemapconf = append(dm.filemapconf,parts[1])
    case "conntimeout":
     dm.connTimeout, _ = strconv.Atoi(parts[1])
	default:
		fmt.Printf("line %s has %s!\n", line, parts[0])
		return nil
	}
	return nil
}
func (dm *DbManager) load() error {
	lines := bufio.NewReader(dm.confFile)
	dm.isLoading = true
	defer func() { dm.isLoading = false }()
	for {
		line, err := util.Readln(lines)
		if err != nil && err != io.EOF {
			return err
		}
		if pe := dm.processEachLine(string(line)); pe != nil {
			return pe
		}
		if err == io.EOF {
			return nil
		}
	}
}

func (dm *DbManager) GetFilemapconf() []string {
     return dm.filemapconf
}











