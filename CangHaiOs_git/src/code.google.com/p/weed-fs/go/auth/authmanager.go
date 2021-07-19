package auth

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

type AuthManager struct {
	url    	      string
	interval      int
	connTimeout   int
	confFile   *os.File
	isLoading bool
	accessLock sync.Mutex
}

func NewAuthManager(confFile string) (am *AuthManager, err error) {
	am = &AuthManager{}
	if am.confFile, err = os.OpenFile(confFile, os.O_RDONLY,0644); err != nil {
		return nil, fmt.Errorf("cannot load conf file %s: %s", confFile, err.Error())
	}
	return am, am.load()
}

func (am *AuthManager) processEachLine(line string) error {
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
	case "interval":
	   am.interval, _  = strconv.Atoi(parts[1])
	case "url":
     am.url = parts[1]
    case "conntimeout":
     am.connTimeout, _ = strconv.Atoi(parts[1])
	default:
		fmt.Printf("line %s has %s!\n", line, parts[0])
		return nil
	}
	return nil
}
func (am *AuthManager) load() error {
	lines := bufio.NewReader(am.confFile)
	am.isLoading = true
	defer func() { am.isLoading = false }()
	for {
		line, err := util.Readln(lines)
		if err != nil && err != io.EOF {
			return err
		}
		if pe := am.processEachLine(string(line)); pe != nil {
			return pe
		}
		if err == io.EOF {
			return nil
		}
	}
}












