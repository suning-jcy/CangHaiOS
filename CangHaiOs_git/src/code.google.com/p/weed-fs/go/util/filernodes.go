package util

import (
	"fmt"
	"math/rand"
	"net/http"
	"strings"

	"code.google.com/p/weed-fs/go/glog"
	"sync"
	"time"
)

//if give me one master ,i can get Available from master .but now ,only filers
//this struct is not good
type FilerNodes struct {
	Nodes           []string
	status          []bool  //该地址是否正常
	selnode         string
	lastrefreshtime int64
	refreshchan     chan struct{}
	sync.RWMutex
}

func NewFilerNodes(filers string) (fn *FilerNodes) {
	fn = &FilerNodes{
		Nodes:       strings.Split(filers, ","),
		status:      make([]bool,len(strings.Split(filers, ","))),
		refreshchan: make(chan struct{}, 1),
	}
	for i:=0;i<len(fn.status);i++{
		fn.status[i]=true
	}

	return
}
func (fn *FilerNodes) refresh() {
	if fn.lastrefreshtime == 0 || fn.lastrefreshtime  < time.Now().Add((-30) * time.Second).Unix() {
		for idx:=0;idx<len(fn.Nodes);idx++{
			testClient := &http.Client{
				Timeout: 2 * time.Second, ///设定超时时间
			}
			url := "http://" + fn.Nodes[idx] + "/sys/ver"
			request, err:= http.NewRequest("GET", url, nil)
			if err != nil {
				glog.V(0).Info("make request fail.filer ", fn.Nodes[idx],idx," err:",err)
				fn.status[idx] = false
				continue
			}
			response, err := testClient.Do(request)
			if err != nil {
				fmt.Println("send request failed:", err,"url:",url)
				fn.status[idx] = false
				continue
			}
			defer response.Body.Close()
			if response.StatusCode < 300 {
				fn.status[idx] = true
			}else{
				fn.status[idx] = false
			}
			glog.V(1).Info("filer ", fn.Nodes[idx], " is online?",fn.status[idx],idx)
			continue
		}
		fn.lastrefreshtime = time.Now().Unix()
	}

	start := rand.Intn(len(fn.Nodes))
	nownode := start
	round := 0
	for {
		//glog.V(2).Info(fn.Nodes[nownode],fn.status[nownode],nownode)
		if fn.status[nownode] == false {
			nownode++
			if nownode >= len(fn.Nodes) {
				nownode = 0
				round++
			}
			if round >= 2{
				break
			}
			continue
		}
		break
	}
	tempfile := ""
	if nownode < 0 {
		tempfile = ""
	} else {
		tempfile = fn.Nodes[nownode]
	}
	fn.Lock()
	fn.selnode = tempfile
	fn.Unlock()
}

//随机获取一个filer。每次调用这个函数都随机。为的就是能尽量的均衡使用filer
func (fn *FilerNodes) GetFiler(refresh bool) string {
	if len(fn.Nodes) == 0 {
		return ""
	}
	if refresh || fn.lastrefreshtime == 0 {
		fn.refresh()
	}else {
		go fn.refresh()
	}
	fn.RLock()
	ss:=fn.selnode
	fn.RUnlock()
	return ss
}
