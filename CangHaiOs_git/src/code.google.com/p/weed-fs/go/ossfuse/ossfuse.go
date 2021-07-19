// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// A Go mirror of libfuse's hello.c

package main

import (
	"bytes"
	_ "net/http/pprof"
	"os"
	"bufio"
	"strings"
	"net/http"
	"strconv"
	"time"
	"runtime"
	"encoding/json"
	"crypto/cipher"
	"crypto/aes"
	"encoding/base64"
	"net"
	"path/filepath"
	"path"
	"io"
	"fmt"
	"flag"
	"math/rand"
	"text/template"
	"unicode/utf8"
	"unicode"
	"syscall"
	"os/signal"
	"sync"

	"code.google.com/p/weed-fs/go/util"
	"github.com/go-fuse/fuse/ossfs"
	"code.google.com/p/weed-fs/go/glog"
	"io/ioutil"
	"github.com/go-fuse/fuse"
)

var (
	fuseop FuseOptions
)
var usageTemplate = `
ossfuse  : fuse file systems supported by oss!

Usage:

	ossfuse command [arguments]

The commands are:
    {{.Name | printf "%-11s"}} {{.Short}}

Use "ossfuse help [command]" for more information about a command.

`

var helpTemplate = `{{if .Runnable}}Usage: ossfuse {{.UsageLine}}
{{end}}
  {{.Long}}
`
var IsDebug *bool

type FuseOptions struct {
	accessKeyId       *string
	account           *string
	bucket            *string
	accessKeysecret   *string
	filer             *string
	mountpoint        *string
	conffile          *string
	numtransfer       *int
	contentMD5        *bool
	diskdir           *string
	cacheclear        *bool

	cachesum          *int
	maxcachesum       *int
	cachetimeout      *int
	minsubmitinterval *int
	dirtyscaninterval *int

	maxwrite          *int
	maxbackground     *int
	rememberinodes    *bool
	debug             *bool
	StaticSwitch      *bool
	vMaxCpu           *int

	MaxInodeSum       *int

	FuseIp            *string
	FusePort          *int
	GPort             *int
	MaxShowFileSum    *int
	MaxCachetime      *int
	AllowOther        *bool
	BlockSize         *int
	PortalAddr        *string
	MinPort           *int  //ossfuse进程使用的端口最小值
	MaxPort           *int  //ossfuse进程使用的端口最大值  如果MinPort或者MaxPort有一个没有配置，就使用默认端口
}

func init() {
	cmdOssFuse.Run = runOssFuse

	fuseop.account = cmdOssFuse.Flag.String("account", "", "account to use")
	fuseop.bucket = cmdOssFuse.Flag.String("bucket", "", "bucket to use")
	fuseop.mountpoint = cmdOssFuse.Flag.String("mountpoint", "", "path to mount")
	fuseop.filer = cmdOssFuse.Flag.String("ossendpoint", "", "oss-endpoint")

	fuseop.conffile = cmdOssFuse.Flag.String("conffile", "", "path of accout&id&sec")
	fuseop.contentMD5 = cmdOssFuse.Flag.Bool("contentMD5", false, " check md5sum ")
	fuseop.diskdir = cmdOssFuse.Flag.String("diskdir", "", "local cache")
	fuseop.cacheclear = cmdOssFuse.Flag.Bool("cacheclear", false, "clean cache on exit")

	fuseop.maxcachesum = cmdOssFuse.Flag.Int("maxcachesum", 600, "max cache block sum")
	fuseop.cachetimeout = cmdOssFuse.Flag.Int("cachetimeout", 1, "timeout for cache  ,in s")
	fuseop.numtransfer = cmdOssFuse.Flag.Int("numtransfer", 36, "max transfersum with oss")
	fuseop.minsubmitinterval = cmdOssFuse.Flag.Int("minsubmitinterval", 100, "min file submit Interval to oss,in ms")
	fuseop.dirtyscaninterval = cmdOssFuse.Flag.Int("dirtyscaninterval", 10, "ossfuse dirty block scan interval,in ms")

	fuseop.debug = cmdOssFuse.Flag.Bool("debug", false, "debug mode ,default false")

	fuseop.maxwrite = cmdOssFuse.Flag.Int("maxwrite", 1 << 17, "maxwrite ,default 128K")
	fuseop.maxbackground = cmdOssFuse.Flag.Int("maxbackground", 12, "This numbers controls the allowed number of requests that relate to  async I/O")
	fuseop.rememberinodes = cmdOssFuse.Flag.Bool("rememberinodes", false, "If RememberInodes is set, we will never forget inodes. This may be useful for NFS.")
	fuseop.vMaxCpu = cmdOssFuse.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")

	fuseop.StaticSwitch = cmdOssFuse.Flag.Bool("StaticSwitch", true, " ")

	fuseop.MaxInodeSum = cmdOssFuse.Flag.Int("maxInodeSum", 3000000, "maximum number of Inodess")
	fuseop.MaxShowFileSum = cmdOssFuse.Flag.Int("maxshowfilesum", 3000, "maximum number of files while list")

	fuseop.FuseIp = cmdOssFuse.Flag.String("ip", "", "ip or server name")
	fuseop.FusePort = cmdOssFuse.Flag.Int("port", 9696, "http listen port")
	fuseop.GPort = cmdOssFuse.Flag.Int("gport", 7786, "")
	fuseop.AllowOther = cmdOssFuse.Flag.Bool("allowOther", false, "allow other users to visit fuse mount dir")
	fuseop.MaxCachetime = cmdOssFuse.Flag.Int("maxCacheTime", 2, "how long does a block cache keeps(second)")
	fuseop.BlockSize = cmdOssFuse.Flag.Int("blockSize", 4*1024*1024, "block buffer size")
	fuseop.PortalAddr = cmdOssFuse.Flag.String("portalAddr", "", "portal server address")
	fuseop.MinPort = cmdOssFuse.Flag.Int("minPort", 0, "min port for fuse and it's pprof")
	fuseop.MaxPort = cmdOssFuse.Flag.Int("maxPort", 0, "max port for fuse and it's pprof")
}

var cmdOssFuse = &Command{
	UsageLine: "ossfuse -bucket=bucket -mountpoint=/tmp/ossfs -ossendpoint=10.27.60.236:8888 ",
	Short:     "ossfuse expired objects",
	Long: `This tool is used to connect oss with fuse

  `,
}

func runOssFuse(cmd *Command, args []string) bool {
	OnSighup(func() {
		glog.RotateHttpFile()
	})
	if *fuseop.vMaxCpu < 1 {
		*fuseop.vMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*fuseop.vMaxCpu)

	//必选参数
	if *fuseop.account == "" || *fuseop.bucket == "" || *fuseop.mountpoint == "" || *fuseop.filer == "" || *fuseop.FuseIp == "" {
		glog.Fatalf("account,bucket、mountpoint、ossendpoint、ip can not be null!")
		return false
	}
	ipok := false
	addrs, _ := net.InterfaceAddrs()
	for _, address := range addrs {
		//检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil && ipnet.IP.String() == *fuseop.FuseIp {
				ipok = true
				break
			}
		}
	}
	if !ipok {
		glog.Fatalf("bad ip ")
		return false
	}

	//挂载点信息
	info, err := os.Stat(*fuseop.mountpoint)
	if err != nil || !info.IsDir() {
		glog.Fatalf("can not find mountpoint or it's not a dir")
		return false
	}
	go func() {
		for {
			time.Sleep(10 * time.Minute)
			runtime.GC()
		}
	}()
	go func() {
		for {
			time.Sleep(30 * time.Second)
			glog.RotateLogFile(false)
		}
	}()
	//缓存相关配置
	cacheopt := &CacheOption{}

	if *fuseop.maxcachesum > 10000 || *fuseop.maxcachesum <= 0 {
		*fuseop.maxcachesum = 600
	}
	cacheopt.MAXCapacity = *fuseop.maxcachesum

	if *fuseop.cachetimeout > 180 || *fuseop.cachetimeout <= 0 {
		*fuseop.cachetimeout = 30
	}
	cacheopt.MaxDirtytime = int64(*fuseop.cachetimeout)

	if *fuseop.numtransfer > 100 || *fuseop.numtransfer <= 0 {
		*fuseop.numtransfer = 12
	}
	cacheopt.FlushThreadSum = *fuseop.numtransfer

	if *fuseop.minsubmitinterval > 10000 || *fuseop.minsubmitinterval <= 0 {
		*fuseop.minsubmitinterval = 500
	}
	cacheopt.MinRefreshInterval = int64(*fuseop.minsubmitinterval)
	cacheopt.DirtyScanInterval = int64(*fuseop.dirtyscaninterval)
	cacheopt.MaxCachetime = int64(*fuseop.MaxCachetime)
	cacheopt.Debug = *fuseop.debug

	if *fuseop.BlockSize < (1024*1024){
		*fuseop.BlockSize = 1024*1024
	}
	if *fuseop.BlockSize > (64*1024*1024){
		*fuseop.BlockSize = 64*1024*1024
	}
	if *fuseop.BlockSize % (1024*1024) != 0{
		*fuseop.BlockSize = *fuseop.BlockSize - *fuseop.BlockSize % (1024*1024)
	}
	cacheopt.BlockSize = *fuseop.BlockSize
	//fuse层面配置
	fuseopt := &ossfs.Options{}
	fuseopt.Debug = *fuseop.debug
	fuseopt.MaxWrite = *fuseop.maxwrite
	fuseopt.MaxBackground = *fuseop.maxbackground
	fuseopt.RememberInodes = *fuseop.rememberinodes
	fuseopt.MaxInodesSum = *fuseop.MaxInodeSum
	if *fuseop.MaxShowFileSum > 0 {
		maxshowfileSum = *fuseop.MaxShowFileSum
	}
	fuseopt.AllowOther = *fuseop.AllowOther
	if *fuseop.conffile == "" {
		//没有配置的时候，默认weed所在位置
		dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		*fuseop.conffile = path.Join(dir, "/passwd-ossfs")
	}
	//从配置文件中读取sec
	info, err = os.Stat(*fuseop.conffile)
	if err != nil || info.IsDir() {
		glog.Fatalf("can not get passwd from conffile,please check conffile")
		return false
	}

	conffile, err := os.OpenFile(*fuseop.conffile, os.O_RDWR, 0666)
	if err != nil {
		glog.Fatalf("error while open conf file:", *fuseop.conffile, err)
		return false
	}
	tempread := bufio.NewReader(conffile)
	for {
		line, _, err := tempread.ReadLine()
		if err != nil {
			if err == io.EOF {
				glog.V(0).Info("no avaliable account in conf file:", *fuseop.conffile)
				return false
			}
			glog.V(0).Info("error while read conf file:", *fuseop.conffile, err)
			conffile.Close()
			return false
		}

		realline, err := Decrypt(*fuseop.FuseIp, string(line))
		if err != nil {
			glog.V(0).Info("bad conf,", err)
			continue
		}
		parts := strings.Split(realline, ":")
		if len(parts) != 4 {
			continue
		}
		if *fuseop.account != parts[0] {
			continue
		}
		if path.Join(strings.Trim(parts[3], "\n")) != path.Join(*fuseop.mountpoint) {
			continue
		}

		fuseop.accessKeyId = &(parts[1])
		tempsec := strings.Trim(parts[2], "\n")
		fuseop.accessKeysecret = &tempsec

		break
	}

	conffile.Close()

	//实例化driver
	tempSdossFs := &SdossDriver{
		Account:         *fuseop.account,
		Bucket:          *fuseop.bucket,
		AccessKeyId:     *fuseop.accessKeyId,
		AccessKeysecret: *fuseop.accessKeysecret,
		Filer:           *fuseop.filer,
		FilerSelecter:   util.NewFilerNodes(*fuseop.filer),
	}
	validFiler:=""
	//验证密码
	err,validFiler= tempSdossFs.CheckPasswd()
	if err != nil {
		glog.Fatalf("error login messages,bad id&sec",err)
		return false
	}

	*fuseop.FusePort,*fuseop.GPort = CheckFusePort(*fuseop.MinPort,*fuseop.MaxPort)
	//向portal注册
	var fusePara = registContent{
		AccountName: *fuseop.account,
		BucketName:  *fuseop.bucket,
		Ip:          *fuseop.FuseIp,
		Dir:         *fuseop.mountpoint,
		Cachepath:   "",
		Description: "",
		GateName:    "",
		Port:        strconv.Itoa(*fuseop.FusePort),
		SdossUrl:    validFiler,
	}
	if *fuseop.FusePort == 0 || *fuseop.GPort == 0 {
		glog.Fatalf("no available port!!!",*fuseop.FusePort,*fuseop.GPort)
		return false
	}
	if *fuseop.PortalAddr == "" {
		glog.Fatalf("no portal server address!!!")
		return false
	}

	regOk := Register(*fuseop.PortalAddr,fusePara)
	//注册失败，无法启动
	if !regOk.Result{
		glog.Fatalf("register fuse to portal:",*fuseop.PortalAddr," failed:",regOk.Message," fusePara:",fusePara)
		return false
	}
	go func() {
		gport := fmt.Sprint(*fuseop.GPort)
		gerr := http.ListenAndServe(":" + gport, nil)
		if gerr != nil {
			glog.V(0).Info("open debug/pprof error:", gerr)
		}
	}()
	//缓存
	cache := NewCacheDataMana(tempSdossFs, cacheopt)
	ofs := ossfs.NewOssfs(tempSdossFs, cache)
	server, _, err := ossfs.MountRoot(*fuseop.mountpoint, ofs.Root(), fuseopt)
	if err != nil {
		glog.Fatalf("Mount fail: %v\n", err)
	}
	OnInterrupt(func() {
		server.Unmount()
	})

	//speed
	/*jjj speed log暂时不使用
	go func() {
		speedFilepath := os.TempDir() + "/speed_" + strconv.Itoa(os.Getpid())
		c := time.Tick(1 * time.Second)
		for _ = range c {
			fi, err := os.Stat(speedFilepath)
			if nil == err && *fuseop.StaticSwitch {
				glog.V(0).Info("Start static")
				//start log statics
				speedLog, se := os.OpenFile(speedFilepath, os.O_WRONLY, 0)
				if se != nil {
					glog.V(0).Info("open speed log file error", se.Error())
					continue
				}
				for {
					fi, err = os.Stat(speedFilepath)
					if err != nil || fi == nil {
						speedLog.Close()
						glog.V(0).Info("speed file err:", err, " Stop static")
						break
					}
					tempstr := server.TpsStats.GetStatic()
					_, err = speedLog.WriteAt([]byte(tempstr), 0)
					if err != nil {
						glog.V(0).Infoln("Write SpeedLog error", err)
						break
					}
					time.Sleep(500 * time.Millisecond)
				}
			}
		}
	}()
	*/
	//开启http服务
	listeningAddress := *fuseop.FuseIp + ":" + strconv.Itoa(*fuseop.FusePort)

	glog.V(0).Infoln("Start Weed OssFuse server", util.VERSION, "at", listeningAddress," gport:",*fuseop.GPort)

	fuselistener, e := util.NewListenerWithStopFlagWithoutStats(listeningAddress, 10 * time.Second, nil)
	if e != nil {
		glog.Fatalf(e.Error())
	}

	r := http.NewServeMux()

	//版本
	r.HandleFunc("/sys/ver", fuseVerHandler)
	//系统资源占用
	r.HandleFunc("/sys/machine/status", fuseMacStatusHandler)
	//tps信息
	r.HandleFunc("/stats/tps", fuseTpsHandler(server))
	r.HandleFunc("/log", fuseLogHandler)
	go func() {
		if e := http.Serve(fuselistener, r); e != nil {
			glog.V(0).Infoln("Fail to serve:%s", e.Error())
			glog.Fatalf("Fail to serve:%s", e.Error())
		}
	}()

	server.Serve()
	return true
}

func fuseTpsHandler(server *fuse.Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		tempstr := server.TpsStats.GetStatic()
		bytes, _ := json.Marshal(tempstr)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", fmt.Sprint(len(bytes)))
		w.Write(bytes)
		return
	}
}
func fuseLogHandler(w http.ResponseWriter, r *http.Request) {
	tmpV := r.FormValue("v")
	force:=r.FormValue("force")
	v, err := strconv.Atoi(tmpV)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	glog.SetVerbosity(v)
	glog.V(-1).Infoln("set glog verbosity to",v)
	w.WriteHeader(http.StatusOK)
	rotate:=false
	if force != "" && force == "true" {
		rotate=true
	}
	glog.RotateLogFile(rotate)
	return
}
func fuseVerHandler(w http.ResponseWriter, r *http.Request) {

	m := make(map[string]interface{})
	verstr := "version " + util.VERSION + " " + runtime.GOOS + " " + runtime.GOARCH
	m["Version"] = verstr
	m["InodeCacheSum"] = fmt.Sprintf("%d",ossfs.Getinodesum())
	//w.WriteHeader(http.StatusOK)
	bytes, _ := json.Marshal(m)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprint(len(bytes)))
	w.Write(bytes)
	return

}

func fuseMacStatusHandler(w http.ResponseWriter, r *http.Request) {
	ioutil.ReadAll(r.Body)
	m := make(map[string]interface{})
	var mstatus runtime.MemStats
	runtime.ReadMemStats(&mstatus)
	m["Mem"] = mstatus
	//w.WriteHeader(http.StatusOK)
	bytes, _ := json.Marshal(m)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprint(len(bytes)))
	_, err := w.Write(bytes)
	if err != nil {
		glog.V(0).Infoln(err)
	}
	return

}

func getKey(ip string) []byte {
	strKey := ip + ":oss_fuse_aes_key_suffix"
	arrKey := []byte(strKey)
	//取前16个字节
	return arrKey[:16]
}

//加密字符串
func Encrypt(ip, strMesg string) (string, error) {
	key := getKey(ip)
	var iv = []byte(key)[:aes.BlockSize]
	encrypted := make([]byte, len(strMesg))
	aesBlockEncrypter, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	aesEncrypter := cipher.NewCFBEncrypter(aesBlockEncrypter, iv)
	aesEncrypter.XORKeyStream(encrypted, []byte(strMesg))
	return base64.URLEncoding.EncodeToString(encrypted), nil
}

//解密字符串
func Decrypt(ip, src string) (strDesc string, err error) {
	defer func() {
		//错误处理
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()
	key := getKey(ip)
	tempdata, err := base64.URLEncoding.DecodeString(src)
	if err != nil {
		return "", err
	}
	var iv = []byte(key)[:aes.BlockSize]
	decrypted := make([]byte, len(tempdata))
	var aesBlockDecrypter cipher.Block
	aesBlockDecrypter, err = aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}
	aesDecrypter := cipher.NewCFBDecrypter(aesBlockDecrypter, iv)
	aesDecrypter.XORKeyStream(decrypted, tempdata)

	return string(decrypted), nil
}

func main() {

	rand.Seed(time.Now().UnixNano())
	flag.Usage = ossfuseusage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		ossfuseusage()
	}

	if args[0] == "help" {
		fusehelp(args[1:])
		fmt.Fprintf(os.Stderr, "Default Parameters:\n")
		cmdOssFuse.Flag.PrintDefaults()
		return
	}
	if args[0] == "version" {
		fmt.Printf("version %s %s %s\n", util.VERSION, runtime.GOOS, runtime.GOARCH)
		return
	}

	if cmdOssFuse.Run != nil {
		cmdOssFuse.Flag.Usage = func() {
			cmdOssFuse.Usage()
		}
		cmdOssFuse.Flag.Parse(args[1:])
		args = cmdOssFuse.Flag.Args()
		IsDebug = cmdOssFuse.IsDebug
		if !cmdOssFuse.Run(cmdOssFuse, args) {
			fmt.Fprintf(os.Stderr, "\n")
			cmdOssFuse.Flag.Usage()
			fmt.Fprintf(os.Stderr, "Default Parameters:\n")
			cmdOssFuse.Flag.PrintDefaults()
		}
		exit()
		return
	}

	fmt.Fprintf(os.Stderr, "ossfuse: unknown subcommand %q\nRun 'weed help' for usage.\n", args[0])
	setExitStatus(2)
	exit()
}

// help 相关----------
func fusehelp(args []string) {
	if len(args) == 0 {
		printUsage(os.Stdout)
		// not exit 2: succeeded at 'weed help'.
		return
	}

	tmpl(os.Stdout, helpTemplate, cmdOssFuse)
	// not exit 2: succeeded at 'weed help cmd'.
	return

}
func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToTitle(r)) + s[n:]
}
func ossfuseusage() {
	printUsage(os.Stderr)
	fmt.Fprintf(os.Stderr, "For Logging, use \"ossfuse [logging_options] [command]\". The logging options are:\n")
	flag.PrintDefaults()
	os.Exit(2)
}
func printUsage(w io.Writer) {
	tmpl(w, usageTemplate, cmdOssFuse)
}
func tmpl(w io.Writer, text string, data interface{}) {
	t := template.New("top")
	t.Funcs(template.FuncMap{"trim": strings.TrimSpace, "capitalize": capitalize})
	template.Must(t.Parse(text))
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

// help 相关----------

// Command模板----------
//从weed文件夹下面直接拷贝过来的，这个基本不会改动和变更，至于代码结构，以后重构再说吧
type Command struct {
	// Run runs the command.
	// The args are the arguments after the command name.
	Run       func(cmd *Command, args []string) bool

	// UsageLine is the one-line usage message.
	// The first word in the line is taken to be the command name.
	UsageLine string

	// Short is the short description shown in the 'go help' output.
	Short     string

	// Long is the long message shown in the 'go help <this-command>' output.
	Long      string

	// Flag is a set of flags specific to this command.
	Flag      flag.FlagSet

	IsDebug   *bool

	//add by hujianfei in 20141010
	//to read the param from conf file
	//paramConf *string

	//the sectorNmae in conf file
	//sectorName string
}

func (c *Command) Name() string {
	name := c.UsageLine
	i := strings.Index(name, " ")
	if i >= 0 {
		name = name[:i]
	}
	return name
}

func (c *Command) Usage() {
	fmt.Fprintf(os.Stderr, "Example: weed %s\n", c.UsageLine)
	fmt.Fprintf(os.Stderr, "Default Usage:\n")
	c.Flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "Description:\n")
	fmt.Fprintf(os.Stderr, "  %s\n", strings.TrimSpace(c.Long))
	os.Exit(2)
}

func (c *Command) Runnable() bool {
	return c.Run != nil
}

// Command模板----------

// 中断退出相关----------
func OnInterrupt(fn func()) {
	// deal with control+c,etc
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan,
		os.Interrupt,
		os.Kill,
		syscall.SIGALRM,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		for _ = range signalChan {
			fn()
			os.Exit(0)
		}
	}()
}

func OnSighup(fn func()) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGUSR1)
	go func() {
		for _ = range signalChan {
			fn()
		}
	}()
}

var exitStatus = 0
var exitMu sync.Mutex

func setExitStatus(n int) {
	exitMu.Lock()
	if exitStatus < n {
		exitStatus = n
	}
	exitMu.Unlock()
}

var atexitFuncs []func()

func exit() {
	for _, f := range atexitFuncs {
		f()
	}
	os.Exit(exitStatus)
}

// 中断退出相关----------


type registResult struct {
	Result  bool
	Message string
}

type registContent struct {
	AccountName string `json:"accountName,omitempty"` //account
	BucketName  string `json:"bucketName,omitempty"`  //bucket
	Ip          string `json:"ip,omitempty"`          //fuse的IP
	Cachepath   string `json:"cachepath,omitempty"`   //缓存路径
	Description string `json:"description,omitempty"` //描述
	Dir         string `json:"dir,omitempty"`         //挂载点
	Port        string `json:"port,omitempty"`        //fuse的port
	SdossUrl    string `json:"sdossUrl,omitempty"`    //sdoss集群的域名
	GateName    string `json:"gateName,omitempty"`    //网关
}

//向portal注册
func Register(portalServer string, fusePara registContent) (ret registResult){
	ret.Result = false
	ParaMeter, err := json.Marshal(fusePara)
	if err != nil {
		glog.V(0).Infoln("marshal err:", err)
		ret.Message=err.Error()
		return ret
	}

	url := portalServer + "gateway/saveSelfGateWay.htm"
	client := &http.Client{
		Timeout: 5 * time.Second, ///设定超时时间
	}
	body_buf := bytes.NewBuffer(ParaMeter)
	contentType := "application/json;charset=utf-8"
	request, _ := http.NewRequest("POST", url, body_buf)
	request.Header.Set("Content-Type", contentType)
	response, err := client.Do(request)
	if err != nil {
		ret.Message = err.Error()
		glog.V(0).Infoln(" post req:",url," fail,err:", err)
		return ret
	}
	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	if response.StatusCode == 200 {
		unmarshal_err := json.Unmarshal(body, &ret)
		if unmarshal_err != nil {
			glog.V(0).Infoln("failing to get resonse", url, " err:", unmarshal_err)
			return ret
		}
		glog.V(0).Infoln("register ok! message:", ret.Message)
	} else {
		ret.Message = response.Status
		glog.V(0).Infoln("register fail! statuscode:", response.StatusCode)
	}
	return ret
}

//ossfuse从本地文件中读取可用端口
func PortTest(ip string,port int) (available bool) {
	address:=ip+":"+strconv.Itoa(port)
	l, err := net.Listen("tcp", address)
	if err != nil {
		available = false
		return available
	}
	l.Close()
	available = true
	return available
}

//从可用的端口范围中找到两个端口，分配给fuse和pprof
func CheckFusePort(min,max int)(fuseport,gport int){
	//如果未指定范围，则使用默认端口
	if min == 0 || max == 0 || min >= max{
		return *fuseop.FusePort,*fuseop.GPort
	}
	fuseport = 0
	gport = 0
	var start = min
	for {
		if fuseport == 0{
			if PortTest(*fuseop.FuseIp,start) {
				fuseport = start
			}
			start++
		}
		if gport == 0{
			if PortTest(*fuseop.FuseIp,start) {
				gport = start
			}
			start++
		}

		if start > max || (fuseport !=0 && gport != 0){
			break
		}
	}
	return
}