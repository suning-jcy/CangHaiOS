package sftp

import (
	"encoding"
	//	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"syscall"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"github.com/pkg/errors"
	"fmt"
)

type acclog struct {
	flag       bool
	method     string
	starttime  int64
	filepath   string
	status     uint32
	filelength int64
	uuid       string
}

var maxTxPacket uint32 = 1 << 15

const failretrytime = 5

type handleHandler func(string) string

// Handlers contains the 4 SFTP server request handlers.
type Handlers struct {
	FileGet  FileReader
	FilePut  FileWriter
	FileCmd  FileCmder
	FileInfo FileInfoer
}

const (
	SftpStatusOK                   = 200
	SftpStatusCreated              = 201 // RFC 7231, 6.3.2
	SftpStatusAccepted             = 202 // RFC 7231, 6.3.3
	SftpStatusNonAuthoritativeInfo = 203 // RFC 7231, 6.3.4
	SftpStatusNoContent            = 204 // RFC 7231, 6.3.5
	SftpStatusResetContent         = 205 // RFC 7231, 6.3.6
	SftpStatusPartialContent       = 206 // RFC 7233, 4.1
	SftpStatusMultiStatus          = 207 // RFC 4918, 11.1
	SftpStatusAlreadyReported      = 208 // RFC 5842, 7.1
	SftpStatusIMUsed               = 226 // RFC 3229, 10.4.1

	SftpStatusMultipleChoices  = 300 // RFC 7231, 6.4.1
	SftpStatusMovedPermanently = 301 // RFC 7231, 6.4.2
	SftpStatusFound            = 302 // RFC 7231, 6.4.3
	SftpStatusSeeOther         = 303 // RFC 7231, 6.4.4
	SftpStatusNotModified      = 304 // RFC 7232, 4.1
	SftpStatusUseProxy         = 305 // RFC 7231, 6.4.5

	SftpStatusTemporaryRedirect = 307 // RFC 7231, 6.4.7
	SftpStatusPermanentRedirect = 308 // RFC 7538, 3

	SftpStatusBadRequest                   = 400 // RFC 7231, 6.5.1
	SftpStatusUnauthorized                 = 401 // RFC 7235, 3.1
	SftpStatusPaymentRequired              = 402 // RFC 7231, 6.5.2
	SftpStatusForbidden                    = 403 // RFC 7231, 6.5.3
	SftpStatusNotFound                     = 404 // RFC 7231, 6.5.4
	SftpStatusMethodNotAllowed             = 405 // RFC 7231, 6.5.5
	SftpStatusNotAcceptable                = 406 // RFC 7231, 6.5.6
	SftpStatusProxyAuthRequired            = 407 // RFC 7235, 3.2
	SftpStatusRequestTimeout               = 408 // RFC 7231, 6.5.7
	SftpStatusConflict                     = 409 // RFC 7231, 6.5.8
	SftpStatusGone                         = 410 // RFC 7231, 6.5.9
	SftpStatusLengthRequired               = 411 // RFC 7231, 6.5.10
	SftpStatusPreconditionFailed           = 412 // RFC 7232, 4.2
	SftpStatusRequestEntityTooLarge        = 413 // RFC 7231, 6.5.11
	SftpStatusRequestURITooLong            = 414 // RFC 7231, 6.5.12
	SftpStatusUnsupportedMediaType         = 415 // RFC 7231, 6.5.13
	SftpStatusRequestedRangeNotSatisfiable = 416 // RFC 7233, 4.4
	SftpStatusExpectationFailed            = 417 // RFC 7231, 6.5.14
	SftpStatusTeapot                       = 418 // RFC 7168, 2.3.3
	SftpStatusUnprocessableEntity          = 422 // RFC 4918, 11.2
	SftpStatusLocked                       = 423 // RFC 4918, 11.3
	SftpStatusFailedDependency             = 424 // RFC 4918, 11.4
	SftpStatusUpgradeRequired              = 426 // RFC 7231, 6.5.15
	SftpStatusPreconditionRequired         = 428 // RFC 6585, 3
	SftpStatusTooManyRequests              = 429 // RFC 6585, 4
	SftpStatusRequestHeaderFieldsTooLarge  = 431 // RFC 6585, 5
	SftpStatusUnavailableForLegalReasons   = 451 // RFC 7725, 3

	SftpStatusInternalServerError           = 500 // RFC 7231, 6.6.1
	SftpStatusNotImplemented                = 501 // RFC 7231, 6.6.2
	SftpStatusBadGateway                    = 502 // RFC 7231, 6.6.3
	SftpStatusServiceUnavailable            = 503 // RFC 7231, 6.6.4
	SftpStatusGatewayTimeout                = 504 // RFC 7231, 6.6.5
	SftpStatusHTTPVersionNotSupported       = 505 // RFC 7231, 6.6.6
	SftpStatusVariantAlsoNegotiates         = 506 // RFC 2295, 8.1
	SftpStatusInsufficientStorage           = 507 // RFC 4918, 11.5
	SftpStatusLoopDetected                  = 508 // RFC 5842, 7.2
	SftpStatusNotExtended                   = 510 // RFC 2774, 7
	SftpStatusNetworkAuthenticationRequired = 511 // RFC 6585, 6
)

// RequestServer abstracts the sftp protocol with an http request-like protocol
type RequestServer struct {
	*serverConn
	user             string
	localip          string
	remoteip         string
	rootPath         string
	Handlers         Handlers
	pktMgr           packetManager
	openRequests     map[string]*Request
	openRequestLock  sync.RWMutex
	handleCount      int
	handleCountMutex sync.Mutex
	failpathexpire   int
	failpath         map[string]bool
	failpathLock     sync.RWMutex
	maxfilelen       int64
}

// NewRequestServer creates/allocates/returns new RequestServer.
// Normally there there will be one server per user-session.
func NewRequestServer(rwc io.ReadWriteCloser, h Handlers, rootpath string, time, worknum int, user, localip, remoteip string) *RequestServer {
	svrConn := &serverConn{
		conn: conn{
			Reader:      rwc,
			WriteCloser: rwc,
		},
	}
	return &RequestServer{
		user:           user,
		localip:        localip,
		remoteip:       remoteip,
		rootPath:       rootpath,
		serverConn:     svrConn,
		Handlers:       h,
		pktMgr:         newPktMgr(svrConn, worknum),
		openRequests:   make(map[string]*Request),
		failpath:       make(map[string]bool),
		failpathexpire: time,
	}
}
func (rs *RequestServer) addfailpath(path string) {
	rs.failpathLock.Lock()
	defer rs.failpathLock.Unlock()
	rs.failpath[path] = true
	glog.V(3).Infoln("save fail path  ", path)
	go rs.failpathtimeout(path)
}
func (rs *RequestServer) delfailpath(path string) {
	rs.failpathLock.Lock()
	defer rs.failpathLock.Unlock()
	delete(rs.failpath, path)
}
func (rs *RequestServer) getfailpath(path string) bool {
	rs.failpathLock.RLock()
	defer rs.failpathLock.RUnlock()
	_, ok := rs.failpath[path]
	return ok
}

func (rs *RequestServer) nextRequest(r *Request) (string, error) {
	rs.openRequestLock.Lock()
	defer rs.openRequestLock.Unlock()
	if ok := rs.getfailpath(r.Filepath); ok {
		rs.delfailpath(r.Filepath)
		return "", errors.New("upload fail before")
	}
	rs.handleCount++
	handle := strconv.Itoa(rs.handleCount)
	glog.V(3).Infoln("put one method session ", r.Method, r.Filepath, handle)
	rs.openRequests[handle] = r
	return handle, nil
}

func (rs *RequestServer) getRequest(handle string) (*Request, bool) {
	rs.openRequestLock.RLock()
	defer rs.openRequestLock.RUnlock()
	r, ok := rs.openRequests[handle]
	return r, ok
}
func (rs *RequestServer) failpathtimeout(path string) {
	<-time.After(time.Duration(rs.failpathexpire) * time.Second)
	rs.delfailpath(path)
	glog.V(3).Infoln("timeout,delete failpath ", path)
}
func (rs *RequestServer) closeRequest(handle string) (al acclog, err error) {
	rs.openRequestLock.Lock()
	defer rs.openRequestLock.Unlock()
	if r, ok := rs.openRequests[handle]; ok {
		al.flag = true
		al.method = r.Method
		al.filepath = r.Filepath
		al.starttime = r.starttime
		al.status = uint32(SFTPCode2BaseCode(r.state.status))
		err := r.close()
		al.filelength = r.Filesize
		delete(rs.openRequests, handle)
		if err != nil {
			rs.addfailpath(r.Filepath)
			al.status = SftpStatusBadRequest
			return al, errors.New("close packet handle err")
		}

	} else {
		return al, errors.New("get handle session err")
	}
	return al, nil
}

// Close the read/write/closer to trigger exiting the main server loop
func (rs *RequestServer) Close() error { return rs.conn.Close() }

// Serve requests for user session
func (rs *RequestServer) Serve() error {
	var wg sync.WaitGroup
	runWorker := func(ch requestChan) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rs.packetWorker(ch); err != nil {
				rs.conn.Close() // shuts down recvPacket
			}
		}()
	}
	pktChan := rs.pktMgr.workerChan(runWorker)

	var err error
	var pkt requestPacket
	var pktType uint8
	var pktBytes []byte
	for {
		pktType, pktBytes, err = rs.recvPacket()
		if err != nil {
			break
		}

		pkt, err = makePacket(rxPacket{fxp(pktType), pktBytes})
		if err != nil {
			debug("makePacket err: %v", err)
			rs.conn.Close() // shuts down recvPacket
			break
		}

		pktChan <- pkt
	}

	close(pktChan) // shuts down sftpServerWorkers
	wg.Wait()      // wait for all workers to exit

	return err
}

func getReader(h FileReader, r *Request) (error, uint32) {
	var err error
	var errcode uint32
	reader := r.getReader()
	if reader == nil {
		glog.V(3).Infoln("first get reader fail from struct ,reget reader", r)
		reader, err, errcode = h.Fileread(r)
		if err != nil {
			glog.V(3).Infoln("first get reader fail")
			return err, errcode
		}
		r.setFileState(reader)
		glog.V(3).Infoln("first set reader struct ", r)

	}
	return nil, 0
}

func getWriter(h FileWriter, r *Request) (error, uint32) {
	var err error
	var errcode uint32
	writer := r.getWriter()
	if writer == nil {
		writer, err, errcode = h.Filewrite(r)
		if err != nil {
			return err, errcode
		}
		r.setFileState(writer)
	}
	return nil, 0
}

func getPflag(pkg *sshFxpOpenPacket) uint32 {
	return pkg.Pflags
}

func (rs *RequestServer) packetWorker(pktChan chan requestPacket) error {
	for pkt := range pktChan {

		var al acclog
		var rpkt responsePacket
		al.uuid = fmt.Sprint(pkt.uid())
		al.starttime = time.Now().UnixNano()
		glog.V(3).Infoln("receive type is ", reflect.TypeOf(pkt))
		switch pkt := pkt.(type) {
		case *sshFxInitPacket:
			glog.V(3).Infoln("InitPacket")
			rpkt = sshFxVersionPacket{sftpProtocolVersion, nil}

		case *sshFxpClosePacket:
			//上传/下载之后会进行close操作
			glog.V(3).Infoln("ClosePacket")
			handle := pkt.getHandle()
			var errclose error
			al, errclose = rs.closeRequest(handle)
			if errclose != nil {
				//rs.addfailpath()
				glog.V(0).Infoln("close err:", errclose.Error())
				errclose = errors.New("close_err")
			}
			rpkt = statusFromError(pkt, errclose)

			//acclog
		case *sshFxpRealpathPacket:
			glog.V(3).Infoln("get real path")
			rpkt = rs.cleanPath(pkt)
		case *sshFxpOpenPacket:
			glog.V(3).Infoln("open packet")
			var er error
			var co uint32
			re := requestFromPacket(pkt)
			handle, er := rs.nextRequest(re)
			if er != nil {
				rpkt = statusFromError2(pkt, er, ssh_FX_FAILURE)
			} else {
				pflag := getPflag(pkt)
				glog.V(3).Infoln("open type is", pflag)
				if (pflag & 1) == ssh_FXF_READ {
					glog.V(3).Infoln("process first init reader ")
					er, co = getReader(rs.Handlers.FileGet, re)
				}
				if (pflag & 2) == ssh_FXF_WRITE {
					glog.V(3).Infoln("process first init writer ")
					er, co = getWriter(rs.Handlers.FilePut, re)
				}
				al.status = uint32(SFTPCode2BaseCode(co))
				if er != nil {
					rpkt = statusFromError2(pkt, er, co)
				} else {
					rpkt = sshFxpHandlePacket{pkt.uid(), pkt.id(), handle}
				}
			}
			al.filepath = pkt.Path
			al.method = "Open"
			al.flag = true
		case *sshFxpOpendirPacket:
			glog.V(3).Infoln("open dir packet")
			re := requestFromPacket(pkt)
			//使用stat验证文件夹在不在
			_, code, statuscode := rs.handle(re, pkt)
			if code != ssh_FX_OK {
				glog.V(3).Infoln("code:", code)
				rpkt = sshFxpStatusPacket{
					Uid: pkt.uid(),
					ID:  pkt.id(),
					StatusError: StatusError{
						Code: code,
					},
				}
				al.method = "Opendir"
				al.status = uint32(statuscode)
				al.flag = true
				//statusFromError2(pkt, nil, code)
			} else {
				handle, er := rs.nextRequest(re)
				if er != nil {
					rpkt = statusFromError2(pkt, er, ssh_FX_FAILURE)
				} else {
					rpkt = sshFxpHandlePacket{pkt.uid(), pkt.id(), handle}
				}
			}

		case isOpener:
			glog.V(3).Infoln("get open handle")
			handle, _ := rs.nextRequest(requestFromPacket(pkt))
			rpkt = sshFxpHandlePacket{pkt.uid(), pkt.id(), handle}
		case *sshFxpFstatPacket:
			glog.V(3).Infoln("get stat information")
			handle := pkt.getHandle()
			request, ok := rs.getRequest(handle)
			if !ok {
				rpkt = statusFromError(pkt, syscall.EBADF)
			} else {
				request = requestFromPacket(
					&sshFxpStatPacket{ID: pkt.id(), Path: request.Filepath})
				rpkt, _, _ = rs.handle(request, pkt)
			}
		case *sshFxpFsetstatPacket:
			glog.V(3).Infoln("set stat information")
			handle := pkt.getHandle()
			request, ok := rs.getRequest(handle)
			if !ok {
				rpkt = statusFromError(pkt, syscall.EBADF)
			} else {
				request = requestFromPacket(
					&sshFxpSetstatPacket{ID: pkt.id(), Path: request.Filepath,
						Flags: pkt.Flags, Attrs: pkt.Attrs,
					})
				rpkt, _, _ = rs.handle(request, pkt)
			}
		case hasHandle:
			var status uint32
			handle := pkt.getHandle()
			glog.V(3).Infoln("get handle ", handle, "pkt.uid:", pkt.uid())
			request, ok := rs.getRequest(handle)
			if !ok {
				rpkt = statusFromError(pkt, syscall.EBADF)
				glog.V(3).Infoln("request:", request.uid, "pkt:", pkt.uid(), "rpkt:", rpkt.uid())
			} else {
				request.update(pkt)
				rpkt, status, _ = rs.handle(request, pkt)
				if status > 1 {
					glog.V(6).Infoln("put_get:", status)
					request.SetStatus(status)
				}
				glog.V(3).Infoln("request:", request.uid, "pkt:", pkt.uid(), "rpkt:", rpkt.uid())
			}

		case hasPath:
			//有路径的文件操作

			var statuscode int
			glog.V(3).Infoln("hasHandle process")
			request := requestFromPacket(pkt)
			rpkt, _, statuscode = rs.handle(request, pkt)
			al.filepath = request.Filepath
			al.method = request.Method
			al.starttime = request.starttime
			al.filelength = request.Filesize;
			al.status = uint32(statuscode)
			al.flag = true
			//acclog
		default:
			glog.V(3).Infoln("error packets")
			return errors.Errorf("unexpected packet type %T", pkt)
		}

		spendtime := fmt.Sprint((float64(time.Now().UnixNano())-float64(al.starttime))/1000000, "ms")

		err := rs.sendPacket(rpkt)
		if err != nil {
			//println("send Packet fail")
			glog.V(0).Infoln(err)
			return err
		}
		if al.flag {
			//print sftp log
			// SftpProtln(fromIP, toIP, user, method, fileName string, startTime, endTime int64)
			//if al.method != "Open" && al.method != "Opendir" {
			intempsize := "0"
			outtempsize := "0"
			if al.method == "Put" {
				intempsize = fmt.Sprint(al.filelength)
			} else if al.method == "Get" {
				outtempsize = fmt.Sprint(al.filelength)
			}
			glog.SFtpProtln(al.uuid, rs.localip, rs.remoteip, al.filepath, fmt.Sprint(al.status), al.method, intempsize, outtempsize, spendtime, "", "", rs.user, "", "")
			//glog.SftpProtln(rs.remoteip, rs.localip, rs.user, al.method, al.filepath, al.status, al.starttime, time.Now().UnixNano())
			//	}
		}
	}
	return nil
}

func (rs *RequestServer) cleanPath(pkt *sshFxpRealpathPacket) responsePacket {
	path := pkt.getPath()
	if !filepath.IsAbs(path) {
		path = "/" + rs.rootPath + "/" + path
	} // all paths are absolute

	cleaned_path := filepath.Clean(path)
	return &sshFxpNamePacket{
		ID:  pkt.id(),
		Uid: pkt.uid(),
		NameAttrs: []sshFxpNameAttr{{
			Name:     cleaned_path,
			LongName: cleaned_path,
			Attrs:    emptyFileStat,
		}},
	}
}

func (rs *RequestServer) handle(request *Request, pkt requestPacket) (responsePacket, uint32, int) {
	// fmt.Println("Request Method: ", request.Method)
	rpkt, err, errcode, statuscode := request.handle(rs.Handlers, pkt.uid())
	if err != nil {
		err = errorAdapter(err, errcode)
		rpkt = statusFromError2(pkt, err, errcode)
		if err.Error() != "EOF" {
			glog.V(0).Infoln("err", err.Error(), pkt.uid(), rpkt.uid())
		}

	}

	return rpkt, errcode, statuscode
}

// Wrap underlying connection methods to use packetManager
func (rs *RequestServer) sendPacket(m encoding.BinaryMarshaler) error {
	if pkt, ok := m.(responsePacket); ok {
		rs.pktMgr.readyPacket(pkt)
	} else {
		glog.V(0).Infoln("unexpected packet type")
		return errors.Errorf("unexpected packet type %T", m)
	}
	return nil
}

func (rs *RequestServer) sendError(p ider, err error) error {
	return rs.sendPacket(statusFromError(p, err))
}

// os.ErrNotExist should convert to ssh_FX_NO_SUCH_FILE, but is not recognized
// by statusFromError. So we convert to syscall.ENOENT which it does.
func errorAdapter(err error, errcode uint32) error {
	if err == os.ErrNotExist {
		return syscall.ENOENT
	}
	return err
}
