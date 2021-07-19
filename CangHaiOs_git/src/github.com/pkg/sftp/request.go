package sftp

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"github.com/pkg/errors"
)

// Request contains the data and state for the incoming service request.
type Request struct {
	uid uint64
	// Get, Put, Setstat, Stat, Rename, Remove
	// Rmdir, Mkdir, List, Readlink, Symlink
	Method   string
	Filepath string
	Filesize int64
	Flags    uint32
	Attrs    []byte // convert to sub-struct
	Target   string // for renames and sym-links
	// packet data
	pkt_id uint32

	packets      map[uint64]*packet_data
	packets_lock sync.Mutex
	// reader/writer/readdir from handlers
	stateLock *sync.RWMutex
	state     *state
	starttime int64
}

type state struct {
	writerAt     io.WriterAt
	readerAt     io.ReaderAt
	endofdir     bool // in case handler doesn't use EOF on file list
	readdirToken string
	finfo        []os.FileInfo
	getfinfoflag bool
	status       uint32
}

type packet_data struct {
	uid    uint64
	id     uint32
	data   []byte
	length uint32
	offset int64
}

// New Request initialized based on packet data
func requestFromPacket(pkt hasPath) *Request {
	method := requestMethod(pkt)
	glog.V(3).Infoln("method is ", method)
	request := NewRequest(method, pkt.getPath())
	request.pkt_id = pkt.id()
	request.uid = pkt.uid()
	switch p := pkt.(type) {
	case *sshFxpSetstatPacket:
		request.Flags = p.Flags
		request.Attrs = p.Attrs.([]byte)
	case *sshFxpRenamePacket:
		request.Target = filepath.Clean(p.Newpath)
	case *sshFxpSymlinkPacket:
		request.Target = filepath.Clean(p.Linkpath)
	}
	return request
}

// NewRequest creates a new Request object.
func NewRequest(method, path string) *Request {
	request := &Request{Method: method, Filepath: filepath.Clean(path)}
	request.packets = make(map[uint64]*packet_data)
	request.state = &state{}
	request.stateLock = &sync.RWMutex{}
	request.starttime = time.Now().UnixNano()
	return request
}

// LsSave takes a token to keep track of file list batches. Openssh uses a
// batch size of 100, so I suggest sticking close to that.
func (r *Request) LsSave(token string) {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	r.state.readdirToken = token
}
func (r *Request) Getfinfo() ([]os.FileInfo, bool) {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	return r.state.finfo, r.state.getfinfoflag
}
func (r *Request) Setfinfo(finfo []os.FileInfo, getfinfoflag bool) {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	r.state.finfo = finfo
	r.state.getfinfoflag = getfinfoflag
}
func (r *Request) SetStatus(status uint32) {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	r.state.status = status
}

//Getfinfoflag
func (r *Request) Getfinfoflag() bool {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	return r.state.getfinfoflag
}
func (r *Request) Setfinfoflag(getfinfoflag bool) {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	r.state.getfinfoflag = getfinfoflag
}

// LsNext should return the token from the previous call to know which batch
// to return next.
func (r *Request) LsNext() string {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	return r.state.readdirToken
}

// manage file read/write state
func (r *Request) setFileState(s interface{}) {
	r.stateLock.Lock()
	defer r.stateLock.Unlock()
	switch s := s.(type) {
	case io.WriterAt:
		r.state.writerAt = s
	case io.ReaderAt:
		r.state.readerAt = s

	}
}

func (r *Request) getWriter() io.WriterAt {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	return r.state.writerAt
}

func (r *Request) getReader() io.ReaderAt {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	return r.state.readerAt
}

// For backwards compatibility. The Handler didn't have batch handling at
// first, and just always assumed 1 batch. This preserves that behavior.
func (r *Request) setEOD(eod bool) {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	r.state.endofdir = eod
}

func (r *Request) getEOD() bool {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	return r.state.endofdir
}

// Close reader/writer if possible
func (r *Request) close() error {
	var err error
	rd := r.getReader()
	if c, ok := rd.(io.Closer); ok {
		err = c.Close()
		if err != nil {
			return err
		}
	}

	wt := r.getWriter()
	if c, ok := wt.(io.Closer); ok {
		err = c.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// push packet_data into fifo
func (r *Request) pushPacket(pd *packet_data) {
	r.packets_lock.Lock()
	r.packets[pd.uid] = pd
	r.packets_lock.Unlock()
}

// pop packet_data into fifo
func (r *Request) popPacket(uid uint64) *packet_data {
	r.packets_lock.Lock()
	v, ok := r.packets[uid]
	if ok {
		delete(r.packets, uid)
	}
	r.packets_lock.Unlock()
	return v
}

// called from worker to handle packet/request
func (r *Request) handle(handlers Handlers, uid uint64) (responsePacket, error, uint32, int) {
	var err error
	var errcode uint32
	var rpkt responsePacket
	var statuscode int
	switch r.Method {
	case "Get":
		glog.V(5).Infoln("handle get request", r.uid)
		rpkt, err, errcode, statuscode = fileget(handlers.FileGet, r, uid)
		if rpkt != nil {
			glog.V(5).Infoln("handle get request over ", r.uid, rpkt.uid())
		}
		//glog.V(0).Infoln("err", err.Error())
	case "Put": // add "Append" to this to handle append only file writes
		glog.V(5).Infoln("handle put request")
		rpkt, err, errcode, statuscode = fileput(handlers.FilePut, r, uid)
	case "Setstat", "Rename", "Rmdir", "Mkdir", "Symlink", "Remove":
		glog.V(3).Infoln("handle  dir request")
		rpkt, err, errcode, statuscode = filecmd(handlers.FileCmd, r)
	case "List", "Stat", "Readlink", "Opendir":
		glog.V(3).Infoln("handle  stat request", uid)
		rpkt, err, errcode, statuscode = fileinfo(handlers.FileInfo, r, uid)
		glog.V(3).Infoln(rpkt, err, errcode)

	default:
		return rpkt, errors.Errorf("unexpected method: %s", r.Method), ssh_FX_OP_UNSUPPORTED, statuscode
	}
	return rpkt, err, errcode, statuscode
}

/*func (r Request) getReader2() io.ReaderAt {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	//if r.state.readerAt==nil
}*/

// wrap FileReader handler
func fileget(h FileReader, r *Request, uid uint64) (responsePacket, error, uint32, int) {
	var err error
	var errcode uint32
	reader := r.getReader()
	if reader == nil {
		glog.V(3).Infoln("get reader fail from struct ,reget reader", r)
		reader, err, errcode = h.Fileread(r)
		if err != nil {
			glog.V(3).Infoln("get reader fail")
			return nil, err, errcode, SftpStatusBadRequest
		}
		if reader == nil {
			glog.V(3).Infoln("get reader fail")
			return nil, io.ErrUnexpectedEOF, errcode, SftpStatusBadRequest
		}
		r.setFileState(reader)
		glog.V(3).Infoln("set reader struct ", r)

	}

	pd := r.popPacket(uid)
	if pd == nil {
		glog.V(4).Infoln("can not get Packet:", uid, "Request:", r.uid)
		return nil, io.ErrUnexpectedEOF, ssh_FX_FAILURE, SftpStatusBadRequest
	}
	//println("length", pd.length, maxTxPacket, pd.id, pd.offset)
	data := make([]byte, clamp(pd.length, maxTxPacket))
	n, err := reader.ReadAt(data, pd.offset)
	glog.V(4).Infoln(pd.id, pd.uid, "offset:", pd.offset, "len:", len(data), "length real:", n, err)
	//println("length real", n)
	if err != nil && (err != io.EOF) {
		//println("length real err,pd.id ", pd.id)
		//println("length real,err ", err.Error())
		return nil, err, ssh_FX_FAILURE, SftpStatusInternalServerError
	}
	stat := ssh_FX_OK
	if err == io.EOF {
		if n == 0 {
			return nil, err, ssh_FX_EOF, SftpStatusInternalServerError
		} else {
			stat = ssh_FX_EOF
		}
	}

	//println("length real,pd.id ", pd.id)
	return &sshFxpDataPacket{
		Uid:    pd.uid,
		ID:     pd.id,
		Length: uint32(n),
		Data:   data[:n],
	}, nil, uint32(stat), SftpStatusOK
}

// wrap FileWriter handler
func fileput(h FileWriter, r *Request, uid uint64) (responsePacket, error, uint32, int) {
	var err error
	var errcode uint32
	writer := r.getWriter()
	if writer == nil {
		writer, err, errcode = h.Filewrite(r)
		if err != nil {
			return nil, err, errcode, SftpStatusBadRequest
		}
		r.setFileState(writer)
	}

	pd := r.popPacket(uid)
	if pd == nil {
		glog.V(4).Infoln("can not get Packet:", uid, "Request:", r.uid)
		return nil, io.ErrUnexpectedEOF, ssh_FX_FAILURE, SftpStatusInternalServerError
	}
	_, err = writer.WriteAt(pd.data, pd.offset)
	if err != nil {
		return nil, err, ssh_FX_FAILURE, SftpStatusInternalServerError
	}
	return &sshFxpStatusPacket{
		Uid: pd.uid,
		ID:  pd.id,
		StatusError: StatusError{
			Code: ssh_FX_OK,
		}}, nil, ssh_FX_OK, SftpStatusCreated
}

func SFTPCode2BaseCode(errcode uint32) int {
	switch errcode {
	case ssh_FX_OK:
		return SftpStatusOK
	case ssh_FX_OP_UNSUPPORTED:
		return SftpStatusBadRequest
	case ssh_FX_EOF:
		return SftpStatusInternalServerError
	case ssh_FX_NO_SUCH_FILE:
		return SftpStatusNotFound
	case ssh_FX_PERMISSION_DENIED:
		return SftpStatusBadRequest
	case ssh_FX_FAILURE:
		return SftpStatusBadRequest
	case ssh_FX_BAD_MESSAGE:
		return SftpStatusBadRequest
	case ssh_FX_NO_CONNECTION:
		return SftpStatusBadRequest
	case ssh_FX_CONNECTION_LOST:
		return SftpStatusBadRequest
	case ssh_FX_FILE_IS_A_DIRECTORY:
		return SftpStatusBadRequest
	case ssh_FX_NOT_A_DIRECTORY:
		return SftpStatusBadRequest

	default:
		return SftpStatusBadRequest
	}
}

// wrap FileCmder handler
func filecmd(h FileCmder, r *Request) (responsePacket, error, uint32, int) {
	err, errcode := h.Filecmd(r)
	statuscode := SFTPCode2BaseCode(errcode)
	//	var errcode uint32
	if err != nil {
		return nil, err, errcode, statuscode
	}
	return &sshFxpStatusPacket{
		Uid: r.uid,
		ID:  r.pkt_id,
		StatusError: StatusError{
			Code: ssh_FX_OK,
		}}, nil, ssh_FX_OK, statuscode
}

// wrap FileInfoer handler
func fileinfo(h FileInfoer, r *Request, uid uint64) (responsePacket, error, uint32, int) {
	glog.V(3).Infoln("fileinfo", r.packets)
	if r.getEOD() {
		return nil, io.EOF, ssh_FX_EOF, SftpStatusBadRequest
	}
	finfo, err, errcode := h.Fileinfo(r)
	statuscode := SFTPCode2BaseCode(errcode)
	if err != nil {
		return nil, err, errcode, statuscode
	}

	switch r.Method {
	case "List":
		glog.V(3).Infoln("list", r.Filepath)
		pd := r.popPacket(uid)
		if pd == nil {
			glog.V(4).Infoln("can not get Packet:", uid, "Request:", r.uid)
			return nil, io.ErrUnexpectedEOF, ssh_FX_FAILURE, SftpStatusBadRequest
		}
		dirname := path.Base(r.Filepath)
		ret := &sshFxpNamePacket{ID: pd.id, Uid: pd.uid}

		for _, fi := range finfo {
			glog.V(1).Infoln("list file", fi.Name())
			ret.NameAttrs = append(ret.NameAttrs, sshFxpNameAttr{
				Name:     fi.Name(),
				LongName: runLs(dirname, fi),
				Attrs:    []interface{}{fi},
			})
		}
		// No entries means we should return EOF as the Handler didn't.
		if len(finfo) == 0 {
			return ret, io.EOF, ssh_FX_EOF, SftpStatusNoContent
		}
		// If files are returned but no token is set, return EOF next call.
		/*
			if r.LsNext() == "" {
				r.setEOD(true)
			}*/
		return ret, nil, ssh_FX_OK, SftpStatusOK
	case "Stat":
		glog.V(3).Infoln("list", r.Filepath)
		if len(finfo) == 0 {
			err = &os.PathError{Op: "stat", Path: r.Filepath,
				Err: syscall.ENOENT}
			return nil, err, ssh_FX_NO_SUCH_FILE, SftpStatusNotFound
		}
		return &sshFxpStatResponse{
			Uid:  uid,
			ID:   r.pkt_id,
			info: finfo[0],
		}, nil, ssh_FX_OK, SftpStatusOK

	case "Opendir":
		glog.V(3).Infoln("Opendir", r.Filepath)
		if len(finfo) == 0 {
			err = &os.PathError{Op: "Opendir", Path: r.Filepath,
				Err: syscall.ENOENT}
			return nil, err, ssh_FX_NO_SUCH_FILE, SftpStatusNotFound
		}
		if !finfo[0].IsDir() {
			return nil, nil, ssh_FX_NOT_A_DIRECTORY, SftpStatusBadRequest
		}
		return nil, nil, ssh_FX_OK, SftpStatusOK

	case "Readlink":
		if len(finfo) == 0 {
			err = &os.PathError{Op: "readlink", Path: r.Filepath,
				Err: syscall.ENOENT}
			return nil, err, ssh_FX_NO_SUCH_FILE, SftpStatusNotFound
		}
		filename := finfo[0].Name()
		return &sshFxpNamePacket{
			Uid: uid,
			ID:  r.pkt_id,
			NameAttrs: []sshFxpNameAttr{{
				Name:     filename,
				LongName: filename,
				Attrs:    emptyFileStat,
			}},
		}, nil, ssh_FX_OK, SftpStatusOK
	}
	return nil, err, ssh_FX_FAILURE, SftpStatusBadRequest
}

// file data for additional read/write packets
func (r *Request) update(p hasHandle) error {
	pd := &packet_data{id: p.id(), uid: p.uid()}
	switch p := p.(type) {
	case *sshFxpReadPacket:
		r.Method = "Get"
		pd.length = p.Len
		pd.offset = int64(p.Offset)
	case *sshFxpWritePacket:
		r.Method = "Put"
		pd.data = p.Data
		pd.length = p.Length
		pd.offset = int64(p.Offset)
	case *sshFxpReaddirPacket:
		r.Method = "List"
		glog.V(3).Infoln("list is add pd ", *pd)
	default:
		return errors.Errorf("unexpected packet type %T", p)
	}
	r.pushPacket(pd)
	//glog.V(3).Infoln("list file", pd)
	return nil
}

// init attributes of request object from packet data
func requestMethod(p hasPath) (method string) {
	switch p.(type) {
	case *sshFxpOpenPacket:
		method = "Open"
	case *sshFxpOpendirPacket:
		method = "Opendir"
	case *sshFxpSetstatPacket:
		method = "Setstat"
	case *sshFxpRenamePacket:
		method = "Rename"
	case *sshFxpSymlinkPacket:
		method = "Symlink"
	case *sshFxpRemovePacket:
		method = "Remove"
	case *sshFxpStatPacket, *sshFxpLstatPacket:
		method = "Stat"
	case *sshFxpRmdirPacket:
		method = "Rmdir"
	case *sshFxpReadlinkPacket:
		method = "Readlink"
	case *sshFxpMkdirPacket:
		method = "Mkdir"
	}
	return method
}
