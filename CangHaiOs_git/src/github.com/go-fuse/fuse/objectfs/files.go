// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package objectfs

import (
	"github.com/go-fuse/fuse"
	"strconv"
	"time"

	"code.google.com/p/weed-fs/go/glog"
)

// objectFile
type ObjectFile struct {
	IsDir        bool
	Name         string
	LastModified string
	Size         int64
	ObjectAcl    string
	Etag         string
	Fid          string
	Expire       string
}

// Called upon registering the filehandle in the inode.
func (o *ObjectFile) SetInode(*Inode) {
	glog.V(0).Infoln("ObjectFile  SetInode ")

	glog.V(0).Infoln("ObjectFile  SetInode done")

}

// Flush is called for close() call on a file descriptor. In
// case of duplicated descriptor, it may be called more than
// once for a file.
func (o *ObjectFile) Flush() fuse.Status {
	glog.V(0).Infoln("ObjectFile  Flush")
	glog.V(0).Infoln("ObjectFile  Flush done")
	return fuse.OK
}

// The String method is for debug printing.
func (o *ObjectFile) String() string {
	glog.V(0).Infoln("ObjectFile  String")
	glog.V(0).Infoln("ObjectFile  String done")
	return ""
}

// Wrappers around other File implementations, should return
// the inner file here.
func (o *ObjectFile) InnerFile() *ObjectFile {
	glog.V(0).Infoln("ObjectFile  InnerFile")
	glog.V(0).Infoln("ObjectFile  InnerFile done")
	return &ObjectFile{}
}

func (o *ObjectFile) Read(dest []byte, off int64, size int64) (fuse.ReadResult, fuse.Status) {
	glog.V(0).Infoln("ObjectFile  Read")
	glog.V(0).Infoln(len(dest), off, size)
	if o.Size < off {
		return nil, fuse.EINVAL
	}

	glog.V(0).Infoln("ObjectFile  Read done")
	return nil, fuse.OK
}

func (o *ObjectFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	glog.V(0).Infoln("ObjectFile  Write")
	glog.V(0).Infoln(len(data), off)
	glog.V(0).Infoln("ObjectFile  Write done")
	return 0, fuse.OK
}

// This is called to before the file handle is forgotten. This
// method has no return value, so nothing can synchronizes on
// the call. Any cleanup that requires specific synchronization or
// could fail with I/O errors should happen in Flush instead.
func (o *ObjectFile) Release() {
	glog.V(0).Infoln("ObjectFile  Release")
	glog.V(0).Infoln("ObjectFile  Release done")
	return
}

func (o *ObjectFile) Fsync(flags int) (code fuse.Status) {
	glog.V(0).Infoln("ObjectFile  Fsync")
	glog.V(0).Infoln("ObjectFile  Fsync done")
	return fuse.OK
}

// The methods below may be called on closed files, due to
// concurrency.  In that case, you should return EBADF.
func (o *ObjectFile) Truncate(size uint64) fuse.Status {
	glog.V(0).Infoln("ObjectFile  Truncate")
	glog.V(0).Infoln("ObjectFile  Truncate done")
	return fuse.OK
}

func (o *ObjectFile) GetAttr(out *fuse.Attr) fuse.Status {
	glog.V(0).Infoln("ObjectFile  GetAttr")
	out.Mtime, _ = strconv.ParseUint(o.LastModified, 10, 64)
	if o.IsDir {
		out.Mode = fuse.S_IFDIR | 0755
	} else {
		out.Size = uint64(o.Size)
		out.Mode = fuse.S_IFREG | 0644
	}
	glog.V(0).Infoln("ObjectFile  GetAttr done", *out)
	return fuse.OK
}

func (o *ObjectFile) Chown(uid uint32, gid uint32) fuse.Status {
	glog.V(0).Infoln("ObjectFile  Chown")
	glog.V(0).Infoln("ObjectFile  Chown done")
	return fuse.OK
}

func (o *ObjectFile) Chmod(perms uint32) fuse.Status {
	glog.V(0).Infoln("ObjectFile  Chmod")
	glog.V(0).Infoln("ObjectFile  Chmod done")
	return fuse.OK
}

func (o *ObjectFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	glog.V(0).Infoln("ObjectFile  Utimens")
	glog.V(0).Infoln("ObjectFile  Utimens done")
	return fuse.OK
}

func (o *ObjectFile) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	glog.V(0).Infoln("ObjectFile  Allocate")
	glog.V(0).Infoln("ObjectFile  Allocate done")
	return fuse.OK
}
