// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package objectfs

import (
	"github.com/go-fuse/fuse"
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"code.google.com/p/weed-fs/go/glog"
)

// refCountedInode is used in clientInodeMap. The reference count is used to decide
// if the entry in clientInodeMap can be dropped.
type refCountedInode struct {
	node     *objectInode
	refCount int
}

// PathNodeFs is the file system that can translate an inode back to a
// path.  The path name is then used to call into an object that has
// the FileSystem interface.
//
// Lookups (ie. FileSystem.GetAttr) may return a inode number in its
// return value. The inode number ("clientInode") is used to indicate
// linked files.
type Objectfs struct {
	debug bool
	//具体实现方法的地方
	fs        FileSystem
	root      *objectInode
	connector *FileSystemConnector

	// protects clientInodeMap
	objectLock sync.RWMutex

	// This map lists all the parent links known for a given inode number.
	clientInodeMap map[uint64]*refCountedInode
}

// NewPathNodeFs returns a file system that translates from inodes to
// path names.
func NewObjectFs(fs FileSystem) *Objectfs {
	root := &objectInode{}
	root.fs = fs
	ofs := &Objectfs{
		fs:             fs,
		root:           root,
		clientInodeMap: map[uint64]*refCountedInode{},
	}
	root.objectFs = ofs
	return ofs
}

// Root returns the root node for the path filesystem.
func (fs *Objectfs) Root() *objectInode {
	return fs.root
}

// This is a combination of dentry (entry in the file/directory and
// the inode). This structure is used to implement glue for FSes where
// there is a one-to-one mapping of paths and inodes.
type objectInode struct {
	objectFs *Objectfs
	fs       FileSystem

	// This is to correctly resolve hardlinks of the underlying
	// real filesystem.
	clientInode uint64
	inode       *Inode
}

func (n *objectInode) OnMount(conn *FileSystemConnector) {
	glog.V(0).Infoln("objectInode  OnMount")

	n.objectFs.connector = conn
	n.objectFs.fs.OnMount(n.objectFs)
}

func (n *objectInode) OnUnmount() {
	glog.V(0).Infoln("objectInode  OnUnmount")
}

// Drop all known client inodes. Must have the treeLock.
func (n *objectInode) forgetClientInodes() {
	glog.V(0).Infoln("objectInode  forgetClientInodes")
	n.clientInode = 0
	for _, ch := range n.Inode().FsChildren() {
		ch.Node().(*objectInode).forgetClientInodes()
	}
}

func (fs *objectInode) Deletable() bool {
	return true
}

func (n *objectInode) Inode() *Inode {
	return n.inode
}

func (n *objectInode) SetInode(node *Inode) {
	n.inode = node
}

// Reread all client nodes below this node.  Must run outside the treeLock.
func (n *objectInode) updateClientInodes() {
	glog.V(0).Infoln("objectInode  updateClientInodes")
	n.GetAttr(&fuse.Attr{}, nil, nil)
	for _, ch := range n.Inode().FsChildren() {
		ch.Node().(*objectInode).updateClientInodes()
	}
}

// GetPath returns the path relative to the mount governing this
// inode.  It returns nil for mount if the file was deleted or the
// filesystem unmounted.
// 17-3-2 zxw
// return the key for
// root path is "/"
// other just like "a/"  "b/c"  "d/e/"
// i can use it at url as dir="" for filer
func (n *objectInode) GetPath() string {
	glog.V(0).Infoln("objectInode  GetPath")
	if n == n.objectFs.root {
		return "/"
	}

	pathLen := 1

	// The simple solution is to collect names, and reverse join
	// them, them, but since this is a hot path, we take some
	// effort to avoid allocations.

	n.objectFs.objectLock.RLock()
	walkUp := n.Inode()

	// TODO - guess depth?
	segments := make([]string, 0, 10)
	for {
		parent, name := walkUp.Parent()
		if parent == nil {
			break
		}
		segments = append(segments, name)
		pathLen += len(name) + 1
		walkUp = parent
	}
	pathLen--

	glog.V(0).Infoln("GetPath  ", len(segments), segments)

	pathBytes := make([]byte, 0, pathLen)
	for i := len(segments) - 1; i >= 0; i-- {
		pathBytes = append(pathBytes, segments[i]...)
		if i > 0 {
			pathBytes = append(pathBytes, '/')
		}
	}
	n.objectFs.objectLock.RUnlock()

	path := string(pathBytes)
	if n.objectFs.debug {
		log.Printf("Inode = %q (%s)", path, n.fs.String())
	}

	if walkUp != n.objectFs.root.Inode() {
		// This might happen if the node has been removed from
		// the tree using unlink, but we are forced to run
		// some file system operation, because the file is
		// still opened.

		// TODO - add a deterministic disambiguating suffix.
		return ".deleted"
	}
	glog.V(0).Infoln("GetPath  ", path, " OK")
	return path
}

func (n *objectInode) OnAdd(parent *Inode, name string) {
	glog.V(0).Infoln("objectInode  OnAdd")

	// TODO it would be logical to increment the clientInodeMap reference count
	// here. However, as the inode number is loaded lazily, we cannot do it
	// yet.
}

func (n *objectInode) rmChild(name string) *objectInode {
	glog.V(0).Infoln("objectInode  rmChild")
	childInode := n.Inode().RmChild(name)
	if childInode == nil {
		return nil
	}
	return childInode.Node().(*objectInode)
}

func (n *objectInode) OnRemove(parent *Inode, name string) {
	glog.V(0).Infoln("objectInode  OnRemove")
	if n.clientInode == 0 || n.Inode().IsDir() {
		return
	}

	n.objectFs.objectLock.Lock()
	r := n.objectFs.clientInodeMap[n.clientInode]
	if r != nil {
		r.refCount--
		if r.refCount == 0 {
			delete(n.objectFs.clientInodeMap, n.clientInode)
		}
	}
	n.objectFs.objectLock.Unlock()
}

// setClientInode sets the inode number if has not been set yet.
// This function exists to allow lazy-loading of the inode number.
func (n *objectInode) setClientInode(ino uint64) {
	glog.V(0).Infoln("objectInode  setClientInode")
	if ino == 0 || n.clientInode != 0 || n.Inode().IsDir() {
		return
	}
	n.objectFs.objectLock.Lock()
	defer n.objectFs.objectLock.Unlock()
	n.clientInode = ino
	n.objectFs.clientInodeMap[ino] = &refCountedInode{node: n, refCount: 1}
}

func (n *objectInode) OnForget() {
	glog.V(0).Infoln("objectInode  OnForget")
	if n.clientInode == 0 || n.Inode().IsDir() {
		return
	}
	n.objectFs.objectLock.Lock()
	delete(n.objectFs.clientInodeMap, n.clientInode)
	n.objectFs.objectLock.Unlock()
}

////////////////////////////////////////////////////////////////
// FS operations

func (n *objectInode) StatFs() *fuse.StatfsOut {
	glog.V(0).Infoln("objectInode  StatFs")
	return n.fs.StatFs(n.GetPath())
}

func (n *objectInode) Readlink(c *fuse.Context) ([]byte, fuse.Status) {
	glog.V(0).Infoln("objectInode  Readlink")
	path := n.GetPath()

	val, err := n.fs.Readlink(path, c)
	return []byte(val), err
}

func (n *objectInode) Access(mode uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("objectInode  Access")
	p := n.GetPath()
	return n.fs.Access(p, mode, context)
}

func (n *objectInode) GetXAttr(attribute string, context *fuse.Context) (data []byte, code fuse.Status) {
	glog.V(0).Infoln("GetXAttr  1")

	return n.fs.GetXAttr(n.GetPath(), attribute, context)
}

func (n *objectInode) RemoveXAttr(attr string, context *fuse.Context) fuse.Status {
	glog.V(0).Infoln("objectInode  RemoveXAttr")
	p := n.GetPath()
	return n.fs.RemoveXAttr(p, attr, context)
}

func (n *objectInode) SetXAttr(attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	glog.V(0).Infoln("objectInode  SetXAttr")
	return n.fs.SetXAttr(n.GetPath(), attr, data, flags, context)
}

func (n *objectInode) ListXAttr(context *fuse.Context) (attrs []string, code fuse.Status) {
	glog.V(0).Infoln("objectInode  ListXAttr")
	return n.fs.ListXAttr(n.GetPath(), context)
}

func (n *objectInode) Flush(file *ObjectFile, openFlags uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("objectInode  Flush")
	return file.Flush()
}

func (n *objectInode) Fsync(openfile *openedFile) (code fuse.Status) {
	return n.fs.Fsync(n.GetPath(), openfile.temp)
}

func (n *objectInode) OpenDir(context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	glog.V(0).Infoln("objectInode  OpenDir")
	//temp is []*ObjectFile

	temp, status := n.fs.OpenDir(n.GetPath(), context)
	res := []fuse.DirEntry{}
	var children map[string]*Inode

	if temp != nil {
		children = make(map[string]*Inode)
		for _, v := range temp {
			i := &objectInode{
				fs:       n.fs,
				objectFs: n.objectFs,
			}
			tempnode := newInode(i, v)
			tempnode.mount = n.Inode().mount
			children[v.Name] = tempnode

			dir := fuse.DirEntry{}
			dir.Name = v.Name
			if v.IsDir {
				dir.Mode = fuse.S_IFDIR | 0755
			} else {
				dir.Mode = fuse.S_IFREG | 0644
			}
			res = append(res, dir)
		}
		n.inode.ChildrenReset(children)
	}

	return res, status
}

func (n *objectInode) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) (*Inode, fuse.Status) {
	fullPath := filepath.Join(n.GetPath(), name)
	code := n.fs.Mknod(fullPath, mode, dev, context)
	var child *Inode
	if code.Ok() {
		obj := &ObjectFile{
			Name:         name,
			IsDir:        false,
			LastModified: strconv.FormatInt(time.Now().Unix(), 10),
			Size:         0,
		}
		pNode := n.createChild(obj)
		child = pNode.Inode()
	}
	return child, code
}

func (n *objectInode) Mkdir(name string, mode uint32, context *fuse.Context) (*Inode, fuse.Status) {
	glog.V(0).Infoln("objectInode  Mkdir")
	fullPath := filepath.Join(n.GetPath(), name)
	code := n.fs.Mkdir(fullPath, mode, context)
	var child *Inode
	if code.Ok() {
		obj := &ObjectFile{
			Name:         name,
			IsDir:        true,
			LastModified: strconv.FormatInt(time.Now().Unix(), 10),
			Size:         0,
		}
		pNode := n.createChild(obj)
		child = pNode.Inode()
	}
	return child, code
}

func (n *objectInode) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("objectInode  Unlink")
	code = n.fs.Unlink(filepath.Join(n.GetPath(), name), context)
	if code.Ok() {
		n.Inode().RmChild(name)
	}
	return code
}

func (n *objectInode) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("objectInode  Rmdir")
	code = n.fs.Rmdir(filepath.Join(n.GetPath(), name), context)
	if code.Ok() {
		n.Inode().RmChild(name)
	}
	return code
}

func (n *objectInode) Symlink(name string, content string, context *fuse.Context) (*Inode, fuse.Status) {
	glog.V(0).Infoln("objectInode  Symlink")
	fullPath := filepath.Join(n.GetPath(), name)
	code := n.fs.Symlink(content, fullPath, context)
	var child *Inode
	if code.Ok() {
		obj := &ObjectFile{
			Name:         name,
			IsDir:        false,
			LastModified: strconv.FormatInt(time.Now().Unix(), 10),
			Size:         0,
		}
		pNode := n.createChild(obj)
		child = pNode.Inode()
	}
	return child, code
}

func (n *objectInode) Rename(oldName string, newParent Node, newName string, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("objectInode  Rename")
	p := newParent.(*objectInode)
	oldPath := filepath.Join(n.GetPath(), oldName)
	newPath := filepath.Join(p.GetPath(), newName)
	code = n.fs.Rename(oldPath, newPath, context)
	if code.Ok() {
		// The rename may have overwritten another file, remove it from the tree
		p.Inode().RmChild(newName)
		ch := n.Inode().RmChild(oldName)
		if ch != nil {
			// oldName may have been forgotten in the meantime.
			p.Inode().AddChild(newName, ch)
		}
	}
	return code
}

func (n *objectInode) Link(name string, existingFsnode Node, context *fuse.Context) (*Inode, fuse.Status) {
	return nil, fuse.ENOSYS
}

func (n *objectInode) Create(name string, flags uint32, mode uint32, context *fuse.Context) (*ObjectFile, *Inode, fuse.Status) {
	glog.V(0).Infoln("objectInode  Create")
	var child *Inode
	//fullPath := filepath.Join(n.GetPath(), name)

	obj := &ObjectFile{
		Name:         name,
		IsDir:        false,
		LastModified: strconv.FormatInt(time.Now().Unix(), 10),
		Size:         0,
	}
	child = n.createChild(obj).Inode()

	return obj, child, fuse.OK
}

func (n *objectInode) createChild(fileinfo *ObjectFile) *objectInode {
	glog.V(0).Infoln("objectInode  createChild")
	i := &objectInode{
		fs:       n.fs,
		objectFs: n.objectFs,
	}

	n.Inode().NewChild(fileinfo, i)

	return i
}

func (n *objectInode) Open(flags uint32, context *fuse.Context) (file *ObjectFile, code fuse.Status) {
	glog.V(0).Infoln("objectInode  Open")
	p := n.GetPath()
	//这个file是从数据库再次获取到的最新文件信息
	file, code = n.fs.Open(p, flags, context)
	//if n.objectFs.debug {
	//	file = &WithFlags{
	//		File:        file,
	//		Description: n.GetPath(),
	//	}
	//}
	return
}

//just find from filer
func (n *objectInode) Lookup(out *fuse.Attr, name string, context *fuse.Context) (node *Inode, code fuse.Status) {
	glog.V(0).Infoln("objectInode  Lookup")
	fullPath := filepath.Join(n.GetPath(), name)
	//temp := fullPath
	//if temp != "/" {
	//	temp = temp[1:]
	//}
	obj, code := n.fs.GetAttr(fullPath, context)
	if code.Ok() {
		node = n.findChild(out, obj, fullPath).Inode()
		obj.GetAttr(out)
	}

	return node, code
}

func (n *objectInode) findChild(fi *fuse.Attr, obj *ObjectFile, fullPath string) (out *objectInode) {
	glog.V(0).Infoln("objectInode  findChild")
	if fi.Ino > 0 {
		n.objectFs.objectLock.RLock()
		r := n.objectFs.clientInodeMap[fi.Ino]
		if r != nil {
			out = r.node
			r.refCount++
			if fi.Nlink == 1 {
				log.Printf("Found linked inode, but Nlink == 1, ino=%d, fullPath=%q", fi.Ino, fullPath)
			}
		}
		n.objectFs.objectLock.RUnlock()
	}

	if out == nil {
		out = n.createChild(obj)
		out.setClientInode(fi.Ino)
	} else {
		n.Inode().AddChild(obj.Name, out.Inode())
	}
	return out
}

//获取到指定属性，直接从inode中获取。
func (n *objectInode) GetAttr(out *fuse.Attr, file *ObjectFile, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("objectInode  GetAttr")
	var fi *fuse.Attr
	if file == nil {
		// Linux currently (tested on v4.4) does not pass a file descriptor for
		// fstat. To be able to stat a deleted file we have to find ourselves
		// an open fd.
		file = n.Inode().Fileinfo()
	}

	if file != nil {
		code = file.GetAttr(out)
	}

	if file == nil || code == fuse.ENOSYS || code == fuse.EBADF {
		obj, code := n.fs.GetAttr(n.GetPath(), context)
		if code == fuse.OK {
			obj.GetAttr(out)
		}

	}

	if fi != nil {
		n.setClientInode(fi.Ino)
	}

	if fi != nil && !fi.IsDir() && fi.Nlink == 0 {
		fi.Nlink = 1
	}

	if fi != nil {
		out = fi
	}
	return code
}

func (n *objectInode) Chmod(file *ObjectFile, perms uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("objectInode  Chmod")
	// Note that Linux currently (Linux 4.4) DOES NOT pass a file descriptor
	// to FUSE for fchmod. We still check because that may change in the future.
	if file != nil {
		code = file.Chmod(perms)
		if code != fuse.ENOSYS {
			return code
		}
	}

	files := n.Inode().Files(fuse.O_ANYWRITE)
	for _, f := range files {
		// TODO - pass context
		code = f.Chmod(perms)
		if code.Ok() {
			return
		}
	}

	if len(files) == 0 || code == fuse.ENOSYS || code == fuse.EBADF {
		code = n.fs.Chmod(n.GetPath(), perms, context)
	}
	return code
}

func (n *objectInode) Chown(file *ObjectFile, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("objectInode  Chown")
	// Note that Linux currently (Linux 4.4) DOES NOT pass a file descriptor
	// to FUSE for fchown. We still check because it may change in the future.
	if file != nil {
		code = file.Chown(uid, gid)
		if code != fuse.ENOSYS {
			return code
		}
	}

	files := n.Inode().Files(fuse.O_ANYWRITE)
	for _, f := range files {
		// TODO - pass context
		code = f.Chown(uid, gid)
		if code.Ok() {
			return code
		}
	}
	if len(files) == 0 || code == fuse.ENOSYS || code == fuse.EBADF {
		// TODO - can we get just FATTR_GID but not FATTR_UID ?
		code = n.fs.Chown(n.GetPath(), uid, gid, context)
	}
	return code
}

func (n *objectInode) Truncate(file *ObjectFile, size uint64, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("objectInode  Truncate")
	// A file descriptor was passed in AND the filesystem implements the
	// operation on the file handle. This the common case for ftruncate.
	if file != nil {
		code = file.Truncate(size)
		if code != fuse.ENOSYS {
			return code
		}
	}

	files := n.Inode().Files(fuse.O_ANYWRITE)
	for _, f := range files {
		// TODO - pass context
		code = f.Truncate(size)
		if code.Ok() {
			return code
		}
	}
	if len(files) == 0 || code == fuse.ENOSYS || code == fuse.EBADF {
		code = n.fs.Truncate(n.GetPath(), size, context)
	}
	return code
}

func (n *objectInode) Utimens(file *ObjectFile, atime *time.Time, mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("objectInode  Utimens")
	// Note that Linux currently (Linux 4.4) DOES NOT pass a file descriptor
	// to FUSE for futimens. We still check because it may change in the future.
	if file != nil {
		code = file.Utimens(atime, mtime)
		if code != fuse.ENOSYS {
			return code
		}
	}

	files := n.Inode().Files(fuse.O_ANYWRITE)
	for _, f := range files {
		// TODO - pass context
		code = f.Utimens(atime, mtime)
		if code.Ok() {
			return code
		}
	}
	if len(files) == 0 || code == fuse.ENOSYS || code == fuse.EBADF {
		code = n.fs.Utimens(n.GetPath(), atime, mtime, context)
	}
	return code
}

func (n *objectInode) Fallocate(file *ObjectFile, off uint64, size uint64, mode uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("objectInode  Fallocate")
	return fuse.ENOSYS

	if file != nil {
		code = file.Allocate(off, size, mode)
		if code.Ok() {
			return code
		}
	}

	files := n.Inode().Files(fuse.O_ANYWRITE)
	for _, f := range files {
		// TODO - pass context
		code = f.Allocate(off, size, mode)
		if code.Ok() {
			return code
		}
	}

	return code
}

func (n *objectInode) Read(file *ObjectFile, dest []byte, off int64, size int64, context *fuse.Context) (fuse.ReadResult, fuse.Status) {
	glog.V(0).Infoln("objectInode  Read")
	if file != nil {

		return n.fs.Read(n.GetPath(), dest, off, size, context)

		//return file.Read(dest, off, size)
	}
	return nil, fuse.EIO
}

func (n *objectInode) Write(file *ObjectFile, data []byte, off int64, context *fuse.Context) (written uint32, code fuse.Status) {
	glog.V(0).Infoln("objectInode  Write")
	if file != nil {

		//return n.fs.Write(n.GetPath(), data, off, context)
		//return file.Write(data, off)
	}
	return 0, fuse.ENOSYS
}
