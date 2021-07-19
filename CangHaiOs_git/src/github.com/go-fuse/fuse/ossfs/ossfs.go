// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//ossfs 这个包 主要是从objectfs  copy过来，然后进行调整性优化。
//为了避免  1  不成熟的修改直接影响代码的本来面目
//          2 假若修改失败，能够进行一定程度的比对
package ossfs

import (
	"github.com/go-fuse/fuse"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"path"

	"code.google.com/p/weed-fs/go/glog"
)

// refCountedInode is used in clientInodeMap. The reference count is used to decide
// if the entry in clientInodeMap can be dropped.
type refCountedInode struct {
	node     *ossInode
	refCount int
}

// PathNodeFs is the file system that can translate an inode back to a
// path.  The path name is then used to call into an object that has
// the FileSystem interface.
//
// Lookups (ie. FileSystem.GetAttr) may return a inode number in its
// return value. The inode number ("clientInode") is used to indicate
// linked files.
type Ossfs struct {
	debug bool
	//具体实现方法的地方
	driver    Driver
	root      *ossInode
	connector *FileSystemConnector

	/*
		// protects clientInodeMap
	objectLock sync.RWMutex
	// This map lists all the parent links known for a given inode number.
	clientInodeMap map[uint64]*refCountedInode
	*/
}

// NewPathNodeFs returns a file system that translates from inodes to
// path names.
func NewOssfs(driver Driver, cache DataCache) *Ossfs {
	root := &ossInode{
		cache:  cache,
		driver: driver,
	}
	ofs := &Ossfs{
		driver: driver,
		root:   root,
		//clientInodeMap: map[uint64]*refCountedInode{},
	}
	root.ossfs = ofs

	return ofs
}

// Root returns the root node for the path filesystem.
func (fs *Ossfs) Root() *ossInode {
	return fs.root
}

// This is a combination of dentry (entry in the file/directory and
// the inode). This structure is used to implement glue for FSes where
// there is a one-to-one mapping of paths and inodes.
// 实现的是Node 接口。服务于inode
//很多方法是ENOSYS，
//注意！！！！这个节点原本是作为事务的封装，
// readtemp 和writetemp 理论上不应该应该结合openfile来看，
// 但是实际业务比较单一，读写场景单一，这次就在这里，以后版本提供文件缓存时候在对他们进行调整
type ossInode struct {
	ossfs  *Ossfs
	driver Driver

	cache DataCache
	// This is to correctly resolve hardlinks of the underlying
	// real filesystem.
	clientInode uint64
	inode       *Inode
}

func (n *ossInode) OnMount(conn *FileSystemConnector) {
	glog.V(4).Infoln("ossInode  OnMount")

	n.ossfs.connector = conn
	n.ossfs.driver.OnMount(n.ossfs)
}

func (n *ossInode) OnUnmount() {
	glog.V(4).Infoln("ossInode  OnUnmount")
}

/*
// Drop all known client inodes. Must have the treeLock.
func (n *ossInode) forgetClientInodes() {
	glog.V(4).Infoln("ossInode  forgetClientInodes")
	n.clientInode = 0
	for _, ch := range n.Inode().FsChildren() {
		ch.Node().(*ossInode).forgetClientInodes()
	}
}
*/

func (fs *ossInode) Deletable() bool {
	return true
}

func (n *ossInode) Inode() *Inode {
	return n.inode
}

func (n *ossInode) SetInode(node *Inode) {
	n.inode = node
}

// Reread all client nodes below this node.  Must run outside the treeLock.
func (n *ossInode) updateClientInodes() {
	glog.V(4).Infoln("ossInode  updateClientInodes")
	n.GetAttr(&fuse.Attr{}, nil, nil)
	for _, ch := range n.Inode().FsChildren() {
		ch.Node().(*ossInode).updateClientInodes()
	}
}

// GetPath returns the path relative to the mount governing this
// inode.  It returns nil for mount if the file was deleted or the
// filesystem unmounted.
func (n *ossInode) GetPath() string {
	//	glog.V(0).Infoln("ossInode  GetPath")
	if n == n.ossfs.root {
		return "/"
	}

	pathLen := 1

	// The simple solution is to collect names, and reverse join
	// them, them, but since this is a hot path, we take some
	// effort to avoid allocations.

	walkUp := n.Inode()
	// TODO - guess depth?
	segments := make([]string, 0, 10)
	//先把自己加进去
	segments = append(segments, walkUp.fileinfo.Name)
	for {
		pardata := walkUp.GetFirstParent()

		if pardata == nil {
			break
		}
		parent, name := pardata.parent, pardata.name
		//glog.V(0).Infoln(walkUp.fileinfo.Name, "'s Parent:", pardata.name)
		if parent == nil {
			break
		}
		segments = append(segments, name)
		pathLen += len(name) + 1
		walkUp = parent
	}
	pathLen--

	//glog.V(0).Infoln("GetPath  ", len(segments), segments)

	pathBytes := make([]byte, 0, pathLen)
	for i := len(segments) - 1; i >= 0; i-- {
		pathBytes = append(pathBytes, segments[i]...)
		if i > 0 {
			pathBytes = append(pathBytes, '/')
		}
	}

	path := string(pathBytes)
	if n.ossfs.debug {
		log.Printf("Inode = %q (%s)", path, n.inode.fileinfo.String())
	}

	if walkUp != n.ossfs.root.Inode() {
		// This might happen if the node has been removed from
		// the tree using unlink, but we are forced to run
		// some file system operation, because the file is
		// still opened.
		glog.V(2).Infoln("error while GetPath,now path: ", path, " walkUp:", walkUp.fileinfo.Name)
		// TODO - add a deterministic disambiguating suffix.
		return ".deleted"
	}
	//glog.V(0).Infoln("GetPath  ", path, " OK")
	return path
}

func (n *ossInode) OnAdd(parent *Inode, name string) {
	glog.V(4).Infoln("ossInode  OnAdd")
	n.inode.addsonscountloop(1)
	if Debug {
		glog.V(0).Info(parent.fileinfo.Name, " after OnAdd ", name, " now total inode sum: ", Getinodesum())
	}
	// TODO it would be logical to increment the clientInodeMap reference count
	// here. However, as the inode number is loaded lazily, we cannot do it
	// yet.
	//	parent.posteritysumlock.Lock()
	//parent.posteritysumlock.Unlock()
}

func (n *ossInode) rmChild(name string) *ossInode {
	glog.V(4).Infoln("ossInode  rmChild")
	childInode := n.Inode().RmChild(name)
	if childInode == nil {
		return nil
	}
	return childInode.Node().(*ossInode)
}

func (n *ossInode) OnRemove(parent *Inode, name string) {

	tempcount := n.inode.gettotalsons()
	n.inode.addsonscountloop(0 - tempcount)
	if Debug {
		glog.V(0).Info(parent.fileinfo.Name, " after OnRemove ", name, " now total inode sum: ", Getinodesum())
	}
	/*
	if n.clientInode == 0 || n.Inode().IsDir() {
		return
	}
		n.ossfs.objectLock.Lock()
	r := n.ossfs.clientInodeMap[n.clientInode]
	if r != nil {
		r.refCount--
		if r.refCount == 0 {
			delete(n.ossfs.clientInodeMap, n.clientInode)
		}
	}
	n.ossfs.objectLock.Unlock()
	*/

}

/*
// setClientInode sets the inode number if has not been set yet.
// This function exists to allow lazy-loading of the inode number.
func (n *ossInode) setClientInode(ino uint64) {
	glog.V(4).Infoln("ossInode  setClientInode")
	if ino == 0 || n.clientInode != 0 || n.Inode().IsDir() {
		return
	}
	n.ossfs.objectLock.Lock()
	defer n.ossfs.objectLock.Unlock()
	n.clientInode = ino
	n.ossfs.clientInodeMap[ino] = &refCountedInode{node: n, refCount: 1}
}

*/

func (n *ossInode) OnForget() {
	glog.V(4).Infoln("ossInode  OnForget")

	/*
		if n.clientInode == 0 || n.Inode().IsDir() {
		return
	}
		n.ossfs.objectLock.Lock()
	delete(n.ossfs.clientInodeMap, n.clientInode)
	n.ossfs.objectLock.Unlock()
	*/

}

////////////////////////////////////////////////////////////////
// FS operations
//文件系统的属性，获取的的bucket的一些容量属性
func (n *ossInode) StatFs() *fuse.StatfsOut {
	glog.V(4).Infoln("ossInode  StatFs")
	return n.driver.StatFs(n.GetPath())
}

func (n *ossInode) Readlink(c *fuse.Context) ([]byte, fuse.Status) {
	glog.V(4).Infoln("ossInode  Readlink")
	return []byte(""), fuse.ENOSYS
}

func (n *ossInode) Access(mode uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("ossInode  Access")
	/*
		if code = n.inode.RefreshCheck(); code != fuse.OK {
			return
		}
	*/

	//n是不是存在
	if n.inode.mountPoint == nil && (n.inode.GetParentCount() == 0 || n.inode.isremoved) {
		return fuse.ENOENT
	}
	return fuse.OK
	//没啥可判定的，只要存在，就是OK
	/*switch mode {
	case fuse.F_OK:
		return fuse.OK
	case fuse.X_OK:
		return fuse.OK
	case fuse.W_OK:
		return fuse.OK
	case fuse.R_OK:
		return fuse.OK
	}
	return fuse.ENOENT*/
}

func (n *ossInode) GetXAttr(attribute string, context *fuse.Context) (data []byte, code fuse.Status) {
	glog.V(4).Infoln("GetXAttr  1")

	return n.driver.GetXAttr(n.GetPath(), attribute, context)
}

func (n *ossInode) RemoveXAttr(attr string, context *fuse.Context) fuse.Status {
	glog.V(4).Infoln("ossInode  RemoveXAttr")
	return fuse.ENOSYS
}

func (n *ossInode) SetXAttr(attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	glog.V(4).Infoln("ossInode  SetXAttr")
	return n.driver.SetXAttr(n.GetPath(), attr, data, flags, context)
}

func (n *ossInode) ListXAttr(context *fuse.Context) (attrs []string, code fuse.Status) {
	glog.V(4).Infoln("ossInode  ListXAttr")
	return n.driver.ListXAttr(n.GetPath(), context)
}

func (n *ossInode) Flush(file *FileInfo, openFlags uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("ossInode  Flush")
	return fuse.OK
}

func (n *ossInode) Fsync(openfile *openedFile) (code fuse.Status) {
	var err error
	glog.V(4).Infoln("ossInode  Fsync file:",n.inode.fileinfo.Name)
	if n.cache != nil {
		err = n.cache.SyncFile(n.inode.fileinfo, openfile.OpenFlags&fuse.O_ANYWRITE != 0)
	}
	if err != nil {
		glog.V(0).Infoln("ossInode  Fsync file:",n.inode.fileinfo.Name, "failed. ",err)
		return fuse.EIO
	}
	return fuse.OK
}

//
func (n *ossInode) OpenDir(context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	start := time.Now()
	if Debug {
		glog.V(0).Infoln("ossInode  OpenDir start", n.inode.fileinfo.Name)
		defer func() {
			glog.V(0).Infoln("ossInode  OpenDir total:", n.inode.fileinfo.Name, time.Since(start))
		}()
	}

	children := n.inode.GetChildren()
	return children, fuse.OK
}

//列出指定目录下的所有文件
func (n *ossInode) ListDir(fixname string, context *fuse.Context) (map[string]*FileInfo, fuse.Status) {
	temp, code := n.driver.OpenDir(n.GetPath(), fixname, context)
	children := make(map[string]*FileInfo)
	for _, v := range temp {
		children[v.Name] = v
	}
	return children, code
	/*
		children := make(map[string]*Inode)
	for _, v := range temp {
		i := &ossInode{
			driver: n.driver,
			ossfs:  n.ossfs,
			cache:  n.cache,
		}
		tempnode := newInode(i, v)
		tempnode.mount = n.Inode().mount
		children[v.Name] = tempnode
	}
	return children, code
	*/
}

func (n *ossInode) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) (*Inode, fuse.Status) {
	return nil, fuse.ENOSYS
}

//需要建立一个真正的文件夹，
func (n *ossInode) Mkdir(name string, mode uint32, context *fuse.Context) (*Inode, fuse.Status) {
	glog.V(4).Infoln("ossInode  Mkdir")
	if Getinodesum() > MaxInodesSum {
		glog.V(0).Infoln(MaxInodeSumErr)
		return nil, fuse.EPERM
	}
	var child *Inode
	//先看文件是不是存在
	child = n.inode.GetChild(name)
	if child != nil {
		return nil, fuse.EINVAL
	}
	if len(path.Join(n.GetPath(), name)) > 256 {
		return nil, fuse.EINVAL
	}
	fullPath := filepath.Join(n.GetPath(), name)
	//需要到oss实际进行建立文件夹操作
	code := n.driver.Mkdir(fullPath, mode, context)

	if code.Ok() {
		obj := &FileInfo{
			Name:         name,
			IsDir:        true,
			LastModified: strconv.FormatInt(time.Now().Unix(), 10),
			Size:         0,
		}
		i := &ossInode{
			driver: n.driver,
			ossfs:  n.ossfs,
			cache:  n.cache,
		}
		child = newInode(i, obj)
		child.mount = n.Inode().mount
		n.inode.AddChild(name, child)
	}
	return child, code
}

//删除文件
func (n *ossInode) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("ossInode  Unlink", name)

	child := n.inode.GetChild(name)
	if child == nil {
		code = fuse.ENOENT
	} else {
		//if child.fileinfo.Size != 0 {
			code = n.driver.Unlink(path.Join(n.GetPath(), name), context)
			if code == fuse.ENOENT && n.inode.fileinfo.Size == 0 {
				code = fuse.OK
			}
	//	} else {
		//	code = fuse.OK
	//	}
	}

	if code.Ok() {
		if n.cache != nil {
			n.cache.RemoveFile(n.inode.fileinfo)
		}
		n.inode.RmChild(name)
	}

	return
}

//删除文件夹
func (n *ossInode) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("ossInode  Rmdir")
	code = n.driver.Rmdir(filepath.Join(n.GetPath(), name), context)
	if code.Ok() {
		n.Inode().RmChild(name)
	}
	return code
}

//不支持
func (n *ossInode) Symlink(name string, content string, context *fuse.Context) (*Inode, fuse.Status) {
	glog.V(4).Infoln("ossInode  Symlink")
	return nil, fuse.ENOSYS
}

func (n *ossInode) Rename(oldName string, newParent Node, newName string, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("ossInode  Rename")

	child := n.inode.GetChild(oldName)
	if child == nil {
		return fuse.ENOENT
	}
	if child.mountPoint != nil {
		return fuse.EBUSY
	}
	checksum := 0
	openfiles := []*FileInfo{}
	for {
		if openfiles = child.GetFilesWithMask(fuse.O_ANYWRITE); len(openfiles) == 0 {
			//等10S
			break
		}
		glog.V(0).Infoln(len(openfiles), openfiles)

		checksum++
		if checksum > 100 {
			return fuse.EBUSY
		}
		time.Sleep(100 * time.Millisecond)
	}

	p := newParent.(*ossInode)
	oldPath := filepath.Join(n.GetPath(), oldName)
	newPath := filepath.Join(p.GetPath(), newName)
	code = n.driver.Rename(child.fileinfo, oldPath, newPath, context)
	if code.Ok() {
		if n.GetPath() != p.GetPath(){
			n.Inode().childrenlock.Lock()
			n.inode.rmChild(oldName)
			n.Inode().childrenlock.Unlock()
		}
		// The rename may have overwritten another file, remove it from the tree
		p.Inode().RenameChild(oldName, newName)
		n.cache.RenameOpFile(child.fileinfo,oldPath,newPath)
	}
	return code
}

func (n *ossInode) Link(name string, existingFsnode Node, context *fuse.Context) (*Inode, fuse.Status) {
	return nil, fuse.ENOSYS
}

//返回的Inode 没有被添加到child中
//建一个假的就好
func (n *ossInode) Create(name string, flags uint32, mode uint32, context *fuse.Context) (*FileInfo, *Inode, fuse.Status) {
	glog.V(4).Infoln("ossInode  Create:",name)
	//并不需要进行实际的文件操作，直接虚构一个文件信息就可以了
	if Getinodesum() > MaxInodesSum {
		glog.V(0).Infoln(MaxInodeSumErr)
		return nil, nil, fuse.EPERM
	}
	if strings.Index(name,"?") != -1 {
		glog.V(0).Infoln("error! filename:",name," has unacceptable string")
		return nil,nil,fuse.EINVAL
	}
	child := n.inode.GetChild(name)
	if child != nil {
		glog.V(4).Infoln("ossInode  Create err:child != nil", name)
		return nil, nil, fuse.EINVAL
	}

	if len(path.Join(n.GetPath(), name)) > 256 {
		glog.V(4).Infoln("ossInode  Create err:len > 256", path.Join(n.GetPath(), name))
		return nil, nil, fuse.EINVAL
	}

	obj := &FileInfo{
		Name:         name,
		IsDir:        false,
		LastModified: strconv.FormatInt(time.Now().Unix(), 10),
		Size:         0,
	}
	i := &ossInode{
		driver: n.driver,
		ossfs:  n.ossfs,
		cache:  n.cache,
	}
	tempnode := newInode(i, obj)
	tempnode.isnewcreate = true
	tempnode.mount = n.Inode().mount
	n.inode.AddChild(name, tempnode)
	return obj, tempnode, fuse.OK
}

func (n *ossInode) CopyInode() *ossInode {
	i := &ossInode{
		driver: n.driver,
		ossfs:  n.ossfs,
		cache:  n.cache,
	}
	return i
}

func (n *ossInode) createChild(fileinfo *FileInfo) *ossInode {
	glog.V(4).Infoln("ossInode  createChild")
	/*
		i := &ossInode{
		driver: n.driver,
		ossfs:  n.ossfs,
		cache:  n.cache,
	}

	n.Inode().NewChild(fileinfo, i)

	*/

	return nil
}

func (n *ossInode) Open(flags uint32, context *fuse.Context) (file *FileInfo, code fuse.Status) {
	glog.V(0).Infoln("ossInode  Open")
	code = fuse.ENOSYS
	return
}

//
//寻找子节点，lookup动作已经被Inode那边完成了，不需要额外的动作了
func (n *ossInode) Lookup(out *fuse.Attr, name string, context *fuse.Context) (node *Inode, code fuse.Status) {
	glog.V(4).Infoln("ossInode  Lookup")
	code = fuse.ENOSYS
	return
}

//获取文件的属性信息，
func (n *ossInode) GetAttr(out *fuse.Attr, file *FileInfo, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("ossInode  GetAttr")
	if file == nil {
		glog.V(4).Infoln("bad fileinfo")
		return fuse.EIO
	}
	out.Mtime, _ = strconv.ParseUint(file.LastModified, 10, 64)
	out.Atime, _ = strconv.ParseUint(file.LastModified, 10, 64)
	out.Ctime, _ = strconv.ParseUint(file.LastModified, 10, 64)
	if file.IsDir {
		out.Size = uint64(4096)
		out.Mode = fuse.S_IFDIR | 0777
		out.Blocks = uint64(4096) / 512
	} else {
		out.Size = uint64(file.Size)
		out.Mode = fuse.S_IFREG | 0777
		if file.Size == 0 {
			out.Blocks = 0
		}else if uint64(file.Size) / 4096 == 0{
			out.Blocks = uint64(4096) / 512
		}else if uint64(file.Size) % 4096 == 0{
			out.Blocks = uint64(file.Size) / 512
		}else{
			out.Blocks = uint64(file.Size + 4096) / 512
		}
	}
	glog.V(4).Infoln("ossInode  GetAttr done:",file.Name," size:",out.Size)
	return fuse.OK
}

func (n *ossInode) Chmod(file *FileInfo, perms uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("ossInode  Chmod")
	return n.driver.Chmod(n.GetPath(), perms, context)

}

func (n *ossInode) Chown(file *FileInfo, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(0).Infoln("ossInode  Chown")
	return n.driver.Chown(n.GetPath(), uid, gid, context)
}

//删除文件，但是保留文件的空白元数据信息
//只支持size=0的
func (n *ossInode) Truncate(file *FileInfo, size uint64, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("ossInode  Truncate:", size)
	if size != 0 {
		return fuse.ENOSYS
	}

	if file.Size == 0 {
		code = fuse.OK
	} else {
		code = n.driver.Unlink(path.Join(n.GetPath()), context)
		if code == fuse.ENOENT && n.inode.fileinfo.Size == 0 {
			code = fuse.OK
		}
		if code != fuse.OK && code != fuse.ENOENT {
			glog.V(0).Infoln("Unlink error:", code)
			return code
		}
	}

	if n.cache != nil {
		n.cache.Truncate(file, 0)
	}
	glog.V(4).Infoln("file Truncate", file)
	return fuse.OK
}

func (n *ossInode) Utimens(file *FileInfo, atime *time.Time, mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("ossInode  Utimens")
	return n.driver.Utimens(n.GetPath(), atime, mtime, context)
}

func (n *ossInode) Fallocate(file *FileInfo, off uint64, size uint64, mode uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("ossInode  Fallocate")
	return fuse.ENOSYS

}

func (n *ossInode) Read(file *FileInfo, dest []byte, off int64, size int64, context *fuse.Context) (fuse.ReadResult, fuse.Status) {
	glog.V(4).Infoln("ossInode  Read")
	if off+size > file.Size {
		if off >= file.Size {
			glog.V(4).Infoln(" Read over ,return eof")
			return fuse.ReadResultData([]byte{}), fuse.OK
		} else {
			size = file.Size - off
		}
	}
	if file != nil {
		//先看缓存有没有。
		//有了直接缓存读，没有去oss获取。
		//缓存中一共最多可以保存8个，每个1M（文件最后一块除外）
		//不是异步预取，所以取缓存中没有的会慢点。
		//超过8块删除前面的。（）
		if n.cache != nil {
			ressize, err := n.cache.ReadAt(uint64(n.inode.handled.handle), n.GetPath(), file, size, off, dest)
			if err != nil {
				glog.V(4).Infoln("cache Read err:", err)
				return fuse.ReadResultData([]byte{}), fuse.EIO
			}
			//copy(dest, data)
			glog.V(4).Infoln("cache Read data:", ressize)
			return fuse.ReadResultData(dest[:ressize]), fuse.OK
		} else {
			return n.driver.Read(n.GetPath(), dest, off, size, context)
		}

		/*
		res, err := n.GetCacheData(off, size)
		if err != nil {
			glog.V(0).Infoln("read err:", err)
			return fuse.ReadResultData([]byte{}), fuse.EIO
		}
		glog.V(4).Infoln("get data from cache:   parts:", len(res))
		newoff, newsize := n.getLessMess(off, size, &res)
		if newsize > 0 {
			glog.V(4).Infoln("want to get file from oss.off:", newoff, "size:", newsize)
			if newoff+newsize > file.Size {
				newsize -= (newoff + newsize) - file.Size
				glog.V(4).Infoln(" after check . off:", newoff, "size:", newsize)
			}
			newdata, code := n.driver.ReadData(n.GetPath(), newoff, newsize, context)
			if !code.Ok() {
				glog.V(4).Infoln("readdata error!!")
				return fuse.ReadResultData([]byte{}), code
			}
			n.FullReadCache(newoff, newsize, newdata)

			res, _ = n.GetCacheData(off, size)
		}

		sort.Sort(res)
		tempdata := make([]byte, 4096)
		resultdata := []byte{}
		for _, v := range res {
			for {
				s, e := v.data.Read(tempdata)
				if e != nil {
					break
				}
				resultdata = append(resultdata, tempdata[:s]...)
			}
		}
		dest = resultdata
		return fuse.ReadResultData(resultdata), fuse.OK

		*/
		//return file.Read(dest, off, size)
	}
	return fuse.ReadResultData([]byte{}), fuse.EIO
}

/*
*
//从散乱的readtemp中获取到需要的数据
func (n *ossInode) GetCacheData(off int64, size int64) (res datacache, err error) {
	n.rcachelock.RLock()
	defer n.rcachelock.RUnlock()

	left := int(size)
	res = datacache{}
	getsize := 0
	for _, v := range n.readtemp {
		if v.createtime+Cachetimeout < time.Now().Unix() {
			continue
		}
		v.data.Seek(0, io.SeekStart)
		tempvlen := v.data.Len()
		if v.off <= off && v.off+int64(tempvlen) > off {
			v.data.Seek(off-v.off, io.SeekStart)
			tempres := make([]byte, left)
			readsize := 0
			readsize, err = v.data.Read(tempres)
			if err != nil {
				if err == io.EOF {
					err = nil
					continue
				}
				return
			}
			tempoffdata := &readoffdata{data: bytes.NewReader(tempres[:readsize]), off: off}
			res = append(res, tempoffdata)
			getsize += readsize
			left -= readsize
			off += int64(readsize)
			if left == 0 {
				break
			}
		} else if v.off >= off && v.off < off+size {
			//能取到一节，中间有空洞
			tempres := make([]byte, left-int(v.off-off))
			readsize := 0
			readsize, err = v.data.Read(tempres)
			if err != nil {
				if err == io.EOF {
					err = nil
					continue
				}
				return
			}
			tempoffdata := &readoffdata{data: bytes.NewReader(tempres[:readsize]), off: v.off}
			res = append(res, tempoffdata)
			getsize += readsize
			left -= readsize
			off = v.off + int64(readsize)
			if left == 0 {
				break
			}
		} else if v.off > off+size {
			break
		}
		if left == 0 {
			break
		}
	}
	return
}

//根据需要的，现有的，获取需要去取的,nowdata 肯定是在off和size之内的！！
//必须是1M 的整数倍
func (n *ossInode) getLessMess(off int64, size int64, nowdata *datacache) (newoff int64, newsize int64) {
	newoff, newsize = off, size
	if nowdata == nil {
		return
	}

	for _, v := range *nowdata {
		if v.off == newoff {
			tempsize := v.data.Size()
			newoff += tempsize
			newsize -= tempsize
		} else {
			break
		}
	}
	if newsize == 0 {
		return
	}
	templen := len(*nowdata)
	for i := templen - 1; i >= 0; i-- {
		v := (*nowdata)[i]
		tempsize := v.data.Size()
		if tempsize+v.off == newsize+newoff {
			newsize -= tempsize
		} else {
			break
		}
	}
	if newsize == 0 {
		return
	}

	tempnewoff := (newoff / Getblocksize) * Getblocksize
	if tempnewoff < newoff {
		newsize += (newoff - tempnewoff)
	}
	newoff = tempnewoff
	if newsize%Getblocksize != 0 {
		newsize = ((newsize / Getblocksize) + 1) * Getblocksize
	}
	return
}

//把新的data加入到ossInode的读缓存
func (n *ossInode) FullReadCache(off int64, size int64, data []byte) {
	//必须是整M的开始
	if off%Getblocksize != 0 {
		return
	}
	tempdatacoll := make(map[int64]*readoffdata)
	startpos := int64(0)
	size = int64(len(data))
	nowoff := off
	for {
		datasize := Getblocksize
		if size-startpos < Getblocksize {
			datasize = size - startpos
		}

		tempdata := &readoffdata{
			data: bytes.NewReader(data[startpos:startpos+datasize]),
			off:  nowoff,
		}
		tempdatacoll[nowoff] = tempdata

		if datasize < Getblocksize {
			break
		}
		nowoff += datasize
		startpos += datasize
	}

	n.rcachelock.Lock()
	defer n.rcachelock.Unlock()

	//数据重复了，旧的数据删除
	for i, v := range n.readtemp {
		if v.createtime+Cachetimeout < time.Now().Unix() {
			delete(tempdatacoll, v.off)
		}
		if newv, ok := tempdatacoll[v.off]; ok {
			n.readtemp[i] = newv
			newv.createtime = time.Now().Unix()
			delete(tempdatacoll, v.off)
		}
	}
	for _, v := range tempdatacoll {
		v.createtime = time.Now().Unix()
		n.readtemp = append(n.readtemp, v)
	}
	sort.Sort(n.readtemp)

	for {
		if len(n.readtemp) <= Maxcachesum || Maxcachesum <= 1 {
			break
		}
		if n.readtemp[0].off-off > n.readtemp[Maxcachesum-1].off-off {
			n.readtemp = n.readtemp[:len(n.readtemp)-1]
		} else {
			n.readtemp = n.readtemp[1:]
		}
	}

	return
}

*
*
*/

//接收写的数据
func (n *ossInode) Write(file *FileInfo, data []byte, off int64, context *fuse.Context) (written uint32, code fuse.Status) {

	glog.V(4).Infoln("ossInode  Write", off, len(data))
	if n.cache != nil {
		err := n.cache.WriteAt(n.inode.handled.handle, n.GetPath(), n.inode.fileinfo, data, off)
		if err != nil {
			glog.V(0).Infoln("ossInode  Write", off, len(data), "error:", err)
			return uint32(0), fuse.EIO
		}
		return uint32(len(data)), fuse.OK
	} else {
		return n.driver.Write(n.GetPath(), data, off, context)
	}
}

//关闭文件，结束文件的读写操作
func (n *ossInode) Release(flags uint32) (fuse.Status) {
	var err error
	glog.V(4).Infoln("ossInode  Release file:", n.inode.fileinfo.Name)
	if n.cache != nil {
		if n.inode.fileinfo != nil {
			glog.V(4).Infoln("test ossInode  Release ", n.inode.fileinfo.Name)
		}
		err = n.cache.CloseFile(n.inode.fileinfo, n.GetPath(),flags&fuse.O_ANYWRITE != 0)
	}
	if err != nil {
		glog.V(0).Infoln("ossInode  Release file:", n.inode.fileinfo.Name," failed. ",err)
		return fuse.EIO
	}
	n.inode.isnewcreate = false
	return fuse.OK
}

/*

type cachedata struct {
	//4MB 大小的缓存空间，跟块是一个大小，除了最后一块，都是4M
	data      []byte
	off       int64
	dirtysize bool
}

func (c *cachedata) test() {

}

//开启1个协程，不停的根据文件的缓存下刷数据，
//下刷动作包括：
//原来就存在，是分块上传，reopen（首次） + replace  块的大小必须保证跟以前的一样
//            是直接上传，reopen（首次），现在超过分块阈值了，采用分块上传方式上传
//                                        没超过阈值，直接覆盖上传
//原来不存在，原来超过阈值了，采用分块上传方式上传
//            原来没超过阈值，直接上传
//close的动作会把最后没提交的一次性都提交了。对于是分块上传的，要进行commit操作
//PS：9-10 还没有把write和read 的缓存合并到一块，这两理论上应该合并。OSS需要新增一个读取正在进行分块上传动作的文件的某一个块内容才可以
type CacheDataMana struct {
	//缓存数据,可以读，可以写。
	blocks        cachedatacoll //key:off value:offdata
	cachedatalock sync.RWMutex

	datalen  int64        //tempdata中的长度总和
	dataLock sync.RWMutex //

	uploadedSize int64 //已经上传了的大小（被read过,整个文件的顺序的最后的位置）

	waitSliceTimeout int64          //等待一个未到客户端分片的最长时间
	isResOver        bool           //是不是结束了
	uploadWait       sync.WaitGroup //
	uploadError      error          //上传结果
	lastreadtime     int64

	//最大的保存在tempdata中的大小
	maxfilelen int64
	filename   string

	//
	StartUpload func()
}

func (wf *writefile) Close() error {
	glog.V(4).Infoln("writefile", wf.filename, "close")
	time.Sleep(time.Duration(5) * time.Millisecond)
	wf.isResOver = true
	wf.uploadWait.Wait()
	if wf.uploadError != nil {
		glog.V(0).Infoln("writefile", wf.filename, " error:", wf.uploadError)
	}
	return wf.uploadError

}

//
func (wf *writefile) WriteAt(p []byte, off int64) (int, error) {
	wf.dataLock.Lock()
	//剩余大小超过规定的最大缓存。
	if wf.datalen > wf.maxfilelen {
		wf.dataLock.Unlock()
		//超过了，就需要等，最多等30S，
		exceed := false
		waitstart := time.Now().Unix()
		for {
			if time.Now().Unix()-waitstart > 30 {
				exceed = true
				break
			}
			wf.dataLock.RLock()
			if wf.datalen < wf.maxfilelen {
				wf.dataLock.RUnlock()
				break
			} else {
				wf.dataLock.RUnlock()
				time.Sleep(500 * time.Millisecond)
			}
		}
		//等30S之后还是超过大小。一直没上传上去。报错退出
		if exceed {
			glog.V(0).Infoln("file size limit,path", wf.filename)
			return 0, errors.New("file size  limit")
		}
		wf.dataLock.Lock()
	}

	defer wf.dataLock.Unlock()

	size := len(p)
	//添加，去重

	//重复了
	if _, ok := wf.tempdata[off]; ok {
		return 0, nil
	}
	tempdata := make([]byte, len(p))
	copy(tempdata, p)

	wf.tempdata[off] = &offdata{data: bytes.NewReader(tempdata), off: off}
	wf.datalen += int64(size)

	return size, nil
}

//io.Reader的实现,向下支持上传文件到oss
//结束以eof为标识
//Close 一定要保证在最后一个WriteAt之后执行
func (wf *writefile) Read(p []byte) (n int, err error) {
	waittime := 0
start:
	wf.dataLock.RLock()
	if v, ok := wf.tempdata[wf.uploadedSize]; ok {
		wf.dataLock.RUnlock()
		wf.dataLock.Lock()
		wf.lastreadtime = 0
		n, err = v.data.Read(p)
		if err != nil {
			wf.dataLock.Unlock()
			glog.V(0).Infoln("bad data,path", wf.filename, err)
			return
		}

		if n == 0 {
			return 0, nil
		}

		v.off += int64(n)
		wf.datalen -= int64(n)
		delete(wf.tempdata, wf.uploadedSize)

		wf.uploadedSize = v.off
		if v.data.Len() != 0 {
			wf.tempdata[wf.uploadedSize] = v
		}
		wf.dataLock.Unlock()
		return
	} else {
		glog.V(4).Infoln("want: ", wf.uploadedSize, " but can not get it")
		wf.dataLock.RUnlock()
		if wf.lastreadtime == 0 {
			wf.lastreadtime = time.Now().Unix()
		}
		if waittime >= 600 { //30S.要能包括最差情况。大文件读末尾。
			glog.V(0).Infoln("write ", wf.filename, "error ", io.ErrShortWrite)
			return 0, io.ErrShortWrite
		}
		if wf.isResOver {
			if wf.datalen > 0 { //还有没写进去的。但是已经EOF了
				time.Sleep(time.Duration(50) * time.Millisecond)
				glog.V(3).Infoln("bad data,path", wf.filename, "uploadedSize:", wf.uploadedSize)
				waittime++
				goto start
			} else { //确实没有了
				glog.V(5).Infoln(wf.filename, "uploadedSize:", wf.uploadedSize)
				return 0, io.EOF
			}
		}
		time.Sleep(time.Duration(1) * time.Millisecond)
		if wf.lastreadtime != 0 && time.Now().Unix()-wf.lastreadtime > wf.waitSliceTimeout {
			glog.V(0).Infoln("write ", wf.filename, "error ,time out ", io.ErrShortWrite)
			return 0, io.ErrShortWrite
		}
		return 0, nil
	}
}













*/
