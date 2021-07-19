// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ossfs

// This file contains FileSystemConnector's implementation of
// RawFileSystem

import (
	"bytes"
	"fmt"
	"github.com/go-fuse/fuse"
	"strings"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"unsafe"
)

// Returns the RawFileSystem so it can be mounted.
func (c *FileSystemConnector) RawFS() fuse.RawFileSystem {
	return (*rawBridge)(c)
}

//对接上层过来的请求，
//原来的动作被封装到Node接口中
//因为元数据调整，Inode中承担了很多的判定动作，所以我调整了很多动作在inode中了
//有时间的话，在进行优化吧。职责分开是最好的。
type rawBridge FileSystemConnector

//设置debug信息
func (c *rawBridge) SetDebug(debug bool) {
	c.fsConn().SetDebug(debug)
}

//
func (c *rawBridge) FsyncDir(input *fuse.FsyncIn) fuse.Status {
	glog.V(4).Infoln("rawBridge  FsyncDir")
	//
	return fuse.ENOSYS
}

//
func (c *rawBridge) fsConn() *FileSystemConnector {
	return (*FileSystemConnector)(c)
}

func (c *rawBridge) String() string {
	if c.rootNode == nil || c.rootNode.mount == nil {
		return "go-fuse:unmounted"
	}

	name := fmt.Sprintf("%T", c.rootNode.Node())
	name = strings.TrimLeft(name, "*")
	return name
}

//根节点和具体的driver赋值，
func (c *rawBridge) Init(s *fuse.Server) {
	c.server = s
	c.rootNode.Node().OnMount((*FileSystemConnector)(c))
}

//返回mount的根路径的inode
func (c *FileSystemConnector) lookupMountUpdate(out *fuse.Attr, mount *fileSystemMount) (node *Inode, code fuse.Status) {
	code = mount.mountInode.Node().GetAttr(out, &FileInfo{}, nil)
	if !code.Ok() {
		glog.V(0).Infoln("Root getattr should not return error", code)
		//log.Println("Root getattr should not return error", code)
		out.Mode = fuse.S_IFDIR | 0755
		return mount.mountInode, fuse.OK
	}

	return mount.mountInode, fuse.OK
}

// internalLookup executes a lookup without affecting NodeId reference counts.
//查找指定节点的子的inode
func (c *FileSystemConnector) internalLookup(out *fuse.Attr, parent *Inode, name string, header *fuse.InHeader) (node *Inode, code fuse.Status) {
	//glog.V(4).Infoln("FileSystemConnector  internalLookup:")
	child := parent.GetChild(name)

	//能找到子，且子上有挂载（？？？为什么子上有额外的挂载？）
	if child != nil && child.mountPoint != nil {
		glog.V(4).Infoln("child != nil && child.mountPoint != nil")
		return c.lookupMountUpdate(out, child.mountPoint)
	}

	if child != nil { //能找到，是普通的子，确定子的信息没有问题
		glog.V(4).Infoln("get child", child.fileinfo)
		code = child.fsInode.GetAttr(out, child.Fileinfo(), nil)
	} else {
		//这是在父里面没找到，
		//原代码是调用driver再到源端找一次，
		//调整，信任父节点内的子节点的完整性，不再额外查询
		//glog.V(4).Infoln("child == nil ,can not get child :", name, " in :", parent.fileinfo)
		code = fuse.ENOENT
		child = nil
	}
	if !code.Ok() {
		glog.V(1).Infoln("child:", child)
	}
	return child, code
}

//look name in Inode(header.NodeId)
//找到并注册使用，永远是从父找子
func (c *rawBridge) Lookup(header *fuse.InHeader, name string, out *fuse.EntryOut) (code fuse.Status) {
	parent := c.toInode(header.NodeId)
	if !parent.IsDir() {
		glog.V(0).Infoln("Lookup %q called on non-Directory node %d", name, header.NodeId)
		return fuse.ENOTDIR
	}
	if Debug {
		glog.V(0).Infoln("rawBridge  Lookup in :", parent.fileinfo.Name, " child:", name)
	}
	outAttr := (*fuse.Attr)(&out.Attr)
	child, code := c.fsConn().internalLookup(outAttr, parent, name, header)
	//没找到，（脏数据可暂留？舍意思）
	if code == fuse.ENOENT && parent.mount.negativeEntry(out) {
		glog.V(4).Infoln("code is ENOENT ,but negativeEntry is true")
		return fuse.OK
	}
	if !code.Ok() {
		glog.V(0).Infoln("rawBridge  Lookup in :", parent.fileinfo.Name, " child:", name, " err :", code)
		return code
	}
	child.mount.fillEntry(out)
	//对这个INODE注册inodeid，并统计计数
	out.NodeId, out.Generation = c.fsConn().lookupUpdate(child)
	if out.Ino == 0 {
		out.Ino = out.NodeId
	}
	if Debug {
		glog.V(0).Infoln("rawBridge  Lookup in :", parent.fileinfo.Name, " child:", name)
	}
	glog.V(4).Infoln(child.fileinfo, " 's inodeid :", out.NodeId)

	return fuse.OK
}

func (c *rawBridge) Forget(nodeID, nlookup uint64) {
	glog.V(4).Infoln("rawBridge  Forget in :", nodeID)
	if Debug {
		inode := c.toInode(nodeID)
		glog.V(0).Infoln("rawBridge  Forget   :", nodeID, inode.fileinfo.Name)
	}
	c.fsConn().forgetUpdate(nodeID, int(nlookup))
}

//获取文件属性动作
func (c *rawBridge) GetAttr(input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	node := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge GetAttr.Name:", node.fileinfo.Name," LastModified:",node.fileinfo.LastModified," Size:",node.fileinfo.Size," Expire:",node.fileinfo.Expire," inodeid:",input.NodeId)

	var f *FileInfo
	if input.Flags()&fuse.FUSE_GETATTR_FH != 0 {
		//已经被打开的文件，获取被打开的文件的属性。（写操作可能会临时的改动文件属性但是还没有提交）
		if opened := node.mount.getOpenedFile(input.Fh()); opened != nil {
			f = *(opened.File)
		}
	}
	//否则就直接使用node节点的
	if f == nil {
		f = node.fileinfo
	}
	node.fsInode.GetAttr((*fuse.Attr)(&out.Attr), f, &input.Context)
	///Nlink的作用  ？？？？？？这个操作的意义？？？？？
	if out.Nlink == 0 {
		out.Nlink = 1
	}
	node.mount.fillAttr(out, input.NodeId)
	return fuse.OK
}

//打开目录
//打开之后会有DirEntry 信息记录在connectorDir
//这个地方有个缺陷，connectorDir的stream 不会变动，实际上children是会变动的
func (c *rawBridge) OpenDir(input *fuse.OpenIn, out *fuse.OpenOut) (code fuse.Status) {
	inode := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge  OpenDir :", inode.fileinfo.Name)

	stream, _ := inode.Node().OpenDir(&input.Context)
	glog.V(4).Infoln(" OpenDir  children sum:", len(stream))
	de := &connectorDir{
		node:   inode.Node(),
		stream: stream,
		rawFS:  c,
	}

	h, opened := inode.mount.registerFileHandle(inode, de, nil, input.Flags)
	out.OpenFlags = opened.FuseFlags
	out.Fh = h
	glog.V(4).Infoln("rawBridge  OpenDir :", inode.fileinfo.Name, "get FH:", h)

	return fuse.OK
}

//读目录信息
func (c *rawBridge) ReadDir(input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	inode := c.toInode(input.NodeId)
	opened := inode.mount.getOpenedFile(input.Fh)
	glog.V(4).Infoln("rawBridge  ReadDir :", inode.fileinfo.Name, " NodeId:", input.NodeId, " Fh:", input.Fh)

	return opened.dir.ReadDir(input, out)
}

func (c *rawBridge) ReadDirPlus(input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	inode := c.toInode(input.NodeId)
	opened := inode.mount.getOpenedFile(input.Fh)
	//glog.V(4).Infoln("rawBridge  ReadDirPlus :", inode.fileinfo, " NodeId:", input.NodeId, " Fh:", input.Fh)

	return opened.dir.ReadDirPlus(input, out)
}

//打开文件
func (c *rawBridge) Open(input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	//根据inodeid找到inode
	inode := c.toInode(input.NodeId)
	//if status = inode.RefreshCheck(); status != fuse.OK {
	//	return
	//}

	glog.V(4).Infoln("rawBridge  Open :", inode.fileinfo.Name, " NodeId:", input.NodeId, input.Context.Pid)

	//假如文件被移除，当元数据刷新之后，他的parent里面
	//已经被删除了
	//父节点完全是空，肯定就是已经散落的节点
	//已经被内核记录过，所以才会被open
	//这个只存在于之 多节点的读写冲突中
	if inode != c.rootNode && inode.GetParentCount() == 0 {
		glog.V(0).Infoln("rawBridge  Open :", inode.fileinfo.Name, " NodeId:", input.NodeId, "but it's been deleted")
		return fuse.ENOENT
	}

	//存在，且有父节点（没被其他节点删除），对于元数据一直保持完整且最新的来说，打开动作没有后端行为了，
	/*
	f, code := inode.Node().Open(input.Flags, &input.Context)
	if !code.Ok() || f == nil {
		return code
	}
	*/

	h, opened := inode.mount.registerFileHandle(inode, nil, inode.fileinfo, input.Flags)
	out.OpenFlags = opened.FuseFlags
	out.Fh = h
	return fuse.OK
}

//设置文件属性 Truncate
//支持度比较小，
func (c *rawBridge) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {

	inode := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge  SetAttr :", inode.fileinfo, " NodeId:", input.NodeId)
	code = fuse.OK

	var f *FileInfo

	if input.Valid&fuse.FATTR_FH != 0 {
		glog.V(4).Infoln("rawBridge  SetAttr :", inode.fileinfo, " NodeId:", input.NodeId, "OP:FATTR_FH")
		opened := inode.mount.getOpenedFile(input.Fh)
		f = *(opened.File)
		if f == nil {
			glog.V(0).Infoln("rawBridge  SetAttr :", inode.fileinfo, " NodeId:", input.NodeId, "but not opened")
			f = inode.fileinfo
			//return fuse.EINVAL
		}
	}

	//改mode chmod
	if code.Ok() && input.Valid&fuse.FATTR_MODE != 0 {
		glog.V(4).Infoln("rawBridge  SetAttr :", inode.fileinfo, " NodeId:", input.NodeId, "OP:FATTR_MODE")
		if f == nil {
			glog.V(0).Infoln("rawBridge  SetAttr :", inode.fileinfo.Name, " NodeId:", input.NodeId, "but not opened")
			f = inode.fileinfo
			//return fuse.EINVAL
		}
		permissions := uint32(07777) & input.Mode
		code = inode.fsInode.Chmod(f, permissions, &input.Context)
	}

	//改uid/gid chown
	if code.Ok() && (input.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0) {
		glog.V(4).Infoln("rawBridge  SetAttr :", inode.fileinfo, " NodeId:", input.NodeId, "OP:FATTR_UID|FATTR_GID")
		if f == nil {
			glog.V(0).Infoln("rawBridge  SetAttr :", inode.fileinfo, " NodeId:", input.NodeId, "but not opened")
			f = inode.fileinfo
			//return fuse.EINVAL
		}
		var uid uint32 = ^uint32(0) // means "do not change" in chown(2)
		var gid uint32 = ^uint32(0)
		if input.Valid&fuse.FATTR_UID != 0 {
			uid = input.Uid
		}
		if input.Valid&fuse.FATTR_GID != 0 {
			gid = input.Gid
		}
		code = inode.fsInode.Chown(f, uid, gid, &input.Context)
	}

	//改大小 truncate
	if code.Ok() && input.Valid&fuse.FATTR_SIZE != 0 {
		glog.V(4).Infoln("rawBridge  SetAttr :", inode.fileinfo, " NodeId:", input.NodeId, "OP:FATTR_SIZE", input.Size)
		if f == nil {
			glog.V(0).Infoln("rawBridge  SetAttr :", inode.fileinfo, " NodeId:", input.NodeId, "but not opened")
			f = inode.fileinfo
		}
		code = inode.fsInode.Truncate(f, input.Size, &input.Context)
		if code.Ok() {
			inode.fileinfo.Size = 0
			inode.isnewcreate = true
		}
	}
	//修改文件的时间属性
	if code.Ok() && (input.Valid&(fuse.FATTR_ATIME|fuse.FATTR_MTIME|fuse.FATTR_ATIME_NOW|fuse.FATTR_MTIME_NOW) != 0) {
		glog.V(4).Infoln("rawBridge  SetAttr :", inode.fileinfo, " NodeId:", input.NodeId, "OP:FATTR_ATIME|FATTR_MTIME|FATTR_ATIME_NOW|FATTR_MTIME_NOW")
		if f == nil {
			f = inode.fileinfo
		//	glog.V(0).Infoln("rawBridge  SetAttr :", inode.fileinfo, " NodeId:", input.NodeId, "but not opened")
		//	return fuse.EINVAL
		}
		now := time.Now()
		var atime *time.Time
		var mtime *time.Time

		if input.Valid&fuse.FATTR_ATIME != 0 {
			if input.Valid&fuse.FATTR_ATIME_NOW != 0 {
				atime = &now
			} else {
				t := time.Unix(int64(input.Atime), int64(input.Atimensec))
				atime = &t
			}
		}

		if input.Valid&fuse.FATTR_MTIME != 0 {
			if input.Valid&fuse.FATTR_MTIME_NOW != 0 {
				mtime = &now
			} else {
				t := time.Unix(int64(input.Mtime), int64(input.Mtimensec))
				mtime = &t
			}
		}
		code = inode.fsInode.Utimens(f, atime, mtime, &input.Context)
	}

	if !code.Ok() {
		return code
	}

	// Must call GetAttr(); the filesystem may override some of
	// the changes we effect here.
	//重新获取文件的属性信息
	attr := (*fuse.Attr)(&out.Attr)
	code = inode.fsInode.GetAttr(attr, inode.fileinfo, &input.Context)
	if code.Ok() {
		inode.mount.fillAttr(out, input.NodeId)
	}
	return code
}

//这个操作需要支持么？
func (c *rawBridge) Fallocate(input *fuse.FallocateIn) (code fuse.Status) {

	inode := c.toInode(input.NodeId)
	opened := inode.mount.getOpenedFile(input.Fh)
	glog.V(4).Infoln("rawBridge  Fallocate :", inode.fileinfo, " NodeId:", input.NodeId)

	return inode.fsInode.Fallocate(*(opened.File), input.Offset, input.Length, input.Mode, &input.Context)
}

//软连接动作不支持
func (c *rawBridge) Readlink(input *fuse.InHeader) (out []byte, code fuse.Status) {

	inode := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge  Readlink :", inode.fileinfo, " NodeId:", input.NodeId)

	return inode.fsInode.Readlink(&input.Context)
}

//
func (c *rawBridge) Mknod(input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	inode := c.toInode(input.NodeId)

	glog.V(4).Infoln("rawBridge  Mknod :", inode.fileinfo, " NodeId:", input.NodeId)
	child, code := inode.fsInode.Mknod(name, input.Mode, uint32(input.Rdev), &input.Context)
	if code.Ok() {
		c.childLookup(out, child, &input.Context)
		code = child.fsInode.GetAttr((*fuse.Attr)(&out.Attr), nil, &input.Context)
	}
	return code
}

//创建文件夹
func (c *rawBridge) Mkdir(input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	inode := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge  Mkdir :", inode.fileinfo, " NodeId:", input.NodeId)
	child, code := inode.fsInode.Mkdir(name, input.Mode, &input.Context)
	if code.Ok() {
		c.childLookup(out, child, &input.Context)
		code = child.fsInode.GetAttr((*fuse.Attr)(&out.Attr), child.Fileinfo(), &input.Context)
	}
	return code
}

//删除文件
func (c *rawBridge) Unlink(input *fuse.InHeader, name string) (code fuse.Status) {
	inode := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge  Unlink :", inode.fileinfo.Name, " ", name, " NodeId:", input.NodeId)
	code = inode.fsInode.Unlink(name, &input.Context)
	return
}

//删除文件夹
func (c *rawBridge) Rmdir(input *fuse.InHeader, name string) (code fuse.Status) {
	inode := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge  Rmdir :", inode.fileinfo.Name, " ", name, " NodeId:", input.NodeId)
	return inode.fsInode.Rmdir(name, &input.Context)
}

func (c *rawBridge) Symlink(input *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) (code fuse.Status) {
	inode := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge  Symlink :", inode.fileinfo.Name, " NodeId:", input.NodeId)
	child, code := inode.fsInode.Symlink(linkName, pointedTo, &input.Context)
	if code.Ok() {
		c.childLookup(out, child, &input.Context)
		code = child.fsInode.GetAttr((*fuse.Attr)(&out.Attr), nil, &input.Context)
	}
	return code
}

func (c *rawBridge) Rename(input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	oldParent := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge  Rename :", oldParent.fileinfo, " NodeId:", input.NodeId)

	newParent := c.toInode(input.Newdir)
	if oldParent.mount != newParent.mount {
		return fuse.EXDEV
	}

	return oldParent.fsInode.Rename(oldName, newParent.fsInode, newName, &input.Context)
}

func (c *rawBridge) Link(input *fuse.LinkIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	existing := c.toInode(input.Oldnodeid)
	parent := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge  Link :", existing.fileinfo, " to ", parent.fsInode, name)

	if existing.mount != parent.mount {
		return fuse.EXDEV
	}

	child, code := parent.fsInode.Link(name, existing.fsInode, &input.Context)
	if code.Ok() {
		c.childLookup(out, child, &input.Context)
		code = child.fsInode.GetAttr((*fuse.Attr)(&out.Attr), nil, &input.Context)
	}

	return code
}

//判定文件是不是可以以某种方式
func (c *rawBridge) Access(input *fuse.AccessIn) (code fuse.Status) {

	inode := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge  Access :", inode.fileinfo, " NodeId:", input.NodeId)
	return inode.fsInode.Access(input.Mask, &input.Context)
}

//创建文件
func (c *rawBridge) Create(input *fuse.CreateIn, name string, out *fuse.CreateOut) (code fuse.Status) {
	parent := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge  Create :", parent.fileinfo.Name, " name:", name, " NodeId:", input.NodeId)

	f, child, code := parent.fsInode.Create(name, uint32(input.Flags), input.Mode, &input.Context)
	if !code.Ok() {
		return code
	}

	//构建，并注册为opend
	c.childLookup(&out.EntryOut, child, &input.Context)
	handle, opened := parent.mount.registerFileHandle(child, nil, f, input.Flags)
	out.OpenOut.OpenFlags = opened.FuseFlags
	out.OpenOut.Fh = handle
	glog.V(4).Infoln("rawBridge  Create OK:", parent.fileinfo.Name, " name:", name, " NodeId:", input.NodeId, " Fh:", handle)
	return code
}

//从open中移除，对应close操作
func (c *rawBridge) Release(input *fuse.ReleaseIn) {
	glog.V(4).Infoln("rawBridge  Release :", input.Fh)
	if input.Fh != 0 {
		inode := c.toInode(input.NodeId)
		filename := inode.fileinfo.Name
		_, obj := openFiles.Forget(input.Fh, 1)
		opened := (*openedFile)(unsafe.Pointer(obj))
		inode.fsInode.Release(opened.OpenFlags)
		inode.ReleaseOpenedFile(input.Fh, opened)
		if Debug {
			glog.V(0).Infoln("Release,Forget,ReleaseOpenedFile", filename, input.Fh, " count:", obj.count)
		}
	}
}

//dir的close
func (c *rawBridge) ReleaseDir(input *fuse.ReleaseIn) {
	glog.V(4).Infoln("rawBridge  ReleaseDir :", input.Fh)
	if input.Fh != 0 {
		node := c.toInode(input.NodeId)
		filename := node.fileinfo.Name
		_, obj := openFiles.Forget(input.Fh, 1)
		opened := (*openedFile)(unsafe.Pointer(obj))
		node.ReleaseOpenedFile(input.Fh, opened)
		if Debug {
			glog.V(0).Infoln("ReleaseDir,Forget,ReleaseOpenedFile", filename, input.Fh, " count:", obj.count)
		}
	}
}

//
func (c *rawBridge) GetXAttrSize(header *fuse.InHeader, attribute string) (sz int, code fuse.Status) {
	inode := c.toInode(header.NodeId)
	glog.V(4).Infoln("rawBridge  GetXAttrSize :", inode.fileinfo)
	if inode.fileinfo == nil {
		code = fuse.ENOENT
	}
	//data, errno := inode.fsInode.GetXAttr(attribute, &header.Context)
	return int(inode.fileinfo.Size), fuse.OK
}

func (c *rawBridge) GetXAttrData(header *fuse.InHeader, attribute string) (data []byte, code fuse.Status) {
	inode := c.toInode(header.NodeId)
	glog.V(4).Infoln("rawBridge  GetXAttrData :", inode.fileinfo)
	return inode.fsInode.GetXAttr(attribute, &header.Context)
}

func (c *rawBridge) RemoveXAttr(header *fuse.InHeader, attr string) fuse.Status {
	inode := c.toInode(header.NodeId)
	if inode.fileinfo != nil {
		glog.V(4).Infoln("rawBridge  RemoveXAttr :", inode.fileinfo.Name, attr)
	}
	return fuse.OK
	//return inode.fsInode.RemoveXAttr(attr, &header.Context)
}

func (c *rawBridge) SetXAttr(input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	inode := c.toInode(input.NodeId)
	glog.V(4).Infoln("rawBridge  SetXAttr :", inode.fileinfo)
	return fuse.OK
	//return inode.fsInode.SetXAttr(attr, data, int(input.Flags), &input.Context)
}

func (c *rawBridge) ListXAttr(header *fuse.InHeader) (data []byte, code fuse.Status) {
	inode := c.toInode(header.NodeId)
	glog.V(4).Infoln("rawBridge  ListXAttr :", inode.fileinfo)
	attrs, code := inode.fsInode.ListXAttr(&header.Context)
	if code != fuse.OK {
		return nil, code
	}

	b := bytes.NewBuffer([]byte{})
	for _, v := range attrs {
		b.Write([]byte(v))
		b.WriteByte(0)
	}

	return b.Bytes(), code
}

////////////////
// files.
func (c *rawBridge) Write(input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	inode := c.toInode(input.NodeId)

	glog.V(4).Infoln("rawBridge  Write :", inode.fileinfo, " Offset:", input.Offset, " input.Fh:", input.Fh)
	opened := inode.mount.getOpenedFile(input.Fh)

	//确保是已经open的文件,没打开的返回eio
	if opened == nil {
		glog.V(0).Infoln("rawBridge  Write :", inode.fileinfo, " Offset:", input.Offset, " input.Fh:", input.Fh, " error:opened == nil")
		return 0, fuse.EIO
	}
	//不可写，只读的
	if opened.OpenFlags&fuse.O_ANYWRITE == 0 {
		return 0, fuse.EACCES
	}

	//node.fileinfo.Size = int64(len(opened.temp))
	//eturn uint32(size), fuse.OK
	written, code = inode.fsInode.Write(*(opened.File), data, int64(input.Offset), &input.Context)
	if code.Ok() {
		//inode.fileinfo.Size += int64(written)
	}

	return
}

//保留
func (c *rawBridge) Read(input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	inode := c.toInode(input.NodeId)
	opened := inode.mount.getOpenedFile(input.Fh)
	glog.V(4).Infoln("rawBridge  Read :", inode.fileinfo, " Offset:", input.Offset, "Size:", input.Size, " input.Fh:", input.Fh)
	if opened == nil {
		return nil, fuse.EIO
	}
	return inode.fsInode.Read(*(opened.File), buf, int64(input.Offset), int64(input.Size), &input.Context)
}

func (c *rawBridge) StatFs(header *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	inode := c.toInode(header.NodeId)
	glog.V(4).Infoln("rawBridge  StatFs")
	s := inode.Node().StatFs()
	if s == nil {
		return fuse.ENOSYS
	}
	*out = *s
	return fuse.OK
}

func (c *rawBridge) Flush(input *fuse.FlushIn) fuse.Status {
	var code fuse.Status
	inode := c.toInode(input.NodeId)
	opened := inode.mount.getOpenedFile(input.Fh)

	glog.V(4).Infoln("rawBridge  Flush :", inode.fileinfo.Name, " input.Fh:", input.Fh)

	if opened != nil {
		if opened.OpenFlags&fuse.O_ANYWRITE != 0 {
			code = inode.fsInode.Fsync(opened)
			if code.Ok() == false {
				glog.V(0).Infoln("rawBridge  Flush :", inode.fileinfo.Name, " err:",code)
			}
			return code
		}
		return fuse.OK
	}
	return fuse.OK
}

func (c *rawBridge) Fsync(input *fuse.FsyncIn) fuse.Status {
	inode := c.toInode(input.NodeId)
	opened := inode.mount.getOpenedFile(input.Fh)
	glog.V(4).Infoln("rawBridge  Fsync :", inode.fileinfo, " input.Fh:", input.Fh)

	if opened != nil {
		//input.FsyncFlags
		return inode.fsInode.Fsync(opened)
	}

	return fuse.OK
}

// childLookup fills entry information for a newly created child inode
//
func (c *rawBridge) childLookup(out *fuse.EntryOut, n *Inode, context *fuse.Context) {
	n.Node().GetAttr((*fuse.Attr)(&out.Attr), n.Fileinfo(), context)
	n.mount.fillEntry(out)

	out.NodeId, out.Generation = inodeMap.Register(&n.handled)
	n.EventNodeUsed()
	if Debug {
		glog.V(0).Infoln(n.fileinfo.Name, " childLookup,Register,EventNodeUsed", out.NodeId, " count:", n.handled.count)
	}
	c.fsConn().verify()
	if out.Ino == 0 {
		out.Ino = out.NodeId
	}
	if out.Nlink == 0 {
		// With Nlink == 0, newer kernels will refuse link
		// operations.
		out.Nlink = 1
	}
}

func (c *rawBridge) toInode(nodeid uint64) *Inode {

	if nodeid == fuse.FUSE_ROOT_ID {
		return c.rootNode
	}

	i := (*Inode)(unsafe.Pointer(inodeMap.Decode(nodeid)))
	i.EventRefreshLastAccessTime()
	//glog.V(0).Infoln("toInode   ", nodeid)
	return i
}
