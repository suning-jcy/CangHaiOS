// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ossfs

import (
	"github.com/go-fuse/fuse"
	"sync"
	"time"
	"unsafe"
	"code.google.com/p/weed-fs/go/glog"
)

// Manage filehandles of open files.
var Debug = false

type fileSystemMount struct {
	// Node that we were mounted on.
	mountInode *Inode

	// Parent to the mountInode.
	parentInode *Inode

	// Options for the mount.
	options *Options

	// Protects the "children" and "parents" hashmaps of the inodes
	// within the mount.
	// treeLock should be acquired before openFilesLock.
	//
	// If multiple treeLocks must be acquired, the treeLocks
	// closer to the root must be acquired first.
	treeLock sync.RWMutex

	connector *FileSystemConnector
}

// Must called with lock for parent held.
func (m *fileSystemMount) mountName() string {
	for k, v := range m.parentInode.children {
		if m.mountInode == v {
			return k
		}
	}
	panic("not found")
}

func (m *fileSystemMount) setOwner(attr *fuse.Attr) {
	if m.options.Owner != nil {
		attr.Owner = *(*fuse.Owner)(m.options.Owner)
	}
}

func (m *fileSystemMount) fillEntry(out *fuse.EntryOut) {
	if m.options == nil {
		m.options = NewOptions()
	}
	splitDuration(m.options.EntryTimeout, &out.EntryValid, &out.EntryValidNsec)
	splitDuration(m.options.AttrTimeout, &out.AttrValid, &out.AttrValidNsec)
	m.setOwner(&out.Attr)
	if out.Mode&fuse.S_IFDIR == 0 && out.Nlink == 0 {
		out.Nlink = 1
	}
}

func (m *fileSystemMount) fillAttr(out *fuse.AttrOut, nodeId uint64) {
	splitDuration(m.options.AttrTimeout, &out.AttrValid, &out.AttrValidNsec)
	m.setOwner(&out.Attr)
	if out.Ino == 0 {
		out.Ino = nodeId
	}
}

//获取到已经打开的文件或者文件夹
func (m *fileSystemMount) getOpenedFile(h uint64) *openedFile {
	var b *openedFile
	if h != 0 {
		b = (*openedFile)(unsafe.Pointer(openFiles.Decode(h)))
	}

	return b
}

//释放文件
func (m *fileSystemMount) unregisterFileHandle(handle uint64, node *Inode) *openedFile {
	//释放handle
	_, obj := openFiles.Forget(handle, 1)
	opened := (*openedFile)(unsafe.Pointer(obj))
	node.ReleaseOpenedFile(handle, opened)
	if Debug {
		glog.V(0).Infoln("unregisterFileHandle ,Forget,ReleaseOpenedFile", handle," count:",obj.count)
	}
	return opened
}

//保存打开的文件/文件夹
//在Inode 的openFiles 内添加一个打开的文件/文件夹
func (m *fileSystemMount) registerFileHandle(node *Inode, dir *connectorDir, f *FileInfo, flags uint32) (uint64, *openedFile) {

	//update  fileifno
	if f != nil {
		node.fileinfo = f
	}

	b := &openedFile{
		dir: dir,
		WithFlags: WithFlags{
			OpenFlags: flags,
		},
	}

	if flags&fuse.O_ANYWRITE != 0 {
		//tempinfo := node.fileinfo.Copy()
		//b.File = &(tempinfo)
		b.File = &(node.fileinfo)
	} else {
		b.File = &(node.fileinfo)
	}
	//获取handle
	handle, _ := openFiles.Register(&b.handled)
	node.AddOpenedFile(b)
	if Debug {
		glog.V(0).Infoln(node.fileinfo.Name, " registerFileHandle,Register,AddOpenedFile",handle," count:",b.handled.count)
	}

	return handle, b
}

// Creates a return entry for a non-existent path.
//为什么要保留这个差异时间？？？？
func (m *fileSystemMount) negativeEntry(out *fuse.EntryOut) bool {
	if m.options.NegativeTimeout > 0.0 {
		out.NodeId = 0
		splitDuration(m.options.NegativeTimeout, &out.EntryValid, &out.EntryValidNsec)
		return true
	}
	return false
}
func splitDuration(dt time.Duration, secs *uint64, nsecs *uint32) {
	ns := int64(dt)
	*nsecs = uint32(ns % 1e9)
	*secs = uint64(ns / 1e9)
}
