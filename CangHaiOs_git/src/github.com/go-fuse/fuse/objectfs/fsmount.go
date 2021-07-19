// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package objectfs

import (
	"github.com/go-fuse/fuse"
	"sync"
	"time"
	"unsafe"
)

// openedFile stores either an open dir or an open file.
type openedFile struct {
	//唯一码
	handled
	//flags
	WithFlags
	//文件夹独有，文件夹下能进行的“读写”操作
	dir *connectorDir
	//指向的文件，文件下进行的“读写”操作
	File **ObjectFile

	temp []byte
}

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

	// Manage filehandles of open files.
	openFiles handleMap

	Debug bool

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
		b = (*openedFile)(unsafe.Pointer(m.openFiles.Decode(h)))
	}

	return b
}

//释放文件
func (m *fileSystemMount) unregisterFileHandle(handle uint64, node *Inode) *openedFile {
	//释放handle
	_, obj := m.openFiles.Forget(handle, 1)
	opened := (*openedFile)(unsafe.Pointer(obj))
	node.openFilesMutex.Lock()
	idx := -1
	for i, v := range node.openFiles {
		if v == opened {
			idx = i
			break
		}
	}

	l := len(node.openFiles)
	if idx == l-1 {
		node.openFiles[idx] = nil
	} else {
		node.openFiles[idx] = node.openFiles[l-1]
	}
	node.openFiles = node.openFiles[:l-1]
	node.openFilesMutex.Unlock()

	return opened
}

//保存打开的文件/文件夹
//在Inode 的openFiles 内添加一个打开的文件/文件夹
func (m *fileSystemMount) registerFileHandle(node *Inode, dir *connectorDir, f *ObjectFile, flags uint32) (uint64, *openedFile) {
	node.openFilesMutex.Lock()
	//update  fileifno
	if f != nil {
		node.fileinfo = f
	}

	b := &openedFile{
		dir: dir,
		WithFlags: WithFlags{
			OpenFlags: flags,
		},
		File: &node.fileinfo,
	}
	//写的才去创建temp
	if flags&fuse.O_ANYWRITE != 0 {
		b.temp = []byte{}
		//return nil, fuse.EPERM
	}
	/*
		if node.fileinfo != nil {
			node.fileinfo.SetInode(node)
		}
	*/
	node.openFiles = append(node.openFiles, b)
	//if b.handled is 0   create new one, if not return old one
	handle, _ := m.openFiles.Register(&b.handled)
	node.openFilesMutex.Unlock()
	return handle, b
}

// Creates a return entry for a non-existent path.
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
