// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ossfs

// This file contains the internal logic of the
// FileSystemConnector. The functions for satisfying the raw interface
// are in fsops.go

import (
	"github.com/go-fuse/fuse"
	"log"
	"path/filepath"
	"strings"
	"time"
	"code.google.com/p/weed-fs/go/glog"
	"errors"
)

// Tests should set to true.
var paranoia = false
var MaxInodesSum = int(5000000)
var totalinodesum *int

var MaxInodeSumErr = errors.New("no free inode")
// FilesystemConnector translates the raw FUSE protocol (serialized
// structs of uint32/uint64) to operations on Go objects representing
// files and directories.
type FileSystemConnector struct {
	debug bool
	// Callbacks for talking back to the kernel.
	server *fuse.Server
	// The root of the FUSE file system.
	rootNode *Inode
}

// NewOptions generates FUSE options that correspond to libfuse's
// defaults.
func NewOptions() *Options {
	return &Options{
		NegativeTimeout: 0,
		AttrTimeout:     time.Second,
		EntryTimeout:    time.Second,
		Owner:           fuse.CurrentOwner(),
	}
}

// NewFileSystemConnector creates a FileSystemConnector with the given
// options.
func NewFileSystemConnector(root Node, opts *Options) (c *FileSystemConnector) {
	c = new(FileSystemConnector)
	if opts == nil {
		opts = NewOptions()
	}

	obj := &FileInfo{}
	obj.Name = ""
	obj.IsDir = true
	obj.Size = 0
	c.rootNode = newInode(root, obj)

	c.verify()
	c.mountRoot(opts)

	// FUSE does not issue a LOOKUP for 1 (obviously), but it does
	// issue a forget.  This lookupUpdate is to make the counts match.
	//c.lookupUpdate(c.rootNode)
	inodeMap.Register(&(c.rootNode.handled))
	c.rootNode.EventNodeUsed()
	c.verify()
	c.debug = opts.Debug

	return c
}

// Server returns the fuse.Server that talking to the kernel.
func (c *FileSystemConnector) Server() *fuse.Server {
	return c.server
}

// SetDebug toggles printing of debug information. This function is
// deprecated. Set the Debug option in the Options struct instead.
func (c *FileSystemConnector) SetDebug(debug bool) {
	c.debug = debug
}

// This verifies invariants of the data structure.  This routine
// acquires tree locks as it walks the inode tree.
func (c *FileSystemConnector) verify() {
	if !paranoia {
		return
	}
	root := c.rootNode
	root.verify(c.rootNode.mountPoint)
}

// Must run outside treeLock.  Returns the nodeId and generation.
//给一个inode一个唯一码，
func (c *FileSystemConnector) lookupUpdate(node *Inode) (id, generation uint64) {

	//取一下inodeid，（没有会新分配）
	id, generation = inodeMap.Register(&(node.handled))
	node.EventNodeUsed()
	//？？？？必要？每次都要验证下挂载点的校验？？
	//c.verify()
	if Debug {
		glog.V(0).Infoln(node.fileinfo.Name, " lookupUpdate,Register,EventNodeUsed", id," count:",node.handled.count)
	}
	return
}

// forgetUpdate decrements the reference counter for "nodeID" by "forgetCount".
// Must run outside treeLock.
func (c *FileSystemConnector) forgetUpdate(nodeID uint64, forgetCount int) {
	if nodeID == fuse.FUSE_ROOT_ID {
		c.rootNode.Node().OnUnmount()
		// We never got a lookup for root, so don't try to
		// forget root.
		return
	}
	//从inode里面删除
	_,h:=inodeMap.Forget(nodeID, forgetCount)
	if Debug {
		glog.V(0).Infoln("forgetUpdate ,Forget", nodeID," count:",h.count)
	}
}

// InodeCount returns the number of inodes registered with the kernel.
func (c *FileSystemConnector) InodeHandleCount() int {
	return inodeMap.Count()
}

// Finds a node within the currently known inodes, returns the last
// known node and the remaining unknown path components.  If parent is
// nil, start from FUSE mountpoint.
func (c *FileSystemConnector) Node(parent *Inode, fullPath string) (*Inode, []string) {
	if parent == nil {
		parent = c.rootNode
	}
	if fullPath == "" {
		return parent, nil
	}

	sep := string(filepath.Separator)
	fullPath = strings.TrimLeft(filepath.Clean(fullPath), sep)
	comps := strings.Split(fullPath, sep)

	node := parent
	if node.mountPoint == nil {
		node.mount.treeLock.RLock()
		defer node.mount.treeLock.RUnlock()
	}

	for i, component := range comps {
		if len(component) == 0 {
			continue
		}

		if node.mountPoint != nil {
			node.mount.treeLock.RLock()
			defer node.mount.treeLock.RUnlock()
		}

		next := node.children[component]
		if next == nil {
			return node, comps[i:]
		}
		node = next
	}

	return node, nil
}

// Follows the path from the given parent, doing lookups as
// necesary. The path should be '/' separated without leading slash.
func (c *FileSystemConnector) LookupNode(parent *Inode, path string) *Inode {
	if path == "" {
		return parent
	}

	components := strings.Split(path, "/")
	for _, r := range components {
		var a fuse.Attr
		// This will not affect inode ID lookup counts, which
		// are only update in response to kernel requests.
		var dummy fuse.InHeader
		child, _ := c.internalLookup(&a, parent, r, &dummy)
		if child == nil {
			return nil
		}

		parent = child
	}

	return parent
}

func (c *FileSystemConnector) mountRoot(opts *Options) {
	c.rootNode.mountFs(opts)
	c.rootNode.mount.connector = c
	c.verify()
}

// Mount() generates a synthetic directory node, and mounts the file
// system there.  If opts is nil, the mount options of the root file
// system are inherited.  The encompassing filesystem should pretend
// the mount point does not exist.
//
// It returns ENOENT if the directory containing the mount point does
// not exist, and EBUSY if the intended mount point already exists.
func (c *FileSystemConnector) Mount(parent *Inode, name string, root Node, opts *Options) fuse.Status {
	node, code := c.lockMount(parent, name, root, opts)
	if !code.Ok() {
		return code
	}
	//node.fsInode.OnMount(c)
	node.Node().OnMount(c)
	return code
}

func (c *FileSystemConnector) lockMount(parent *Inode, name string, root Node, opts *Options) (*Inode, fuse.Status) {
	defer c.verify()
	parent.mount.treeLock.Lock()
	defer parent.mount.treeLock.Unlock()
	node := parent.children[name]
	if node != nil {
		return nil, fuse.EBUSY
	}

	node = newInode(root, nil)
	if opts == nil {
		opts = c.rootNode.mountPoint.options
	}

	node.mountFs(opts)
	node.mount.connector = c
	parent.addChild(name, node)

	node.mountPoint.parentInode = parent
	if c.debug {
		log.Printf("Mount %T on subdir %s, parent %d", node,
			name, inodeMap.Handle(&parent.handled))
	}
	return node, fuse.OK
}

// Unmount() tries to unmount the given inode.  It returns EINVAL if the
// path does not exist, or is not a mount point, and EBUSY if there
// are open files or submounts below this node.
func (c *FileSystemConnector) Unmount(node *Inode) fuse.Status {
	// TODO - racy.
	if node.mountPoint == nil {
		log.Println("not a mountpoint:", inodeMap.Handle(&node.handled))
		return fuse.EINVAL
	}

	nodeID := inodeMap.Handle(&node.handled)

	// Must lock parent to update tree structure.
	parentNode := node.mountPoint.parentInode
	parentNode.mount.treeLock.Lock()
	defer parentNode.mount.treeLock.Unlock()

	mount := node.mountPoint
	name := node.mountPoint.mountName()
	if openFiles.Count() > 0 {
		return fuse.EBUSY
	}

	node.mount.treeLock.Lock()
	defer node.mount.treeLock.Unlock()

	if mount.mountInode != node {
		log.Panicf("got two different mount inodes %v vs %v",
			inodeMap.Handle(&mount.mountInode.handled),
			inodeMap.Handle(&node.handled))
	}

	if !node.canUnmount() {
		return fuse.EBUSY
	}

	delete(parentNode.children, name)
	node.Node().OnUnmount()

	parentId := inodeMap.Handle(&parentNode.handled)
	if parentNode == c.rootNode {
		// TODO - test coverage. Currently covered by zipfs/multizip_test.go
		parentId = fuse.FUSE_ROOT_ID
	}

	// We have to wait until the kernel has forgotten the
	// mountpoint, so the write to node.mountPoint is no longer
	// racy.
	mount.treeLock.Unlock()
	parentNode.mount.treeLock.Unlock()
	code := c.server.DeleteNotify(parentId, nodeID, name)

	if code.Ok() {
		delay := 100 * time.Microsecond

		for {
			// This operation is rare, so we kludge it to avoid
			// contention.
			time.Sleep(delay)
			delay = delay * 2
			if !inodeMap.Has(nodeID) {
				break
			}

			if delay >= time.Second {
				// We limit the wait at one second. If
				// it takes longer, something else is
				// amiss, and we would be waiting forever.
				log.Println("kernel did not issue FORGET for node on Unmount.")
				break
			}
		}

	}

	parentNode.mount.treeLock.Lock()
	mount.treeLock.Lock()
	mount.mountInode = nil
	node.mountPoint = nil

	return fuse.OK
}

// FileNotify notifies the kernel that data and metadata of this inode
// has changed.  After this call completes, the kernel will issue a
// new GetAttr requests for metadata and new Read calls for content.
// Use negative offset for metadata-only invalidation, and zero-length
// for invalidating all content.
//这个动作之后的inode是不允许删除的。
//也就是说这个inode的handle已经被系统记录了
func (c *FileSystemConnector) FileNotify(node *Inode, off int64, length int64) fuse.Status {
	var nId uint64
	if node == c.rootNode {
		nId = fuse.FUSE_ROOT_ID
	} else {
		nId = inodeMap.Handle(&node.handled)
	}

	if nId == 0 {
		return fuse.OK
	}
	return c.server.InodeNotify(nId, off, length)
}

// EntryNotify makes the kernel forget the entry data from the given
// name from a directory.  After this call, the kernel will issue a
// new lookup request for the given name when necessary. No filesystem
// related locks should be held when calling this.
func (c *FileSystemConnector) EntryNotify(node *Inode, name string) fuse.Status {
	var nId uint64
	if node == c.rootNode {
		nId = fuse.FUSE_ROOT_ID
	} else {
		nId = inodeMap.Handle(&node.handled)
	}

	if nId == 0 {
		return fuse.OK
	}
	return c.server.EntryNotify(nId, name)
}

// DeleteNotify signals to the kernel that the named entry in dir for
// the child disappeared. No filesystem related locks should be held
// when calling this.
func (c *FileSystemConnector) DeleteNotify(dir *Inode, child *Inode, name string) fuse.Status {
	var nId uint64

	if dir == c.rootNode {
		nId = fuse.FUSE_ROOT_ID
	} else {
		nId = inodeMap.Handle(&dir.handled)
	}

	if nId == 0 {
		return fuse.OK
	}

	chId := inodeMap.Handle(&child.handled)

	return c.server.DeleteNotify(nId, chId, name)
}

func Getinodesum() int {
	if totalinodesum == nil {
		return 0
	}
	return *totalinodesum
}
