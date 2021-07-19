// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package objectfs

import (
	"time"

	"github.com/go-fuse/fuse"
)

// A filesystem API that uses paths rather than inodes.  A minimal
// file system should have at least a functional GetAttr method.
// Typically, each call happens in its own goroutine, so take care to
// make the file system thread-safe.
//
// NewDefaultFileSystem provides a null implementation of required
// methods.
type FileSystem interface {
	// Used for pretty printing.
	String() string

	// Attributes.  This function is the main entry point, through
	// which FUSE discovers which files and directories exist.
	//
	// If the filesystem wants to implement hard-links, it should
	// return consistent non-zero FileInfo.Ino data.  Using
	// hardlinks incurs a performance hit.
	GetAttr(name string, context *fuse.Context) (*ObjectFile, fuse.Status)

	// These should update the file's ctime too.
	Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status)
	Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status)
	Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (code fuse.Status)

	Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status)

	Access(name string, mode uint32, context *fuse.Context) (code fuse.Status)

	// Tree structure
	Link(oldName string, newName string, context *fuse.Context) (code fuse.Status)
	Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status
	Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status
	Rename(oldName string, newName string, context *fuse.Context) (code fuse.Status)
	Rmdir(name string, context *fuse.Context) (code fuse.Status)
	Unlink(name string, context *fuse.Context) (code fuse.Status)

	// Extended attributes.
	GetXAttr(name string, attribute string, context *fuse.Context) (data []byte, code fuse.Status)
	ListXAttr(name string, context *fuse.Context) (attributes []string, code fuse.Status)
	RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status
	SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status

	// Called after mount.
	OnMount(nodeFs *Objectfs)
	OnUnmount()

	// File handling.  If opening for writing, the file's mtime
	// should be updated too.
	Open(name string, flags uint32, context *fuse.Context) (file *ObjectFile, code fuse.Status)
	Create(name string, flags uint32, mode uint32, context *fuse.Context) (file *ObjectFile, code fuse.Status)
	Read(name string, dest []byte, off int64, size int64, context *fuse.Context) (fuse.ReadResult, fuse.Status)
	Write(name string, data []byte, off int64, context *fuse.Context) (written uint32, code fuse.Status)
	Release(file *ObjectFile)
	Flush(file *ObjectFile) fuse.Status
	Fsync(name string, data []byte) (code fuse.Status)

	// Directory handling
	OpenDir(name string, context *fuse.Context) (stream []*ObjectFile, code fuse.Status)

	// Symlinks.
	Symlink(value string, linkName string, context *fuse.Context) (code fuse.Status)
	Readlink(name string, context *fuse.Context) (string, fuse.Status)

	StatFs(name string) *fuse.StatfsOut
}

// Wrap a File return in this to set FUSE flags.  Also used internally
// to store open file data.
type WithFlags struct {

	// For debugging.
	Description string

	// Put FOPEN_* flags here.
	FuseFlags uint32

	// O_RDWR, O_TRUNCATE, etc.
	OpenFlags uint32
}

// Options contains time out options for a node FileSystem.  The
// default copied from libfuse and set in NewMountOptions() is
// (1s,1s,0s).
type Options struct {
	EntryTimeout    time.Duration
	AttrTimeout     time.Duration
	NegativeTimeout time.Duration

	// If set, replace all uids with given UID.
	// NewOptions() will set this to the daemon's
	// uid/gid.
	*fuse.Owner

	// This option exists for compatibility and is ignored.
	PortableInodes bool

	// If set, print debug information.
	Debug bool
}

// The Node interface implements the user-defined file system
// functionality
type Node interface {
	// Inode and SetInode are basic getter/setters.  They are
	// called by the FileSystemConnector. You get them for free by
	// embedding the result of NewDefaultNode() in your node
	// struct.
	Inode() *Inode
	SetInode(node *Inode)

	// OnMount is called on the root node just after a mount is
	// executed, either when the actual root is mounted, or when a
	// filesystem is mounted in-process. The passed-in
	// FileSystemConnector gives access to Notify methods and
	// Debug settings.
	OnMount(conn *FileSystemConnector)

	// OnUnmount is executed just before a submount is removed,
	// and when the process receives a forget for the FUSE root
	// node.
	OnUnmount()

	// Lookup finds a child node to this node; it is only called
	// for directory Nodes.
	Lookup(out *fuse.Attr, name string, context *fuse.Context) (*Inode, fuse.Status)

	// Deletable() should return true if this node may be discarded once
	// the kernel forgets its reference.
	// If it returns false, OnForget will never get called for this node. This
	// is appropriate if the filesystem has no persistent backing store
	// (in-memory filesystems) where discarding the node loses the stored data.
	// Deletable will be called from within the treeLock critical section, so you
	// cannot look at other nodes.
	Deletable() bool

	// OnForget is called when the kernel forgets its reference to this node and
	// sends a FORGET request. It should perform cleanup and free memory as
	// appropriate for the filesystem.
	// OnForget is not called if the node is a directory and has children.
	// This is called from within a treeLock critical section.
	OnForget()

	// Misc.
	Access(mode uint32, context *fuse.Context) (code fuse.Status)
	Readlink(c *fuse.Context) ([]byte, fuse.Status)

	// Namespace operations; these are only called on directory Nodes.

	// Mknod should create the node, add it to the receiver's
	// inode, and return it
	Mknod(name string, mode uint32, dev uint32, context *fuse.Context) (newNode *Inode, code fuse.Status)

	// Mkdir should create the directory Inode, add it to the
	// receiver's Inode, and return it
	Mkdir(name string, mode uint32, context *fuse.Context) (newNode *Inode, code fuse.Status)
	Unlink(name string, context *fuse.Context) (code fuse.Status)
	Rmdir(name string, context *fuse.Context) (code fuse.Status)

	// Symlink should create a child inode to the receiver, and
	// return it.
	Symlink(name string, content string, context *fuse.Context) (*Inode, fuse.Status)
	Rename(oldName string, newParent Node, newName string, context *fuse.Context) (code fuse.Status)

	// Link should return the Inode of the resulting link. In
	// a POSIX conformant file system, this should add 'existing'
	// to the receiver, and return the Inode corresponding to
	// 'existing'.
	Link(name string, existing Node, context *fuse.Context) (newNode *Inode, code fuse.Status)

	// Create should return an open file, and the Inode for that file.
	Create(name string, flags uint32, mode uint32, context *fuse.Context) (file *ObjectFile, child *Inode, code fuse.Status)

	// Open opens a file, and returns a File which is associated
	// with a file handle. It is OK to return (nil, OK) here. In
	// that case, the Node should implement Read or Write
	// directly.
	Open(flags uint32, context *fuse.Context) (file *ObjectFile, code fuse.Status)
	OpenDir(context *fuse.Context) ([]fuse.DirEntry, fuse.Status)
	Read(file *ObjectFile, dest []byte, off int64, size int64, context *fuse.Context) (fuse.ReadResult, fuse.Status)
	Write(file *ObjectFile, data []byte, off int64, context *fuse.Context) (written uint32, code fuse.Status)

	// XAttrs
	GetXAttr(attribute string, context *fuse.Context) (data []byte, code fuse.Status)
	RemoveXAttr(attr string, context *fuse.Context) fuse.Status
	SetXAttr(attr string, data []byte, flags int, context *fuse.Context) fuse.Status
	ListXAttr(context *fuse.Context) (attrs []string, code fuse.Status)

	// Attributes
	GetAttr(out *fuse.Attr, file *ObjectFile, context *fuse.Context) (code fuse.Status)
	Chmod(file *ObjectFile, perms uint32, context *fuse.Context) (code fuse.Status)
	Chown(file *ObjectFile, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status)
	Truncate(file *ObjectFile, size uint64, context *fuse.Context) (code fuse.Status)
	Utimens(file *ObjectFile, atime *time.Time, mtime *time.Time, context *fuse.Context) (code fuse.Status)
	Fallocate(file *ObjectFile, off uint64, size uint64, mode uint32, context *fuse.Context) (code fuse.Status)

	Fsync(openfile *openedFile) (code fuse.Status)

	StatFs() *fuse.StatfsOut
}
