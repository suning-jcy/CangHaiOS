// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package objectfs

import (
	"github.com/go-fuse/fuse"
	"log"
	"sync"

	"code.google.com/p/weed-fs/go/glog"
)

type connectorDir struct {
	node  Node
	rawFS fuse.RawFileSystem

	// Protect stream and lastOffset.  These are written in case
	// there is a seek on the directory.
	mu     sync.Mutex
	stream []fuse.DirEntry

	// lastOffset stores the last offset for a readdir. This lets
	// readdir pick up changes to the directory made after opening
	// it.
	lastOffset uint64
}

func (d *connectorDir) ReadDir(input *fuse.ReadIn, out *fuse.DirEntryList) (code fuse.Status) {
	glog.V(1).Infoln("connectorDir  ReadDir")

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.stream == nil {
		return fuse.OK
	}
	// rewinddir() should be as if reopening directory.
	// TODO - test this.
	glog.V(0).Infoln("1")
	if d.lastOffset > 0 && input.Offset == 0 {
		glog.V(0).Infoln("2")
		d.stream, code = d.node.OpenDir((*fuse.Context)(&input.Context))
		if !code.Ok() {
			return code
		}
	}
	glog.V(0).Infoln("3")
	if input.Offset > uint64(len(d.stream)) {
		// This shouldn't happen, but let's not crash.
		return fuse.EINVAL
	}
	glog.V(0).Infoln("4")
	todo := d.stream[input.Offset:]
	for _, e := range todo {
		glog.V(0).Infoln("5")
		if e.Name == "" {
			log.Printf("got empty directory entry, mode %o.", e.Mode)
			continue
		}
		ok, off := out.AddDirEntry(e)
		d.lastOffset = off
		if !ok {
			break
		}
	}
	glog.V(0).Infoln("6")
	return fuse.OK
}

func (d *connectorDir) ReadDirPlus(input *fuse.ReadIn, out *fuse.DirEntryList) (code fuse.Status) {
	glog.V(1).Infoln("connectorDir ReadDirPlus")

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.stream == nil {
		return fuse.OK
	}

	// rewinddir() should be as if reopening directory.
	if d.lastOffset > 0 && input.Offset == 0 {
		d.stream, code = d.node.OpenDir((*fuse.Context)(&input.Context))
		if !code.Ok() {
			return code
		}
	}

	if input.Offset > uint64(len(d.stream)) {
		// This shouldn't happen, but let's not crash.
		return fuse.EINVAL
	}
	todo := d.stream[input.Offset:]
	for _, e := range todo {
		if e.Name == "" {
			log.Printf("got empty directory entry, mode %o.", e.Mode)
			continue
		}

		// we have to be sure entry will fit if we try to add
		// it, or we'll mess up the lookup counts.
		entryDest, off := out.AddDirLookupEntry(e)
		if entryDest == nil {
			break
		}
		entryDest.Ino = uint64(fuse.FUSE_UNKNOWN_INO)

		// No need to fill attributes for . and ..
		if e.Name == "." || e.Name == ".." {
			continue
		}

		// Clear entryDest before use it, some fields can be corrupted if does not set all fields in rawFS.Lookup
		*entryDest = fuse.EntryOut{}

		d.rawFS.Lookup(&input.InHeader, e.Name, entryDest)
		d.lastOffset = off
	}
	return fuse.OK

}

type rawDir interface {
	ReadDir(out *fuse.DirEntryList, input *fuse.ReadIn, c *fuse.Context) fuse.Status
	ReadDirPlus(out *fuse.DirEntryList, input *fuse.ReadIn, c *fuse.Context) fuse.Status
}
