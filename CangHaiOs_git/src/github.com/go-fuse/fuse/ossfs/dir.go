// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ossfs

import (
	"github.com/go-fuse/fuse"
	"log"
	"sync"

	"code.google.com/p/weed-fs/go/glog"
)

//管理被open的dir
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

//connectorDir的stream 不会变动，实际上children是会变动的，todo，以后stream也会相应变化
func (d *connectorDir) ReadDir(input *fuse.ReadIn, out *fuse.DirEntryList) (code fuse.Status) {
	glog.V(4).Infoln("connectorDir  ReadDir")

	d.mu.Lock()
	defer d.mu.Unlock()
	//
	if d.stream == nil {
		return fuse.OK
	}
	//远端过来的是从0开始的，本地的不是从0 开始，重新填充stream
	if d.lastOffset > 0 && input.Offset == 0 {
		d.stream, _ = d.node.OpenDir(nil)
	}

	if input.Offset > uint64(len(d.stream)) {
		glog.V(0).Infoln(input.Offset, len(d.stream))

		return fuse.EINVAL
	}
	//直接从stream中取数据
	todo := d.stream[input.Offset:]
	for _, e := range todo {

		if e.Name == "" {
			log.Printf("got empty directory entry, mode %o.", e.Mode)
			continue
		}
		//输出到out中
		ok, off := out.AddDirEntry(e)
		d.lastOffset = off
		if !ok {
			break
		}
	}
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
		d.stream, _ = d.node.OpenDir(nil)
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
