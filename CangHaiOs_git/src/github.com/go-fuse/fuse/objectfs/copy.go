// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package objectfs

import (
	"os"

	"github.com/go-fuse/fuse"
)

func CopyFile(srcFs, destFs FileSystem, srcFile, destFile string, context *fuse.Context) fuse.Status {
	src, code := srcFs.Open(srcFile, uint32(os.O_RDONLY), context)
	if !code.Ok() {
		return code
	}
	defer src.Release()
	defer src.Flush()

	attr, code := srcFs.GetAttr(srcFile, context)
	if !code.Ok() {
		return code
	}
	var mode uint32
	if attr.IsDir {
		mode = fuse.S_IFDIR | 0755
	} else {
		mode = fuse.S_IFREG | 0644
	}

	dst, code := destFs.Create(destFile, uint32(os.O_WRONLY|os.O_CREATE|os.O_TRUNC), mode, context)
	if !code.Ok() {
		return code
	}
	defer dst.Release()
	defer dst.Flush()

	buf := make([]byte, 128*(1<<10))
	off := int64(0)
	for {
		res, code := src.Read(buf, off, int64(attr.Size))
		if !code.Ok() {
			return code
		}
		data, code := res.Bytes(buf)
		if !code.Ok() {
			return code
		}

		if len(data) == 0 {
			break
		}
		n, code := dst.Write(data, off)
		if !code.Ok() {
			return code
		}
		if int(n) < len(data) {
			return fuse.EIO
		}
		if len(data) < len(buf) {
			break
		}
		off += int64(len(data))
	}
	return fuse.OK
}
