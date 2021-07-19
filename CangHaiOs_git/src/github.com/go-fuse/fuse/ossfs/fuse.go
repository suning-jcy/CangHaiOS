// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ossfs

import (
	"github.com/go-fuse/fuse"
	"os"
	"time"
	"code.google.com/p/weed-fs/go/glog"
)

// Mounts a filesystem with the given root node on the given directory
//保留每个node都有mount的意义的意义在于，以后可以根据用户权限来设定不同的挂载根目录
func MountRoot(mountpoint string, root Node, opts *Options) (*fuse.Server, *FileSystemConnector, error) {

	//记录和分配inode的handle
	inodeMap = newPortableHandleMap()
	//记录和分配file的handle
	openFiles = newPortableHandleMap()

	//FileSystemConnector 就是rawBridge  是实现跟底层对接的，实现的RawFileSystem
	conn := NewFileSystemConnector(root, opts)
	mountOpts := opts.MountOptions
	if opts.MaxInodesSum == 0 {
		opts.MaxInodesSum = 5000000
	}
	MaxInodesSum = opts.MaxInodesSum
	totalinodesum = &(root.Inode().totalsons)
	//mountOpts.Debug = true
	s, err := fuse.NewServer(conn.RawFS(), mountpoint, &mountOpts)
	if err != nil {
		return nil, nil, err
	}
	CleanLoop(root)
	return s, conn, nil
}

func InodeCacheCheck(nodesum int){
	//向/proc写入命令，释放掉inode和Dentry缓存
	filename:="/proc/sys/vm/drop_caches"
	file,err:=os.OpenFile(filename,os.O_TRUNC|os.O_RDWR,0644)
	if err != nil {
		glog.V(0).Infoln("open",filename," err:",err)
		return
	}
	defer file.Close()
	data:=[]byte("2")
	_,err=file.Write(data)
	if err != nil {
		glog.V(0).Infoln("write",filename," err:",err)
		return
	}
	glog.V(0).Infoln("Notice! Free Cache By Force.oldSum:",nodesum," newSum:",Getinodesum())
}

func CleanLoop(root Node) {
	glog.V(0).Infoln("start CleanLoop")
	rootinode := root.Inode()
	if rootinode == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Duration(inodelifecycle) * time.Nanosecond)
		for _ = range ticker.C {
			nodesum := Getinodesum()
			if Debug {
				glog.V(0).Infoln("recursionReduceChild start.inodeSum:",nodesum)
			}
			rootinode.recursionReduceChild()
			if nodesum *100 > MaxInodesSum * 80 {
				InodeCacheCheck(nodesum)
			}
		}

	}()
}
