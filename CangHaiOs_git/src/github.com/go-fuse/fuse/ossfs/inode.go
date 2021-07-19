// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ossfs

import (
	"log"
	"sync"

	"github.com/go-fuse/fuse"
	"time"
	"code.google.com/p/weed-fs/go/glog"
	"container/list"
)

//文件过期时间，写端可以设置大一些。读端看时延要求，单位秒，默认10S
var fileinfolifecycle = int64(10 * 1e9)
var inodelifecycle = 2 * fileinfolifecycle

//对同一个文件的刷新子请求，大于指定值，直接刷新全目录
var FullRefreshThreshold = int(20)

type parentData struct {
	parent *Inode
	name   string
}

// Translate between uint64 handles and *Inode.
var inodeMap handleMap

// An Inode reflects the kernel's idea of the inode.  Inodes have IDs
// that are communicated to the kernel, and they have a tree
// structure: a directory Inode may contain named children.  Each
// Inode object is paired with a Node object, which file system
// implementers should supply.

//linux中的INODE 已经包含了所有文件信息，
//这里主要也是给更底层调用，
//定位节点一定要从上往下定位，（已知nodeid的除外）
type Inode struct {
	//句柄，请求的交互 通过handled来识别
	//每个绝对文件的handle必须是唯一的，
	//存在一个hashmap用于保存inode 的信息
	handled handled

	//自己以及整个子节点的打开文件的统计
	openFilesstatistics

	//自己以及整个子节点的handle的使用统计
	handlestatistics

	//
	generation uint64

	allopenFiles *openedFileColl

	//干活的，具体操作方法的具体实现
	fsInode Node

	// Each inode belongs to exactly one fileSystemMount. This
	// pointer is constant during the lifetime, except upon
	// Unmount() when it is set to nil.
	mount *fileSystemMount

	// All data below is protected by treeLock.
	//子，认为是全的（受oss的目录缓存上限限制/有一定的时延）,假如存在遍历，这个锁一定是从上往下获取
	childrenlock sync.RWMutex
	children     map[string]*Inode

	//子是不是少的
	childreduced bool
	reducedlock  sync.RWMutex

	//只允许1个进行回收动作
	refreshchan chan struct{}

	//这个信息的刷新时间由父节点的刷新时间决定，节点信息过期之后会刷新
	//文件信息，实际是准实时的，默认是OK 的，写端肯定是最新的（fsync、被动刷新 之后大小会变化，最后更新时间也会变动，最终结果以close之后为准），
	fileinfo *FileInfo

	// Due to hard links, an Inode can have many parents.
	//承接之前的，硬链接先没实现,不过保留了多父的结构
	//调整从map->数据
	//parents map[parentData]struct{}
	//默认0是直属
	parents     []*parentData
	parentslock sync.RWMutex

	// Non-nil if this inode is a mountpoint, ie. the Root of a
	// NodeFileSystem.
	//根挂载点才会有
	mountPoint *fileSystemMount

	//最后接触时间
	lastaccesstime int64

	//自己被刷新的时间,
	lastrefreshtime int64
	//刷新自己的子节点的时间，给文件夹类型的使用
	lastrefreshdirtime int64

	//用于标识是否已经被删除（文件可能已经被删除，元数据已经被刷新，但是本地还没有释放inode，属于离散状态）
	isremoved   bool
	isnewcreate bool

	totalsons int
	sonslock  sync.RWMutex

	refreshqueue *InodeRefreshQueue
}

func (n *Inode) gettotalsons() int {
	n.sonslock.RLock()
	defer n.sonslock.RUnlock()
	return n.totalsons
}

func (n *Inode) addsonscount(count int) {
	n.sonslock.Lock()
	n.totalsons += count
	n.sonslock.Unlock()
}

func (n *Inode) addsonscountloop(count int) {
	n.addsonscount(count)
	var p *parentData
	n.parentslock.Lock()
	if len(n.parents) > 0 {
		p = n.parents[0]
	}
	n.parentslock.Unlock()
	if p != nil {
		p.parent.addsonscountloop(count)
	}
}

func newInode(fsNode Node, fileinfo *FileInfo) *Inode {
	me := new(Inode)
	me.parents = []*parentData{}
	me.lastaccesstime = time.Now().UnixNano()
	if fileinfo.IsDir {
		me.children = make(map[string]*Inode, initDirSize)
	}
	me.lastrefreshtime = me.lastaccesstime
	me.childreduced = true
	me.fileinfo = fileinfo
	me.fsInode = fsNode
	me.fsInode.SetInode(me)
	me.refreshchan = make(chan struct{}, 1)
	me.allopenFiles = &openedFileColl{
		locopenedFiles: make(map[uint64]*openedFile),
	}
	if me.fileinfo.IsDir {
		me.refreshqueue = &InodeRefreshQueue{
			queue:       list.New(),
			quemap:      make(map[string]*InodeRefreshQueueNode),
			issearching: false,
			node:        me,
		}
	}

	return me
}

func (n *Inode) EventNodeUsed() {
	n.nodeUsedloop(true, 1)
}

func (n *Inode) GetRefreshSolt() bool {

	if n.ischildreduced() || time.Now().UnixNano()-fileinfolifecycle > n.lastrefreshdirtime {
		select {
		case n.refreshchan <- struct{}{}:
			if n.ischildreduced() || time.Now().UnixNano()-fileinfolifecycle > n.lastrefreshdirtime {
				return true
			}
			<-n.refreshchan

			return false
		case <-time.After(time.Duration(500) * time.Millisecond):
			glog.V(0).Info("want to GetRefreshSolt: ", n.fileinfo.Name, " but timeout")
			//别人在刷，就先等等，最长等1S
			return false
		}
	} else {
		return false
	}
}

func (n *Inode) ischildreduced() bool {
	n.reducedlock.RLock()
	defer n.reducedlock.RUnlock()
	return n.childreduced
}

func (n *Inode) setchildreduced(value bool) {
	n.reducedlock.RLock()
	if n.childreduced == value {
		n.reducedlock.RUnlock()
		return
	}
	n.reducedlock.RUnlock()

	n.reducedlock.Lock()
	n.childreduced = value
	n.reducedlock.Unlock()
}
func (n *Inode) ReleaseRefreshSolt(isfull bool) {
	if isfull {
		n.lastrefreshdirtime = time.Now().UnixNano()
	}
	select {
	case <-n.refreshchan:

	default:
		glog.V(0).Info("want to ReleaseRefreshSolt: ", n.fileinfo.Name, " but timeout")
	}
}

//刷新自己的子，假如fixname是“”，表示刷新所有，否则指定刷新
//同一时间只有1个在刷新
//优先刷新fixname="" 的
//
func (n *Inode) RefreshCheck(fixname string) {
	if Debug {
		glog.V(0).Infoln(n.fileinfo.Name, " refresh ", n.mountPoint == nil, n.IsDir(), " fixname:", fixname)
	}
	//
	if n.IsDir() {
		//刷新自己
		n.refreshqueue.insertRefresh(fixname)
	}
	//glog.V(4).Infoln(n.fileinfo.Name, " lastrefreshtime:", n.lastrefreshtime, " fileinfolifecycle:", fileinfolifecycle)
	//不需要刷新的
	return
}

func (n *Inode) EventNodeFree() {
	n.nodeUsedloop(true, -1)
}

//inode信息被kernel被使用了，这个节点就不能被释放
func (n *Inode) nodeUsedloop(isloc bool, count int) {
	if isloc {
		n.handlestatistics.addloc(count)
	} else {
		n.handlestatistics.addtol(count)
	}

	if n.mountPoint != nil {
		pars := n.GetAllParent()
		for _, v := range pars {
			v.parent.nodeUsedloop(false, count)
		}
	}
}

//open之后，需要把这个节点统计信息更新
func (n *Inode) EventFileUsed(opflags uint32) {
	w := 0
	r := 0
	if opflags&fuse.O_ANYWRITE != 0 {
		w = 1
	} else {
		r = 1
	}
	n.fileUsedloop(true, r, w)
}

//close之后需要更新统计信息
func (n *Inode) EventFileFree(opflags uint32) {
	w := 0
	r := 0
	if opflags&fuse.O_ANYWRITE != 0 {
		w = -1
	} else {
		r = -1
	}
	n.fileUsedloop(true, r, w)
}

func (n *Inode) EventRefreshLastAccessTime() {
	n.lastaccesstime = time.Now().UnixNano()
	pars := n.GetAllParent()
	for _, v := range pars {
		v.parent.EventRefreshLastAccessTime()
	}
}

func (n *Inode) fileUsedloop(isloc bool, r, w int) {

	if isloc {
		n.openFilesstatistics.addloc(r, w)
	} else {
		n.openFilesstatistics.addtol(r, w)
	}

	if n.mountPoint != nil {
		pars := n.GetAllParent()
		for _, v := range pars {
			v.parent.fileUsedloop(false, r, w)
		}
	}
}

// Children returns all children of this inode.
func (n *Inode) Children() (out map[string]*Inode) {
	n.mount.treeLock.RLock()
	out = make(map[string]*Inode, len(n.children))
	for k, v := range n.children {
		out[k] = v
	}
	n.mount.treeLock.RUnlock()

	return out
}

func (n *Inode) AddOpenedFile(opfile *openedFile) {
	if opfile == nil {
		return
	}
	n.allopenFiles.addOpenedFile(opfile)
	n.EventFileUsed(opfile.OpenFlags)
}

func (n *Inode) ReleaseOpenedFile(handle uint64, opfile *openedFile) {
	if opfile == nil {
		return
	}
	n.allopenFiles.releaseOpenedFile(handle)
	n.EventFileFree(opfile.OpenFlags)
}

// FsChildren returns all the children from the same filesystem.  It
// will skip mountpoints.
func (n *Inode) FsChildren() (out map[string]*Inode) {
	n.mount.treeLock.RLock()
	out = map[string]*Inode{}
	for k, v := range n.children {
		if v.mount == n.mount {
			out[k] = v
		}
	}
	n.mount.treeLock.RUnlock()

	return out
}

// Node returns the file-system specific node.
func (n *Inode) Node() Node {
	return n.fsInode
}
func (n *Inode) Fileinfo() *FileInfo {
	return n.fileinfo
}

// IsDir returns true if this is a directory.
func (n *Inode) IsDir() bool {
	return n.fileinfo.IsDir
}

func (n *Inode) GetFilesWithMask(mask uint32) (files []*FileInfo) {
	return n.allopenFiles.getFilesWithMask(mask)
}

// NewChild adds a new child inode to this inode.
func (n *Inode) NewChild(fileinfo *FileInfo, fsi Node) *Inode {
	/*
		ch := newInode(fsi, fileinfo)
		ch.mount = n.mount
		n.AddChild(fileinfo.Name, ch)
		return ch
	*/
	return nil
}

//创建一个子文件
//先直接创建一个有标识的元数据，直到close。节点刷新的时候碰到带新建标识的不会进行移除
func (n *Inode) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file *FileInfo, child *Inode, code fuse.Status) {

	/*
	//确定没人使用name
	n.childrenlock.Lock()
	defer n.childrenlock.Unlock()
	if v, ok := n.children[name]; ok {
		if !v.isremoved {
			//已经存在了
			code = fuse.EINVAL
			return
		}
	}
	file, child, code = n.fsInode.Create(name, flags, mode, context)
	if Debug {
		glog.V(0).Infoln(n.fileinfo.Name, " add child:", name)
	}
	n.addChild(name, child)
	*/

	return
}

// GetChild returns a child inode with the given name, or nil if it
// does not exist.
//1 先看自己的缓存时间
//2没过期，直接返回查询结果
//3 过期了，需要刷新下本地的缓存信息
func (n *Inode) GetChild(name string) (*Inode) {
	if name == "" {
		glog.V(0).Info("can not get child without name")
		return nil
	}
	n.childrenlock.RLock()
	child, ok := n.children[name]
	if child != nil {
		child.lastaccesstime = time.Now().UnixNano()
	}
	n.childrenlock.RUnlock()

	//没有或者有但是过期了并且还不是新建的就刷新一次
	if !ok || (ok && !child.isnewcreate && time.Now().UnixNano()-fileinfolifecycle > child.lastrefreshtime ) {
		if Debug {
			if ok {
				glog.V(0).Info(child.isnewcreate, time.Now().UnixNano()-fileinfolifecycle > child.lastrefreshtime, name)
			}
		}
		n.RefreshCheck(name)
		n.childrenlock.RLock()
		child, ok = n.children[name]
		n.childrenlock.RUnlock()
	}

	if ok && child.isremoved {
		child = nil
	}
	return child
}

// AddChild adds a child inode. The parent inode must be a directory
// node.
func (n *Inode) AddChild(name string, child *Inode) {
	if child == nil {
		log.Panicf("adding nil child as %q", name)
	}
	if Debug {
		glog.V(0).Infoln(n.fileinfo.Name, " add child:", name)
	}
	n.childrenlock.Lock()
	n.addChild(name, child)
	n.childrenlock.Unlock()

}

func (n *Inode) GetChildren() []fuse.DirEntry {
	if n.ischildreduced() || time.Now().UnixNano()-fileinfolifecycle > n.lastrefreshdirtime {
		if Debug {
			glog.V(0).Info(n.ischildreduced(), time.Now().UnixNano()-fileinfolifecycle > n.lastrefreshdirtime)
		}
		n.RefreshCheck("")
	}
	glog.V(4).Infoln("ossInode  OpenDir RefreshCheck:", n.fileinfo.Name)
	res := []fuse.DirEntry{}
	n.childrenlock.RLock()
	defer n.childrenlock.RUnlock()

	for name, v := range n.children {
		//跳过被删除的
		if v.isremoved {
			glog.V(4).Infoln(n.fileinfo.Name, "'s child :", name, "is deleted")
			continue
		}
		dir := fuse.DirEntry{}
		dir.Name = name
		if v.fileinfo.IsDir {
			dir.Mode = fuse.S_IFDIR | 0755
		} else {
			dir.Mode = fuse.S_IFREG | 0644
		}
		res = append(res, dir)
	}
	return res
}

func (n *Inode) RefreshChild(fixname string) map[string]*FileInfo {

	/*
		if !n.GetRefreshSolt() {
			glog.V(1).Infoln(n.fileinfo.Name, fixname, " need not RefreshChild")
			return nil
		}
		defer n.ReleaseRefreshSolt(fixname == "")
	*/
	if Debug {
		glog.V(0).Infoln("RefreshChild: ", n.fileinfo.Name, " now child sum:", len(n.children), " fixname:", fixname)
	}
	n.childrenlock.Lock()
	defer n.childrenlock.Unlock()

	glog.V(4).Infoln(n.fileinfo.Name, " need RefreshChild")
	children, code := n.fsInode.ListDir(fixname, nil)
	if !code.Ok() {
		glog.V(0).Infoln("refresh dir children err!!:", code)
		glog.V(0).Infoln("now children is dirty fileinfo")
		return nil
	}

	if fixname == "" {
		defer func() {
			n.lastrefreshdirtime = time.Now().UnixNano()
			n.setchildreduced(false)
		}()
	}

	for name, child := range children {
		//add not exist
		if _, ok := n.children[name]; !ok {
			if Getinodesum() > MaxInodesSum {
				glog.V(0).Infoln(MaxInodeSumErr)
				continue
			}
			if Debug {
				glog.V(0).Infoln(n.fileinfo.Name, " add child:", name)
			}
			tempinode := n.fsInode.(*ossInode)
			i := &ossInode{
				driver: tempinode.driver,
				ossfs:  tempinode.ossfs,
				cache:  tempinode.cache,
			}
			tempnode := newInode(i, child)
			tempnode.mount = n.mount
			n.addChild(name, tempnode)
		} else {
			//refresh
			n.children[name].fileinfo.IsDir = child.IsDir
			n.children[name].fileinfo.Name = child.Name
			n.children[name].fileinfo.Size = child.Size
			n.children[name].fileinfo.LastModified = child.LastModified
			n.children[name].fileinfo.Expire = child.Expire
			n.children[name].fileinfo.Etag = child.Etag
			//只要后端有了，缓存中的isnewcreate 标识可以不要了
			n.isnewcreate = false
			//删除都是实时的，后端有，就没有被删除
			n.children[name].isremoved = false
			glog.V(4).Infoln(n.fileinfo.Name, " refresh child:", name, " fixname:", fixname)

			n.children[name].lastrefreshtime = time.Now().UnixNano()

		}
	}
	if fixname == "" {
		for name, child := range n.children {
			if _, ok := children[name]; !ok {
				if !child.isnewcreate {
					if child.handled.count != 0 {
						//本地还有保持这handle，标识为已删除
						glog.V(4).Infoln(n.fileinfo.Name, " set child:", name, "removed")
						child.isremoved = true;
					} else {
						if Debug {
							glog.V(0).Infoln(n.fileinfo.Name, "can del child:", name)
						}
						//不是新建的文件？
						glog.V(4).Infoln(n.fileinfo.Name, " remove child:", name)
						n.rmChild(name)
					}
				}
			}
		}
	}
	return children
	//glog.V(4).Infoln("after refresh ,", n.fileinfo.Name, " left children :", len(n.children))
}

//删除自己的子
func (n *Inode) reduceChild() {

	n.childrenlock.Lock()
	defer n.childrenlock.Unlock()
	for k, v := range n.children {
		if v.canDel(inodelifecycle) {
			n.setchildreduced(true)
			n.rmChild(k)
		}
	}
	if Debug {
		glog.V(0).Infoln("after reduceChild ", n.fileinfo.Name, len(n.children))
	}
}

func (n *Inode) recursionReduceChild() {

	//先删除自己的子节点
	//root的不减，很多情况下root下会被频繁调用
	if n.mountPoint == nil {
		if Debug {
			glog.V(0).Infoln(n.fileinfo.Name, "want to reduceChild ")
		}
		n.reduceChild()
	}
	//让自己的子，再去删除。
	dirsons := n.GetDirChildren()
	for _, v := range dirsons {
		v.recursionReduceChild()
	}
}

func (n *Inode) GetDirChildren() []*Inode {
	res := []*Inode{}
	n.childrenlock.RLock()
	defer n.childrenlock.RUnlock()
	for _, v := range n.children {
		if v.IsDir() && !v.isremoved {
			res = append(res, v)
		}
	}
	return res
}

func (n *Inode) GetFirstParent() *parentData {
	if n.mountPoint != nil {
		//glog.V(0).Infoln(n.fileinfo.Name, "'s mountPoint is not null")
		return nil
	}
	n.parentslock.RLock()
	defer n.parentslock.RUnlock()
	if len(n.parents) == 0 {
		return nil
	}
	return n.parents[0]
}

func (n *Inode) AddParent(parent *parentData) {
	n.parentslock.Lock()
	defer n.parentslock.Unlock()
	n.parents = append(n.parents, parent)
}

func (n *Inode) GetParentCount() int {
	n.parentslock.RLock()
	defer n.parentslock.RUnlock()
	return len(n.parents)
}

func (n *Inode) RemoveParent(parentinode *Inode) {
	n.parentslock.Lock()
	defer n.parentslock.Unlock()
	for i, v := range n.parents {
		if v.parent == parentinode {
			if i == len(n.parents)-1 {
				n.parents = n.parents[:i]
			} else {
				n.parents = append(n.parents[:i], n.parents[i+1:]...)
			}
		}
	}
}

func (n *Inode) GetAllParent() []*parentData {
	res := []*parentData{}
	n.parentslock.RLock()
	defer n.parentslock.RUnlock()
	for _, v := range n.parents {
		res = append(res, v)
	}
	return res
}

// TreeWatcher is an additional interface that Nodes can implement.
// If they do, the OnAdd and OnRemove are called for operations on the
// file system tree. These functions run under a lock, so they should
// not do blocking operations.
type TreeWatcher interface {
	OnAdd(parent *Inode, name string)
	OnRemove(parent *Inode, name string)
}

// RmChild removes an inode by name, and returns it. It returns nil if
// child does not exist.
func (n *Inode) RmChild(name string) (ch *Inode) {
	n.childrenlock.Lock()
	glog.V(4).Infoln("par:", n.fileinfo.Name, " child:", name, " befor:", len(n.children))
	if Debug {
		glog.V(0).Infoln(n.fileinfo.Name, "can del child:", name)
	}
	ch = n.rmChild(name)
	n.childrenlock.Unlock()
	return
}

func (n *Inode) RenameChild(name, newname string) bool {
	n.childrenlock.Lock()
	defer n.childrenlock.Unlock()
	ch := n.children[name]
	if ch == nil {
		return false
	}
	//删除旧的
	delete(n.children, name)
	//删除可能存在的新的
	delete(n.children, newname)
	ch.fileinfo.Name = newname
	n.children[newname] = ch
	return true
}

//////////////////////////////////////////////////////////////
// private

// addChild adds "child" to our children under name "name".
// Must be called with treeLock for the mount held.
//新增一个inode
//没有锁
func (n *Inode) addChild(name string, child *Inode) {
	if paranoia {
		ch := n.children[name]
		if ch != nil && !ch.isremoved {
			log.Panicf("Already have an Inode with same name: %v: %v", name, ch)
		}
	}
	n.children[name] = child
	glog.V(4).Infoln(name, "has parent:", n.fileinfo.Name)
	child.AddParent(&parentData{n, n.fileinfo.Name})
	if w, ok := child.Node().(TreeWatcher); ok && child.mountPoint == nil {
		w.OnAdd(n, name)
	}
	return
}

// rmChild throws out child "name". This means (1) deleting "name" from our
// "children" map and (2) deleting ourself from the child's "parents" map.
// Must be called with treeLock for the mount held.
//移除inode只是从children中移除，并会更新他的parents
func (n *Inode) rmChild(name string) *Inode {
	ch := n.children[name]
	if ch != nil {
		if w, ok := ch.Node().(TreeWatcher); ok && ch.mountPoint == nil {
			w.OnRemove(n, name)
		}
		ch.isremoved = true
		delete(n.children, name)
		ch.RemoveParent(n)
		/*
		//ch的parents中移除
		for i, v := range ch.parents {
			if v.parent == n {
				if i == len(ch.parents)-1 {
					ch.parents = ch.parents[:i]
				} else {
					ch.parents = append(ch.parents[:i], ch.parents[i+1:]...)
				}
			}
		}
		*/
		//delete(ch.parents, parentData{n, name})

	}
	return ch
}

// Can only be called on untouched root inodes.
func (n *Inode) mountFs(opts *Options) {
	n.mountPoint = &fileSystemMount{
		mountInode: n,
		options:    opts,
	}
	n.mount = n.mountPoint
	Debug = opts.Debug
}

// Must be called with treeLock held.
func (n *Inode) canUnmount() bool {
	for _, v := range n.children {
		if v.mountPoint != nil {
			// This access may be out of date, but it is no
			// problem to err on the safe side.
			return false
		}
		if !v.canUnmount() {
			return false
		}
	}
	_, _, t := n.GetLocOpenedCount()
	ok := t == 0
	return ok
}

func (n *Inode) getMountDirEntries() (out []fuse.DirEntry) {
	n.childrenlock.RLock()
	for k, v := range n.children {
		if v.mountPoint != nil {
			out = append(out, fuse.DirEntry{
				Name: k,
				Mode: fuse.S_IFDIR,
			})
		}
	}
	n.childrenlock.RUnlock()

	return out
}

const initDirSize = 20

//保证inode这棵树上只有一个挂载点
func (n *Inode) verify(cur *fileSystemMount) {
	n.handled.verify()
	if n.mountPoint != nil {
		if n != n.mountPoint.mountInode {
			log.Panicf("mountpoint mismatch %v %v", n, n.mountPoint.mountInode)
		}
		cur = n.mountPoint

		cur.treeLock.Lock()
		defer cur.treeLock.Unlock()
	}
	if n.mount != cur {
		log.Panicf("n.mount not set correctly %v %v", n.mount, cur)
	}

	for nm, ch := range n.children {
		if ch == nil {
			log.Panicf("Found nil child: %q", nm)
		}
		ch.verify(cur)
	}
}

func (n *Inode) getused() int {
	return n.handled.GetCount()
}

//间隔多少时间没人用了
//单位秒
func (n *Inode) canDel(timeout int64) bool {
	//info是空？正常的业务流程不可能，
	if n.fileinfo == nil {
		if Debug {
			glog.V(0).Infoln("canDelcheck, fileinfo is null !!! error!!! delete it", n.fileinfo.Name, Getinodesum())
		}
		return true
	}

	if n.isremoved {
		if Debug {
			glog.V(0).Infoln("canDelcheck, n.isremoved!!!  delete it", n.fileinfo.Name)
		}
		return true
	}
	//只删除文件夹节点
	/*
		if !n.fileinfo.IsDir {
			//glog.V(4).Infoln(n.fileinfo.Name, " is not a dir")
			return false
		}
	*/

	if n.lastaccesstime+timeout > time.Now().UnixNano() {
		if Debug {
			glog.V(0).Infoln("lastaccesstime bigger than timeout!!! can not delete it", n.fileinfo.Name)
		}
		return false
	}
	usedsum := n.getused()
	if usedsum != 0 {
		if Debug {
			glog.V(0).Infoln("used is not zero!!!can not   delete it", n.fileinfo.Name)
		}
		return false
	}
	opendsum := 0
	if _, _, opendsum = n.GetTolOpenedCount(); opendsum > 0 {
		if Debug {
			glog.V(0).Infoln("opened is not zero!!!can not   delete it", n.fileinfo.Name)
		}
		return false
	}
	if Debug {
		glog.V(0).Infoln("canDelcheck, can del ", n.fileinfo.Name, " isdir:", n.fileinfo.IsDir)
	}
	return true
}

type InodeRefreshQueue struct {
	queue       *list.List
	quemap      map[string]*InodeRefreshQueueNode
	issearching bool
	node        *Inode
	sync.RWMutex
}

const (
	PROCESSING = iota
	DONE
)

type InodeRefreshQueueNode struct {
	name      string
	queuenode *list.Element
	cond      *sync.Cond
	status    int
	optime    int64
}

//指定了name就返回指定的，没指定或者找不到就不返回
func (irq *InodeRefreshQueue) insertRefresh(name string) {
	var cond *sync.Cond
	var queueNode *InodeRefreshQueueNode
	var ok bool
	if irq == nil {
		return
	}
	irq.Lock()
	if !irq.node.ischildreduced() && time.Now().UnixNano()-fileinfolifecycle < irq.node.lastrefreshdirtime {
		//刚刚被完全刷新，直接返回了
		irq.Unlock()
		return
	}
	irq.node.childrenlock.RLock()
	child, ok := irq.node.children[name]
	irq.node.childrenlock.RUnlock()

	if ok && (child.isnewcreate || time.Now().UnixNano()-fileinfolifecycle < child.lastrefreshtime ) {
		irq.Unlock()
		return
	}

	if queueNode, ok = irq.quemap[name]; ok {
		cond = queueNode.cond
	} else {
		if Debug {
			glog.V(0).Info("now irq.queue.Len()", irq.queue.Len(), name)
		}
		//超过FullRefreshThreshold个等待，直接整个文件夹刷新
		if irq.queue.Len() > FullRefreshThreshold {
			name = ""
		}
		var templock sync.RWMutex
		cond = sync.NewCond(&templock)
		queueNode = &InodeRefreshQueueNode{
			name:   name,
			cond:   cond,
			status: PROCESSING,
		}
		if name == "" {
			//glog.V(0).Info("now irq.queue.Len()", irq.queue.Len(), "PushFront:", name)
			listelement := irq.queue.PushFront(queueNode)
			queueNode.queuenode = listelement
		} else {
			//glog.V(0).Info("now irq.queue.Len()", irq.queue.Len(), "PushBack:", name)
			listelement := irq.queue.PushBack(queueNode)
			queueNode.queuenode = listelement
		}
		irq.quemap[name] = queueNode
	}
	if !irq.issearching {
		irq.issearching = true
		go irq.startRefresh()
	}

	irq.Unlock()

	cond.L.Lock()
	if queueNode.status != DONE {
		cond.Wait()
	}
	cond.L.Unlock()
}
func (irq *InodeRefreshQueue) startRefresh() {
	//glog.V(0).Info("InodeRefreshQueue startRefresh")
	searchname := ""
	var ok bool
	var refreshnode *InodeRefreshQueueNode
	var refreshnodetemp *list.Element
	//查询
	for {
		irq.Lock()
		if irq.queue.Len() != 0 {
			refreshnodetemp = irq.queue.Front()
			refreshnode, ok = (refreshnodetemp.Value).(*InodeRefreshQueueNode)
			if !ok {
				//glog.V(0).Info("dirty node in InodeRefreshQueue queue")
				irq.queue.Remove(refreshnodetemp)
				irq.Unlock()
				continue
			}
			searchname = refreshnode.name
		} else {
			irq.issearching = false
			irq.Unlock()
			break
		}

		irq.Unlock()
		if Debug {
			glog.V(0).Info(irq.node.fileinfo.Name, " RefreshChild:", searchname, "now irq.queue.Len()", irq.queue.Len())
		}
		children := irq.node.RefreshChild(searchname)
		//glog.V(0).Info(irq.node.fileinfo.Name, " RefreshChild:", searchname, " ok ")
		irq.Lock()
		if searchname == "" {
			for k, l := range irq.quemap {
				l.cond.L.Lock()
				l.status = DONE
				l.cond.Broadcast()
				l.cond.L.Unlock()
				l.optime = time.Now().UnixNano()
				irq.queue.Remove(l.queuenode)
				delete(irq.quemap, k)
				//glog.V(0).Info(irq.node.fileinfo.Name, " get and release searchnode:", k, " ", len(irq.quemap), " ", irq.queue.Len())
			}
			refreshnode.cond.L.Lock()
			refreshnode.status = DONE
			refreshnode.cond.Broadcast()
			refreshnode.cond.L.Unlock()
			irq.queue.Remove(refreshnodetemp)
			delete(irq.quemap, searchname)
		} else {
			for k, _ := range children {
				if l, ok := irq.quemap[k]; ok {
					l.cond.L.Lock()
					l.status = DONE
					l.cond.Broadcast()
					l.cond.L.Unlock()
					l.optime = time.Now().UnixNano()
					irq.queue.Remove(l.queuenode)
					delete(irq.quemap, k)
					//glog.V(0).Info(irq.node.fileinfo.Name, " get and release searchnode:", k, " ", len(irq.quemap), " ", irq.queue.Len())
				}
			}
			//不管有没有，肯定要移除自己
			if l, ok := irq.quemap[searchname]; ok {
				l.cond.L.Lock()
				l.status = DONE
				l.cond.Broadcast()
				l.cond.L.Unlock()
				l.optime = time.Now().UnixNano()
				irq.queue.Remove(l.queuenode)
				delete(irq.quemap, searchname)
				//glog.V(0).Info(irq.node.fileinfo.Name, " get node,and release searchnode:", searchname, " ", len(irq.quemap), " ", irq.queue.Len())
			}

		}
		irq.Unlock()
	}
}
