package ossfs

import (
	"code.google.com/p/weed-fs/go/glog"
	"fmt"
	"github.com/go-fuse/fuse"
	"sync"
	"strconv"
)

//一个用于记录和分配打开文件的handle
var openFiles handleMap

// 文件的基本信息，没有实际操作
type FileInfo struct {
	IsDir        bool
	Name         string
	LastModified string
	Size         int64
	Etag         string
	Expire       string
}

// The String method is for debug printing.
func (o *FileInfo) String() string {
	info := ""
	info += "IsDir" + fmt.Sprint(o.IsDir) + "\n"
	info += "Name" + fmt.Sprint(o.Name) + "\n"
	info += "LastModified" + fmt.Sprint(o.LastModified) + "\n"
	info += "Size" + fmt.Sprint(o.Size) + "\n"
	info += "Expire" + fmt.Sprint(o.Expire) + "\n"
	return info
}

func (o *FileInfo) GetAttrbak(out *fuse.Attr) fuse.Status {
	glog.V(0).Infoln("ObjectFile  GetAttr", o.Name, o.IsDir, o.LastModified)
	out.Mtime, _ = strconv.ParseUint(o.LastModified, 10, 64)
	if o.IsDir {
		out.Mode = fuse.S_IFDIR | 0755
		out.Size = uint64(4096)
	} else {
		out.Size = uint64(o.Size)
		out.Mode = fuse.S_IFREG | 0644
	}
	glog.V(0).Infoln("ObjectFile  GetAttr done", *out)
	return fuse.OK
}

//复制一个
func (o *FileInfo) Copy() *FileInfo {
	newinfo := &FileInfo{}
	newinfo.IsDir = o.IsDir
	newinfo.Name = o.Name
	newinfo.LastModified = o.LastModified
	newinfo.Size = o.Size
	newinfo.Etag = o.Etag
	newinfo.Expire = o.Expire
	return newinfo
}

// openedFile stores either an open dir or an open file.
type openedFile struct {
	//唯一码
	//
	handled
	//flags
	WithFlags
	//文件夹独有，文件夹下能进行的“读写”操作
	dir *connectorDir
	//指向的文件，文件下进行的“读写”操作,基于对象存储，不存在过多的文件属性的变化，所以fileinfo都是用的同一个
	//读动作，同原始fileinfo，一直是最新的。写动作这个值是变化的。
	File **FileInfo
}

// String provides a debug string for the given file.
func (f *openedFile) String() string {
	return fmt.Sprintf("File %s (%s) %s %s",
		*(f.File), f.Description, fuse.FlagString(fuse.OpenFlagNames, int64(f.OpenFlags), "O_RDONLY"),
		fuse.FlagString(fuse.FuseOpenFlagNames, int64(f.FuseFlags), ""))
}

//inode节点的文件打开状态
type openedFileColl struct {
	//本节点的
	locopenedFiles map[uint64]*openedFile
	sync.RWMutex
}

func (ofc *openedFileColl) addOpenedFile(opfile *openedFile) {
	ofc.Lock()
	defer ofc.Unlock()
	ofc.locopenedFiles[opfile.handle] = opfile
}

func (ofc *openedFileColl) releaseOpenedFile(handle uint64) {
	ofc.Lock()
	defer ofc.Unlock()
	delete(ofc.locopenedFiles, handle)

}

func (ofc *openedFileColl) getOpenedFile(handle uint64) *openedFile {
	ofc.RLock()
	defer ofc.RUnlock()
	v, ok := ofc.locopenedFiles[handle]
	if ok {
		return v
	}
	return nil
}
func (ofc *openedFileColl) getFilesWithMask(mask uint32) (files []*FileInfo) {
	ofc.RLock()
	defer ofc.RUnlock()
	for _, f := range ofc.locopenedFiles {
		if mask == 0 || f.WithFlags.OpenFlags&mask != 0 {
			files = append(files, *(f.File))
		}
	}
	return files
}
