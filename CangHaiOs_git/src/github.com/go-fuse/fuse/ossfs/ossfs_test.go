package ossfs

import (
	"testing"
)

//主要的测试范围是元数据
//特别是元数据的保持/刷新/删除

func TestInodeCleanLoop(t *testing.T) {
	temproot := NewOssfs(nil)
	CleanLoop(temproot.root)
	conn := NewFileSystemConnector(temproot.root, nil)
	conn.rootNode.Create("file1", 0, 0, nil)
	conn.rootNode.Create("file2", 0, 0, nil)
	conn.rootNode.Create("file3", 0, 0, nil)

	for _, v := range conn.rootNode.children {
		v.isnewcreate = false
	}


}

func TestRawBridge_Lookup(t *testing.T) {

}

func TestRawBridge_Access(t *testing.T) {

}

func TestRawBridge_Create(t *testing.T) {

}

func TestRawBridge_Fallocate(t *testing.T) {

}

func TestRawBridge_Forget(t *testing.T) {

}

func TestRawBridge_Fsync(t *testing.T) {

}

func TestRawBridge_Flush(t *testing.T) {

}

func TestRawBridge_FsyncDir(t *testing.T) {

}

func TestRawBridge_GetAttr(t *testing.T) {

}

func TestRawBridge_GetXAttrData(t *testing.T) {

}

func TestRawBridge_GetXAttrSize(t *testing.T) {

}

func TestRawBridge_Init(t *testing.T) {

}

func TestRawBridge_Link(t *testing.T) {

}

func TestRawBridge_ListXAttr(t *testing.T) {

}

func TestRawBridge_Mkdir(t *testing.T) {

}

func TestRawBridge_Mknod(t *testing.T) {

}

func TestRawBridge_Open(t *testing.T) {

}

func TestRawBridge_OpenDir(t *testing.T) {

}

func TestRawBridge_Read(t *testing.T) {

}

func TestRawBridge_ReadDir(t *testing.T) {

}

func TestRawBridge_ReadDirPlus(t *testing.T) {

}

func TestRawBridge_Readlink(t *testing.T) {

}

func TestRawBridge_Release(t *testing.T) {

}

func TestRawBridge_ReleaseDir(t *testing.T) {

}

func TestRawBridge_RemoveXAttr(t *testing.T) {

}

func TestRawBridge_Rename(t *testing.T) {

}

func TestRawBridge_Rmdir(t *testing.T) {

}

func TestRawBridge_SetAttr(t *testing.T) {

}

func TestRawBridge_SetDebug(t *testing.T) {

}

func TestRawBridge_SetXAttr(t *testing.T) {

}

func TestRawBridge_StatFs(t *testing.T) {

}
func TestRawBridge_String(t *testing.T) {

}
func TestRawBridge_Symlink(t *testing.T) {

}
func TestRawBridge_Unlink(t *testing.T) {

}
func TestRawBridge_Write(t *testing.T) {

}
