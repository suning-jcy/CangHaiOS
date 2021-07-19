package sftp

import "syscall"

func FakeFileInfoSys() interface{} {
	return syscall.Win32FileAttributeData{}
}

func testOsSys(sys interface{}) error {
	return nil
}
