package filemap

import (

)

type FilesMap interface {
	 CreateFile(dirId DirectoryId, fileName string, fid string) (err error)
	 FindFile(dirId DirectoryId, fileName string) (fid string, err error) 
	 DeleteFile(dirId DirectoryId, fileName string) (fid string, err error)
	 ListFiles(dirId DirectoryId, lastFileName string, limit int) (files []FileEntry)
	
}