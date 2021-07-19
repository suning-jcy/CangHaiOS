package main

import (
	"bytes"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"
	"path"
	"mime"
	"io"
	"io/ioutil"
	"net/http"
	"errors"
	"fmt"
	"code.google.com/p/weed-fs/go/auth"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/bucket"
	"code.google.com/p/weed-fs/go/filemap"
	"github.com/go-fuse/fuse"
	"github.com/go-fuse/fuse/ossfs"
)

var (
	//for handler
	FUSEErrBadSerBuc = errors.New("fuse:bad server(bucket)")
	FUSEErrBadSerAcc = errors.New("fuse:bad server(account)")
	FUSEErrBadSerFiler = errors.New("fuse:bad server(filer)")
	FUSEErrNameExist = errors.New("fuse:user name exist")
	FUSEErrNoPermission = errors.New("fuse:Permission denied")
	FUSEErrInvalidPath = errors.New("fuse:Invalid path")
	FUSEErrReNameDir = errors.New("fuse:can not rename dir")
	FUSEErrMvFile = errors.New("fuse:can not mv file to different dir")
	FUSEErrNoFile = errors.New("fuse:No such file or directory")
	FUSEErrBadRequest = errors.New("fuse:Bad Request ")
	FUSEErrBadConf = errors.New("fuse:Bad conf file ")
	FUSEErrBadConfVersion = errors.New("fuse:Bad conf file version")
	FUSEErrOpened = errors.New("fuse:ftp had opened ")
	FUSEErrClosed = errors.New("fuse:ftp had closed ")
	FUSEErrUserNotExist = errors.New("fuse:user not exist ")
	FUSEErrUserPassWdMisMatch = errors.New("fuse:user password mismatch ")
	FUSEErrNomalUserExist = errors.New("fuse:nomal user exist ")
	FUSEErrAdminNeeded = errors.New("fuse:admin user needed ")
	FUSEErrUserExist = errors.New("fuse:user exist ")
	FUSEErrAcessKeyAuthFailed = errors.New("fuse:acesskey auth failed ")
	FUSEErrBucketIsSystemDef = errors.New("fuse:bucket is sys-define")
	FUSEErrBucketIsPublicWR = errors.New("fuse:bucket is public-read-write")
	FUSEErrBucketNotExist = errors.New("fuse:bucket not exist")
	FUSEErrCreated = errors.New("fuse:ftp had created ")
	FUSEErrNoCreated = errors.New("fuse:ftp not created ")
	FUSEErrEncrypt = errors.New("fuse:error to Encrypt ")
	FUSEErrDecrypt = errors.New("fuse:error to Decrypt ")

	FUSEErrBadUserName = errors.New("fuse:bad name ")
	FUSEErrBadPass = errors.New("fuse:bad pass ")
	FUSEErrBadRootPath = errors.New("fuse:bad root path ")
	FUSEErrRootPathInUse = errors.New("fuse:rootpath is in use ")
	FUSEErrBadMode = errors.New("fuse:bad mode ")

	FUSEErrRead = errors.New("fuse:read file or directory error")
	FUSEErrWrite = errors.New("fuse:write file or directory error")

	//inside
	ErrBadAuth = errors.New("Error Authorization")
	ErrNotExist = errors.New("Error Not Exist")
	ErrDirnotnull = errors.New("dir Not null")
	ErrRequest = errors.New("Error Request")
	ErrUnknow = errors.New("Error Unknow")
	ErrBaseConf = errors.New("Error BaseConf")
	ErrUserConf = errors.New("Error UserConf")
)
//超过4M 就使用分块上传
var fusemultthreshold = int64(4 * 1024 * 1024)
//每个分块4M
var fuseblocksize = int64(4 * 1024 * 1024)

var maxshowfileSum = 3000
//SdossFs----》SdossDriver
//实现跟SdossDriver的具体对接
type SdossDriver struct {
	//基于基本的objectfs 添加业务
	ossfs.Driver

	//账户
	Account               string
	//bucket
	Bucket                string
	//id
	AccessKeyId           string
	//sec
	AccessKeysecret       string
	//filer地址，可以多个
	Filer                 string
	FilerSelecter         *util.FilerNodes

	bucketattr            *bucket.Bucket
	lastbucketrefreshtime time.Time
}

func (sf *SdossDriver) String() string {
	res := "---------------SdossDriver---------------" + "\n"
	res += "Account:" + sf.Account + "\n"
	res += "Bucket:" + sf.Bucket + "\n"
	res += "Access-key:" + sf.AccessKeyId + "\n"
	res += "Access-Secret :" + sf.AccessKeysecret + "\n"
	res += "Oss-endpoint:" + sf.Filer + "\n"
	res += "------------------------------------"
	return res
}

//获取对象属性
func (sf *SdossDriver) GetAttr(name string, context *fuse.Context) (*ossfs.FileInfo, fuse.Status) {
	glog.V(4).Infoln("SdossDriver Op GetObjAttr:", name)
	_, obj, sta := sf.getObjAttr(name)
	return obj, sta
}

//获取指定名称的文件属性
//以当前目录为根本
func (sf *SdossDriver) getObjAttr(path string) (*fuse.Attr, *ossfs.FileInfo, fuse.Status) {
	starttime := time.Now()
	defer func() {
		glog.V(4).Infoln("getObjAttr total spend:", time.Since(starttime).String())
	}()
	if path != "/" {
		path = "/" + path
	}
	if path == "/" {
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, &ossfs.FileInfo{
			Name: "/",
		}, fuse.OK
	}

	_, name, parentKey, mykey, patherr := path2namebase(path)
	if patherr != nil {
		glog.V(0).Infoln("error while getObjAttr:path2name", patherr)
		return nil, nil, fuse.EINVAL
	}

	//stat file or dir
	var ret, root, objstr map[string]interface{}
	var objects []interface{}
	obj := &filemap.Object{}
	var objattr *fuse.Attr
	var marker string
	flag := true
	var ok bool
	wantdir := false

	//要是带有/结尾的，要把/ 去掉
	real_name, _ := url.QueryUnescape(mykey)
	if strings.HasSuffix(real_name, "/") {
		real_name = real_name[:len(real_name) - 1]
		wantdir = true
	}

	for flag {
		parameters := make(map[string]string)
		parameters["type"] = "dir"
		parameters["max-keys"] = "500"
		parameters["prefix"] = name
		parameters["dir"] = parentKey
		parameters["marker"] = marker
		if parentKey == "" {
			parameters["dir"] = "/"
		}
		data, err := sf.urlRequest("", "GET", parameters, nil)
		if err != nil {
			glog.V(0).Infoln("error while  get " + path + " urlRequest: " + err.Error())
			return nil, nil, fuse.EIO
		}
		if data == nil {
			glog.V(0).Infoln("get empty reply from server")
			return nil, nil, fuse.ENOENT
		}
		err = json.Unmarshal(data, &ret)
		if err != nil {
			glog.V(0).Infoln("error while  get " + path + " Unmarshal: " + err.Error(), "data:", string(data))
			return nil, nil, fuse.EIO
		}
		root, ok = ret["List-Bucket-Result"].(map[string]interface{})
		if !ok {
			break
		}
		if root["Contents"] == nil {
			break
		}
		objects, ok = root["Contents"].([]interface{})
		if !ok || len(objects) == 0 {
			break
		}
		if root["Next-Marker"] == nil {
			flag = false
			marker = ""
		} else {
			flag = true
			marker, ok = (root["Next-Marker"]).(string)
			if !ok {
				flag = false
			}
		}
		listsize := len(objects)
		for i := 0; i < listsize; i++ {
			objstr, ok = objects[i].(map[string]interface{})
			if ok && objstr["Key"] != nil {
				tempname, _ := objstr["Key"].(string)
				tempisdir := false
				if strings.HasSuffix(tempname, "/") {
					tempname = tempname[:len(tempname) - 1]
					tempisdir = true
				}
				if tempname != real_name {
					continue
				}
				if tempisdir != wantdir {
					continue
				}
				obj.Key, _ = objstr["Key"].(string)
			} else {
				continue
			}
			if objstr["Last-Modified"] != nil {
				obj.LastModified, _ = objstr["Last-Modified"].(string)
			}
			if objstr["Size"] != nil {
				tempsize, ok := objstr["Size"].(float64)
				if ok {
					obj.Size = int64(tempsize)
				}
			}
		}
	}

	if obj.Key == "" {
		//找不到
		return nil, nil, fuse.ENOENT
	}

	baseobj := &ossfs.FileInfo{}
	objattr = &fuse.Attr{}

	baseobj.LastModified = obj.LastModified
	baseobj.IsDir, baseobj.Name, _, _, _ = path2namebase("/" + obj.Key)
	if baseobj.IsDir {
		objattr.Mode = fuse.S_IFDIR | 0755
	} else {
		baseobj.Size = obj.Size
		objattr.Size = uint64(obj.Size)
		objattr.Mode = fuse.S_IFREG | 0644
	}
	return objattr, baseobj, fuse.OK
}

//现在只有statfs使用，缓存的机制不费事了
func (sf *SdossDriver) getBukAttr() (*bucket.Bucket, error) {
	if sf.bucketattr == nil || (sf.bucketattr != nil && sf.lastbucketrefreshtime.Before(time.Now().Add(5 * time.Minute))) {
		parameters := make(map[string]string)
		parameters["type"] = "attribute"
		data, err := sf.urlRequest("", "GET", parameters, nil)
		if err != nil {
			return nil, err
		}
		buk := new(bucket.Bucket)
		err = json.Unmarshal(data, buk)
		if err != nil {
			return nil, err
		}
		sf.bucketattr = buk
		sf.lastbucketrefreshtime = time.Now()
		return buk, nil
	}
	return sf.bucketattr, nil
}

//变更文件的权限
func (sf *SdossDriver) Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("SdossDriver Op GetObjAttr:", name, " mode:", mode)
	return fuse.OK
}

//变更文件的拥有者,不支持
func (sf *SdossDriver) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("SdossDriver Op Chown:", name, " uid:", uid, " gid:", gid)
	return fuse.OK
}

//改变文件时间戳,不支持
func (sf *SdossDriver) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("SdossDriver Op Utimens:", name, " Atime:", Atime, " Mtime", Mtime)
	return fuse.OK
}

//文件缩减至指定大小,不支持
//可以通过类似reopen的操作来进行,看需求再调整
func (sf *SdossDriver) Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("SdossDriver Op Truncate:", name, " size:", size)
	return fuse.OK
}

//检查是否可以读/写某一已存在的文件？？？
func (sf *SdossDriver) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("SdossDriver Op Access:", name, " mode:", mode)
	return fuse.OK
}

//链接，从oldName指向newName
func (sf *SdossDriver) Link(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	glog.V(4).Infoln("SdossDriver Op Link, oldName:", oldName, " newName:", newName)
	return fuse.ENOSYS
}

//创建文件夹
func (sf *SdossDriver) Mkdir(path string, mode uint32, context *fuse.Context) fuse.Status {
	starttime := time.Now()
	defer func() {
		glog.V(4).Infoln("Mkdir total spend:", time.Since(starttime).String())
	}()
	glog.V(4).Infoln("SdossDriver Op Mkdir:", path, "  mode :", mode)
	if path == "/" {
		glog.V(0).Infoln("error while MakeDir " + path + ": can not create root ")
		return fuse.EINVAL
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	_, _, _, myKey, patherr := path2namebase(path)
	if patherr != nil {
		glog.V(0).Infoln("error while MakeDir " + path + ": " + patherr.Error())
		return fuse.EINVAL
	}

	//get one object
	parameters := make(map[string]string)
	parameters["type"] = "dir"
	parameters["dir"] = myKey
	parameters["recursive"] = "1"
	_, err := sf.urlRequest("", "PUT", parameters, nil)
	if err != nil {
		glog.V(0).Infoln("error while MakeDir " + path + ": " + err.Error())
		return fuse.EINVAL
	}
	return fuse.OK
}

//创建设备文件
func (sf *SdossDriver) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
	glog.V(4).Infoln("SdossDriver Op Mkdir:", name, "  mode :", mode, " dev:", dev)
	return fuse.ENOSYS
}

//重命名文件夹/文件
func (sf *SdossDriver) Rename(fromInfo *ossfs.FileInfo, oldName string, newName string, context *fuse.Context) (code fuse.Status) {

	glog.V(4).Infoln("SdossDriver Op Rename ,oldName:", oldName, "  newName :", newName)

	_, _, fromParentKey, myKey, patherr := path2namebase(oldName)
	if patherr != nil {
		glog.V(0).Infoln("SdossDriver Op Rename error :", patherr, oldName)
		return fuse.EINVAL
	}

	_, _, toParentKey, to_name, patherr2 := path2namebase(newName)
	if patherr2 != nil {
		glog.V(0).Infoln("SdossDriver Op Rename error :", patherr2, newName)
		return fuse.EINVAL
	}

	//重命名文件
	if !fromInfo.IsDir {
		parameters := make(map[string]string)
		parameters["type"] = "rename"
		parameters["toname"] = to_name
		_, err := sf.urlRequest(myKey, "PUT", parameters, nil)
		if err != nil {
			tempkey1, _ := url.QueryUnescape(myKey)
			tempkey2, _ := url.QueryUnescape(to_name)
			glog.V(0).Infoln("error while Rename " + tempkey1 + " to name " + tempkey2 + " : " + err.Error())
			if err == ErrNotExist {
				return fuse.ENOENT
			}
			return fuse.EIO
		}
		return fuse.OK
	}
	//文件夹的重命名，
	//1 只能同文件夹
	//2 只能空文件夹（filer验证）
	if fromParentKey != toParentKey {
		glog.V(0).Infoln("error while Rename " + oldName + ": " + FUSEErrMvFile.Error())
		return fuse.EINVAL
	}
	//get one object
	parameters := make(map[string]string)
	parameters["type"] = "dir"
	parameters["rename"] = "1"
	parameters["to_name"] = to_name + "/"
	parameters["dir"] = myKey + "/"
	_, err := sf.urlRequest("", "PUT", parameters, nil)
	if err != nil {
		tempkey1, _ := url.QueryUnescape(myKey)
		tempkey2, _ := url.QueryUnescape(to_name)
		glog.V(0).Infoln("error while Rename " + tempkey1 + " to name " + tempkey2 + " : " + err.Error())
		if err == ErrNotExist {
			return fuse.ENOENT
		}
		return fuse.EIO
	}

	return fuse.OK
}

//删除文件夹
func (sf *SdossDriver) Rmdir(path string, context *fuse.Context) (code fuse.Status) {

	glog.V(4).Infoln("SdossDriver Op Rmdir:", path)
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	if path == "/" {
		glog.V(0).Infoln("error while Rmdir " + path + ": can not rm root ")
		return fuse.EINVAL
	}

	_, _, _, myKey, patherr := path2namebase(path)
	if patherr != nil {
		glog.V(0).Infoln("error while  DeleteDir " + path + ": " + patherr.Error())
		return fuse.EINVAL
	}

	//get one object
	parameters := make(map[string]string)
	parameters["type"] = "dir"
	parameters["dir"] = myKey
	_, err := sf.urlRequest("", "DELETE", parameters, nil)
	if err != nil {
		if err == ErrNotExist {
			glog.V(0).Infoln("error while Rmdir " + path + err.Error())
			return fuse.ENOENT
		}
		glog.V(0).Infoln("error while Rmdir " + path + err.Error())
		return fuse.EIO
	}
	return fuse.OK

}

//解开链接,删除文件
func (sf *SdossDriver) Unlink(path string, context *fuse.Context) (code fuse.Status) {

	glog.V(4).Infoln("SdossDriver Op Unlink:", path)
	_, _, _, myKey, patherr := path2namebase(path)
	if patherr != nil || myKey == "" {
		glog.V(0).Infoln(patherr)
		return fuse.EINVAL
	}

	_, err := sf.urlRequest(myKey, "DELETE", nil, nil)
	if err != nil {
		glog.V(0).Infoln("error while  DeleteFile " + path + ": " + err.Error())
		if err == ErrNotExist {
			return fuse.ENOENT
		}
		return fuse.EIO
	}
	return fuse.OK
}

//打开指定的文件夹
//类似就是dirlist
func (sf *SdossDriver) OpenDir(path string, fixname string, context *fuse.Context) (c map[string]*ossfs.FileInfo, code fuse.Status) {

	glog.V(2).Infoln("SdossDriver Op OpenDir:", path)
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	c = make(map[string]*ossfs.FileInfo)

	_, _, _, myKey, patherr := sf.path2name(path)
	if patherr != nil {
		glog.V(0).Infoln(patherr.Error())
		return nil, fuse.EINVAL
	}

	breakflag := false
	marker := ""
	filesum := 0
	var ok bool
	var bucketRoot, bucketRet map[string]interface{}
	var objects []interface{}
	for {
		parameters := make(map[string]string)
		parameters["type"] = "dir"
		parameters["max-keys"] = "1000"
		parameters["dir"] = myKey
		if marker != "" {
			parameters["marker"] = marker
		}
		if fixname != "" {
			parameters["prefix"] = fixname
			parameters["max-keys"] = "10"
		}
		data, err := sf.urlRequest("", "GET", parameters, nil)
		if err != nil {
			glog.V(0).Infoln("error while  list " + path + ": " + err.Error())
			return nil, fuse.EIO
		}
		err = json.Unmarshal(data, &bucketRet)
		if err != nil {
			glog.V(0).Infoln("error while  list " + path + ": " + err.Error())
			return nil, fuse.EIO
		}
		bucketRoot, ok = bucketRet["List-Bucket-Result"].(map[string]interface{})
		if !ok || bucketRoot["Contents"] == nil || bucketRoot["Contents"] == "null" {
			glog.V(4).Infoln("break for no children left")
			break
		}
		objects, ok = bucketRoot["Contents"].([]interface{})
		if !ok {
			break
		}
		if bucketRoot["Next-Marker"] == nil {
			breakflag = true
			marker = ""
		} else {
			breakflag = false
			marker, ok = (bucketRoot["Next-Marker"]).(string)
			if !ok {
				breakflag = true
			}
		}
		//S_IFDIR
		for _, v := range objects {
			tempobject := &ossfs.FileInfo{}
			objstr, ok := v.(map[string]interface{})
			if !ok {
				continue
			}
			if objstr["Key"] != nil {
				Key, _ := objstr["Key"].(string)
				isDir, fileName, _, _, _ := path2namebase("/" + Key)
				tempobject.IsDir = isDir
				if isDir {
					tempobject.Name = fileName[:len(fileName) - 1]
				} else {
					tempobject.Name = fileName
				}
			}
			if objstr["Last-Modified"] != nil {
				tempobject.LastModified, ok = objstr["Last-Modified"].(string)
				if ok {
					lmtime, err := time.Parse(http.TimeFormat, tempobject.LastModified)
					if err == nil {
						tempobject.LastModified = strconv.FormatInt(lmtime.Unix(), 10)
					}
				}
			}

			if objstr["Size"] != nil {
				tempsize, ok := objstr["Size"].(float64)
				if ok {
					tempobject.Size = int64(tempsize)
				}
			}
			c[tempobject.Name] = tempobject
			//	c = append(c, tempobject)
			filesum++
			if filesum >= maxshowfileSum {
				breakflag = true
				LeftSons := 0 - maxshowfileSum
				if bucketRoot["Sub-Dir-Count"] != nil {
					//LeftSons += int(bucketRoot["Sub-Dir-Count"].(float64))
					temp_D_sum, ok := bucketRoot["Sub-Dir-Count"].(float64)
					if ok {
						LeftSons += int(temp_D_sum)
					}
				}
				if bucketRoot["Sub-File-Count"] != nil {
					//LeftSons += int(bucketRoot["Sub-File-Count"].(float64))
					temp_F_sum, ok := bucketRoot["Sub-File-Count"].(float64)
					if ok {
						LeftSons += int(temp_F_sum)
					}
				}
				objover := &ossfs.FileInfo{}
				objover.IsDir = true
				objover.Name = "剩余" + strconv.Itoa(LeftSons) + "个文件或者目录未展示"
				objover.Size = 0
				tempobject.LastModified = strconv.FormatInt(time.Now().Unix(), 10)
				break
			}
		}
		if breakflag || fixname != "" {
			break
		}

	}

	return c, fuse.OK
}

//打开文件，以不同的模式来进行
func (sf *SdossDriver) Open(name string, flags uint32, context *fuse.Context) (file *ossfs.FileInfo, code fuse.Status) {
	glog.V(4).Infoln("SdossDriver Op Open:", name, " with flag :", flags)
	if name == "/" {
		return nil, fuse.EINVAL
	}
	//从filer 获取下属性，验证文件存在性。前面的有一定可能是缓存中的。
	_, obj, status := sf.getObjAttr(name)
	if status == fuse.ENOENT {
		//没文件
		return nil, fuse.EINVAL
	}
	//now can not write
	if flags & fuse.O_ANYWRITE != 0 {

		//return nil, fuse.EPERM
	}
	return obj, fuse.OK
}

//读文件
func (sf *SdossDriver) Read(name string, dest []byte, off int64, size int64, context *fuse.Context) (fuse.ReadResult, fuse.Status) {
	glog.V(4).Infoln("SdossDriver Op Read:", name, "  off :", off, "size", size)
	var data []byte
	header := make(map[string]string)
	//拼接range
	if size >= 1 {
		header["Range"] = fmt.Sprintf("bytes=%d-%d", off, off + size - 1)
	} else {
		header["Range"] = fmt.Sprintf("bytes=%d-", off)
	}

	data, err := sf.urlRequest(name, "GET", nil, header)
	if err != nil {
		glog.V(0).Infoln("error while GetFile " + name + ": " + err.Error())
		return nil, fuse.EIO
	}
	if len(data) == 0 {
		return nil, fuse.EIO
	}
	templen := int64(len(data))
	if templen < size {
		return nil, fuse.EIO
	}
	dest = data[: size]
	glog.V(0).Infoln("get length :", len(dest), "offset:", off, "size:", size)
	res := fuse.ReadResultData(data)
	return res, fuse.OK
}

func (sf *SdossDriver) ReadData(name string, off int64, size int64, context *fuse.Context) ([]byte, fuse.Status) {
	glog.V(4).Infoln("SdossDriver Op ReadData:", name, "  off :", off, "size", size)
	var data []byte
	header := make(map[string]string)
	//拼接range
	if size >= 1 {
		header["Range"] = fmt.Sprintf("bytes=%d-%d", off, off + size - 1)
	} else {
		header["Range"] = fmt.Sprintf("bytes=%d-", off)
	}

	data, err := sf.urlRequest(name, "GET", nil, header)
	if err != nil {
		glog.V(0).Infoln("error while GetFile " + name + ": " + err.Error())
		return nil, fuse.EIO
	}
	if len(data) == 0 {
		return nil, fuse.EIO
	}
	templen := int64(len(data))
	if templen < size {
		return nil, fuse.EIO
	}
	return data[: size], fuse.OK
}

//借鉴sftp，维护一个缓存管理。
func (sf *SdossDriver) Write(name string, data []byte, off int64, context *fuse.Context) (written uint32, code fuse.Status) {
	glog.V(4).Infoln("SdossDriver Op Write:", name, "  off :", off)
	//off 是不是0
	//  写动作带range？？？？？
	//  替换某一个块
	//

	return 0, fuse.OK
}
func path2name2(path string) (isDir bool, fileName, parentKey, myKey string, err error) {

	if !strings.HasPrefix(path, "/") {
		err = errors.New("Invalid path")
		return
	}
	if path == "/" {
		myKey = "/"
		parentKey = ""
		fileName = "/"
		isDir = true
		return
	}
	if strings.HasSuffix(path, "/") {
		isDir = true
	}
	parts := strings.Split(path, "/")

	if isDir {
		fileName = parts[len(parts) - 2] + "/"
	} else {
		fileName = parts[len(parts) - 1]
	}
	myKey = path[1:]
	parentKey = myKey[:len(myKey) - len(fileName)]

	fileName = url.QueryEscape(fileName)
	myKey = url.QueryEscape(myKey)
	parentKey = url.QueryEscape(parentKey)
	return
}

//path是全路径
func (sf *SdossDriver) WriteReader(path string, data io.Reader, off int64, context *fuse.Context) (written uint32, code fuse.Status) {

	var appfile *util.Appendfile
	var err error
	_, _, _, myKey, patherr := path2name2(path)
	if patherr != nil {
		glog.V(0).Infoln(patherr)
		return 0, fuse.EINVAL
	}
	//var data []byte
	if off > 0 {
		glog.V(0).Infoln("  appendData  file " + path, off)
		//追加写
		//1 重新打开
		appfile, err = sf.reopen(myKey)
		//2 返回非404错误，错误
		if err != nil && err != ErrNotExist {
			glog.V(0).Infoln("reopen error:", err)
			return 0, fuse.EIO
		}
	}
	glog.V(4).Infoln(" start upload  file " + path)
	size, err := sf.uploadreader(myKey, data, appfile)
	if err != nil {
		glog.V(0).Infoln("upload err:", err)
		if err == ErrBadAuth {
			return 0, fuse.EACCES
		} else {
			return 0, fuse.EIO
		}
	}
	return uint32(size), fuse.OK
}

func (sf *SdossDriver) reopen(object string) (appfile *util.Appendfile, err error) {

	tempfiler := sf.FilerSelecter.GetFiler(false)
	if tempfiler == "" {
		return nil, errors.New("no available filer server")
	}
	appfile, err = sf.urlRequestreopen(tempfiler, object)
	if err != nil {
		if err == ErrBadAuth {
			return nil, err
		}
		tempfiler = sf.FilerSelecter.GetFiler(true)
		if tempfiler == "" {
			return nil, errors.New("no available filer server")
		}
		appfile, err = sf.urlRequestreopen(tempfiler, object)
		if err != nil {
			glog.V(0).Infoln(err.Error())
			return
		}
	}
	return appfile, err
}

//从io.Reader上传一个文件，返回错误或者上传长度
//鉴权使用url，
//注意错误里面有Error Authorization，表示鉴权错误
func (sf *SdossDriver) uploadreader(object string, reader io.Reader, appfile *util.Appendfile) (int64, error) {

	urlpath := "/" + sf.Account + "/" + sf.Bucket + "/"
	if object != "" && object != "/" {
		urlpath += object
	} else {
		return 0, ErrRequest
	}
	glog.V(4).Infoln("upload", urlpath)
	MimeType := ""
	ext := strings.ToLower(path.Ext(object))
	//IsGzipped := ext == ".gz"
	IsGzipped := false
	if ext != "" {
		MimeType = mime.TypeByExtension(ext)
	}

	realpath, unescapeerr := url.QueryUnescape(object)
	if unescapeerr != nil {
		return 0, ErrRequest
	}
	//获取鉴权的方法。
	//用于url方式鉴权
	get_sign := func(method string) map[string]string {
		exdate := fmt.Sprint(time.Now().Add(24 * time.Hour).Unix())
		Signature := auth.GetSignature(sf.Bucket, realpath, method, exdate, sf.AccessKeysecret, "")
		parms := make(map[string]string)
		parms["SDOSSAccessKeyId"] = sf.AccessKeyId
		parms["Expires"] = exdate
		parms["Signature"] = url.QueryEscape(Signature)
		/*
			header["User-Agent"] = public.FTP_User_Agent
			if ctx != nil {
				reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
				parms[public.SDOSS_REQUEST_ID] = reqid
			}
		*/
		return parms
	}
	headers := make(map[string]string)
	headers["User-Agent"] = public.FUSE_User_Agent

	tempfiler := sf.FilerSelecter.GetFiler(false)
	if tempfiler == "" {
		return 0, errors.New("no available filer server")
	}
	onretry := func() {
		tempfiler = sf.FilerSelecter.GetFiler(true)
	}
	size, err := operation.UploadByFlow(&(tempfiler), urlpath, urlpath, reader, appfile, IsGzipped, MimeType, get_sign, headers, onretry)
	if err != nil {
		glog.V(0).Infoln("upload error ", err)
		if err.Error() == "Error Authorization" {
			return 0, ErrBadAuth
		}
		return 0, err
	} else {
		return size, nil
	}

}

//获取鉴权的方法。
//用于url方式鉴权
func (sf *SdossDriver) get_sign(method string, realpath string) map[string]string {
	exdate := fmt.Sprint(time.Now().Add(24 * time.Hour).Unix())
	Signature := auth.GetSignature(sf.Bucket, realpath, method, exdate, sf.AccessKeysecret, "")
	parms := make(map[string]string)
	parms["SDOSSAccessKeyId"] = sf.AccessKeyId
	parms["Expires"] = exdate
	parms["Signature"] = url.QueryEscape(Signature)
	return parms
}

func (sf *SdossDriver) uploadnomalfile(object string, data []byte) (int64, error) {
	tempobject := url.QueryEscape(object)
	urlpath := "/" + sf.Account + "/" + sf.Bucket + "/"
	if object != "" && object != "/" {
		urlpath = path.Join(urlpath,tempobject)
	} else {
		return 0, ErrRequest
	}
	_, filename := path.Split(tempobject)
	glog.V(4).Infoln("upload", urlpath," object:",object," tempobject:",tempobject)
	size := len(data)
	MimeType := ""
	ext := strings.ToLower(path.Ext(tempobject))
	//IsGzipped := ext == ".gz"
	IsGzipped := false
	if ext != "" {
		MimeType = mime.TypeByExtension(ext)
	}

	realpath, unescapeerr := url.QueryUnescape(tempobject)
	if unescapeerr != nil {
		return 0, ErrRequest
	}

	headers := make(map[string]string)
	headers["User-Agent"] = public.FUSE_User_Agent

	urlsign := sf.get_sign("POST", realpath)

	tempfiler := sf.FilerSelecter.GetFiler(false)
	if tempfiler == "" {
		return 0, errors.New("no available  filer server")
	}

	uploadurl := "http://" + tempfiler + urlpath + "?"
	for k, v := range urlsign {
		uploadurl += "&" + k + "=" + v
	}

	_, status, err := operation.Upload4(uploadurl, filename, data, IsGzipped, MimeType, headers, false)

	if err != nil {
		glog.V(0).Infoln("upload file:",object," failed.err:", err)
		if err.Error() == "Error Authorization" {
			return 0, ErrBadAuth
		}
		return 0, err
	}else if status >= 400 {
		glog.V(0).Infoln("upload file:",object,"failed.status:", status)
		err = errors.New("upload failed")
		return 0, err
	} else {
		return int64(size), nil
	}
}

//重新打开文件，object不可能是空
func (sf *SdossDriver) urlRequestreopen(server string, object string) (res *util.Appendfile, err error) {
	//先把account的id和sec处理下
	urlpath := "/" + sf.Account + "/" + sf.Bucket
	tempkey1, _ := url.QueryUnescape(object)
	if object != "" && tempkey1 != "/" {
		urlpath = path.Join(urlpath, object)
	} else {
		return nil, errors.New("bad file path,obj can not be null")
	}
	urlpath += "?type=reopen&blocksize=" + fmt.Sprint(BlockSize)

	realpath, unescapeerr := url.QueryUnescape(object)
	if unescapeerr != nil {
		glog.V(0).Infoln(unescapeerr)
		err = ErrRequest
		return nil, err
	}
	//获取鉴权信息
	header := getHeaders(sf.AccessKeyId, sf.AccessKeysecret, sf.Bucket, realpath, "GET")

	//获取到文件
	glog.V(4).Infoln("make request:", server, urlpath, header)
	resp, err := util.MakeRequest(server, urlpath, "GET", nil, header)
	if err != nil {
		glog.V(0).Infoln(" error ", err)
		return nil, err
	}
	if resp.StatusCode == 403 {
		ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		return nil, ErrBadAuth
	}
	//不存在就是可以直接上传的
	if resp.StatusCode == 404 {
		ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		return nil, ErrNotExist
	}

	if resp.StatusCode < 200 || resp.StatusCode > 300 {
		ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		return nil, errors.New("Error with StatusCode :" + fmt.Sprint(resp.StatusCode))
	}

	af := &util.Appendfile{}
	//最后一个块的长度

	af.Size = util.ParseInt64(resp.Header.Get("Content-Length"), 0)
	af.Ftype = resp.Header.Get("Object-Type")
	af.Tag = resp.Header.Get("Tag")
	af.Fixidx = util.ParseInt(resp.Header.Get("Fixidx"), 0)
	af.Uploadid = resp.Header.Get("Uploadid")
	af.Blocksize = util.ParseInt64(resp.Header.Get("Block-Size"), 0)
	af.Lastblock = resp.Body

	return af, nil
}

func (sf *SdossDriver) complete(object string, uploadid string, idx int) error {
	tempobject := url.QueryEscape(object)
	urlpath := "/" + sf.Account + "/" + sf.Bucket + "/"
	if object != "" && object != "/" {
		urlpath = path.Join(urlpath, tempobject)
	} else {
		return ErrRequest
	}
	realpath, unescapeerr := url.QueryUnescape(tempobject)
	if unescapeerr != nil {
		return ErrRequest
	}

	headers := make(map[string]string)
	headers["User-Agent"] = public.FUSE_User_Agent

	urlsign := sf.get_sign("POST", realpath)

	//提交
	commiturl := urlpath + "?upload-id=" + uploadid
	for k, v := range urlsign {
		commiturl += "&" + k + "=" + v
	}

	headers["Content-Type"] = "application/json"

	parts := operation.AuditInfoSt{}
	parts.ObjectParts = make([]operation.AuditPartSt, idx)
	for i := 0; i < idx; i++ {
		parts.ObjectParts[i] = operation.AuditPartSt{
			PartNumber: i,
		}
	}
	by, _ := json.Marshal(parts)
	glog.V(4).Infoln(string(by))

	tempfiler := sf.FilerSelecter.GetFiler(false)
	if tempfiler == "" {
		return errors.New("no available filer server")
	}
	resp, err := util.MakeRequest(tempfiler, commiturl, "POST", by, headers)
	if err != nil {
		glog.V(0).Infoln(" error ", err)
		tempfiler = sf.FilerSelecter.GetFiler(true)
		if tempfiler == "" {
			return errors.New("no available filer server")
		}
		resp, err = util.MakeRequest(tempfiler, commiturl, "POST", by, headers)
		if err != nil {
			glog.V(0).Infoln(" error ", err)
			return err
		}
	}
	tempdata, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	glog.V(4).Infoln("UploadByFlow ", urlpath, "commit")
	if resp.StatusCode == 403 {
		return errors.New("Complete Error Authorization")
	}
	if resp.StatusCode == 404 {
		return errors.New("Complete Error Not Exist")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 300 {
		return errors.New("Complete Error  with code from remote:" + fmt.Sprint(resp.StatusCode) + string(tempdata))
	}
	return nil
}

func (sf *SdossDriver) getuploadid(object string) (uploadid string, err error) {
	tempobject := url.QueryEscape(object)
	urlsign := sf.get_sign("POST", object)
	urlpath := "/" + sf.Account + "/" + sf.Bucket + "/"
	if object != "" && object != "/" {
		urlpath = path.Join(urlpath, tempobject)
	} else {
		return "", ErrRequest
	}
	headers := make(map[string]string)
	headers["User-Agent"] = public.FUSE_User_Agent

	getidurl := urlpath + "?type=uploads"
	for k, v := range urlsign {
		getidurl += "&" + k + "=" + v
	}

	tempfiler := sf.FilerSelecter.GetFiler(false)
	if tempfiler == "" {
		return "", errors.New("no available filer server")
	}
	resp, err := util.MakeRequest(tempfiler, getidurl, "POST", nil, headers)
	if err != nil {
		glog.V(0).Infoln(" error ", err)
		tempfiler = sf.FilerSelecter.GetFiler(true)
		if tempfiler == "" {
			return "", errors.New("no available filer server")
		}
		resp, err = util.MakeRequest(tempfiler, getidurl, "POST", nil, headers)
		if err != nil {
			glog.V(0).Infoln(" error ", err)
			return "", err
		}
	}

	iddata, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode == 403 {
		return "", errors.New("Error Authorization")
	}
	if resp.StatusCode == 404 {
		return "", errors.New("Error Not Exist")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 300 {
		return "", errors.New("Error with code:" + fmt.Sprint(resp.StatusCode))
	}
	var tempmap map[string]interface{}
	err = json.Unmarshal(iddata, &tempmap)
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return "", errors.New("Error :" + err.Error())
	}
	if _, ok := tempmap["upload-id"]; !ok {
		glog.V(0).Infoln("error while get upload-id")
		return "", errors.New(" error while get upload-id")
	}
	uploadid = tempmap["upload-id"].(string)
	return
}

func (sf *SdossDriver) uploadblock(object string, uploadid string, idx int, data []byte) error {
	tempobject := url.QueryEscape(object)
	urlsign := sf.get_sign("POST", object)
	urlpath := "/" + sf.Account + "/" + sf.Bucket + "/"
	glog.V(4).Infoln("want to upload block", object, uploadid, idx," tempobject:",tempobject)
	if object != "" && object != "/" {
		urlpath = path.Join(urlpath, tempobject)
	} else {
		return ErrRequest
	}
	headers := make(map[string]string)
	headers["User-Agent"] = public.FUSE_User_Agent

	tempfiler := sf.FilerSelecter.GetFiler(false)
	if tempfiler == "" {
		return errors.New("no available  filer server")
	}
	uploadurl := "http://" + tempfiler + urlpath + "?type=auto&upload-id=" + uploadid + "&part-number=" + fmt.Sprint(idx)
	for k, v := range urlsign {
		uploadurl += "&" + k + "=" + v
	}
	_, filename := path.Split(tempobject)
	glog.V(4).Infoln("upload", urlpath)
	MimeType := ""
	ext := strings.ToLower(path.Ext(tempobject))
	//IsGzipped := ext == ".gz"
	IsGzipped := false
	if ext != "" {
		MimeType = mime.TypeByExtension(ext)
	}
	_, status, err := operation.Upload4(uploadurl, filename, data, IsGzipped, MimeType, headers, false)
	if err != nil {
		glog.V(0).Infoln("upload file:",object," failed.err:", err)
		if err.Error() == "Error Authorization" {
			return ErrBadAuth
		}
		return err
	} else if status >= 400 {
		glog.V(0).Infoln("upload file:", object, "failed.status:", status)
		err = errors.New("upload failed")
		return err
	}else {
		return nil
	}
}

// 列出给定文件的所有的扩展属性
func (sf *SdossDriver) ListXAttr(name string, context *fuse.Context) (attributes []string, code fuse.Status) {
	return nil, fuse.ENOSYS
}

//移除属性
func (sf *SdossDriver) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	return fuse.OK
}

//设置属性信息
func (sf *SdossDriver) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	return fuse.OK
}

//列出扩展属性的值
func (sf *SdossDriver) GetXAttr(name string, attribute string, context *fuse.Context) (data []byte, code fuse.Status) {
	glog.V(1).Infoln(name, attribute)
	return nil, fuse.ENOSYS
}

//解挂载时候触发
//没有调用，实现接口
func (sf *SdossDriver) OnUnmount() {
	return
}

//解挂载时候触发
//没有调用，实现接口
func (sf *SdossDriver) OnMount(nodeFs *ossfs.Ossfs) {
	return
}

// File handling.  If opening for writing, the file's mtime
// should be updated too.
func (sf *SdossDriver) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file *ossfs.FileInfo, code fuse.Status) {
	//create file inode ,return
	return nil, fuse.ENOSYS
}

//软链接
func (sf *SdossDriver) Symlink(value string, linkName string, context *fuse.Context) (code fuse.Status) {
	return fuse.ENOSYS
}

//符号链接指向的文件路径
func (sf *SdossDriver) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	return "", fuse.ENOSYS
}

//文件系统信息
func (sf *SdossDriver) StatFs(name string) *fuse.StatfsOut {
	var MaxSpace uint64 = 1024*1024*1024*1024*1024  //默认最大配额1PB
	Bsize := uint32(4096)

	Blocks := MaxSpace / uint64(Bsize)
	Bfree  := Blocks
	Bavail := Bfree

	SumInodes := uint64(10000000000)  //默认最大文件数，100亿
	freeInodes := SumInodes
	var UsedInodes uint64 = 0

	buk, err := sf.getBukAttr()
	if err == nil {
		if buk != nil && buk.Thresholds != nil && buk.Thresholds.MaxSpace != 0 {
			//一共多少个块
			Blocks = uint64(buk.Thresholds.MaxSpace) * 1024 * 1024 * 1024 / uint64(Bsize)
		}
		UsedInodes = uint64(buk.ObjectCount)

		//剩余多少个剩余块
		Bfree = Blocks - uint64(buk.ByteUsed / int64(Bsize))
		Bavail = Bfree
		freeInodes = SumInodes - UsedInodes
		glog.V(2).Infoln(Bfree, Bavail, freeInodes,UsedInodes,buk.ByteUsed,Blocks,buk.ObjectCount)
	} else {
		glog.V(4).Infoln("get bucket info err:", err)
	}

	temp := &fuse.StatfsOut{
		Blocks:  Blocks, /* 文件系统数据块总数*/
		Bfree:   Bfree, /* 可用块数*/
		Bavail:  Bavail, /* 非超级用户可获取的块数*/
		Files:   SumInodes, /* 文件结点总数*/
		Ffree:   freeInodes, /* 可用文件结点数*/
		Bsize:   Bsize, /* 块大小,对于ossfuse没有太真实意义*/
		NameLen: 250, /* 文件名的最大长度*/
		Frsize:  Bsize,
		Padding: 99999999,
		Spare:   [6]uint32{1000, 2000, 3000, 4000, 5000, 6000},
	}
	return temp
}

func (sf *SdossDriver) Fsync(name string, data []byte) (code fuse.Status) {
	glog.V(1).Infoln("Fsync:" + name, len(data))

	_, err := sf.upload(name, bytes.NewReader(data))
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

//验证密码是不是正确就是一个列目录动作，只要失败了，
func (sf *SdossDriver) CheckPasswd() (error,string) {

	//get header

	date := util.ParseUnix(strconv.FormatInt(time.Now().Unix(), 10))
	Signature := auth.GetSignature(sf.Bucket, "", "GET", date, sf.AccessKeysecret, "")
	urlpath := "/" + sf.Account + "/" + sf.Bucket + "/?type=dir&dir=/"
	glog.V(4).Infoln("check user URL:" + urlpath)
	header := make(map[string]string)
	header["Authorization"] = " SDOSS " + sf.AccessKeyId + ":" + Signature
	header["Date"] = date

	tempfiler := sf.FilerSelecter.GetFiler(false)
	if tempfiler == "" {
		return errors.New("no available filer server"),""

	}
	_, err := util.MakeRequest_timeout(tempfiler, urlpath, "GET", nil, header)
	if err == nil {
		return nil,tempfiler
	}
	tempfiler = sf.FilerSelecter.GetFiler(true)
	if tempfiler == "" {
		return errors.New("no available filer server"),""
	}
	_, err = util.MakeRequest_timeout(tempfiler, urlpath, "GET", nil, header)

	return err,tempfiler
}

//对文件路径进行解析
//需要进行url转码？？？
func path2name(path string) (isDir bool, fileName, parentKey, myKey string, err error) {

	isDir, fileName, parentKey, myKey, err = path2namebase(path)

	fileName = url.QueryEscape(fileName)
	myKey = url.QueryEscape(myKey)
	parentKey = url.QueryEscape(parentKey)
	return
}
func path2namebase(path string) (isDir bool, fileName, parentKey, myKey string, err error) {

	if !strings.HasPrefix(path, "/") {
		err = FUSEErrInvalidPath
		return
	}
	if path == "/" {
		myKey = "/"
		parentKey = ""
		fileName = "/"
		isDir = true
		return
	}
	if strings.HasSuffix(path, "/") {
		isDir = true
	}
	parts := strings.Split(path, "/")

	if isDir {
		fileName = parts[len(parts) - 2] + "/"
	} else {
		fileName = parts[len(parts) - 1]
	}
	myKey = path[1:]
	parentKey = myKey[:len(myKey) - len(fileName)]

	return
}
