package storage

import (
	"code.google.com/p/weed-fs/go/public"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/images"
	"code.google.com/p/weed-fs/go/util"
)

var ErrNotSupportedPostMethod = errors.New("Storage: not supported post method")

const (
	MAX_HTTP_HEADER_LENGTH = 1 << 20
)
const (
	NeedleHeaderSize      = 16 //should never change this
	NeedlePaddingSize     = 8
	NeedleChecksumSize    = 4
	MaxPossibleVolumeSize = 4 * 1024 * 1024 * 1024 * 8
)
const (
	NeedleHeaderMagicNumber = "sdossneedleheadx"
	NeedleFooterMagicNumber = "sdossneedlefootx"
	NeedleMagicNumberSize   = 16
)

var Ismutipart = true

/*
* Needle file size is limited to 4GB for now.
 */
type Needle struct {
	Cookie uint32 `comment:"random number to mitigate brute force lookups"`
	Id     uint64 `comment:"needle id"`
	Size   uint32 `comment:"sum of DataSize,Data,NameSize,Name,MimeSize,Mime"`

	HeaderMagic  []byte `comment:"Header magic number"` //version3
	OriginDataSize uint32 `comment:"OriginData size"`   //version4
	DataSize     uint32 `comment:"Data size"`           //version2
	Data         []byte `comment:"The actual file data"`
	Flags        byte   `comment:"boolean flags"`          //version2
	NameSize     uint8                                     //version2
	Name         []byte `comment:"maximum 256 characters"` //version2
	MimeSize     uint8                                     //version2
	Mime         []byte `comment:"maximum 256 characters"` //version2
	LastModified uint64                                    //only store LastModifiedBytesLength bytes, which is 5 bytes to disk
	FooterMagic  []byte `comment:"Footer magic number"`    //version3

	Checksum CRC    `comment:"CRC32 to check integrity"`
	Padding  []byte `comment:"Aligned to 8 bytes"`
}

//func ParseUpload(r *http.Request) (fileName string, data []byte, mimeType string, isGzipped bool, modifiedTime uint64, e error) {
func ParseUpload(r *http.Request, isCompress bool) (fileName string, data []byte, mimeType string, isGzipped bool, modifiedTime uint64, e error) {
	if !Ismutipart {
		fileName, data, mimeType, _, _,isGzipped, modifiedTime, e = ParseUploadBareDataWithMaxLength(r, isCompress, -1,false)
		return
	}
	form, fe := r.MultipartReader()
	if fe != nil {
		glog.V(0).Infoln("MultipartReader [ERROR]", fe)
		e = fe
		return
	}
	part, fe := form.NextPart()
	if fe != nil {
		glog.V(0).Infoln("Reading Multi part [ERROR]", fe, r.URL.Path)
		e = fe
		return
	}

	fileName = part.FileName()
	/*if fileName != "" {
		fileName = path.Base(fileName)
	}*/
	data, e = ioutil.ReadAll(part)
	if e != nil {
		glog.V(0).Infoln("Reading Content [ERROR]", e)
		return
	}
	dotIndex := strings.LastIndex(fileName, ".")
	ext, mtype := "", ""
	if dotIndex > 0 {
		ext = strings.ToLower(fileName[dotIndex:])
		mtype = mime.TypeByExtension(ext)
	}
	contentType := part.Header.Get("Content-Type")
	if contentType != "" && mtype != contentType {
		mimeType = contentType //only return mime type if not deductable
		mtype = contentType
	}
	if part.Header.Get("Content-Encoding") == "gzip" {
		isGzipped = true
	} else if IsGzippable(ext, mtype) && public.STORAGE_MIN_COMPRESS <= int64(len(data)) {
		if data, isGzipped, e = GzipDataLevel(data, r.Header.Get(public.SDOSS_COMPRESS_LEVEL)); e != nil {
			glog.V(0).Infoln("gzip data error:", e)
			return
		}
	}
	glog.V(3).Infoln("data gzipped:", isGzipped, len(data))
	modifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)
	return
}

func ParseUpload2(r *http.Request, isCompress bool, maxLength int64,rawData,checkMd5 bool) (fileName string, data []byte, mimeType, contentType,etag string, isGzipped bool, modifiedTime uint64, e error) {
	if !Ismutipart || rawData == true{
		return ParseUploadBareDataWithMaxLength(r, isCompress, maxLength,checkMd5)
	}

	form, fe := r.MultipartReader()
	if fe != nil {
		glog.V(0).Infoln("MultipartReader [ERROR]", fe)
		e = fe
		return
	}
	var part *multipart.Part
	for {
		if part, e = form.NextPart(); e != nil {
			glog.V(0).Infoln("Reading Multi part [ERROR]", e, r.URL.Path)
			return
		}
		if fileName = part.FileName(); fileName == "" {
			continue
		}
		if r.ContentLength > maxLength+MAX_HTTP_HEADER_LENGTH {
			e = errors.New("SizeExceedLimitation!")
			return
		}
		if fileName != "" {
			fileName = path.Base(fileName)
		}

		if data, e = ioutil.ReadAll(part); e != nil {
			glog.V(0).Infoln("Reading Content [ERROR]", e)
			return
		}
		if int64(len(data)) > maxLength {
			e = errors.New("SizeExceedLimitation!")
			return
		}
		orl_md5 := r.Header.Get("Content-MD5")
		if orl_md5 != "" || checkMd5 {
			md5ctx := md5.New()
			md5ctx.Write(data)
			//etag = base64.URLEncoding.EncodeToString(md5ctx.Sum(nil))
			etag = hex.EncodeToString(md5ctx.Sum(nil))
			if orl_md5 != "" && etag != orl_md5 {
				glog.V(0).Infoln("warning! md5sum not matched! head_md5:",orl_md5," vs "," body_md5:",etag)
				e = errors.New("Data Damaged!")
				return
			}
		}

		ext, mtype := "", ""
		if dotIndex := strings.LastIndex(fileName, "."); dotIndex > 0 {
			ext = strings.ToLower(fileName[dotIndex:])
			mtype = mime.TypeByExtension(ext)
		}
		contentType = part.Header.Get("Content-Type")
		if contentType != "" && mtype != contentType {
			mimeType = contentType //only return mime type if not deductable
			mtype = contentType
		}
		if part.Header.Get("Content-Encoding") == "gzip" {
			isGzipped = true
		} else if IsGzippable(ext, mtype) && public.STORAGE_MIN_COMPRESS <= int64(len(data)) {
			if data, isGzipped, e = GzipDataLevel(data, r.Header.Get(public.SDOSS_COMPRESS_LEVEL)); e != nil {
				glog.V(0).Infoln("gzip error:", e)
				return
			}
		}
		glog.V(3).Infoln("data gzipped:", isGzipped, len(data))
		modifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)
		return
	}
	e = ErrNotSupportedPostMethod
	return
}

func ParseUploadBareDataWithMaxLength(r *http.Request, isCompress bool, maxLength int64,checkMd5 bool) (fileName string, data []byte, mimeType, contentType,etag string, isGzipped bool, modifiedTime uint64, e error) {
	//假如是裸数据的传输,各种信息从头中获取
	if maxLength > 0 {
		if r.ContentLength > maxLength+MAX_HTTP_HEADER_LENGTH {
			e = errors.New("SizeExceedLimitation!")
			return
		}
	}
	contentDisposition := r.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		if strings.HasPrefix(contentDisposition[0], "filename=") {
			fileName = contentDisposition[0][len("filename="):]
			fileName = strings.Trim(fileName, "\"")
		}
	}
	if fileName != "" {
		fileName = path.Base(fileName)
	}
	data, e = ioutil.ReadAll(r.Body)
	if e != nil {
		glog.V(0).Infoln("Reading Content [ERROR]", e)
		return
	}
	if maxLength > 0 {
		if int64(len(data)) > maxLength {
			e = errors.New("SizeExceedLimitation!")
			return
		}
	}
	orl_md5 := r.Header.Get("Content-MD5")
	if orl_md5 != "" || checkMd5 == true{
		md5ctx := md5.New()
		md5ctx.Write(data)
		//etag = base64.URLEncoding.EncodeToString(md5ctx.Sum(nil))
		etag = hex.EncodeToString(md5ctx.Sum(nil))
		if orl_md5 !="" && etag != orl_md5 {
			glog.V(0).Infoln("warning! md5sum not matched! head_md5:",orl_md5," vs "," body_md5:",etag)
		// 这个地方先这样，等业务的MD5计算统一后，此处还是要恢复的，jjj@20190412 SDOSPOOL-362 PP云IOS端带md5值上传失败
		//	e = errors.New("Data Damaged!")
		//	return
		}
	}

	ext, mtype := "", ""
	if dotIndex := strings.LastIndex(fileName, "."); dotIndex > 0 {
		ext = strings.ToLower(fileName[dotIndex:])
		mtype = mime.TypeByExtension(ext)
	}
	contentType = r.Header.Get("Content-Type")
	if contentType != "" && mtype != contentType {
		mimeType = contentType //only return mime type if not deductable
		mtype = contentType
	}
	if r.Header.Get("Content-Encoding") == "gzip" {
		isGzipped = true
	} else if IsGzippable(ext, mtype) && public.STORAGE_MIN_COMPRESS <= int64(len(data)) {
		if data, isGzipped, e = GzipDataLevel(data, r.Header.Get(public.SDOSS_COMPRESS_LEVEL)); e != nil {
			glog.V(0).Infoln("gzip error:", e)
			return
		}
	}
	glog.V(3).Infoln("data gzipped:", isGzipped, len(data))
	modifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)

	return
}

//func NewNeedle(r *http.Request, fixJpgOrientation bool) (n *Needle, e error) {
func NewNeedle(r *http.Request, fixJpgOrientation bool, isCompress bool) (n *Needle, e error) {
	fname, mimeType, isGzipped := "", "", false
	var originSize uint64 = 0
	n = new(Needle)
	//fname, n.Data, mimeType, isGzipped, n.LastModified, e = ParseUpload(r)
	fname, n.Data, mimeType, isGzipped, n.LastModified, e = ParseUpload(r, isCompress)
	ors:=r.FormValue("originSize")//EC模式下，该切片所属源数据的长度
	if ors != ""{
		originSize, _ = strconv.ParseUint(ors, 10, 64)
		n.OriginDataSize = uint32(originSize)
		n.SetHasOriginSize()
	}

	if e != nil {
		return
	}
	if n.Data == nil {
		glog.V(0).Infoln("can not upload empty file")
		e = fmt.Errorf("can not upload empty file.maybe net timeout")
		return
	}
	/*if len(n.Data) == 0 {
		glog.V(0).Infoln("can not upload empty file")
		e = fmt.Errorf("can not upload empty file.maybe net timeout")
		return
	}*/
	if len(fname) < 256 {
		n.Name = []byte(fname)
		n.SetHasName()
	}
	if len(mimeType) < 256 {
		n.Mime = []byte(mimeType)
		n.SetHasMime()
	}
	if isGzipped {
		n.SetGzipped()
	}
	if n.LastModified == 0 {
		n.LastModified = uint64(time.Now().Unix())
	}
	n.SetHasLastModifiedDate()

	if fixJpgOrientation {
		loweredName := strings.ToLower(fname)
		if mimeType == "image/jpeg" || strings.HasSuffix(loweredName, ".jpg") || strings.HasSuffix(loweredName, ".jpeg") {
			n.Data = images.FixJpgOrientation(n.Data)
		}
	}
	n.Checksum = NewCRC(n.Data)
	commaSep := strings.LastIndex(r.URL.Path, ",")
	dotSep := strings.LastIndex(r.URL.Path, ".")
	fid := r.URL.Path[commaSep+1:]
	if dotSep > 0 {
		fid = r.URL.Path[commaSep+1: dotSep]
	}
	e = n.ParsePath(fid)

	return
}
func (n *Needle) ParsePath(fid string) (err error) {
	length := len(fid)
	if length <= 8 {
		return errors.New("Invalid fid:" + fid)
	}
	delta := ""
	deltaIndex := strings.LastIndex(fid, "_")
	if deltaIndex > 0 {
		fid, delta = fid[0:deltaIndex], fid[deltaIndex+1:]
	}
	n.Id, n.Cookie, err = ParseKeyHash(fid)
	if err != nil {
		return err
	}
	if delta != "" {
		if d, e := strconv.ParseUint(delta, 10, 64); e == nil {
			n.Id += d
		} else {
			return e
		}
	}
	return err
}

func ParseKeyHash(key_hash_string string) (uint64, uint32, error) {
	key_hash_bytes, khe := hex.DecodeString(key_hash_string)
	key_hash_len := len(key_hash_bytes)
	if khe != nil || key_hash_len <= 4 {
		glog.V(0).Infoln("Invalid key_hash", key_hash_string, "length:", key_hash_len, "error", khe)
		return 0, 0, errors.New("Invalid key and hash:" + key_hash_string)
	}
	key := util.BytesToUint64(key_hash_bytes[0: key_hash_len-4])
	hash := util.BytesToUint32(key_hash_bytes[key_hash_len-4: key_hash_len])
	return key, hash, nil
}
