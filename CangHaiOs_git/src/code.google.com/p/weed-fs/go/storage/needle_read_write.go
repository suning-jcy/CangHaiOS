package storage

import (
	"errors"
	"fmt"
	"io"
	"os"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
)

const (
	FlagGzip                = 0x01
	FlagHasName             = 0x02
	FlagHasMime             = 0x04
	FlagHasLastModifiedDate = 0x08
	FlagHasOrginSize        = 0x010
	LastModifiedBytesLength = 5
)

var (
	ErrBadNeedleData = errors.New("bad needle data error")
)

func (n *Needle) DiskSize() int64 {
	padding := NeedlePaddingSize - ((NeedleHeaderSize + int64(n.Size) + NeedleChecksumSize) % NeedlePaddingSize)
	return NeedleHeaderSize + int64(n.Size) + padding + NeedleChecksumSize
}
func (n *Needle) Append(w io.Writer, version Version,valid byte) (size uint32, err error) {
	if s, ok := w.(io.Seeker); ok {
		if end, e := s.Seek(0, 1); e == nil {
			defer func(s io.Seeker, off int64) {
				if err != nil {
					if _, e = s.Seek(off, 0); e != nil {
						glog.V(0).Infof("Failed to seek %s back to %d with error: %s", w, off, e.Error())
					}
				}
			}(s, end)
		} else {
			err = fmt.Errorf("Cnnot Read Current Volume Position: %s", e.Error())
			return
		}
	}
	switch version {
	case Version1:
		header := make([]byte, NeedleHeaderSize)
		util.Uint32toBytes(header[0:4], n.Cookie)
		util.Uint64toBytes(header[4:12], n.Id)
		n.Size = uint32(len(n.Data))
		size = n.Size
		util.Uint32toBytes(header[12:16], n.Size)
		if _, err = w.Write(header); err != nil {
			return
		}
		if _, err = w.Write(n.Data); err != nil {
			return
		}
		padding := NeedlePaddingSize - ((NeedleHeaderSize + n.Size + NeedleChecksumSize) % NeedlePaddingSize)
		util.Uint32toBytes(header[0:NeedleChecksumSize], n.Checksum.Value())
		_, err = w.Write(header[0 : NeedleChecksumSize+padding])
		return
	case Version2, Version3, Version4:
		header := make([]byte, NeedleHeaderSize)
		util.Uint32toBytes(header[0:4], n.Cookie)
		util.Uint64toBytes(header[4:12], n.Id)
		n.DataSize, n.NameSize, n.MimeSize = uint32(len(n.Data)), uint8(len(n.Name)), uint8(len(n.Mime))
		if n.DataSize >= 0 && valid == 1{
			n.Size = 4 + n.DataSize + 1
			if n.HasName() {
				n.Size = n.Size + 1 + uint32(n.NameSize)
			}
			if n.HasMime() {
				n.Size = n.Size + 1 + uint32(n.MimeSize)
			}
			if n.HasLastModifiedDate() {
				n.Size = n.Size + LastModifiedBytesLength
			}
			if version >= Version3 {
				n.Size = n.Size + NeedleMagicNumberSize*2
			}
			if n.HasOriginSize() && version >= Version4{
				n.Size += 4 //originSize
			}
		}
		size = n.DataSize
		util.Uint32toBytes(header[12:16], n.Size)
		if _, err = w.Write(header); err != nil {
			return
		}
		if n.DataSize >=0 && valid == 1 {
			if version >= Version3 {
				if _, err = w.Write([]byte(NeedleHeaderMagicNumber)); err != nil {
					return
				}
			}
			util.Uint32toBytes(header[0:4], n.DataSize)
			if _, err = w.Write(header[0:4]); err != nil {
				return
			}
			if _, err = w.Write(n.Data); err != nil {
				return
			}
			util.Uint8toBytes(header[0:1], n.Flags)
			if _, err = w.Write(header[0:1]); err != nil {
				return
			}
			if n.HasName() {
				util.Uint8toBytes(header[0:1], n.NameSize)
				if _, err = w.Write(header[0:1]); err != nil {
					return
				}
				if _, err = w.Write(n.Name); err != nil {
					return
				}
			}
			if n.HasMime() {
				util.Uint8toBytes(header[0:1], n.MimeSize)
				if _, err = w.Write(header[0:1]); err != nil {
					return
				}
				if _, err = w.Write(n.Mime); err != nil {
					return
				}
			}
			if n.HasLastModifiedDate() {
				util.Uint64toBytes(header[0:8], n.LastModified)
				if _, err = w.Write(header[8-LastModifiedBytesLength : 8]); err != nil {
					return
				}
			}
			if n.HasOriginSize() && version >= Version4{
				util.Uint32toBytes(header[0:4], n.OriginDataSize)
				if _, err = w.Write(header[0:4]); err != nil {
					return
				}
			}
			if version >= Version3 {
				if _, err = w.Write([]byte(NeedleFooterMagicNumber)); err != nil {
					return
				}
			}
		}
		padding := NeedlePaddingSize - ((NeedleHeaderSize + n.Size + NeedleChecksumSize) % NeedlePaddingSize)
		util.Uint32toBytes(header[0:NeedleChecksumSize], n.Checksum.Value())
		_, err = w.Write(header[0 : NeedleChecksumSize+padding])
		return n.DataSize, err
	}
	return 0, fmt.Errorf("Unsupported Version! (%d)", version)
}

func (n *Needle) Read(r *os.File, offset int64, size uint32, version Version) (ret int, err error) {
	switch version {
	case Version1:
		bytes := make([]byte, NeedleHeaderSize+size+NeedleChecksumSize)
		if ret, err = r.ReadAt(bytes, offset); err != nil {
			return
		}
		n.readNeedleHeader(bytes)
		n.Data = bytes[NeedleHeaderSize : NeedleHeaderSize+size]
		checksum := util.BytesToUint32(bytes[NeedleHeaderSize+size : NeedleHeaderSize+size+NeedleChecksumSize])
		newChecksum := NewCRC(n.Data)
		if checksum != newChecksum.Value() {
			return 0, errors.New("CRC error! Data On Disk Corrupted!")
		}
		n.Checksum = newChecksum
		return
	case Version2, Version3, Version4:
		if size == 0 {
			return 0, nil
		}
		bytes := make([]byte, NeedleHeaderSize+size+NeedleChecksumSize)
		if ret, err = r.ReadAt(bytes, offset); err != nil {
			return
		}
		if ret != int(NeedleHeaderSize+size+NeedleChecksumSize) {
			return 0, errors.New("File Entry Not Found!")
		}
		n.readNeedleHeader(bytes)
		if n.Size != size {
			return 0, fmt.Errorf("File Entry Not Found! Needle %d Memory %d", n.Size, size)
		}
		if version == Version2 {
			err = n.readNeedleDataVersion2(bytes[NeedleHeaderSize : NeedleHeaderSize+int(n.Size)])
		} else if version == Version3  {
			err = n.readNeedleDataVersion3(bytes[NeedleHeaderSize : NeedleHeaderSize+int(n.Size)])
		}else {
			err = n.readNeedleDataVersion4(bytes[NeedleHeaderSize : NeedleHeaderSize+int(n.Size)])
		}
		if err != nil {
			return -1, err
		}
		//作为工具时，使用fmt导出结果
		//fmt.Println("version:",version,",needle file:","size:",n.Size," Cookie:",n.Cookie," name:",string(n.Name)," datasize:",n.DataSize," id:",n.Id," datalen:",len(n.Data))	
		checksum := util.BytesToUint32(bytes[NeedleHeaderSize+n.Size : NeedleHeaderSize+n.Size+NeedleChecksumSize])
		newChecksum := NewCRC(n.Data)
		if checksum != newChecksum.Value() {
			glog.V(0).Infoln("checksum:",checksum,"newChecksum.Value():",newChecksum.Value(),"datalen:",len(n.Data),"n.Size:",n.Size)
			return 0, errors.New("CRC error! Data On Disk Corrupted!")
		}
		n.Checksum = newChecksum
		return
	}
	return 0, fmt.Errorf("Unsupported Version! (%d)", version)
}
func (n *Needle) readNeedleHeader(bytes []byte) {
	n.Cookie = util.BytesToUint32(bytes[0:4])
	n.Id = util.BytesToUint64(bytes[4:12])
	n.Size = util.BytesToUint32(bytes[12:NeedleHeaderSize])
}
func (n *Needle) readNeedleDataVersion2(bytes []byte) (err error) {
	index, lenBytes := 0, len(bytes)
	if index < lenBytes {
		n.DataSize = util.BytesToUint32(bytes[index : index+4])
		index = index + 4
		if index+int(n.DataSize) > lenBytes {
			return ErrDataCorrup
		}
		n.Data = bytes[index : index+int(n.DataSize)]
		index = index + int(n.DataSize)
		n.Flags = bytes[index]
		index = index + 1
	}
	if index < lenBytes && n.HasName() {
		n.NameSize = uint8(bytes[index])
		index = index + 1
		if index+int(n.NameSize) > lenBytes {
			return ErrDataCorrup
		}
		n.Name = bytes[index : index+int(n.NameSize)]
		index = index + int(n.NameSize)
	}
	if index < lenBytes && n.HasMime() {
		n.MimeSize = uint8(bytes[index])
		index = index + 1
		if index+int(n.MimeSize) > lenBytes {
			return ErrDataCorrup
		}
		n.Mime = bytes[index : index+int(n.MimeSize)]
		index = index + int(n.MimeSize)
	}
	if index < lenBytes && n.HasLastModifiedDate() {
		if index+LastModifiedBytesLength > lenBytes {
			return ErrDataCorrup
		}
		n.LastModified = util.BytesToUint64(bytes[index : index+LastModifiedBytesLength])
		index = index + LastModifiedBytesLength
	}
	return nil
}

func (n *Needle) readNeedleDataVersion3(bytes []byte)(err error) {
	index, lenBytes := 0, len(bytes)
	if (index + NeedleMagicNumberSize) <= lenBytes {
		n.HeaderMagic = bytes[index : index+NeedleMagicNumberSize]
		index = index + NeedleMagicNumberSize
	}
	if index < lenBytes {
		n.DataSize = util.BytesToUint32(bytes[index : index+4])
		index = index + 4
		if index+int(n.DataSize) > lenBytes {
			return ErrDataCorrup
		}
		n.Data = bytes[index : index+int(n.DataSize)]
		index = index + int(n.DataSize)
		n.Flags = bytes[index]
		index = index + 1
	}
	if index < lenBytes && n.HasName() {
		n.NameSize = uint8(bytes[index])
		index = index + 1
		if index+int(n.NameSize) > lenBytes {
			return ErrDataCorrup
		}
		n.Name = bytes[index : index+int(n.NameSize)]
		index = index + int(n.NameSize)
	}
	if index < lenBytes && n.HasMime() {
		n.MimeSize = uint8(bytes[index])
		index = index + 1
		if index+int(n.MimeSize) > lenBytes {
			return ErrDataCorrup
		}
		n.Mime = bytes[index : index+int(n.MimeSize)]
		index = index + int(n.MimeSize)
	}
	if index < lenBytes && n.HasLastModifiedDate() {
		if index+LastModifiedBytesLength > lenBytes{
			return ErrDataCorrup
		}
		n.LastModified = util.BytesToUint64(bytes[index : index+LastModifiedBytesLength])
		index = index + LastModifiedBytesLength
	}
	if (index + NeedleMagicNumberSize) <= lenBytes {
		n.FooterMagic = bytes[index : index+NeedleMagicNumberSize]
		index = index + NeedleMagicNumberSize
	}
	return nil
}
func (n *Needle) readNeedleDataVersion4(bytes []byte)(err error) {
	index, lenBytes := 0, len(bytes)
	if (index + NeedleMagicNumberSize) <= lenBytes {
		n.HeaderMagic = bytes[index : index+NeedleMagicNumberSize]
		index = index + NeedleMagicNumberSize
	}
	if index < lenBytes {
		n.DataSize = util.BytesToUint32(bytes[index : index+4])
		index = index + 4
		if index+int(n.DataSize) > lenBytes {
			return ErrDataCorrup
		}
		n.Data = bytes[index : index+int(n.DataSize)]
		index = index + int(n.DataSize)
		n.Flags = bytes[index]
		index = index + 1
	}
	if index < lenBytes && n.HasName() {
		n.NameSize = uint8(bytes[index])
		index = index + 1
		if index+int(n.NameSize) > lenBytes {
			return ErrDataCorrup
		}
		n.Name = bytes[index : index+int(n.NameSize)]
		index = index + int(n.NameSize)
	}
	if index < lenBytes && n.HasMime() {
		n.MimeSize = uint8(bytes[index])
		index = index + 1
		if index+int(n.MimeSize) > lenBytes {
			return ErrDataCorrup
		}
		n.Mime = bytes[index : index+int(n.MimeSize)]
		index = index + int(n.MimeSize)
	}
	if index < lenBytes && n.HasLastModifiedDate() {
		if index+LastModifiedBytesLength > lenBytes{
			return ErrDataCorrup
		}
		n.LastModified = util.BytesToUint64(bytes[index : index+LastModifiedBytesLength])
		index = index + LastModifiedBytesLength
	}
	if n.HasOriginSize() && index < lenBytes{
		if index+4 > lenBytes{
			return ErrDataCorrup
		}
		n.OriginDataSize = util.BytesToUint32(bytes[index : index+4])
		index = index + 4
	}
	if (index + NeedleMagicNumberSize) <= lenBytes {
		n.FooterMagic = bytes[index : index+NeedleMagicNumberSize]
		index = index + NeedleMagicNumberSize
	}
	return nil
}

func ReadNeedleHeader(r *os.File, version Version, offset int64) (n *Needle, bodyLength uint32, err error) {
	n = new(Needle)
	if version == Version1 || version == Version2 || version == Version3 || version == Version4 {
		bytes := make([]byte, NeedleHeaderSize)
		var count int
		count, err = r.ReadAt(bytes, offset)
		if count <= 0 || err != nil {
			return nil, 0, err
		}
		n.readNeedleHeader(bytes)
		padding := NeedlePaddingSize - ((n.Size + NeedleHeaderSize + NeedleChecksumSize) % NeedlePaddingSize)
		bodyLength = n.Size + NeedleChecksumSize + padding
	}
	return
}

//n should be a needle already read the header
//the input stream will read until next file entry
func (n *Needle) ReadNeedleBody(r *os.File, version Version, offset int64, bodyLength uint32) (err error) {
	if bodyLength <= 0 {
		return nil
	}
	switch version {
	case Version1:
		bytes := make([]byte, bodyLength)
		if _, err = r.ReadAt(bytes, offset); err != nil {
			return err
		}
		n.Data = bytes[:n.Size]
		n.Checksum = NewCRC(n.Data)
	case Version2:
		bytes := make([]byte, bodyLength)
		if _, err = r.ReadAt(bytes, offset); err != nil {
			return
		}
		if err = n.readNeedleDataVersion2(bytes[0:n.Size]); err != nil {
			return err
		}
		n.Checksum = NewCRC(n.Data)
	case Version3:
		bytes := make([]byte, bodyLength)
		if _, err = r.ReadAt(bytes, offset); err != nil {
			return
		}
		if err = n.readNeedleDataVersion3(bytes[0:n.Size]); err != nil {
			return err
		}
		if len(n.Data) <= 0 && n.DataSize > 0 {
			return ErrBadNeedleData
		}
		n.Checksum = NewCRC(n.Data)
	case Version4:
		bytes := make([]byte, bodyLength)
		if _, err = r.ReadAt(bytes, offset); err != nil {
			return
		}
		if err = n.readNeedleDataVersion4(bytes[0:n.Size]); err != nil {
			return err
		}
		if len(n.Data) <= 0 && n.DataSize > 0 {
			return ErrBadNeedleData
		}
		n.Checksum = NewCRC(n.Data)
	default:
		err = fmt.Errorf("Unsupported Version! (%d)", version)
	}
	return
}

func (n *Needle) IsGzipped() bool {
	return n.Flags&FlagGzip > 0
}
func (n *Needle) SetGzipped() {
	n.Flags = n.Flags | FlagGzip
}
func (n *Needle) HasName() bool {
	return n.Flags&FlagHasName > 0
}
func (n *Needle) SetHasName() {
	n.Flags = n.Flags | FlagHasName
}
func (n *Needle) HasMime() bool {
	return n.Flags&FlagHasMime > 0
}
func (n *Needle) SetHasMime() {
	n.Flags = n.Flags | FlagHasMime
}
func (n *Needle) HasLastModifiedDate() bool {
	return n.Flags&FlagHasLastModifiedDate > 0
}
func (n *Needle) SetHasLastModifiedDate() {
	n.Flags = n.Flags | FlagHasLastModifiedDate
}

func (n *Needle) HasOriginSize() bool {
	return n.Flags&FlagHasOrginSize > 0
}
func (n *Needle) SetHasOriginSize() {
	n.Flags = n.Flags | FlagHasOrginSize
}
//从EC卷将数据读出来后，解析数据
func (n *Needle) ParseData(bytes []byte) (ret int, err error) {
	size:=len(bytes)
	if size == 0 {
		return 0, nil
	}
	n.readNeedleHeader(bytes)
	if n.Size != uint32(size-NeedleHeaderSize-NeedleChecksumSize) {
		return 0, fmt.Errorf("File Entry Not Found! Needle %d Memory %d", n.Size, size)
	}
	err = n.readNeedleDataVersion4(bytes[NeedleHeaderSize : NeedleHeaderSize+int(n.Size)])
	if err != nil {
		return -1, err
	}
	//作为工具时，使用fmt导出结果
	//fmt.Println("version:",version,",needle file:","size:",n.Size," Cookie:",n.Cookie," name:",string(n.Name)," datasize:",n.DataSize," id:",n.Id," datalen:",len(n.Data))
	checksum := util.BytesToUint32(bytes[NeedleHeaderSize+n.Size : NeedleHeaderSize+n.Size+NeedleChecksumSize])
	newChecksum := NewCRC(n.Data)
	if checksum != newChecksum.Value() {
		glog.V(0).Infoln("checksum:",checksum,"newChecksum.Value():",newChecksum.Value(),"datalen:",len(n.Data),"n.Size:",n.Size)
		return 0, errors.New("CRC error! Data On Disk Corrupted!")
	}
	n.Checksum = newChecksum
	ret = int(n.Size)
	return
}