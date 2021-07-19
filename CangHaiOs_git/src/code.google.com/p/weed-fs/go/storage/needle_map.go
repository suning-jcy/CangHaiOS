package storage

import (
	"fmt"
	"io"
	"os"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
)

type NeedleMapper interface {
	Put(key uint64, offset uint32, size uint32) (int, error)
	Get(key uint64) (element *NeedleValue, ok bool)
	Delete(key uint64) error
	Close()
	Destroy() error
	ContentSize() uint64
	DeletedSize() uint64
	FileCount() int
	DeletedCount() int
	Visit(visit func(NeedleValue) error) (err error)
	MaxFileKey() uint64
}

type mapMetric struct {
	DeletionCounter     int    `json:"DeletionCounter"`
	FileCounter         int    `json:"FileCounter"`
	DeletionByteCounter uint64 `json:"DeletionByteCounter"`
	FileByteCounter     uint64 `json:"FileByteCounter"`
	MaximumFileKey      uint64 `json:"MaxFileKey"`
}

type NeedleMap struct {
	indexFile *os.File
	//m         CompactList
	m         CompactArray
	Offset    int64
	mapMetric
}

func NewNeedleMap(file *os.File) *NeedleMap {
	nm := &NeedleMap{
		m:         NewCompactArray(),
		indexFile: file,
	}
	return nm
}

const (
	RowsToRead = 1024
)

func LoadNeedleMap(file *os.File) (*NeedleMap, error) {
	nm := NewNeedleMap(file)
	e := WalkIndexFile(file, func(key uint64, offset, size uint32) error {
		if key > nm.MaximumFileKey {
			nm.MaximumFileKey = key
		}
		nm.FileByteCounter = nm.FileByteCounter + uint64(size)
		if offset > 0 {
			oldSize := nm.m.Set(Key(key), offset, size)
			glog.V(3).Infoln("reading key", key, "offset", offset, "size", size, "oldSize", oldSize)
			if oldSize > 0 {
				nm.DeletionCounter++
				nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
			} else {
				nm.FileCounter++
			}
		} else {
			oldSize := nm.m.Delete(Key(key))
			glog.V(3).Infoln("removing key", key, "offset", offset, "size", size, "oldSize", oldSize)
			nm.DeletionCounter++
			nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
		}
		return nil
	})
	glog.V(1).Infoln("max file key:", nm.MaximumFileKey)
	return nm, e
}
func IncLoadNeedleMap(file *os.File, inm *NeedleMap) (nm *NeedleMap, err error) {
	if inm != nil {
		nm = inm
		inm.indexFile.Close()
		nm.indexFile = file
	} else {
		nm = NewNeedleMap(file)
		nm.indexFile.Seek(nm.Offset, 0)
	}
	e := WalkIndexFileVzOffset(file, nm.Offset, func(key uint64, offset, size uint32) error {
		if key > nm.MaximumFileKey {
			nm.MaximumFileKey = key
		}
		nm.FileByteCounter = nm.FileByteCounter + uint64(size)
		if offset > 0 {
			oldSize := nm.m.Set(Key(key), offset, size)
			glog.V(3).Infoln("reading key", key, "offset", offset, "size", size, "oldSize", oldSize)
			if oldSize > 0 {
				nm.DeletionCounter++
				nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
			} else {
				nm.FileCounter++
			}
		} else {
			oldSize := nm.m.Delete(Key(key))
			glog.V(3).Infoln("removing key", key, "offset", offset, "size", size, "oldSize", oldSize)
			nm.DeletionCounter++
			nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
		}
		return nil
	})
	glog.V(1).Infoln("max file key:", nm.MaximumFileKey)
	return nm, e
}

// walks through the index file, calls fn function with each key, offset, size
// stops with the error returned by the fn function
func WalkIndexFile(r *os.File, fn func(key uint64, offset, size uint32) error) error {
	var readerOffset int64
	bytes := make([]byte, 16*RowsToRead)
	count, e := r.ReadAt(bytes, readerOffset)
	glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
	readerOffset += int64(count)
	var (
		key          uint64
		offset, size uint32
		i            int
	)

	for count > 0 && e == nil || e == io.EOF {
		for i = 0; i+16 <= count; i += 16 {
			key = util.BytesToUint64(bytes[i : i+8])
			offset = util.BytesToUint32(bytes[i+8 : i+12])
			size = util.BytesToUint32(bytes[i+12 : i+16])
			if e = fn(key, offset, size); e != nil {
				return e
			}
		}
		if e == io.EOF {
			return nil
		}
		count, e = r.ReadAt(bytes, readerOffset)
		glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
		readerOffset += int64(count)
	}
	return e
}

// walks through the index file, calls fn function with each key, offset, size
// stops with the error returned by the fn function
func WalkIndexFileVzOffset(r *os.File, readerOffset int64, fn func(key uint64, offset, size uint32) error) error {
	bytes := make([]byte, 16*RowsToRead)
	count, e := r.ReadAt(bytes, readerOffset)
	glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
	readerOffset += int64(count)
	var (
		key          uint64
		offset, size uint32
		i            int
	)

	for count > 0 && e == nil || e == io.EOF {
		for i = 0; i+16 <= count; i += 16 {
			key = util.BytesToUint64(bytes[i : i+8])
			offset = util.BytesToUint32(bytes[i+8 : i+12])
			size = util.BytesToUint32(bytes[i+12 : i+16])
			if e = fn(key, offset, size); e != nil {
				return e
			}
		}
		if e == io.EOF {
			return nil
		}
		count, e = r.ReadAt(bytes, readerOffset)
		glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
		readerOffset += int64(count)
	}
	return e
}
func (nm *NeedleMap) Put(key uint64, offset uint32, size uint32) (int, error) {
	if key > nm.MaximumFileKey {
		nm.MaximumFileKey = key
	}
	oldSize := nm.m.Set(Key(key), offset, size)
	bytes := make([]byte, 16)
	util.Uint64toBytes(bytes[0:8], key)
	util.Uint32toBytes(bytes[8:12], offset)
	util.Uint32toBytes(bytes[12:16], size)

	nm.FileByteCounter = nm.FileByteCounter + uint64(size)
	if oldSize > 0 {
		nm.DeletionCounter++
		nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
	} else {
		nm.FileCounter++
	}
	if _, err := nm.indexFile.Seek(0, 2); err != nil {
		return 0, fmt.Errorf("cannot go to the end of indexfile %s: %s", nm.indexFile.Name(), err.Error())
	}
	return nm.indexFile.Write(bytes)
}
func (nm *NeedleMap) Get(key uint64) (element *NeedleValue, ok bool) {
	element, ok = nm.m.Get(Key(key))
	return
}
func (nm *NeedleMap) Delete(key uint64) error {
	nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(nm.m.Delete(Key(key)))
	bytes := make([]byte, 16)
	util.Uint64toBytes(bytes[0:8], key)
	util.Uint32toBytes(bytes[8:12], 0)
	util.Uint32toBytes(bytes[12:16], 0)
	if _, err := nm.indexFile.Seek(0, 2); err != nil {
		return fmt.Errorf("cannot go to the end of indexfile %s: %s", nm.indexFile.Name(), err.Error())
	}
	if _, err := nm.indexFile.Write(bytes); err != nil {
		return fmt.Errorf("error writing to indexfile %s: %s", nm.indexFile.Name(), err.Error())
	}
	nm.DeletionCounter++
	return nil
}
func (nm *NeedleMap) Close() {
	_ = nm.indexFile.Close()
}
func (nm *NeedleMap) Destroy() error {
	nm.Close()
	return os.Remove(nm.indexFile.Name())
}
func (nm NeedleMap) ContentSize() uint64 {
	return nm.FileByteCounter
}
func (nm NeedleMap) DeletedSize() uint64 {
	return nm.DeletionByteCounter
}
func (nm NeedleMap) FileCount() int {
	return nm.FileCounter
}
func (nm NeedleMap) DeletedCount() int {
	return nm.DeletionCounter
}
func (nm *NeedleMap) Visit(visit func(NeedleValue) error) (err error) {
	return nm.m.Visit(visit)
}
func (nm NeedleMap) MaxFileKey() uint64 {
	return nm.MaximumFileKey
}
func GetIdxIndicatedLen(file *os.File) (dataSize int64, err error) {
	tmpSize := uint32(0)
	dataOffset := uint32(8)
	nm := NewNeedleMap(file)
	err = WalkIndexFile(file, func(key uint64, offset, size uint32) error {
		if key > nm.MaximumFileKey {
			nm.MaximumFileKey = key
		}
		if offset > 0 {
			oldSize := nm.m.Set(Key(key), offset, size)
			if oldSize > 0 {
				tmpSize = tmpSize + 16 + 8
			}
			dataOffset = offset
			padding := NeedlePaddingSize - ((NeedleHeaderSize + size + NeedleChecksumSize) % NeedlePaddingSize)
			tmpSize = size + NeedleHeaderSize + NeedleChecksumSize + padding
		} else {
			nm.m.Delete(Key(key))
			tmpSize = tmpSize + 16 + 8
		}
		return nil
	})
	dataSize = int64(dataOffset)*NeedlePaddingSize + int64(tmpSize)
	return dataSize, err
}
