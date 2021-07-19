package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/textproto"
	"strconv"
	"strings"
)

var BLOCK_SIZE = int64(67108864)
var MAX_BLOCK_NUM = int(100000)
var BadPartSizeErr = errors.New("Not Proper Block Part Size error!")
var BadPrePartSizeErr = errors.New("Not Proper Pre Block Part Size error!")
var BadPartNumErr = errors.New("Not Proper Block Part Num error!")
var PartExistedErr = errors.New("Block Part Existed error!")
var MultiUploadNotInitErr = errors.New("Multi Upload Event Not Init error!")
var EmptyUploadErr = errors.New("Empty upload error!")
// httpRange specifies the byte range to be sent to the client.

type HttpRange struct {
	Start, Length int64
}

func (r HttpRange) ContentRange(size int64) string {
	return fmt.Sprintf("bytes %d-%d/%d", r.Start, r.Start+r.Length-1, size)
}

func (r HttpRange) MimeHeader(contentType string, size int64) textproto.MIMEHeader {
	return textproto.MIMEHeader{
		"Content-Range": {r.ContentRange(size)},
		"Content-Type":  {contentType},
	}
}

type MultiParts struct {
	multiObjects MultiPartObject
}
type MultiPartObject struct {
	Id          string       `json:"Id,omitempty"`
	Size        int64        `json:"Size,omitempty"`
	Count       int          `json:"count,omitempty"`
	ObjectParts []PartObject `json:"object_parts,omitempty"`
	IsTruncated bool         `json:"IsTruncated"`
	NextMaker   int          `json:"nextMarker,omitempty"`
	Key         string       `json:"Key,omitempty"`
	/*
		只用于reopen操作，在提交之后，会删除这个块
	*/
	ReplacedPart PartObject `json:"ReplacedPart,omitempty"`
}

type PartObject struct {
	PartNum  int    `json:"part_number"`
	CheckSum string `json:"md5sum,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Fid      string `json:"Fid,omitempty"`
}
type PartRange struct {
	Fid    string
	Num    int
	Start  int64
	Length int64
}
type MultiUploadEvent struct {
	Id string `json:"Upload-id,omitempty"`
}
type MultiPartObjectSDK struct {
	Id          string          `json:"Id,omitempty"`
	Size        int64           `json:"Size,omitempty"`
	Count       int             `json:"count,omitempty"`
	ObjectParts []PartObjectSDK `json:"ObjectParts,omitempty"`
	IsTruncated bool            `json:"IsTruncated"`
	NextMaker   int             `json:"nextMarker,omitempty"`
	Key         string          `json:"Key,omitempty"`
}
type PartObjectSDK struct {
	PartNum  int    `json:"PartNumber"`
	CheckSum string `json:"md5sum,omitempty"`
	Size     int64  `json:"Size,omitempty"`
	Fid      string `json:"Fid,omitempty"`
}

type AuditInfoSt struct {
	ObjectParts []AuditPartSt `json:"ObjectParts"`
}
type AuditPartSt struct {
	PartNumber int `json:"PartNumber"`
}
type MultiPartObjectComp struct {
	Id          string       `json:"Id,omitempty"`
	Size        int64        `json:"Size,omitempty"`
	Count       int          `json:"count,omitempty"`
	ObjectParts []PartObject `json:"ObjectParts,omitempty"`
	IsTruncated bool         `json:"IsTruncated"`
	NextMaker   int          `json:"nextMarker,omitempty"`
	Key         string       `json:"Key,omitempty"`
}

func ParseMultiParts(data []byte) (*MultiPartObject, error) {
	object := &MultiPartObject{}
	if err := json.Unmarshal(data, object); err != nil {
		return nil, err
	}
	return object, nil
}
func ParseMultiPartsComp(data []byte) (*MultiPartObjectComp, error) {
	object := &MultiPartObjectComp{}
	if err := json.Unmarshal(data, object); err != nil {
		return nil, err
	}
	return object, nil
}

func (ai *AuditInfoSt) GetMap() map[int]int {
	m := make(map[int]int)
	for _, part := range ai.ObjectParts {
		m[part.PartNumber] = part.PartNumber
	}
	return m
}
func (mp *MultiPartObject) Range(rng HttpRange) (pRange []PartRange, err error) {
	left := rng.Length
	start := rng.Start
	lens := len(mp.ObjectParts)
	if len(mp.ObjectParts) == 0 {
		return nil, errors.New("error MultiPartObject")
	}
	bsize := mp.ObjectParts[0].Size
	cycleCnt:=0
	partRange := PartRange{}
	for left > 0 {
		//如果循环次数超过分块数，则表示进入死循环
		if cycleCnt > lens{
			return nil, errors.New("Infinite Loop! ")
		}
		partRange = PartRange{
			Num:   int(start / bsize),
			Start: start % bsize,
		}
		partRange.Fid = mp.ObjectParts[partRange.Num].Fid
		inLen := mp.ObjectParts[partRange.Num].Size - partRange.Start
		if inLen > left {
			partRange.Length = left
		} else {
			partRange.Length = inLen
		}
		//如果left没有变化，则进入死循环
		//if partRange.length == 0 {
		//	return nil, errors.New("Infinite Loop! ")
		//}
		left = left - partRange.Length
		start = start + partRange.Length
		pRange = append(pRange, partRange)
		cycleCnt ++
	}
	return
}

func (mp *MultiPartObject) ToBytes() []byte {
	if data, err := json.Marshal(*mp); err == nil {
		return data
	}
	return nil
}
func (mp *MultiPartObject) HasPart(num int) bool {
	for _, v := range mp.ObjectParts {
		if v.PartNum == num {
			return true
		}
	}
	return false
}
func (mp *MultiPartObject) IsPartNumValid(num int, override bool) bool {
	if num > MAX_BLOCK_NUM || num < 0 {
		return false
	}
	pLen := len(mp.ObjectParts)
	if pLen == num || (pLen > num && override) {
		return true
	}
	return false
}
func (mp *MultiPartObject) AppendPart(fid, crc string, size int64, num int, override bool) (string, error) {
	//最大64MB
	if size <= 0 || size > BLOCK_SIZE {
		return "", BadPartSizeErr
	}
	//分片数合法
	if !mp.IsPartNumValid(num, override) {
		return "", BadPartNumErr
	}
	//已存？替换
	if mp.HasPart(num) {
		if override {
			return mp.ReplacePart(fid, crc, size, num)
		} else {
			return "", PartExistedErr
		}
	}

	//分块是不是合法的
	//num=0无所谓
	//否则，前面的必须是2的n次幂
	//而且这一块不能大于上一块。
	//要保证，大小都一样
	if num != 0 {
		lastsize := mp.ObjectParts[num-1].Size
		if lastsize < size {
			return "", BadPrePartSizeErr
		}
		//必须是整M倍数
		if lastsize%int64(0x00000001<<20) == 0 {
			//超过2块的，必须前面两块都一样大
			if num >= 2 {
				if lastsize != mp.ObjectParts[num-2].Size {
					return "", BadPrePartSizeErr
				}
			}
		} else {
			return "", BadPrePartSizeErr
		}
	}
	/*
		if num != 0 && mp.ObjectParts[num-1].Size != BLOCK_SIZE {
			return "", BadPrePartSizeErr
		}
	*/

	partObject := PartObject{
		Fid:      fid,
		CheckSum: crc,
		PartNum:  num,
		Size:     size,
	}
	mp.ObjectParts = append(mp.ObjectParts, partObject)
	mp.Count++
	mp.Size += size
	return "", nil
}

func (mp *MultiPartObject) AutoAppendPart(fid, crc string, size int64, num int) (string, error) {
	//最大64MB
	if size <= 0 || size > BLOCK_SIZE {
		return "", BadPartSizeErr
	}
	partObject := PartObject{
		Fid:      fid,
		CheckSum: crc,
		PartNum:  num,
		Size:     size,
	}
	for {
		if len(mp.ObjectParts) < num+1 {
			mp.ObjectParts = append(mp.ObjectParts, PartObject{})
		} else {
			break
		}
	}
	oldsize := mp.ObjectParts[num].Size
	mp.ObjectParts[num] = partObject
	mp.Size += (size - oldsize)
	if oldsize == 0 {
		mp.Count++
	}
	//取到现有非最后一块里面的非零size，必须相等且为整数M
	partsize := int64(0)
	for i, v := range mp.ObjectParts {
		if i == len(mp.ObjectParts)-1 {
			break
		}
		if v.Size != 0 && partsize == 0 {
			partsize = v.Size
			//非零size为整数M
			if partsize % int64(0x00000001<<20) != 0 {
				if i != num{
					return "", BadPrePartSizeErr
				}
				return "", BadPartSizeErr
			}
		}
		//非最后一块所有非零size必须相等
		if v.Size != 0 && partsize != 0 && v.Size != partsize{
			if i != num{
				return "", BadPrePartSizeErr
			}
			return "", BadPartSizeErr
		}
	}
	//最后一块size不能大于前面的非零size
	if partsize!=0 && partsize < mp.ObjectParts[len(mp.ObjectParts)-1].Size {
		if len(mp.ObjectParts)-1 != num{
			return "", BadPrePartSizeErr
		}
		return "", BadPartSizeErr
	}
	return "", nil
}

func (mp *MultiPartObject) ReplacePart(fid string, checkSum string, size int64, num int) (ret string, err error) {
	partObject := PartObject{
		Fid:      fid,
		CheckSum: checkSum,
		PartNum:  num,
		Size:     size,
	}
	for k, v := range mp.ObjectParts {
		if v.PartNum == num {
			ret = v.Fid
			delta := size - v.Size
			mp.Size = mp.Size + delta
			mp.ObjectParts[k] = partObject
			return ret, nil
		}
	}
	return "", BadPartNumErr
}

func (mp *MultiPartObject) GetAudioInfo() (ai *AuditInfoSt) {
	ai = &AuditInfoSt{}
	for _, part := range mp.ObjectParts {
		if part.Fid == "" {
			continue
		}
		auditPart := AuditPartSt{
			PartNumber: part.PartNum,
		}
		ai.ObjectParts = append(ai.ObjectParts, auditPart)
	}
	return
}

func (mp *MultiPartObject) AddPart(parto *PartObject) error {
	if parto.Size <= 0 || parto.Size > BLOCK_SIZE {
		return BadPartSizeErr
	}

	if parto.PartNum >= len(mp.ObjectParts) {
		for i := parto.PartNum - len(mp.ObjectParts); i >= 0; i-- {
			mp.ObjectParts = append(mp.ObjectParts, PartObject{})
		}
	}
	if mp.ObjectParts[parto.PartNum].Fid == "" {
		mp.ObjectParts[parto.PartNum] = *parto
	} else {
		mp.Count--
		mp.Size -= mp.ObjectParts[parto.PartNum].Size
		mp.ObjectParts[parto.PartNum] = *parto
	}

	mp.Count++
	mp.Size += parto.Size
	return nil
}


// parseRange parses a Range header string as per RFC 2616.
func ParseRange(s string, size int64) ([]HttpRange, error) {
	if s == "" {
		return nil, nil // header not present
	}
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return nil, errors.New("invalid range")
	}
	var ranges []HttpRange
	for _, ra := range strings.Split(s[len(b):], ",") {
		ra = strings.TrimSpace(ra)
		if ra == "" {
			continue
		}
		i := strings.Index(ra, "-")
		if i < 0 {
			return nil, errors.New("invalid range")
		}
		start, end := strings.TrimSpace(ra[:i]), strings.TrimSpace(ra[i+1:])
		var r HttpRange
		if start == "" {
			// If no start is specified, end specifies the
			// range start relative to the end of the file.
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil {
				return nil, errors.New("invalid range")
			}
			if i > size {
				i = size
			}
			r.Start = size - i
			r.Length = size - r.Start
		} else {
			i, err := strconv.ParseInt(start, 10, 64)
			if err != nil || i >= size || i < 0 {
				return nil, errors.New("invalid range")
			}
			r.Start = i
			if end == "" {
				// If no end is specified, range extends to end of the file.
				r.Length = size - r.Start
			} else {
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || r.Start > i {
					return nil, errors.New("invalid range")
				}
				if i >= size {
					i = size - 1
				}
				r.Length = i - r.Start + 1
			}
		}
		if r.Start > size {
			return nil,errors.New("invalid range")
		}
		ranges = append(ranges, r)
	}
	return ranges, nil
}
