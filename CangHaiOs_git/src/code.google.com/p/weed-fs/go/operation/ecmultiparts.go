package operation

import (
	"code.google.com/p/weed-fs/go/glog"
	"encoding/json"
	"errors"
)

/*EC分块操作*/
const (
	MAX_RETRY_NUM = 3
)

type ECMultiPartObject struct {
	Id    string				`json:"Id,omitempty"` //uploadid,complete时，清空
	Size  int64					`json:"Size"`
	Count int					`json:"Cnt"`
	ObjectParts []ECPartObject	`json:"ObP,omitempty"` //存每个分块信息
	IsTruncated bool			`json:"IsTr"`
	NextMarker int				`json:"NM,omitempty"`
	Key  string 				`json:"Key,omitempty"` //fileName
	ReplacedPart ECPartObject   `json:"RP,omitempty"`
	CheckSum string             `json:"Crc,omitempty"`//存储etag
}

type ECPartObject struct {
	PartNum  int			   `json:"PN"`   //分块号
	IsGzip   bool              `json:"Gzip"` //该组分块是否被压缩了
	Fid      string            `json:"Fid,omitempty"`//分块fid
	Size     int64             `json:"Size"`   //分块大小
	CheckSum string            `json:"MD5,omitempty"` //分块MD5值
	SliceParts []ECSliceObject `json:"EcOP,omitempty"`//存该分块的各个切片信息
}
type ECSliceObject struct {
	Size int 		 	   `json:"Size"`     //该切片的size
	SerialNum string       `json:"Sn"`         //该切片的序列号。该切片是写在了EC卷的哪一个副本上
}

type ECAuditInfoSt struct {
	ObjectParts []ECAuditPartSt `json:"ObjectParts"`
}
type ECAuditPartSt struct {
	PartNumber int `json:"PartNumber"`
}

func (ai *ECAuditInfoSt) GetMap() map[int]int {
	m := make(map[int]int)
	for _, part := range ai.ObjectParts {
		m[part.PartNumber] = part.PartNumber
	}
	return m
}

func (mp *ECMultiPartObject) GetAudioInfo() (ai *ECAuditInfoSt) {
	ai = &ECAuditInfoSt{}
	for _, part := range mp.ObjectParts {
		if part.Size == 0 {
			continue
		}
		auditPart := ECAuditPartSt{
			PartNumber: part.PartNum,
		}
		ai.ObjectParts = append(ai.ObjectParts, auditPart)
	}
	return
}

func ParseECMultiParts(data []byte) (*ECMultiPartObject, error) {
	object := &ECMultiPartObject{}
	if err := json.Unmarshal(data, object); err != nil {
		return nil, err
	}
	return object, nil
}

func (mp *ECMultiPartObject) ToBytes() []byte {
	if data, err := json.Marshal(*mp); err == nil {
		return data
	}
	return nil
}

//EC切片信息不写入描述文件
func (mp *ECMultiPartObject) Shrink() error {
	if mp == nil {
		return errors.New("invalid mp")
	}
	for k,_:=range mp.ObjectParts{
		mp.ObjectParts[k].SliceParts=mp.ObjectParts[k].SliceParts[:0]
	}
	return nil
}
func (mp *ECMultiPartObject) IsPartNumValid(num int, override bool) bool {
	if num > MAX_BLOCK_NUM || num < 0 {
		return false
	}
	pLen := len(mp.ObjectParts)
	if pLen == num || (pLen > num && override) {
		return true
	}
	return false
}

func (mp *ECMultiPartObject) HasPart(num int) bool {
	for _, v := range mp.ObjectParts {
		if v.PartNum == num {
			return true
		}
	}
	return false
}

type ECPartRange struct {
	PartNum    int
	Start  int64
	Length int64
}
func (mp *ECMultiPartObject) Range(rng HttpRange) (pRange []ECPartRange, err error) {
	left := rng.Length
	start := rng.Start
	if len(mp.ObjectParts) == 0 {
		return nil, errors.New("error MultiPartObject")
	}
	bsize := mp.ObjectParts[0].Size
	for left > 0 {
		partRange := ECPartRange{
			PartNum:   int(start / bsize),
			Start: start % bsize,
		}
		inLen := mp.ObjectParts[partRange.PartNum].Size - partRange.Start
		if inLen > left {
			partRange.Length = left
		} else {
			partRange.Length = inLen
		}
		left = left - partRange.Length
		start = start + partRange.Length
		pRange = append(pRange, partRange)
	}
	return
}

func (mp *ECMultiPartObject) ReplacePart(partObj ECPartObject) (ret string, err error) {
	for k, v := range mp.ObjectParts {
		if v.PartNum == partObj.PartNum {
			delta := partObj.Size - v.Size
			mp.Size = mp.Size + delta
			mp.ObjectParts[k] = partObj
			return ret, nil
		}
	}
	return "", BadPartNumErr
}

func (mp *ECMultiPartObject) AppendPart(partObj ECPartObject, override bool) (string, error) {
	//最大64MB
	if partObj.Size <= 0 || partObj.Size > BLOCK_SIZE {
		glog.V(0).Infoln("partObj.Size:",partObj.Size,"operation.BLOCK_SIZE:",BLOCK_SIZE)
		return "", BadPartSizeErr
	}
	//分片数合法
	if !mp.IsPartNumValid(partObj.PartNum, override) {
		return "", BadPartNumErr
	}
	//已存？替换
	if mp.HasPart(partObj.PartNum) {
		if override {
			return mp.ReplacePart(partObj)
		} else {
			return "", PartExistedErr
		}
	}

	//分块是不是合法的
	//num=0无所谓
	//否则，前面的必须是2的n次幂
	//而且这一块不能大于上一块。
	//要保证，大小都一样
	if partObj.PartNum != 0 {
		lastsize := mp.ObjectParts[partObj.PartNum-1].Size
		if lastsize < partObj.Size {
			return "", BadPrePartSizeErr
		}
		//必须是整M倍数
		if lastsize%int64(0x00000001<<20) == 0 {
			//超过2块的，必须前面两块都一样大
			if partObj.PartNum >= 2 {
				if lastsize != mp.ObjectParts[partObj.PartNum-2].Size {
					return "", BadPrePartSizeErr
				}
			}
		} else {
			return "", BadPrePartSizeErr
		}
	}

	mp.ObjectParts = append(mp.ObjectParts, partObj)
	mp.Count++
	mp.Size += partObj.Size
	return "", nil
}

func(mp *ECMultiPartObject) AutoAppendPart(partObj ECPartObject) (string, error) {
	//最大64MB
	size := partObj.Size
	num := partObj.PartNum
	if size <= 0 || size > BLOCK_SIZE {
		glog.V(0).Infoln("partObj.Size:",partObj.Size,"operation.BLOCK_SIZE:",BLOCK_SIZE)
		return "", BadPartSizeErr
	}
	for {
		if len(mp.ObjectParts) < num+1 {
			mp.ObjectParts = append(mp.ObjectParts, ECPartObject{})

		} else {
			break
		}
	}
	partsize := int64(0)
	//取到现有非最后一块里面的最小的那个size
	for i, v := range mp.ObjectParts {
		if i == len(mp.ObjectParts)-1 {
			break
		}
		if v.Size != 0 && (partsize > v.Size || partsize == 0) {
			partsize = v.Size
		}
	}
	oldsize := mp.ObjectParts[num].Size
	//不是最后一块要做一些校验,保证是整数M块，保证是相等的块
	if num+1 != len(mp.ObjectParts) {
		if partsize != 0 {
			//中间有块不是整数M倍，最小的这个都不是整数倍，
			if partsize%int64(0x00000001<<20) != 0 {
				return "", BadPartSizeErr
			}
			//新块不是结尾，就必须等于partsize
			if size != partsize {
				return "", BadPartSizeErr
			}
		} else {
			//还没有老块，那这块就必须是整数M倍数
			if size%int64(0x00000001<<20) != 0 {
				return "", BadPartSizeErr
			}
			//中间块一定要比最后一块大
			if size < mp.ObjectParts[len(mp.ObjectParts)-1].Size {
				return "", BadPartSizeErr
			}
		}
	} else {
		//是最后一块，必须保证比之前的小
		if partsize != 0 {
			if size > partsize {
				return "", BadPartSizeErr
			}
		}
	}
	mp.ObjectParts[num] = partObj
	mp.Count++
	mp.Size += partObj.Size - oldsize
	return "", nil
}
