package operation
import (
"code.google.com/p/weed-fs/go/glog"
"crypto/aes"
"crypto/cipher"
"encoding/base64"
"strconv"
"sync"
"time"
)

const (
	CIPHERRQIDKEY = "sdoss-request-id"
)

type RequestId interface {
	NextId() (id string)
	ParseId(string) string
}
type CipherRequestId struct {
	count  uint64
	server string
	cipher cipher.Block
	lock   sync.Mutex
}

func NewCipherRequestId(ip, port string) *CipherRequestId {
	requestId := &CipherRequestId{
		server: ip + ":" + port,
	}
	cipher, err := aes.NewCipher([]byte(CIPHERRQIDKEY))
	if err != nil {
		glog.V(0).Infoln(err.Error())
		return nil
	}
	requestId.cipher = cipher
	return requestId
}
func (rig *CipherRequestId) NextId() (id string) {
	rig.lock.Lock()
	defer rig.lock.Unlock()
	id = rig.server + "|" + strconv.FormatInt(time.Now().Unix(), 10) + "|" + strconv.FormatUint(rig.count, 10)
	rig.count = rig.count + 1
	return base64.URLEncoding.EncodeToString([]byte(id))
	/*
	18-2-23  不加密了
	cipherlen := len(id) + 4
	if cipherlen%aes.BlockSize != 0 {
		cipherlen = (cipherlen/aes.BlockSize + 1) * aes.BlockSize
	}
	buf := make([]byte, cipherlen)
	util.Uint32toBytes(buf[0:4], uint32(len(id)+4))
	copy(buf[4:], []byte(id))
	i := 0
	for {
		rig.cipher.Encrypt(buf[i:i+aes.BlockSize], buf[i:i+aes.BlockSize])
		i = i + aes.BlockSize
		if i >= cipherlen {
			break
		}
	}
	id = base64.URLEncoding.EncodeToString(buf)
	rig.count = rig.count + 1
	return
	*/

}
func (rig *CipherRequestId) ParseId(id string) (hId string) {
	idPrefix, err := base64.URLEncoding.DecodeString(id)
	if err != nil {
		return ""
	}
	return string(idPrefix)
	/*
		if len(idPrefix)%aes.BlockSize != 0 {
		return ""
	}
	i := 0
	for {
		rig.cipher.Decrypt(idPrefix[i:i+aes.BlockSize], idPrefix[i:i+aes.BlockSize])
		i = i + aes.BlockSize
		if i >= len(idPrefix) {
			break
		}

	}
	strlen := util.BytesToUint32(idPrefix[0:4])
	if strlen > uint32(len(idPrefix)) {
		return ""
	}
	hId = string(idPrefix[4:strlen])
	return hId
	*/
}
