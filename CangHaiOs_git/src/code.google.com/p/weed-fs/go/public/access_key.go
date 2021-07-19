package public

import (
	"errors"
	"strings"
	"sync"
)

const (
	USR_SEP = "|"
	KEY_SEP = "&"
)
const (
	MAX_USR = 2
)

var (
	ErrBadIdKeyPair = errors.New("can not parse access id and key error")
	ErrUsrNotFound  = errors.New("can not find user specified error")
	ErrUsrExist     = errors.New("usr key and id already existed error")
	ErrUsrMaxNum    = errors.New("already have the maximum num of usrs error")
)

type AccessKeyMgt struct {
	usrs map[string]string
	sync.RWMutex
}

func NewAccessKeyMgt(src string) *AccessKeyMgt {
	mgt := &AccessKeyMgt{}
	mgt.usrs = make(map[string]string)
	mgt.Parse(src)
	return mgt
}

func (mgt *AccessKeyMgt) Parse(src string) {
	src = strings.TrimSpace(src)
	parts := strings.Split(src, USR_SEP)
	for _, part := range parts {
		if part == "" {
			continue
		}
		idKeyParts := strings.Split(part, KEY_SEP)
		if len(idKeyParts) != 2 {
			continue
		}
		mgt.Lock()
		mgt.usrs[idKeyParts[0]] = idKeyParts[1]
		mgt.Unlock()
	}
}
func (mgt *AccessKeyMgt) DelUsr(id string) bool {
	var (
		have = false
	)
	mgt.Lock()
	if _, have = mgt.usrs[id]; have {
		delete(mgt.usrs, id)
	}
	mgt.Unlock()
	return have
}
func (mgt *AccessKeyMgt) AddUsr(id, key string) (err error) {
	mgt.RLock()
	if len(mgt.usrs) >= MAX_USR {
		mgt.RUnlock()
		return ErrUsrMaxNum
	}
	mgt.RUnlock()
	if id == "" || key == "" {
		return ErrBadIdKeyPair
	}
	mgt.RLock()
	if _, ok := mgt.usrs[id]; ok {
		mgt.RUnlock()
		return ErrUsrExist
	}
	mgt.RUnlock()
	mgt.Lock()
	mgt.usrs[id] = key
	mgt.Unlock()
	return nil
}
func (mgt *AccessKeyMgt) ToString() (accesskey string) {
	mgt.RLock()
	defer mgt.RUnlock()
	for id, key := range mgt.usrs {
		idKey := id + KEY_SEP + key
		accesskey = accesskey + USR_SEP + idKey
	}
	return
}
func (mgt *AccessKeyMgt) Get(id string) (idKey string) {
	mgt.RLock()
	defer mgt.RUnlock()
	return mgt.usrs[id]
}
func (mgt *AccessKeyMgt) List() []map[string]interface{} {
	var list []map[string]interface{}
	mgt.RLock()
	for key, sec := range mgt.usrs {
		mi := make(map[string]interface{})
		mii := make(map[string]string)
		mii["Id"] = key
		mii["Secret"] = sec
		mi["AccessKey"] = mii
		list = append(list, mi)
	}
	mgt.RUnlock()
	return list
}
func ParseIdKey(idKey string) (id, key string, err error) {
	parts := strings.Split(idKey, KEY_SEP)
	if len(parts) != 2 {
		return "", "", ErrBadIdKeyPair
	}
	return parts[0], parts[1], nil
}

func (mgt *AccessKeyMgt) GetAll() map[string]string {
	return mgt.usrs
}
