package cache

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"code.google.com/p/weed-fs/go/account"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/public"
	"code.google.com/p/weed-fs/go/util"
	"context"
)

type AccountCacheType struct {
	acc    *account.AccountStat
	accMgt *public.AccessKeyMgt
	expire time.Time
}
type AccountCache struct {
	m map[string]AccountCacheType
	sync.RWMutex
}

func NewAccountCache() *AccountCache {
	accCache := &AccountCache{m: make(map[string]AccountCacheType)}
	return accCache
}

var (
	accountCache = AccountCache{m: make(map[string]AccountCacheType)}
)

func (ac *AccountCache) Get(name string) (*account.AccountStat, error) {
	return ac.get(name)
}
func (ac *AccountCache) get(name string) (*account.AccountStat, error) {
	ac.RLock()
	defer ac.RUnlock()
	if act, ok := ac.m[name]; ok {
		if act.expire.After(time.Now()) {
			return act.acc, nil
		} else {
			return act.acc, errors.New("Expired")
		}
	}
	return nil, errors.New("Not Found")
}
func (ac *AccountCache) getAccKey(name, id string) (string, error) {
	ac.RLock()
	defer ac.RUnlock()
	if act, ok := ac.m[name]; ok {
		if act.expire.After(time.Now()) {
			return act.accMgt.Get(id), nil
		} else {
			return act.accMgt.Get(id), errors.New("Expired")
		}
	}
	return "", errors.New("Not Found")
}
func (ac *AccountCache) Set(acc *account.AccountStat, expTime time.Duration) {
	ac.set(acc, expTime)
	return
}
func (ac *AccountCache) set(acc *account.AccountStat, expTime time.Duration) {
	accMgt := public.NewAccessKeyMgt(acc.AccUser)
	act := AccountCacheType{
		acc:    acc,
		accMgt: accMgt,
		expire: time.Now().Add(expTime),
	}
	ac.Lock()
	defer ac.Unlock()
	ac.m[acc.Name] = act
}
func (ac *AccountCache) deleteCache(name string) {
	ac.Lock()
	defer ac.Unlock()
	delete(ac.m, name)
}

func LookupAccount(name string, master string, direct bool, ctx *context.Context) (accPtr *account.AccountStat, err error) {
	var acc *account.AccountStat
	if direct == false {
		acc, err = accountCache.get(name)
	}
	if direct == true || err != nil {
		lookup, pos, err := operation.PartitionLookup2(master, name, public.ACCOUNT, false, ctx)
		if err != nil {
			accPtr = nil
			//return nil, err
		} else {
			headers := make(map[string]string)
			if ctx != nil {
				reqid, _ := ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
				headers[public.SDOSS_REQUEST_ID] = reqid
			}

			path := "/get?parId=" + pos + "&account=" + name
			rspStatus, data, err := util.MakeRequestsQuickTimeout(lookup.List(), path, "GET", nil, headers)
			if err != nil {
				if rspStatus == http.StatusNotFound {
					accPtr = nil
					err = errors.New("NOT-FOUND-ERROR,do not get account:" + name + " information ")
					//return nil, errors.New("NOT-FOUND-ERROR,do not get account:" + name + " information ")
				} else {
					accPtr = nil
					//return nil, err
				}

			} else {
				if rspStatus != http.StatusOK {
					if rspStatus == http.StatusNotFound {
						accPtr = nil
						err = errors.New("NOT-FOUND-ERROR,do not to get account:" + name + " information")
						//return nil, errors.New("NOT-FOUND-ERROR,do not to get account:" + name + " information")
					} else {
						accPtr = nil
						err = errors.New("ServiceUnavilable,fail to get account:" + name + " information")
						//return nil, errors.New("ServiceUnavilable,fail to get account:" + name + " information")
					}
				} else {
					accPtr = &account.AccountStat{}
					err = json.Unmarshal(data, &accPtr)
					if err != nil {
						err = errors.New("fail to resolve account:" + name + " information")
						accPtr = nil
						//return nil, err
					} else {
						accountCache.set(accPtr, 5*time.Minute)
						err = nil
						//return accPtr, nil
					}
				}
			}
		}
	} else {
		accPtr = acc
	}
	if accPtr != nil {
		return accPtr, nil
	} else {
		//如果缓存过期，跟as要都没有，就用过期的缓存
		accPtr = acc
		if accPtr != nil {
			accountCache.set(accPtr, 30*time.Second)
			return accPtr, nil
		}
		return nil, errors.New("NOT-FOUND-ERROR,do not to get account:" + name + " information")
	}
}
func LookupAccountUserKey(name, id, master string, direct bool, ctx *context.Context) (key string) {
	if _, err := LookupAccount(name, master, direct, ctx); err != nil {
		return ""
	}
	key, _ = accountCache.getAccKey(name, id)
	return
}
func LookupAccountUserKeyList(name, master string, direct bool, ctx *context.Context) []map[string]interface{} {
	if _, err := LookupAccount(name, master, direct, ctx); err != nil {
		return nil
	}
	accountCache.RLock()
	defer accountCache.RUnlock()
	m := accountCache.m[name].accMgt.List()
	return m
}
func DeleteAccountCache(name string) {
	accountCache.deleteCache(name)
}
