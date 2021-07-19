package cache

import (
	"code.google.com/p/weed-fs/go/glog"
	"github.com/hoisie/redis"
)

type MemcacheClient struct {
	master *string
}
type MemcacheError string

func (err MemcacheError) Error() string { return "memcache Error: " + string(err) }

var ParitionNotExist = MemcacheError("Parition does not exist ")

func newMemcacheClient() *MemcacheClient {
	return &MemcacheClient{}

}

func NewMemcacheClient(master *string) (mcm *MemcacheClient) {
	mcm = &MemcacheClient{master: master}
	return
}

func (client *MemcacheClient) SetMaster(master *string) {
	client.master = master
}

func (client *MemcacheClient) Set(key string, val []byte) error {
	mcms, err := LookupMemcache(*client.master, key, false)
	if err != nil {
		return err
	}
	if len(mcms) <= 0 {
		return ParitionNotExist
	}
	err = mcms[0].client.Set(key, val)
	glog.V(4).Infoln("memcache client set:", mcms[0].url, key, string(val), err)
	for i := 1; i < len(mcms); i++ {
		go mcms[i].client.Set(key, val)
		glog.V(4).Infoln("go memcache client set:", mcms[i].url, key, string(val))
	}

	return err
}

func (client *MemcacheClient) Get(key string) ([]byte, error) {
	mcms, err := LookupMemcache(*client.master, key, false)
	if err != nil {
		return nil, err
	}
	if len(mcms) <= 0 {
		return nil, ParitionNotExist
	}
	res, err := mcms[0].client.Get(key)
	if err != nil && err != redis.NotExist {
		for i := 1; i < len(mcms); i++ {
			res, err = mcms[i].client.Get(key)
			if err == nil {
				break
			}
		}
	}

	return res, err
}

func (client *MemcacheClient) Getset(key string, val []byte) ([]byte, error) {
	mcms, err := LookupMemcache(*client.master, key, false)
	if err != nil {
		return nil, err
	}
	if len(mcms) <= 0 {
		return nil, ParitionNotExist
	}
	res, err := mcms[0].client.Getset(key, val)

	for i := 1; i < len(mcms); i++ {
		go mcms[i].client.Getset(key, val)
	}

	return res, err
}

func (client *MemcacheClient) Incrby(key string, val int64) (int64, error) {
	mcms, err := LookupMemcache(*client.master, key, false)
	if err != nil {
		return 0, err
	}
	if len(mcms) <= 0 {
		return 0, ParitionNotExist
	}
	res, err := mcms[0].client.Incrby(key, val)

	for i := 1; i < len(mcms); i++ {
		go mcms[i].client.Incrby(key, val)
	}

	return res, err
}
func (client *MemcacheClient) Decrby(key string, val int64) (int64, error) {
	mcms, err := LookupMemcache(*client.master, key, false)
	if err != nil {
		return 0, err
	}
	if len(mcms) <= 0 {
		return 0, ParitionNotExist
	}
	res, err := mcms[0].client.Decrby(key, val)

	for i := 1; i < len(mcms); i++ {
		go mcms[i].client.Decrby(key, val)
	}

	return res, err
}
func (client *MemcacheClient) Del(key string) error {
	mcms, err := LookupMemcache(*client.master, key, false)
	if err != nil {
		return err
	}
	if len(mcms) <= 0 {
		return ParitionNotExist
	}
	_, err = mcms[0].client.Del(key)
	glog.V(4).Infoln("memcache client del:", mcms[0].url, key, err)
	for i := 1; i < len(mcms); i++ {
		go mcms[i].client.Del(key)
		glog.V(4).Infoln("go memcache client del:", mcms[i].url, key)
	}
	return err
}
func (client *MemcacheClient) SetNX(key string, val []byte) (bool, error) {
	mcms, err := LookupMemcache(*client.master, key, false)
	if err != nil {
		return false, err
	}
	if len(mcms) <= 0 {
		return false, ParitionNotExist
	}
	res, err := mcms[0].client.Setnx(key, val)
	glog.V(4).Infoln("memcache client SetNX:", mcms[0].url, key, string(val), err)
	for i := 1; i < len(mcms); i++ {
		go mcms[i].client.Setnx(key, val)
	}

	return res, err
}
func (client *MemcacheClient) Exists(key string) (bool, error) {
	mcms, err := LookupMemcache(*client.master, key, false)
	if err != nil {
		return false, err
	}
	if len(mcms) <= 0 {
		return false, ParitionNotExist
	}
	res, err := mcms[0].client.Exists(key)
	if err != nil && err != redis.NotExist {
		for i := 1; i < len(mcms); i++ {
			res, err = mcms[i].client.Exists(key)
			if err == nil {
				break
			}
		}
	}

	return res, err
}
func (client *MemcacheClient) Keys(pattern string) ([]string, error) {
	mcms, err := LookupAllMemcache(*client.master, false)
	var res []string
	for _, mcm := range mcms {
		if len(mcm) <= 0 {
			continue
		}
		ret, err := mcm[0].client.Keys(pattern)
		if err != nil && err != redis.NotExist {
			for i := 1; i < len(mcm); i++ {
				res, err = mcm[i].client.Keys(pattern)
				if err == nil {
					break
				}
			}
		}
		res = append(res, ret...)
	}
	return res, err
}
func (client *MemcacheClient) Expire(key string, time int64) (bool, error) {
	mcms, err := LookupMemcache(*client.master, key, false)
	if err != nil {
		return false, err
	}
	if len(mcms) <= 0 {
		return false, ParitionNotExist
	}
	res, err := mcms[0].client.Expire(key, time)
	if err != nil && err != redis.NotExist {
		for i := 1; i < len(mcms); i++ {
			res, err = mcms[i].client.Expire(key, time)
			if err == nil {
				break
			}
		}
	}
	return res, err
}
func (c *MemcacheClient) ZAdd(key string, members ...interface{}) (int64, error) {
	return 0, nil
}
func (c *MemcacheClient) ZAddNX(key string, members ...interface{}) (int64, error) {
	return 0, nil
}
func (c *MemcacheClient) ZAddXX(key string, members ...interface{}) (int64, error) {
	return 0, nil
}
func (c *MemcacheClient) ZCount(key, min, max string) (int64, error) {
	return 0, nil
}
func (c *MemcacheClient) ZRangeByLex(key string, Min, Max string, Offset, Count int64) ([]string, error) {
	return nil, nil
}
func (c *MemcacheClient) ZRem(key string, members ...interface{}) (int64, error) {
	return 0, nil
}
func (c *MemcacheClient) HGetAll(key string) (map[string]string, error) {
	return nil, nil
}
func (c *MemcacheClient) HGet(key string, field string) (string, error) {
	return "", nil
}
func (c *MemcacheClient) HSet(key string, field string, val string) (bool, error) {
	return false, nil
}
func (c *MemcacheClient) HDel(key string, field string) (int64, error) {
	return 0, nil
}
func (c *MemcacheClient) HINCRBY(key string, field string, num int64) (int64, error) {
	return 0, nil
}
func (c *MemcacheClient) Scan(cursor uint64, match string, percount int64, maxcount int) ([]string, error) {
	return nil, nil
}
func (c *MemcacheClient) ScanPerRedis(cursor uint64, match string, percount int64, maxcount int, redisidx int) ([]string, uint64, error) {
	return nil, 0, nil
}
func (c *MemcacheClient) GetRedisCount() int {
	return 0
}