package cache

import (
	"time"
	"code.google.com/p/weed-fs/go/glog"
	"strconv"
	"github.com/hoisie/redis"
	"github.com/pkg/errors"
)
var (
	//最后修改时间不一致，表示时序性不对
	GetLockTimeOut         = errors.New("Get Lock TimeOut")
)

type Client interface {
	Set(key string, val []byte) error
	Get(key string) ([]byte, error)
	Getset(key string, val []byte) ([]byte, error)
	Incrby(key string, val int64) (int64, error)
	Decrby(key string, val int64) (int64, error)
	Del(key string) error
	SetNX(key string, val []byte) (bool, error)
	Exists(key string) (bool, error)
	Keys(pattern string) ([]string, error)
	Scan(cursor uint64, match string, percount int64, maxcount int) ([]string, error)
	Expire(key string, time int64) (bool, error)
	ScanPerRedis(cursor uint64, match string, percount int64, maxcount int, redisidx int) ([]string, uint64, error)
	GetRedisCount() int
	/*
		zset
	*/
	ZAdd(key string, members ...interface{}) (int64, error)
	ZAddNX(key string, members ...interface{}) (int64, error)
	ZAddXX(key string, members ...interface{}) (int64, error)
	ZCount(key, min, max string) (int64, error)
	ZRangeByLex(key string, Min, Max string, Offset, Count int64) ([]string, error)
	ZRem(key string, members ...interface{}) (int64, error)

	HGetAll(key string) (map[string]string, error)
	HGet(key string, field string) (string, error)
	HSet(key string, field string, val string) (bool, error)
	HDel(key string, field string) (int64, error)
	HINCRBY(key string, field string, num int64) (int64, error)
}

func CreateRedisClient(style string) Client {
	switch style {
	case "CLUSTER":
		return newClusterClient()
	case "STENTINEL":
		return newSentinelClient()
	case "MEMCACHE":
		return newMemcacheClient()
	default:
		return nil
	}
}

//get a distributed  lock for dir
//use it befor change memcache dirtree
//delete key after using it
//timeout 单位毫秒
//假如存在需要获取多个锁的情况。需要先获取深的，再获取浅的
func GetRedisLock(client Client, key string, timeout int, gettimeout int) error {

	//整个获取lock的时间要是过长（超过3倍timeout），返回失败。
	starttime := time.Now().UnixNano()
	delta := int64(timeout) * 1e6
	gettimeoutns := int64(gettimeout) * 1e6
	for {
		nowdate := time.Now().UnixNano()
		if nowdate-starttime > int64(gettimeoutns) {
			glog.V(0).Infoln("error while getLock ,can not get it for a long time")
			return GetLockTimeOut
		}

		nowdate += delta
		value := strconv.FormatInt(nowdate, 10)
		res, err := client.SetNX(key, []byte(value))
		if err != nil {
			glog.V(0).Infoln("error while getLock : " + key + " for " + err.Error())
			return err
		}
		if res {
			// got it
			return nil
		}

		/*timeout?  现在redisclient没有超时，自己校验*/
		resget, err := client.Get(key)
		if err != nil && err != Nil && err != redis.NotExist {
			glog.V(0).Infoln("error while getLock : " + key + " for " + err.Error())
			return err
		}
		if resget == nil || err == Nil || err == redis.NotExist {
			//nobody lock it,try to get it again
			continue
		}
		v1, err := strconv.ParseInt(string(resget), 10, 64)
		if err != nil && string(resget) != "" {
			glog.V(0).Infoln("error while getLock : " + key + " for " + err.Error())
			return err
		}
		if v1 > time.Now().UnixNano() {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		nowdate = time.Now().UnixNano()
		nowdate += int64(timeout) * 1e6
		value = strconv.FormatInt(nowdate, 10)

		resgetset, err := client.Getset(key, []byte(value))

		if err != nil {
			glog.V(0).Infoln("error while getLock : " + key + " for " + err.Error())
			if err.Error() == "Key does not exist" || err == Nil {
				continue
			}
			return err
		}
		if resgetset == nil || err == Nil || err == redis.NotExist {
			//GOT it
			return nil
		}

		v2, err := strconv.ParseInt(string(resgetset), 10, 64)
		if err != nil && string(resget) != "" {
			glog.V(0).Infoln("error while getLock : " + key + " for " + err.Error())
			return err
		}
		if v1 == v2 {
			//GOT it
			return nil
		}

		time.Sleep(10 * time.Millisecond)
		continue
	}

}
