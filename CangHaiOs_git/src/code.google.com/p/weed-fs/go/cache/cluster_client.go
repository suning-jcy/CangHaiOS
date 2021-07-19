package cache

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
	"github.com/go-redis/redis"
)

var (
	clusterclient *ClusterClient
)

type ClusterClient struct {
	client *redis.ClusterClient
	opt    *redis.ClusterOptions
}

func newClusterClient() *ClusterClient {
	once.Do(func() {
		clusterclient = &ClusterClient{
			opt: &redis.ClusterOptions{
				MaxRetries:         0,
				DialTimeout:        5 * time.Second,
				ReadTimeout:        3 * time.Second,
				WriteTimeout:       3 * time.Second,
				PoolSize:           40,
				PoolTimeout:        4 * time.Second,
				IdleTimeout:        300000 * time.Millisecond,
				IdleCheckFrequency: 60000 * time.Millisecond,
			},
		}
		loadAndInitClusterClient(clusterclient)
	})
	return clusterclient
}

func loadAndInitClusterClient(clusterclient *ClusterClient) {
	confFile, err := os.OpenFile(filepath.Join(dir, "clusterclient.conf"), os.O_RDONLY, 0644)
	if err != nil {
		if !os.IsNotExist(err) {
			panic(err.Error())
		}
		glog.Errorln("Open File faild: ", dir, "clusterclient.conf")
		return
	}

	lines := bufio.NewReader(confFile)
	for {
		line, err := util.Readln(lines)
		if err != nil && err != io.EOF {
			panic(err.Error())
			return
		}
		if err == io.EOF {
			break
		}
		str := string(line)
		if strings.HasPrefix(str, "#") {
			continue
		}
		if str == "" {
			continue
		}
		parts := strings.Split(str, "=")
		if len(parts) < 2 {
			continue
		}
		switch parts[0] {
		case "Cluster":
			parstr := parts[1]

			addrs := strings.Split(parstr, ",")
			lens := len(parts)
			if lens < 1 {
				continue
			}
			clusterclient.pushAddres(addrs)
		case "Password":
			clusterclient.opt.Password = parts[1]
		case "MaxRetries":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				clusterclient.opt.MaxRetries = val
			}
		case "DialTimeout":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				clusterclient.opt.DialTimeout = time.Duration(val) * time.Second
			}
		case "ReadTimeout":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				clusterclient.opt.ReadTimeout = time.Duration(val) * time.Second
			}
		case "WriteTimeout":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				clusterclient.opt.WriteTimeout = time.Duration(val) * time.Second
			}
		case "PoolSize":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				clusterclient.opt.PoolSize = val
			}
		case "PoolTimeout":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				clusterclient.opt.PoolTimeout = time.Duration(val) * time.Second
			}
		case "IdleTimeout":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				clusterclient.opt.IdleTimeout = time.Duration(val) * time.Millisecond
			}
		case "IdleCheckFrequency":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				clusterclient.opt.IdleCheckFrequency = time.Duration(val) * time.Millisecond
			}
		default:
			continue
		}

	}
	clusterclient.initClusterClient()
}

func (c *ClusterClient) initClusterClient() {
	glog.V(4).Infoln("initClusterClient:", c.opt.Addrs)
	c.client = redis.NewClusterClient(c.opt)
}

func (c *ClusterClient) pushAddres(addrs []string) {
	c.opt.Addrs = append(c.opt.Addrs, addrs...)

}

func (c *ClusterClient) Set(key string, val []byte) error {
	err := c.client.Set(key, val, 0).Err()

	glog.V(4).Infoln("Cluster client Set:", key, string(val), err)
	return err
}

func (c *ClusterClient) Get(key string) ([]byte, error) {
	val, err := c.client.Get(key).Result()

	glog.V(4).Infoln("Cluster client Get:", key, string(val), err)
	return []byte(val), err
}

func (c *ClusterClient) Getset(key string, value []byte) ([]byte, error) {
	val, err := c.client.GetSet(key, value).Result()

	glog.V(4).Infoln("Cluster client Getset:", key, string(value), val, err)
	return []byte(val), err
}
func (c *ClusterClient) Incrby(key string, value int64) (int64, error) {
	val, err := c.client.IncrBy(key, value).Result()

	glog.V(4).Infoln("Cluster client Incrby:", key, value, val, err)
	return val, err
}
func (c *ClusterClient) Decrby(key string, value int64) (int64, error) {
	val, err := c.client.DecrBy(key, value).Result()

	glog.V(4).Infoln("Cluster client Decrby:", key, value, val, err)
	return val, err
}
func (c *ClusterClient) Del(key string) error {
	_, err := c.client.Del(key).Result()

	glog.V(4).Infoln("Cluster client Del:", key, err)
	return err
}
func (c *ClusterClient) SetNX(key string, value []byte) (bool, error) {
	val, err := c.client.SetNX(key, value, 0).Result()

	glog.V(4).Infoln("Cluster client SetNX:", key, string(value), val, err)
	return val, err
}
func (c *ClusterClient) Exists(key string) (bool, error) {
	var OK bool
	val, err := c.client.Exists(key).Result()
	if val != 0 {
		OK = true
	} else {
		OK = false
	}
	glog.V(4).Infoln("Cluster client Exists:", key, OK, err)
	return OK, err
}
func (c *ClusterClient) Keys(pattern string) ([]string, error) {
	val, err := c.client.Keys(pattern).Result()

	glog.V(4).Infoln("Cluster client Keys:", pattern, err)
	return val, err
}
func (c *ClusterClient) Expire(key string, t int64) (bool, error) {
	val, err := c.client.Expire(key, time.Duration(t)*time.Second).Result()

	glog.V(4).Infoln("Cluster client Expire:", key, t, val, err)
	return val, err
}
func (c *ClusterClient) ZAdd(key string, members ...interface{}) (int64, error) {
	return 0, nil
}
func (c *ClusterClient) ZAddNX(key string, members ...interface{}) (int64, error) {
	return 0, nil
}
func (c *ClusterClient) ZAddXX(key string, members ...interface{}) (int64, error) {
	return 0, nil
}
func (c *ClusterClient) ZCount(key, min, max string) (int64, error) {
	return 0, nil
}
func (c *ClusterClient) ZRangeByLex(key string, Min, Max string, Offset, Count int64) ([]string, error) {
	return nil, nil
}
func (c *ClusterClient) ZRem(key string, members ...interface{}) (int64, error) {
	return 0, nil
}
func (c *ClusterClient) HGetAll(key string) (map[string]string, error) {
	return nil, nil
}
func (c *ClusterClient) HGet(key string, field string) (string, error) {
	return "", nil
}
func (c *ClusterClient) HSet(key string, field string, val string) (bool, error) {
	return false, nil
}
func (c *ClusterClient) HDel(key string, field string) (int64, error) {
	return 0, nil
}
func (c *ClusterClient) HINCRBY(key string, field string, num int64) (int64, error) {
	return 0, nil
}
func (c *ClusterClient) Scan(cursor uint64, match string, percount int64, maxcount int) ([]string, error) {
	return nil, nil
}
func (c *ClusterClient) ScanPerRedis(cursor uint64, match string, percount int64, maxcount int, redisidx int) ([]string, uint64, error) {
	return nil, 0, nil
}
func (c *ClusterClient) GetRedisCount() int {
	return 0
}