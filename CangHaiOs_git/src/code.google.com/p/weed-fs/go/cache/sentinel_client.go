package cache

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/util"
	"github.com/go-redis/redis"
	"code.google.com/p/weed-fs/go/stats"
)

// Redis nil reply, .e.g. when key does not exist.
const Nil = redis.Nil

const (
	Inconsistent = ClientError("client error: master is inconsistent")
	WrongNumber  = ClientError("ERR wrong number of arguments for command")
	BadParms     = ClientError("Bad parms")
)

type ClientError string

func (e ClientError) Error() string { return string(e) }

type SentinelClient struct {
	clients                map[int]*redis.Client
	sentinel_groups        map[string][]string
	sentinel_group_count   int
	Options                *ClientOptions
	master_status          map[string]bool //每个分片redis是否出现双主的状态（true：正常/false:异常）
	status_lock            sync.RWMutex
	masters                []string            //排序之后的redis主服务器名字，用于映射
	master_addrs           map[string][]string //每组分片中，sentinel组记录的redis主服务器地址（key：master name/value：redis主服务器）
	addrs_lock             sync.RWMutex
	master_sentinel_client map[string][]*redis.Client //每组中sentinel的client
}

type ClientOptions struct {
	Password   string
	DB         int
	MaxRetries int

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	PoolSize           int
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
}

type MasterInfo struct {
	name string
	addr string
}

var (
	sentinelclient *SentinelClient
	once           sync.Once
	dir            string
	style          string
)

func SetRedisClientConfDir(d string) {
	dir = d
}

func SetRedisClientStyle(s string) {
	style = s
}

func GetRedisClientStyle() string {
	return style
}

func (c *SentinelClient) GetRedisCount() int {
	return c.sentinel_group_count
}

func newSentinelClient() *SentinelClient {
	once.Do(func() {
		sentinelclient = &SentinelClient{
			clients:                make(map[int]*redis.Client, 1),
			sentinel_groups:        make(map[string][]string, 1),
			master_status:          make(map[string]bool, 1),
			master_addrs:           make(map[string][]string, 1),
			master_sentinel_client: make(map[string][]*redis.Client, 1),
			sentinel_group_count:   0,
			Options: &ClientOptions{
				DB:                 0,
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
		loadAndInitSentinelClient(sentinelclient)
		go sentinelclient.loopDetectSentinel()
	})
	return sentinelclient
}

func loadAndInitSentinelClient(sentinelclient *SentinelClient) {
	confFile, err := os.OpenFile(filepath.Join(dir, "sentinelclient.conf"), os.O_RDONLY, 0644)
	if err != nil {
		if !os.IsNotExist(err) {
			panic(err.Error())
		}
		glog.Errorln("Open File faild: ", dir, "sentinelclient.conf")
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
		case "Sentinel":
			parstr := parts[1]

			parts := strings.Split(parstr, ",")
			lens := len(parts)
			if lens < 2 {
				continue
			}
			master := parts[0]
			addrs := parts[1:lens]
			sentinelclient.PushSentinelGroup(master, addrs)
		case "Password":
			sentinelclient.Options.Password = parts[1]
		case "DB":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				sentinelclient.Options.DB = val
			}
		case "MaxRetries":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				sentinelclient.Options.MaxRetries = val
			}
		case "DialTimeout":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				sentinelclient.Options.DialTimeout = time.Duration(val) * time.Second
			}
		case "ReadTimeout":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				sentinelclient.Options.ReadTimeout = time.Duration(val) * time.Second
			}
		case "WriteTimeout":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				sentinelclient.Options.WriteTimeout = time.Duration(val) * time.Second
			}
		case "PoolSize":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				sentinelclient.Options.PoolSize = val
			}
		case "PoolTimeout":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				sentinelclient.Options.PoolTimeout = time.Duration(val) * time.Second
			}
		case "IdleTimeout":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				sentinelclient.Options.IdleTimeout = time.Duration(val) * time.Millisecond
			}
		case "IdleCheckFrequency":
			if val, err := strconv.Atoi(parts[1]); err == nil {
				sentinelclient.Options.IdleCheckFrequency = time.Duration(val) * time.Millisecond
			}
		default:
			continue
		}

	}
	sentinelclient.InitSentinelCollection()
}

func (c *SentinelClient) PushSentinelGroup(master string, addrs []string) {
	c.sentinel_groups[master] = addrs
	c.sentinel_group_count++
}

func (c *SentinelClient) InitSentinelCollection() {

	//sort sentinel groups by master name
	i := 0
	c.masters = make([]string, len(c.sentinel_groups))
	for name, _ := range c.sentinel_groups {
		c.masters[i] = name
		i++
	}
	sort.Strings(c.masters)

	for j, name := range c.masters {
		c.clients[j] = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:         name,
			SentinelAddrs:      c.sentinel_groups[name],
			Password:           c.Options.Password,
			DB:                 c.Options.DB,
			MaxRetries:         c.Options.MaxRetries,
			DialTimeout:        c.Options.DialTimeout,
			ReadTimeout:        c.Options.ReadTimeout,
			WriteTimeout:       c.Options.WriteTimeout,
			PoolSize:           c.Options.PoolSize,
			PoolTimeout:        c.Options.PoolTimeout,
			IdleTimeout:        c.Options.IdleTimeout,
			IdleCheckFrequency: c.Options.IdleCheckFrequency})
		c.master_status[name] = true

		c.master_sentinel_client[name] = make([]*redis.Client, len(c.sentinel_groups[name]))
		for k, _ := range c.master_sentinel_client[name] {
			c.master_sentinel_client[name][k] = nil
		}

		for name, addrs := range c.sentinel_groups {
			c.master_addrs[name] = make([]string, len(addrs))
		}
	}
}

func (c *SentinelClient) detectMasterStatus() {
	for name, addrs := range c.sentinel_groups {
		c.addrs_lock.Lock()
		c.master_addrs[name] = c.master_addrs[name][:0]
		c.addrs_lock.Unlock()
		for j, addr := range addrs {
			if c.master_sentinel_client[name][j] == nil {
				c.master_sentinel_client[name][j] = redis.NewClient(&redis.Options{
					Addr:               addr,
					Password:           "", // no password set
					MaxRetries:         c.Options.MaxRetries,
					DialTimeout:        c.Options.DialTimeout,
					ReadTimeout:        c.Options.ReadTimeout,
					WriteTimeout:       c.Options.WriteTimeout,
					PoolSize:           c.Options.PoolSize,
					PoolTimeout:        c.Options.PoolTimeout,
					IdleTimeout:        c.Options.IdleTimeout,
					IdleCheckFrequency: c.Options.IdleCheckFrequency,
				})
			}

			val, err := c.master_sentinel_client[name][j].Info("Sentinel").Result()

			if err != nil {
				continue
			}
			/*
				解析如下信息
				# Sentinel
				sentinel_masters:8
				sentinel_tilt:0
				sentinel_running_scripts:0
				sentinel_scripts_queue_length:0
				master0:name=master5,status=ok,address=10.37.57.100:6386,slaves=1,sentinels=5
			*/
			lines := strings.Split(val, "\n")
			for _, line := range lines {
				contents := strings.Split(line, ",")
				if len(contents) < 5 {
					continue
				}

				master := strings.Split(contents[0], "=")
				if len(master) < 2 {
					continue
				}
				if name != master[1] {
					continue
				}

				address := strings.Split(contents[2], "=")
				if len(contents) < 2 {
					continue
				}
				c.addrs_lock.Lock()
				c.master_addrs[name] = append(c.master_addrs[name], address[1])
				c.addrs_lock.Unlock()
				break
			}
		}
		c.addrs_lock.RLock()
		addrscount := len(c.master_addrs[name])
		c.addrs_lock.RUnlock()
		//客户端可能与sentinel之间不通，或sentinel已掉线，无法判断
		if addrscount < 1 {
			c.status_lock.Lock()
			c.master_status[name] = true
			c.status_lock.Unlock()
			continue
		}
		c.addrs_lock.RLock()
		//检测每个sentinel记录的master地址是否一致
		addr_tmp := c.master_addrs[name][0]
		flag := true
		for _, _addr := range c.master_addrs[name] {
			if addr_tmp != _addr {
				flag = false
				break
			}
		}
		c.addrs_lock.RUnlock()

		if flag {
			c.status_lock.Lock()
			c.master_status[name] = true
			c.status_lock.Unlock()
		} else {
			c.status_lock.Lock()
			c.master_status[name] = false
			c.status_lock.Unlock()
		}
	}
}

func (c *SentinelClient) FailoverHandleClient(id int) *redis.Client {
	var client *redis.Client
	c.addrs_lock.RLock()
	addrs := c.master_addrs[c.masters[id]]
	c.addrs_lock.RUnlock()
	glog.V(0).Infoln("FailoverHandleClient :", c.masters[id], addrs)
	addr_vot := make(map[string]int, 2)
	for _, addr := range addrs {
		addr_vot[addr] = 0
	}
	for _, addr := range addrs {
		addr_vot[addr]++
	}
	for addr, _ := range addr_vot {
		if addr_vot[addr] > len(c.sentinel_groups[c.masters[id]])/2 {
			glog.V(0).Infoln("FailoverHandleClient Client:", addr_vot[addr], addr)
			client = redis.NewClient(&redis.Options{
				Addr:               addr,
				Password:           c.Options.Password,
				DB:                 c.Options.DB,
				MaxRetries:         c.Options.MaxRetries,
				DialTimeout:        c.Options.DialTimeout,
				ReadTimeout:        c.Options.ReadTimeout,
				WriteTimeout:       c.Options.WriteTimeout,
				PoolSize:           c.Options.PoolSize,
				PoolTimeout:        c.Options.PoolTimeout,
				IdleTimeout:        c.Options.IdleTimeout,
				IdleCheckFrequency: c.Options.IdleCheckFrequency,
			})
			return client
		}
	}
	glog.V(0).Infoln("FailoverHandleClient failed: no enough sentinel support")
	return nil
}

func (c *SentinelClient) loopDetectSentinel() {
	ticker := time.NewTicker(5 * time.Second)
	c.detectMasterStatus()
	for _ = range ticker.C {
		c.detectMasterStatus()
	}
}

func (c *SentinelClient) isMasterOK(id int) bool {
	c.status_lock.RLock()
	glog.V(4).Infoln("isMasterOK:", c.masters[id], c.master_status[c.masters[id]])
	res := c.master_status[c.masters[id]]
	c.status_lock.RUnlock()
	return res
}

func (c *SentinelClient) Set(key string, val []byte) error {
	var err error
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_WRITE)
	defer stats.RedisRequestClose(stats.STATS_WRITE, start)

	defer func() {
		if err == nil {
			stats.RedisTps("GET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("GET", 400, 0)
		} else {
			stats.RedisTps("GET", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client Set error:", sentinel_group_Id, key, string(val), Inconsistent)
			err = Inconsistent
			return err
		}
		err = client.Set(key, val, 0).Err()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		err = client.Set(key, val, 0).Err()
	}

	glog.V(4).Infoln("Sentinel client Set:", sentinel_group_Id, key, string(val), err)
	return err
}

func (c *SentinelClient) Get(key string) ([]byte, error) {
	var err error
	var val string
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_READ)
	defer stats.RedisRequestClose(stats.STATS_READ, start)

	defer func() {
		if err == nil {
			stats.RedisTps("GET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("GET", 400, 0)
		} else {
			stats.RedisTps("GET", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client Get error:", sentinel_group_Id, key, Inconsistent)
			err = Inconsistent
			return nil, err
		}
		val, err = client.Get(key).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		val, err = client.Get(key).Result()
	}

	glog.V(4).Infoln("Sentinel client Get:", sentinel_group_Id, key, string(val), err)
	return []byte(val), err
}

func (c *SentinelClient) Getset(key string, value []byte) ([]byte, error) {
	var err error
	var val string
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_WRITE)
	defer stats.RedisRequestClose(stats.STATS_WRITE, start)

	defer func() {
		if err == nil {
			stats.RedisTps("POST", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("POST", 400, 0)
		} else {
			stats.RedisTps("POST", 500, 0)
		}
	}()

	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client Getset error:", sentinel_group_Id, key, string(value), Inconsistent)
			err = Inconsistent
			return nil, err
		}
		val, err = client.GetSet(key, value).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		val, err = client.GetSet(key, value).Result()
	}

	glog.V(4).Infoln("Sentinel client Getset:", sentinel_group_Id, key, string(value), val, err)
	return []byte(val), err
}

func (c *SentinelClient) Incrby(key string, value int64) (int64, error) {
	var err error
	var val int64
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_WRITE)
	defer stats.RedisRequestClose(stats.STATS_WRITE, start)

	defer func() {
		if err == nil {
			stats.RedisTps("POST", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("POST", 400, 0)
		} else {
			stats.RedisTps("POST", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client Incrby error:", sentinel_group_Id, key, value, Inconsistent)
			err = Inconsistent
			return 0, err
		}
		val, err = client.IncrBy(key, value).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		val, err = client.IncrBy(key, value).Result()
	}

	glog.V(4).Infoln("Sentinel client Incrby:", sentinel_group_Id, key, value, val, err)
	return val, err
}
func (c *SentinelClient) Decrby(key string, value int64) (int64, error) {
	var err error
	var val int64
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_WRITE)
	defer stats.RedisRequestClose(stats.STATS_WRITE, start)

	defer func() {
		if err == nil {
			stats.RedisTps("POST", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("POST", 400, 0)
		} else {
			stats.RedisTps("POST", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client Decrby error:", sentinel_group_Id, key, value, Inconsistent)
			err = Inconsistent
			return 0, err
		}
		val, err = client.DecrBy(key, value).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		val, err = client.DecrBy(key, value).Result()
	}

	glog.V(4).Infoln("Sentinel client Decrby:", sentinel_group_Id, key, value, val, err)
	return val, err
}
func (c *SentinelClient) Del(key string) error {
	var err error
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_DELETE)
	defer stats.RedisRequestClose(stats.STATS_DELETE, start)

	defer func() {
		if err == nil {
			stats.RedisTps("POST", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("POST", 400, 0)
		} else {
			stats.RedisTps("POST", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client Del error:", sentinel_group_Id, key, Inconsistent)
			err = Inconsistent
			return err
		}
		_, err = client.Del(key).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		_, err = client.Del(key).Result()
	}
	glog.V(4).Infoln("Sentinel client Del:", sentinel_group_Id, key, err)
	return err
}
func (c *SentinelClient) SetNX(key string, value []byte) (bool, error) {
	var err error
	var val bool
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_WRITE)
	defer stats.RedisRequestClose(stats.STATS_WRITE, start)

	defer func() {
		if err == nil {
			stats.RedisTps("POST", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("POST", 400, 0)
		} else {
			stats.RedisTps("POST", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client SetNX error:", sentinel_group_Id, key, string(value), Inconsistent)
			err = Inconsistent
			return false, err
		}
		val, err = client.SetNX(key, value, 0).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		val, err = client.SetNX(key, value, 0).Result()
	}

	glog.V(4).Infoln("Sentinel client SetNX:", sentinel_group_Id, key, string(value), val, err)
	return val, err
}
func (c *SentinelClient) Exists(key string) (bool, error) {
	var OK bool
	var err error
	var val int64
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_READ)
	defer stats.RedisRequestClose(stats.STATS_READ, start)

	defer func() {
		if err == nil {
			stats.RedisTps("GET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("GET", 400, 0)
		} else {
			stats.RedisTps("GET", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client SetNX error:", sentinel_group_Id, key, string(val), Inconsistent)
			err = Inconsistent
			return false, err
		}
		val, err = client.Exists(key).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		val, err = client.Exists(key).Result()
	}

	if val != 0 {
		OK = true
	} else {
		OK = false
	}
	glog.V(4).Infoln("Sentinel client Exists:", sentinel_group_Id, key, OK, err)
	return OK, err
}
func (c *SentinelClient) Keys(pattern string) ([]string, error) {
	var err error
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_READ)
	defer stats.RedisRequestClose(stats.STATS_READ, start)

	defer func() {
		if err == nil {
			stats.RedisTps("GET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("GET", 400, 0)
		} else {
			stats.RedisTps("GET", 500, 0)
		}
	}()
	keys := make([]string, 0)

	for _, client := range c.clients {
		val, e := client.Keys(pattern).Result()
		if err != nil {
			continue
		}
		keys = append(keys, val...)
		err = e
	}
	glog.V(4).Infoln("Sentinel client Keys:", pattern, err)
	return keys, err
}
func (c *SentinelClient) Expire(key string, t int64) (bool, error) {
	var err error
	var val bool
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_WRITE)
	defer stats.RedisRequestClose(stats.STATS_WRITE, start)

	defer func() {
		if err == nil {
			stats.RedisTps("POST", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("POST", 400, 0)
		} else {
			stats.RedisTps("POST", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client SetNX error:", sentinel_group_Id, key, t, Inconsistent)
			err = Inconsistent
			return false, err
		}
		val, err = client.Expire(key, time.Duration(t)*time.Second).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		val, err = client.Expire(key, time.Duration(t)*time.Second).Result()
	}

	glog.V(4).Infoln("Sentinel client Expire:", sentinel_group_Id, key, t, val, err)
	return val, err
}

func getZsetAddParms(members ...interface{}) ([]redis.Z, error) {

	memberssize := len(members)
	if memberssize == 0 || memberssize%2 != 0 {
		return nil, WrongNumber
	}
	parms := []redis.Z{}
	for i := 0; i < memberssize; i++ {
		Score, ok := members[i].(float64)
		if !ok {
			return nil, BadParms
		}
		NewZ := redis.Z{
			Score:  Score,
			Member: members[i+1],
		}
		i++
		parms = append(parms, NewZ)
	}
	return parms, nil
}

/*
Min, Max 同时为空或者都不为空

*/
func getZsetRangeParm(Min, Max string, Offset, Count int64) (redis.ZRangeBy, error) {

	if Min+Max != "" && (Min == "" || Max == "") {
		return redis.ZRangeBy{}, BadParms
	}

	NewZR := redis.ZRangeBy{
		Min:    Min,
		Max:    Max,
		Offset: Offset,
		Count:  Count,
	}
	return NewZR, nil
}

func (c *SentinelClient) ZAdd(key string, members ...interface{}) (int64, error) {
	var err error
	var res int64
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_WRITE)
	defer stats.RedisRequestClose(stats.STATS_WRITE, start)
	defer func() {
		if err == nil {
			stats.RedisTps("POST", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("POST", 400, 0)
		} else {
			stats.RedisTps("POST", 500, 0)
		}
	}()
	parms, err := getZsetAddParms(members...)
	if err != nil {
		glog.V(0).Infoln("Sentinel client ZAdd error:", err)
		return int64(0), err
	}

	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client ZAdd error:", sentinel_group_Id, key, Inconsistent)
			err = Inconsistent
			return int64(0), err
		}
		res, err = client.ZAdd(key, parms...).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		res, err = client.ZAdd(key, parms...).Result()
	}
	glog.V(4).Infoln("Sentinel client ZAdd:", sentinel_group_Id, key, err)
	return res, err
}

func (c *SentinelClient) ZAddNX(key string, members ...interface{}) (int64, error) {
	var err error
	var res int64
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_WRITE)
	defer stats.RedisRequestClose(stats.STATS_WRITE, start)
	defer func() {
		if err == nil {
			stats.RedisTps("POST", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("POST", 400, 0)
		} else {
			stats.RedisTps("POST", 500, 0)
		}
	}()
	parms, err := getZsetAddParms(members...)
	if err != nil {
		glog.V(0).Infoln("Sentinel client ZAddNX error:", err)
		return int64(0), err
	}

	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client ZAddNX error:", sentinel_group_Id, key, Inconsistent)
			err = Inconsistent
			return int64(0), err
		}
		res, err = client.ZAddNX(key, parms...).Result()
		glog.V(4).Infoln(key, parms, res, err)
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		res, err = client.ZAddNX(key, parms...).Result()
	}
	glog.V(4).Infoln("Sentinel client ZAddNX:", sentinel_group_Id, key, err)
	return res, err
}

func (c *SentinelClient) ZAddXX(key string, members ...interface{}) (int64, error) {
	var err error
	var res int64
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_WRITE)
	defer stats.RedisRequestClose(stats.STATS_WRITE, start)
	defer func() {
		if err == nil {
			stats.RedisTps("POST", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("POST", 400, 0)
		} else {
			stats.RedisTps("POST", 500, 0)
		}
	}()
	parms, err := getZsetAddParms(members...)
	if err != nil {
		glog.V(0).Infoln("Sentinel client ZAddXX error:", err)
		return int64(0), err
	}
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client ZAddXX error:", sentinel_group_Id, key, Inconsistent)
			err = Inconsistent
			return int64(0), err
		}
		res, err = client.ZAddXX(key, parms...).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		res, err = client.ZAddXX(key, parms...).Result()
	}
	glog.V(4).Infoln("Sentinel client ZAddXX:", sentinel_group_Id, key, err)
	return res, err
}

func (c *SentinelClient) ZCount(key, min, max string) (int64, error) {
	var err error
	var res int64
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_READ)
	defer stats.RedisRequestClose(stats.STATS_READ, start)
	defer func() {
		if err == nil {
			stats.RedisTps("GET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("GET", 400, 0)
		} else {
			stats.RedisTps("GET", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client ZCount error:", sentinel_group_Id, key, Inconsistent)
			err = Inconsistent
			return int64(0), err
		}
		res, err = client.ZCount(key, min, max).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		res, err = client.ZCount(key, min, max).Result()
	}
	glog.V(4).Infoln("Sentinel client ZCount:", sentinel_group_Id, key, err)
	return res, err
}

func (c *SentinelClient) ZRangeByLex(key string, Min, Max string, Offset, Count int64) ([]string, error) {
	var err error
	var res []string
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_READ)
	defer stats.RedisRequestClose(stats.STATS_READ, start)
	defer func() {
		if err == nil {
			stats.RedisTps("GET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("GET", 400, 0)
		} else {
			stats.RedisTps("GET", 500, 0)
		}
	}()
	parm, err := getZsetRangeParm(Min, Max, Offset, Count)
	if err != nil {
		glog.V(0).Infoln("Sentinel client ZRangeByLex error:", err)
		return nil, err
	}
	glog.V(4).Infoln(key, parm)
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client ZRangeByLex error:", sentinel_group_Id, key, Inconsistent)
			err = Inconsistent
			return nil, err
		}
		res, err = client.ZRangeByLex(key, parm).Result()
		glog.V(4).Infoln(res, err)
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		glog.V(4).Infoln(key)
		glog.V(4).Infoln(parm.Min)
		glog.V(4).Infoln(parm.Max)
		res, err = client.ZRangeByLex(key, parm).Result()
		glog.V(4).Infoln(res, err)
	}
	glog.V(4).Infoln("Sentinel client ZRangeByLex:", sentinel_group_Id, key, err)
	return res, err
}

func (c *SentinelClient) ZRem(key string, members ...interface{}) (int64, error) {
	var err error
	var res int64
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_DELETE)
	defer stats.RedisRequestClose(stats.STATS_DELETE, start)
	defer func() {
		if err == nil {
			stats.RedisTps("GET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("GET", 400, 0)
		} else {
			stats.RedisTps("GET", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client ZRem error:", sentinel_group_Id, key, Inconsistent)
			err = Inconsistent
			return int64(0), err
		}
		res, err = client.ZRem(key, members...).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		res, err = client.ZRem(key, members...).Result()
	}
	glog.V(4).Infoln("Sentinel client ZRem:", sentinel_group_Id, key, err)
	return res, err
}

func (c *SentinelClient) HGetAll(key string) (map[string]string, error) {
	var err error
	res := make(map[string]string)

	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_READ)
	defer stats.RedisRequestClose(stats.STATS_READ, start)
	defer func() {
		if err == nil {
			stats.RedisTps("GET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("GET", 400, 0)
		} else {
			stats.RedisTps("GET", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client HGet error:", sentinel_group_Id, key, Inconsistent)
			err = Inconsistent
			return nil, err
		}
		res, err = client.HGetAll(key).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		res, err = client.HGetAll(key).Result()
	}
	glog.V(4).Infoln("Sentinel client HGetAll:", sentinel_group_Id, key, err)
	return res, err
}

func (c *SentinelClient) HGet(key string, field string) (string, error) {
	var err error
	var res string

	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_READ)
	defer stats.RedisRequestClose(stats.STATS_READ, start)
	defer func() {
		if err == nil {
			stats.RedisTps("GET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("GET", 400, 0)
		} else {
			stats.RedisTps("GET", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client HGet error:", sentinel_group_Id, key, field, Inconsistent)
			err = Inconsistent
			return "", err
		}
		res, err = client.HGet(key, field).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		res, err = client.HGet(key, field).Result()
	}
	glog.V(4).Infoln("Sentinel client HGet:", sentinel_group_Id, key, field, err)
	return res, err
}

func (c *SentinelClient) HSet(key string, field string, val string) (bool, error) {
	var err error
	var res bool

	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_WRITE)
	defer stats.RedisRequestClose(stats.STATS_WRITE, start)
	defer func() {
		if err == nil {
			stats.RedisTps("SET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("SET", 400, 0)
		} else {
			stats.RedisTps("SET", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client HSet error:", sentinel_group_Id, key, field, val, Inconsistent)
			err = Inconsistent
			return false, err
		}
		res, err = client.HSet(key, field, val).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		res, err = client.HSet(key, field, val).Result()
	}
	glog.V(4).Infoln("Sentinel client HSet:", sentinel_group_Id, key, field, val, err)
	return res, err
}

func (c *SentinelClient) HDel(key string, field string) (int64, error) {
	var err error
	var res int64

	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_WRITE)
	defer stats.RedisRequestClose(stats.STATS_WRITE, start)
	defer func() {
		if err == nil {
			stats.RedisTps("SET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("SET", 400, 0)
		} else {
			stats.RedisTps("SET", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client HSet error:", sentinel_group_Id, key, field, Inconsistent)
			err = Inconsistent
			return 0, err
		}
		res, err = client.HDel(key, field).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		res, err = client.HDel(key, field).Result()
	}
	glog.V(4).Infoln("Sentinel client HSet:", sentinel_group_Id, key, field, err)
	return res, err
}

func (c *SentinelClient) HINCRBY(key string, field string, num int64) (int64, error) {
	var err error
	var res int64

	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_WRITE)
	defer stats.RedisRequestClose(stats.STATS_WRITE, start)
	defer func() {
		if err == nil {
			stats.RedisTps("SET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("SET", 400, 0)
		} else {
			stats.RedisTps("SET", 500, 0)
		}
	}()
	sentinel_group_Id := operation.HashPos(key, c.sentinel_group_count)
	if !c.isMasterOK(sentinel_group_Id) {
		client := c.FailoverHandleClient(sentinel_group_Id)
		if client == nil {
			glog.V(0).Infoln("Sentinel client HSet error:", sentinel_group_Id, key, field, num, Inconsistent)
			err = Inconsistent
			return 0, err
		}
		res, err = client.HIncrBy(key, field, num).Result()
		client.Close()
	} else {
		client := c.clients[sentinel_group_Id]
		res, err = client.HIncrBy(key, field, num).Result()
	}
	glog.V(4).Infoln("Sentinel client HIncrBy:", sentinel_group_Id, key, field, num, res, err)
	return res, err
}
func (c *SentinelClient) Scan(cursor uint64, match string, percount int64, maxcount int) ([]string, error) {
	var err error
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_READ)
	defer stats.RedisRequestClose(stats.STATS_READ, start)

	defer func() {
		if err == nil {
			stats.RedisTps("GET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("GET", 400, 0)
		} else {
			stats.RedisTps("GET", 500, 0)
		}
	}()
	keys := make([]string, 0)

	for _, client := range c.clients {
		cursorfirst := cursor
		for {
			if maxcount != 0 && len(keys) >= maxcount {
				break
			}
			val, cursornext, e := client.Scan(cursorfirst,match,percount).Result()
			if e != nil {
				glog.V(0).Infoln("Sentinel client Scan error:", cursorfirst, match, percount, err)
				err = e
				break
			}
			keys = append(keys, val...)
			err = e
			if cursornext == 0 {
				break
			} else {
				cursorfirst = cursornext
			}
		}
		glog.V(4).Infoln("Sentinel client Scan keys:", keys)
	}
	glog.V(4).Infoln("Sentinel client Scan:", cursor, match, percount, maxcount, keys, err)
	return keys, err
}

func (c *SentinelClient) ScanPerRedis(cursor uint64, match string, percount int64, maxcount int, redisidx int) ([]string, uint64, error) {
	var err error
	start := time.Now().UnixNano()
	stats.RedisRequestOpen(stats.STATS_READ)
	defer stats.RedisRequestClose(stats.STATS_READ, start)

	defer func() {
		if err == nil {
			stats.RedisTps("GET", 200, 0)
		} else if err == redis.Nil || err.Error() == "redis: nil" {
			stats.RedisTps("GET", 400, 0)
		} else {
			stats.RedisTps("GET", 500, 0)
		}
	}()

	glog.V(4).Infoln("len(c.clients):", len(c.clients))
	if redisidx >= len(c.clients) {
		return nil, 0, errors.New("redisidx exceed")
	}
	keys := make([]string, 0)
	val :=  make([]string, 0)
	cursornext := uint64(0)

	for idx, client := range c.clients {
		if idx == redisidx {
			cursorfirst := cursor
			for {
				if maxcount != 0 && len(keys) >= maxcount {
					break
				}
				val, cursornext, err = client.Scan(cursorfirst, match, percount).Result()
				if err != nil {
					glog.V(0).Infoln("Sentinel client Scan error:", cursorfirst, match, percount, err)
					break
				}
				keys = append(keys, val...)
				if cursornext == 0 {
					break
				} else {
					cursorfirst = cursornext
				}
			}
			//glog.V(5).Infoln("Sentinel client Scan keys:", keys)
		}
	}
	//glog.V(5).Infoln("Sentinel client Scan:", cursor, match, percount, maxcount, cursornext, keys, err)
	return keys, cursornext, err
}
