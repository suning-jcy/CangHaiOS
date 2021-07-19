package util

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"

	"code.google.com/p/weed-fs/go/glog"
)

const DEFAULT_REPLICAS = 500

type HashRing []uint32

func (c HashRing) Len() int {
	return len(c)
}

func (c HashRing) Less(i, j int) bool {
	return c[i] < c[j]
}

func (c HashRing) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

type Node struct {
	Ip     string
	Value  interface{}
	Weight int
}

type Consistent struct {
	Nodes     map[uint32]*Node
	Resources map[string]*Node
	numReps   int
	ring      HashRing
	sync.RWMutex
}



func NewConsistent() *Consistent {
	return &Consistent{
		Nodes:     make(map[uint32]*Node),
		Resources: make(map[string]*Node),
		ring:      HashRing{},
		numReps:   DEFAULT_REPLICAS,
	}
}

func (c *Consistent) Add(ip string, weight int, value interface{}) bool {
	node := &Node{Ip: ip, Value: value, Weight: weight}
	c.Lock()
	defer c.Unlock()

	if _, ok := c.Resources[ip]; ok {
		return false
	}

	count := c.numReps * node.Weight
	for i := 0; i < count; i++ {
		str := c.joinStr(i, node)
		c.Nodes[c.hashStr(str)] = node
	}
	c.Resources[ip] = node
	glog.V(4).Infoln("Consistent add node:", ip)
	glog.V(4).Infoln("Consistent now:", len(c.Nodes), len(c.ring))
	c.sortHashRing()
	return true
}

func (c *Consistent) Remove(ip string) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.Resources[ip]; !ok {
		return
	}
	node := c.Resources[ip]
	delete(c.Resources, ip)

	count := c.numReps * node.Weight
	for i := 0; i < count; i++ {
		str := c.joinStr(i, node)
		if _, ok := c.Nodes[c.hashStr(str)]; ok {
			delete(c.Nodes, c.hashStr(str))
		}

	}
	c.sortHashRing()
}


func (c *Consistent) Has(ip string)bool {
	c.RLock()
	defer c.RUnlock()
	_, ok := c.Resources[ip]
	return ok
}

//重排
//重要！！！！add/del 之后一定要变更
func (c *Consistent) sortHashRing() {

	c.ring = HashRing{}
	for k := range c.Nodes {
		c.ring = append(c.ring, k)
	}
	sort.Sort(c.ring)
}

func (c *Consistent) hashStr(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) Get(key string) string {
	c.RLock()
	defer c.RUnlock()

	hash := c.hashStr(key)
	i := c.search(hash)
	if len(c.Nodes) == 0 || len(c.ring) == 0 {
		return ""
	}

	return c.Nodes[c.ring[i]].Ip
}

func (c *Consistent) search(hash uint32) int {

	i := sort.Search(len(c.ring), func(i int) bool { return c.ring[i] >= hash })
	if i < len(c.ring) {
		if i == len(c.ring)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(c.ring) - 1
	}
}

func (c *Consistent) joinStr(i int, node *Node) string {
	return node.Ip +
		"-" + fmt.Sprint(i)
}
