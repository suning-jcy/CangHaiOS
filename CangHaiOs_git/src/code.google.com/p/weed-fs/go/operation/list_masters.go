package operation

import (
	"encoding/json"
	"errors"
	"math/rand"

	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
)

type ClusterStatusResult struct {
	IsLeader bool     `json:"IsLeader,omitempty"`
	Leader   string   `json:"Leader,omitempty"`
	Peers    []string `json:"Peers,omitempty"`
}

func ListMasters(server string) ([]string, error) {
	jsonBlob, err := util.GetQuickTimeout("http://" + server + "/cluster/status")
	glog.V(5).Info("list masters result :", server, string(jsonBlob))
	if err != nil {
		return nil, err
	}
	var ret ClusterStatusResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	/*
		masters := ret.Peers
		if ret.IsLeader {
			masters = append(masters, ret.Leader)
		}
	*/
	masters := ret.Peers
	if ret.Leader != "" {
		masters = append(masters, ret.Leader)
	}
	return masters, nil
}

type MasterNodes struct {
	nodes    []string
	lastNode int
}

func NewMasterNodes(bootstrapNode string) (mn *MasterNodes) {
	mn = &MasterNodes{nodes: []string{bootstrapNode}, lastNode: -1}
	return
}
func (mn *MasterNodes) Reset() {
	glog.V(2).Info("Reset master node :", len(mn.nodes), mn.lastNode)
	mn.lastNode = -1
}

func (mn *MasterNodes) FindMaster() (string, error) {
	if len(mn.nodes) == 0 {
		return "", errors.New("No master node found!")
	}
	if mn.lastNode < 0 || len(mn.nodes) < 2 {
		for _, m := range mn.nodes {
			if masters, e := ListMasters(m); e == nil {
				if len(masters) == 0 {
					continue
				}
				mn.updateMasters(masters)
				mn.lastNode = rand.Intn(len(mn.nodes))
				glog.V(2).Info("current master node have :", mn.nodes)
				glog.V(2).Info("current master node is :", mn.nodes[mn.lastNode])
				break
			}
		}
	}
	if mn.lastNode < 0 {
		return "", errors.New("No master node avalable!")
	}
	return mn.nodes[mn.lastNode], nil
}
func (mn *MasterNodes) updateMasters(masters []string) {
	for _, m := range masters {
		if m == "" {
			continue
		}
		isExist := false
		for _, node := range mn.nodes {
			if m == node {
				isExist = true
				break
			}
		}
		if !isExist {
			mn.nodes = append(mn.nodes, m)
		}
	}
}
