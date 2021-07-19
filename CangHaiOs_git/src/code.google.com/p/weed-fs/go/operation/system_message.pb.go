// Code generated by protoc-gen-go.
// source: system_message.proto
// DO NOT EDIT!

/*
Package operation is a generated protocol buffer package.

It is generated from these files:
	system_message.proto

It has these top-level messages:
	VolInfoMessage
	DiskVolInfoMessage
	JoinMessage
	PartitionInformationMessage
	PartitionJoinMessage
	FolderInfoMessage
	DiskFolderInfoMessage
	FolderJoinMessage
*/
package operation

import proto "code.google.com/p/goprotobuf/proto"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

type VolInfoMessage struct {
	Id               *uint32 `protobuf:"varint,1,req,name=id" json:"id,omitempty"`
	Sn               *string `protobuf:"bytes,2,req,name=sn" json:"sn,omitempty"`
	Size             *uint64 `protobuf:"varint,3,req,name=size" json:"size,omitempty"`
	MaxFileKey       *uint64 `protobuf:"varint,4,req,name=max_file_key" json:"max_file_key,omitempty"`
	Collection       *string `protobuf:"bytes,5,opt,name=collection" json:"collection,omitempty"`
	FileCount        *uint64 `protobuf:"varint,6,req,name=file_count" json:"file_count,omitempty"`
	DeleteCount      *uint64 `protobuf:"varint,7,req,name=delete_count" json:"delete_count,omitempty"`
	DeletedByteCount *uint64 `protobuf:"varint,8,req,name=deleted_byte_count" json:"deleted_byte_count,omitempty"`
	ReadOnly         *bool   `protobuf:"varint,9,opt,name=read_only" json:"read_only,omitempty"`
	CanWrite         *bool   `protobuf:"varint,10,opt,name=can_write" json:"can_write,omitempty"`
	ReplicaPlacement *uint32 `protobuf:"varint,11,req,name=replica_placement" json:"replica_placement,omitempty"`
	Version          *uint32 `protobuf:"varint,12,opt,name=version,def=2" json:"version,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *VolInfoMessage) Reset()         { *m = VolInfoMessage{} }
func (m *VolInfoMessage) String() string { return proto.CompactTextString(m) }
func (*VolInfoMessage) ProtoMessage()    {}

const Default_VolInfoMessage_Version uint32 = 2

func (m *VolInfoMessage) GetId() uint32 {
	if m != nil && m.Id != nil {
		return *m.Id
	}
	return 0
}

func (m *VolInfoMessage) GetSn() string {
	if m != nil && m.Sn != nil {
		return *m.Sn
	}
	return ""
}

func (m *VolInfoMessage) GetSize() uint64 {
	if m != nil && m.Size != nil {
		return *m.Size
	}
	return 0
}

func (m *VolInfoMessage) GetMaxFileKey() uint64 {
	if m != nil && m.MaxFileKey != nil {
		return *m.MaxFileKey
	}
	return 0
}

func (m *VolInfoMessage) GetCollection() string {
	if m != nil && m.Collection != nil {
		return *m.Collection
	}
	return ""
}

func (m *VolInfoMessage) GetFileCount() uint64 {
	if m != nil && m.FileCount != nil {
		return *m.FileCount
	}
	return 0
}

func (m *VolInfoMessage) GetDeleteCount() uint64 {
	if m != nil && m.DeleteCount != nil {
		return *m.DeleteCount
	}
	return 0
}

func (m *VolInfoMessage) GetDeletedByteCount() uint64 {
	if m != nil && m.DeletedByteCount != nil {
		return *m.DeletedByteCount
	}
	return 0
}

func (m *VolInfoMessage) GetReadOnly() bool {
	if m != nil && m.ReadOnly != nil {
		return *m.ReadOnly
	}
	return false
}

func (m *VolInfoMessage) GetCanWrite() bool {
	if m != nil && m.CanWrite != nil {
		return *m.CanWrite
	}
	return false
}

func (m *VolInfoMessage) GetReplicaPlacement() uint32 {
	if m != nil && m.ReplicaPlacement != nil {
		return *m.ReplicaPlacement
	}
	return 0
}

func (m *VolInfoMessage) GetVersion() uint32 {
	if m != nil && m.Version != nil {
		return *m.Version
	}
	return Default_VolInfoMessage_Version
}

type DiskVolInfoMessage struct {
	Path             *string           `protobuf:"bytes,1,req,name=path" json:"path,omitempty"`
	Volumes          []*VolInfoMessage `protobuf:"bytes,2,rep,name=volumes" json:"volumes,omitempty"`
	XXX_unrecognized []byte            `json:"-"`
}

func (m *DiskVolInfoMessage) Reset()         { *m = DiskVolInfoMessage{} }
func (m *DiskVolInfoMessage) String() string { return proto.CompactTextString(m) }
func (*DiskVolInfoMessage) ProtoMessage()    {}

func (m *DiskVolInfoMessage) GetPath() string {
	if m != nil && m.Path != nil {
		return *m.Path
	}
	return ""
}

func (m *DiskVolInfoMessage) GetVolumes() []*VolInfoMessage {
	if m != nil {
		return m.Volumes
	}
	return nil
}

type JoinMessage struct {
	IsInit           *bool                 `protobuf:"varint,1,opt,name=is_init" json:"is_init,omitempty"`
	Ip               *string               `protobuf:"bytes,2,req,name=ip" json:"ip,omitempty"`
	Port             *uint32               `protobuf:"varint,3,req,name=port" json:"port,omitempty"`
	PublicUrl        *string               `protobuf:"bytes,4,opt,name=public_url" json:"public_url,omitempty"`
	MaxVolumeCount   *uint32               `protobuf:"varint,5,req,name=max_volume_count" json:"max_volume_count,omitempty"`
	MaxFileKey       *uint64               `protobuf:"varint,6,req,name=max_file_key" json:"max_file_key,omitempty"`
	DataCenter       *string               `protobuf:"bytes,7,opt,name=data_center" json:"data_center,omitempty"`
	Rack             *string               `protobuf:"bytes,8,opt,name=rack" json:"rack,omitempty"`
	DiskVolumes      []*DiskVolInfoMessage `protobuf:"bytes,9,rep,name=diskVolumes" json:"diskVolumes,omitempty"`
	Collection       *string               `protobuf:"bytes,10,opt,name=collection" json:"collection,omitempty"`
	CapacityAll      *uint64               `protobuf:"varint,11,opt,name=capacity_all" json:"capacity_all,omitempty"`
	CapacityUsed     *uint64               `protobuf:"varint,12,opt,name=capacity_used" json:"capacity_used,omitempty"`
	CapacityAvail    *uint64               `protobuf:"varint,13,opt,name=capacity_avail" json:"capacity_avail,omitempty"`
	XXX_unrecognized []byte                `json:"-"`
}

func (m *JoinMessage) Reset()         { *m = JoinMessage{} }
func (m *JoinMessage) String() string { return proto.CompactTextString(m) }
func (*JoinMessage) ProtoMessage()    {}

func (m *JoinMessage) GetIsInit() bool {
	if m != nil && m.IsInit != nil {
		return *m.IsInit
	}
	return false
}

func (m *JoinMessage) GetIp() string {
	if m != nil && m.Ip != nil {
		return *m.Ip
	}
	return ""
}

func (m *JoinMessage) GetPort() uint32 {
	if m != nil && m.Port != nil {
		return *m.Port
	}
	return 0
}

func (m *JoinMessage) GetPublicUrl() string {
	if m != nil && m.PublicUrl != nil {
		return *m.PublicUrl
	}
	return ""
}

func (m *JoinMessage) GetMaxVolumeCount() uint32 {
	if m != nil && m.MaxVolumeCount != nil {
		return *m.MaxVolumeCount
	}
	return 0
}

func (m *JoinMessage) GetMaxFileKey() uint64 {
	if m != nil && m.MaxFileKey != nil {
		return *m.MaxFileKey
	}
	return 0
}

func (m *JoinMessage) GetDataCenter() string {
	if m != nil && m.DataCenter != nil {
		return *m.DataCenter
	}
	return ""
}

func (m *JoinMessage) GetRack() string {
	if m != nil && m.Rack != nil {
		return *m.Rack
	}
	return ""
}

func (m *JoinMessage) GetDiskVolumes() []*DiskVolInfoMessage {
	if m != nil {
		return m.DiskVolumes
	}
	return nil
}

func (m *JoinMessage) GetCollection() string {
	if m != nil && m.Collection != nil {
		return *m.Collection
	}
	return ""
}

func (m *JoinMessage) GetCapacityAll() uint64 {
	if m != nil && m.CapacityAll != nil {
		return *m.CapacityAll
	}
	return 0
}

func (m *JoinMessage) GetCapacityUsed() uint64 {
	if m != nil && m.CapacityUsed != nil {
		return *m.CapacityUsed
	}
	return 0
}

func (m *JoinMessage) GetCapacityAvail() uint64 {
	if m != nil && m.CapacityAvail != nil {
		return *m.CapacityAvail
	}
	return 0
}

type PartitionInformationMessage struct {
	Id               *uint32 `protobuf:"varint,1,req,name=id" json:"id,omitempty"`
	Collection       *string `protobuf:"bytes,2,opt,name=collection" json:"collection,omitempty"`
	ReplicaPlacement *uint32 `protobuf:"varint,3,req,name=replica_placement" json:"replica_placement,omitempty"`
	AccessUrl        *string `protobuf:"bytes,4,opt,name=access_url" json:"access_url,omitempty"`
	Version          *uint32 `protobuf:"varint,5,opt,name=version,def=2" json:"version,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *PartitionInformationMessage) Reset()         { *m = PartitionInformationMessage{} }
func (m *PartitionInformationMessage) String() string { return proto.CompactTextString(m) }
func (*PartitionInformationMessage) ProtoMessage()    {}

const Default_PartitionInformationMessage_Version uint32 = 2

func (m *PartitionInformationMessage) GetId() uint32 {
	if m != nil && m.Id != nil {
		return *m.Id
	}
	return 0
}

func (m *PartitionInformationMessage) GetCollection() string {
	if m != nil && m.Collection != nil {
		return *m.Collection
	}
	return ""
}

func (m *PartitionInformationMessage) GetReplicaPlacement() uint32 {
	if m != nil && m.ReplicaPlacement != nil {
		return *m.ReplicaPlacement
	}
	return 0
}

func (m *PartitionInformationMessage) GetAccessUrl() string {
	if m != nil && m.AccessUrl != nil {
		return *m.AccessUrl
	}
	return ""
}

func (m *PartitionInformationMessage) GetVersion() uint32 {
	if m != nil && m.Version != nil {
		return *m.Version
	}
	return Default_PartitionInformationMessage_Version
}

type PartitionJoinMessage struct {
	IsInit           *bool                          `protobuf:"varint,1,opt,name=is_init" json:"is_init,omitempty"`
	Ip               *string                        `protobuf:"bytes,2,req,name=ip" json:"ip,omitempty"`
	Port             *uint32                        `protobuf:"varint,3,req,name=port" json:"port,omitempty"`
	PublicUrl        *string                        `protobuf:"bytes,4,opt,name=public_url" json:"public_url,omitempty"`
	DataCenter       *string                        `protobuf:"bytes,5,opt,name=data_center" json:"data_center,omitempty"`
	Rack             *string                        `protobuf:"bytes,6,opt,name=rack" json:"rack,omitempty"`
	Partitions       []*PartitionInformationMessage `protobuf:"bytes,7,rep,name=partitions" json:"partitions,omitempty"`
	Collection       *string                        `protobuf:"bytes,8,opt,name=collection" json:"collection,omitempty"`
	CapacityAll      *uint64                        `protobuf:"varint,9,opt,name=capacity_all" json:"capacity_all,omitempty"`
	CapacityUsed     *uint64                        `protobuf:"varint,10,opt,name=capacity_used" json:"capacity_used,omitempty"`
	CapacityAvail    *uint64                        `protobuf:"varint,11,opt,name=capacity_avail" json:"capacity_avail,omitempty"`
	XXX_unrecognized []byte                         `json:"-"`
}

func (m *PartitionJoinMessage) Reset()         { *m = PartitionJoinMessage{} }
func (m *PartitionJoinMessage) String() string { return proto.CompactTextString(m) }
func (*PartitionJoinMessage) ProtoMessage()    {}

func (m *PartitionJoinMessage) GetIsInit() bool {
	if m != nil && m.IsInit != nil {
		return *m.IsInit
	}
	return false
}

func (m *PartitionJoinMessage) GetIp() string {
	if m != nil && m.Ip != nil {
		return *m.Ip
	}
	return ""
}

func (m *PartitionJoinMessage) GetPort() uint32 {
	if m != nil && m.Port != nil {
		return *m.Port
	}
	return 0
}

func (m *PartitionJoinMessage) GetPublicUrl() string {
	if m != nil && m.PublicUrl != nil {
		return *m.PublicUrl
	}
	return ""
}

func (m *PartitionJoinMessage) GetDataCenter() string {
	if m != nil && m.DataCenter != nil {
		return *m.DataCenter
	}
	return ""
}

func (m *PartitionJoinMessage) GetRack() string {
	if m != nil && m.Rack != nil {
		return *m.Rack
	}
	return ""
}

func (m *PartitionJoinMessage) GetPartitions() []*PartitionInformationMessage {
	if m != nil {
		return m.Partitions
	}
	return nil
}

func (m *PartitionJoinMessage) GetCollection() string {
	if m != nil && m.Collection != nil {
		return *m.Collection
	}
	return ""
}

func (m *PartitionJoinMessage) GetCapacityAll() uint64 {
	if m != nil && m.CapacityAll != nil {
		return *m.CapacityAll
	}
	return 0
}

func (m *PartitionJoinMessage) GetCapacityUsed() uint64 {
	if m != nil && m.CapacityUsed != nil {
		return *m.CapacityUsed
	}
	return 0
}

func (m *PartitionJoinMessage) GetCapacityAvail() uint64 {
	if m != nil && m.CapacityAvail != nil {
		return *m.CapacityAvail
	}
	return 0
}

type FolderInfoMessage struct {
	Id               *uint32 `protobuf:"varint,1,req,name=id" json:"id,omitempty"`
	Version          *uint32 `protobuf:"varint,2,opt,name=version,def=1" json:"version,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *FolderInfoMessage) Reset()         { *m = FolderInfoMessage{} }
func (m *FolderInfoMessage) String() string { return proto.CompactTextString(m) }
func (*FolderInfoMessage) ProtoMessage()    {}

const Default_FolderInfoMessage_Version uint32 = 1

func (m *FolderInfoMessage) GetId() uint32 {
	if m != nil && m.Id != nil {
		return *m.Id
	}
	return 0
}

func (m *FolderInfoMessage) GetVersion() uint32 {
	if m != nil && m.Version != nil {
		return *m.Version
	}
	return Default_FolderInfoMessage_Version
}

type DiskFolderInfoMessage struct {
	Path             *string              `protobuf:"bytes,1,req,name=path" json:"path,omitempty"`
	Volumes          []*FolderInfoMessage `protobuf:"bytes,2,rep,name=volumes" json:"volumes,omitempty"`
	XXX_unrecognized []byte               `json:"-"`
}

func (m *DiskFolderInfoMessage) Reset()         { *m = DiskFolderInfoMessage{} }
func (m *DiskFolderInfoMessage) String() string { return proto.CompactTextString(m) }
func (*DiskFolderInfoMessage) ProtoMessage()    {}

func (m *DiskFolderInfoMessage) GetPath() string {
	if m != nil && m.Path != nil {
		return *m.Path
	}
	return ""
}

func (m *DiskFolderInfoMessage) GetVolumes() []*FolderInfoMessage {
	if m != nil {
		return m.Volumes
	}
	return nil
}

type FolderJoinMessage struct {
	IsInit           *bool                    `protobuf:"varint,1,opt,name=is_init" json:"is_init,omitempty"`
	Ip               *string                  `protobuf:"bytes,2,req,name=ip" json:"ip,omitempty"`
	Port             *uint32                  `protobuf:"varint,3,req,name=port" json:"port,omitempty"`
	PublicUrl        *string                  `protobuf:"bytes,4,opt,name=public_url" json:"public_url,omitempty"`
	DataCenter       *string                  `protobuf:"bytes,5,opt,name=data_center" json:"data_center,omitempty"`
	Rack             *string                  `protobuf:"bytes,6,opt,name=rack" json:"rack,omitempty"`
	DiskVolumes      []*DiskFolderInfoMessage `protobuf:"bytes,7,rep,name=diskVolumes" json:"diskVolumes,omitempty"`
	Collection       *string                  `protobuf:"bytes,8,opt,name=collection" json:"collection,omitempty"`
	CapacityAll      *uint64                  `protobuf:"varint,9,opt,name=capacity_all" json:"capacity_all,omitempty"`
	CapacityUsed     *uint64                  `protobuf:"varint,10,opt,name=capacity_used" json:"capacity_used,omitempty"`
	CapacityAvail    *uint64                  `protobuf:"varint,11,opt,name=capacity_avail" json:"capacity_avail,omitempty"`
	XXX_unrecognized []byte                   `json:"-"`
}

func (m *FolderJoinMessage) Reset()         { *m = FolderJoinMessage{} }
func (m *FolderJoinMessage) String() string { return proto.CompactTextString(m) }
func (*FolderJoinMessage) ProtoMessage()    {}

func (m *FolderJoinMessage) GetIsInit() bool {
	if m != nil && m.IsInit != nil {
		return *m.IsInit
	}
	return false
}

func (m *FolderJoinMessage) GetIp() string {
	if m != nil && m.Ip != nil {
		return *m.Ip
	}
	return ""
}

func (m *FolderJoinMessage) GetPort() uint32 {
	if m != nil && m.Port != nil {
		return *m.Port
	}
	return 0
}

func (m *FolderJoinMessage) GetPublicUrl() string {
	if m != nil && m.PublicUrl != nil {
		return *m.PublicUrl
	}
	return ""
}

func (m *FolderJoinMessage) GetDataCenter() string {
	if m != nil && m.DataCenter != nil {
		return *m.DataCenter
	}
	return ""
}

func (m *FolderJoinMessage) GetRack() string {
	if m != nil && m.Rack != nil {
		return *m.Rack
	}
	return ""
}

func (m *FolderJoinMessage) GetDiskVolumes() []*DiskFolderInfoMessage {
	if m != nil {
		return m.DiskVolumes
	}
	return nil
}

func (m *FolderJoinMessage) GetCollection() string {
	if m != nil && m.Collection != nil {
		return *m.Collection
	}
	return ""
}

func (m *FolderJoinMessage) GetCapacityAll() uint64 {
	if m != nil && m.CapacityAll != nil {
		return *m.CapacityAll
	}
	return 0
}

func (m *FolderJoinMessage) GetCapacityUsed() uint64 {
	if m != nil && m.CapacityUsed != nil {
		return *m.CapacityUsed
	}
	return 0
}

func (m *FolderJoinMessage) GetCapacityAvail() uint64 {
	if m != nil && m.CapacityAvail != nil {
		return *m.CapacityAvail
	}
	return 0
}

func init() {
}