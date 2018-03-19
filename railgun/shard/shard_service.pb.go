// Code generated by protoc-gen-go. DO NOT EDIT.
// source: shard_service.proto

/*
Package shard is a generated protocol buffer package.

It is generated from these files:
	shard_service.proto

It has these top-level messages:
	ShardProvisionRequest
	ShardProvisionResponse
*/
package shard

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import sigpb "github.com/google/trillian/crypto/sigpb"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type ShardProvisionRequest struct {
	// This should contain the signature of the configuration proto.
	ConfigSig *sigpb.DigitallySigned `protobuf:"bytes,1,opt,name=config_sig,json=configSig" json:"config_sig,omitempty"`
	// This should contain a marshalled ShardProto.
	ShardConfig []byte `protobuf:"bytes,2,opt,name=shard_config,json=shardConfig,proto3" json:"shard_config,omitempty"`
}

func (m *ShardProvisionRequest) Reset()                    { *m = ShardProvisionRequest{} }
func (m *ShardProvisionRequest) String() string            { return proto.CompactTextString(m) }
func (*ShardProvisionRequest) ProtoMessage()               {}
func (*ShardProvisionRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *ShardProvisionRequest) GetConfigSig() *sigpb.DigitallySigned {
	if m != nil {
		return m.ConfigSig
	}
	return nil
}

func (m *ShardProvisionRequest) GetShardConfig() []byte {
	if m != nil {
		return m.ShardConfig
	}
	return nil
}

type ShardProvisionResponse struct {
	// This should contain the signature of the configuration proto.
	ConfigSig *sigpb.DigitallySigned `protobuf:"bytes,1,opt,name=config_sig,json=configSig" json:"config_sig,omitempty"`
	// This should contain a marshalled ShardProto. This represents the
	// config that was instated by the shard and may differ from the request.
	// Keys may be redacted and timestamps updated for example.
	ProvisionedConfig []byte `protobuf:"bytes,2,opt,name=provisioned_config,json=provisionedConfig,proto3" json:"provisioned_config,omitempty"`
}

func (m *ShardProvisionResponse) Reset()                    { *m = ShardProvisionResponse{} }
func (m *ShardProvisionResponse) String() string            { return proto.CompactTextString(m) }
func (*ShardProvisionResponse) ProtoMessage()               {}
func (*ShardProvisionResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *ShardProvisionResponse) GetConfigSig() *sigpb.DigitallySigned {
	if m != nil {
		return m.ConfigSig
	}
	return nil
}

func (m *ShardProvisionResponse) GetProvisionedConfig() []byte {
	if m != nil {
		return m.ProvisionedConfig
	}
	return nil
}

func init() {
	proto.RegisterType((*ShardProvisionRequest)(nil), "shard.ShardProvisionRequest")
	proto.RegisterType((*ShardProvisionResponse)(nil), "shard.ShardProvisionResponse")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for ShardProvisioning service

type ShardProvisioningClient interface {
	Provision(ctx context.Context, in *ShardProvisionRequest, opts ...grpc.CallOption) (*ShardProvisionResponse, error)
}

type shardProvisioningClient struct {
	cc *grpc.ClientConn
}

func NewShardProvisioningClient(cc *grpc.ClientConn) ShardProvisioningClient {
	return &shardProvisioningClient{cc}
}

func (c *shardProvisioningClient) Provision(ctx context.Context, in *ShardProvisionRequest, opts ...grpc.CallOption) (*ShardProvisionResponse, error) {
	out := new(ShardProvisionResponse)
	err := grpc.Invoke(ctx, "/shard.ShardProvisioning/Provision", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for ShardProvisioning service

type ShardProvisioningServer interface {
	Provision(context.Context, *ShardProvisionRequest) (*ShardProvisionResponse, error)
}

func RegisterShardProvisioningServer(s *grpc.Server, srv ShardProvisioningServer) {
	s.RegisterService(&_ShardProvisioning_serviceDesc, srv)
}

func _ShardProvisioning_Provision_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ShardProvisionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShardProvisioningServer).Provision(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/shard.ShardProvisioning/Provision",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShardProvisioningServer).Provision(ctx, req.(*ShardProvisionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _ShardProvisioning_serviceDesc = grpc.ServiceDesc{
	ServiceName: "shard.ShardProvisioning",
	HandlerType: (*ShardProvisioningServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Provision",
			Handler:    _ShardProvisioning_Provision_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "shard_service.proto",
}

func init() { proto.RegisterFile("shard_service.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 291 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x51, 0x41, 0x6b, 0x32, 0x31,
	0x14, 0xfc, 0xf6, 0x83, 0x16, 0x8c, 0x5e, 0x4c, 0xa9, 0x88, 0xb4, 0x60, 0xf7, 0xe4, 0xc5, 0x84,
	0x2a, 0xfd, 0x03, 0xb6, 0xa7, 0x9e, 0x96, 0xdd, 0x5b, 0x2f, 0xcb, 0xee, 0x9a, 0xc6, 0x07, 0xd9,
	0x24, 0x26, 0x59, 0xa9, 0x97, 0xfe, 0xf6, 0x62, 0x1e, 0x4a, 0x2b, 0xf5, 0xd2, 0x4b, 0x20, 0xf3,
	0x32, 0x6f, 0x26, 0x33, 0xe4, 0xc6, 0x6f, 0x2a, 0xb7, 0x2e, 0xbd, 0x70, 0x3b, 0x68, 0x04, 0xb3,
	0xce, 0x04, 0x43, 0xaf, 0x22, 0x38, 0x59, 0x4a, 0x08, 0x9b, 0xae, 0x66, 0x8d, 0x69, 0xb9, 0x34,
	0x46, 0x2a, 0xc1, 0x83, 0x03, 0xa5, 0xa0, 0xd2, 0xbc, 0x71, 0x7b, 0x1b, 0x0c, 0xf7, 0x20, 0x6d,
	0x8d, 0x27, 0x72, 0xd3, 0x2d, 0xb9, 0x2d, 0x0e, 0xec, 0xcc, 0x99, 0x1d, 0x78, 0x30, 0x3a, 0x17,
	0xdb, 0x4e, 0xf8, 0x40, 0x9f, 0x08, 0x69, 0x8c, 0x7e, 0x07, 0x59, 0x7a, 0x90, 0xe3, 0x64, 0x9a,
	0xcc, 0xfa, 0x8b, 0x11, 0x43, 0xea, 0x0b, 0x48, 0x08, 0x95, 0x52, 0xfb, 0x02, 0xa4, 0x16, 0xeb,
	0xbc, 0x87, 0x2f, 0x0b, 0x90, 0xf4, 0x81, 0x0c, 0xd0, 0x22, 0x42, 0xe3, 0xff, 0xd3, 0x64, 0x36,
	0xc8, 0xfb, 0x11, 0x7b, 0x8e, 0x50, 0xfa, 0x49, 0x46, 0xe7, 0x92, 0xde, 0x1a, 0xed, 0xc5, 0x5f,
	0x35, 0xe7, 0x84, 0xda, 0xe3, 0x2e, 0x71, 0xa6, 0x3c, 0xfc, 0x36, 0x41, 0xfd, 0x45, 0x49, 0x86,
	0x3f, 0xf5, 0x41, 0x4b, 0xfa, 0x4a, 0x7a, 0xa7, 0x3b, 0xbd, 0x63, 0xd1, 0x2f, 0xfb, 0x35, 0x99,
	0xc9, 0xfd, 0x85, 0x29, 0x7e, 0x22, 0xfd, 0xb7, 0x52, 0x24, 0x6d, 0x4c, 0xcb, 0xb0, 0x03, 0x76,
	0xec, 0x60, 0x2e, 0x3e, 0xaa, 0xd6, 0x2a, 0xe1, 0x31, 0xf9, 0x15, 0x9a, 0x28, 0xb0, 0xc9, 0xec,
	0x00, 0x65, 0xc9, 0xdb, 0xe3, 0xe5, 0x0e, 0x4f, 0x7c, 0xee, 0x2a, 0x50, 0xb2, 0xd3, 0x3c, 0xda,
	0xa8, 0xaf, 0xe3, 0xba, 0xe5, 0x57, 0x00, 0x00, 0x00, 0xff, 0xff, 0x82, 0x8e, 0x28, 0x95, 0x1b,
	0x02, 0x00, 0x00,
}
