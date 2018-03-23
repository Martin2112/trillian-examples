// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/google/trillian-examples/railgun/shard (interfaces: ShardServiceServer)

// Package mock_shard is a generated GoMock package.
package mock_shard

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	shard "github.com/google/trillian-examples/railgun/shard"
	reflect "reflect"
)

// MockShardServiceServer is a mock of ShardServiceServer interface
type MockShardServiceServer struct {
	ctrl     *gomock.Controller
	recorder *MockShardServiceServerMockRecorder
}

// MockShardServiceServerMockRecorder is the mock recorder for MockShardServiceServer
type MockShardServiceServerMockRecorder struct {
	mock *MockShardServiceServer
}

// NewMockShardServiceServer creates a new mock instance
func NewMockShardServiceServer(ctrl *gomock.Controller) *MockShardServiceServer {
	mock := &MockShardServiceServer{ctrl: ctrl}
	mock.recorder = &MockShardServiceServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockShardServiceServer) EXPECT() *MockShardServiceServerMockRecorder {
	return m.recorder
}

// GetConfig mocks base method
func (m *MockShardServiceServer) GetConfig(arg0 context.Context, arg1 *shard.GetShardConfigRequest) (*shard.GetShardConfigResponse, error) {
	ret := m.ctrl.Call(m, "GetConfig", arg0, arg1)
	ret0, _ := ret[0].(*shard.GetShardConfigResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetConfig indicates an expected call of GetConfig
func (mr *MockShardServiceServerMockRecorder) GetConfig(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetConfig", reflect.TypeOf((*MockShardServiceServer)(nil).GetConfig), arg0, arg1)
}

// Provision mocks base method
func (m *MockShardServiceServer) Provision(arg0 context.Context, arg1 *shard.ShardProvisionRequest) (*shard.ShardProvisionResponse, error) {
	ret := m.ctrl.Call(m, "Provision", arg0, arg1)
	ret0, _ := ret[0].(*shard.ShardProvisionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Provision indicates an expected call of Provision
func (mr *MockShardServiceServerMockRecorder) Provision(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Provision", reflect.TypeOf((*MockShardServiceServer)(nil).Provision), arg0, arg1)
}

// ProvisionHandshake mocks base method
func (m *MockShardServiceServer) ProvisionHandshake(arg0 context.Context, arg1 *shard.ProvisionHandshakeRequest) (*shard.ProvisionHandshakeResponse, error) {
	ret := m.ctrl.Call(m, "ProvisionHandshake", arg0, arg1)
	ret0, _ := ret[0].(*shard.ProvisionHandshakeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ProvisionHandshake indicates an expected call of ProvisionHandshake
func (mr *MockShardServiceServerMockRecorder) ProvisionHandshake(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProvisionHandshake", reflect.TypeOf((*MockShardServiceServer)(nil).ProvisionHandshake), arg0, arg1)
}
