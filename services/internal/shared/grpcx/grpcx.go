package grpcx

import "google.golang.org/grpc"

// NewServer 提供 gRPC 服务骨架构造能力，供后续协议层扩展。
func NewServer(opts ...grpc.ServerOption) *grpc.Server {
	return grpc.NewServer(opts...)
}
