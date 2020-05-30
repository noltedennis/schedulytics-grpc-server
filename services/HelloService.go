package services

import (
	"context"
	"log"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/noltedennis/schedulytics-backend/model"
)

type HelloServiceServer struct{}

// Functions of Hello service
func newHelloSever() *HelloServiceServer {
	log.Printf("Registered helloServiceServer handler")
	return &HelloServiceServer{}
}

func (s *HelloServiceServer) SayHello(ctx context.Context, req *empty.Empty) (*model.ResponseHello, error) {
	return &model.ResponseHello{Response: "Hello you!"}, nil
}
