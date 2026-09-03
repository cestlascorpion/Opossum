package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/cestlascorpion/opossum/proto"
	"github.com/cestlascorpion/opossum/service"
	"github.com/cestlascorpion/opossum/utils"
	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	log.SetLevel(log.InfoLevel)

	lis, err := net.Listen("tcp", utils.ServerAddr)
	if err != nil {
		log.Errorf("listen failed err %+v", err)
		return
	}
	conf := &utils.Config{}
	err = configor.Load(conf, "./conf.json")
	if err != nil {
		log.Errorf("config failed err %+v", err)
		return
	}
	svr, err := service.NewServer(context.Background(), conf)
	if err != nil {
		log.Errorf("new server failed err %+v", err)
		return
	}
	defer func() {
		err := svr.Close(context.Background())
		if err != nil {
			log.Errorf("close server failed err %+v", err)
		}
	}()

	s := grpc.NewServer()
	pb.RegisterOpossumServer(s, svr)
	reflection.Register(s)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()

	err = s.Serve(lis)
	if err != nil {
		log.Errorf("serve failed err %+v", err)
	}
}
