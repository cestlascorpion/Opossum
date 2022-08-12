package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	pb "github.com/cestlascorpion/opossum/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	address = "0.0.0.0:9090"
)

func main() {
	log.SetLevel(log.DebugLevel)

	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("connect fail %v", err)
		return
	}
	defer conn.Close()
	client := pb.NewOpossumClient(conn)

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			req := &pb.GetSegmentIdReq{}
			for {
				req.Key = fmt.Sprintf("test_tag_%d", rand.Intn(10))
				resp, err := client.GetSegment(context.Background(), req)
				if err != nil {
					log.Errorf("get segment err %+v", err)
					return
				}
				log.Debugf("id %d", resp.Id)
			}
		}()
	}
	wg.Wait()
}
