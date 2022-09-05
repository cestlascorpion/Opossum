package client

import (
	"context"
	"fmt"
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

func TestClient_GetSegment(t *testing.T) {
	client, err := NewClient(context.Background(), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < 1024; j++ {
				resp, err := client.GetSegment(context.Background(), "test")
				if err != nil {
					log.Errorf("get segment err %+v", err)
					return
				}
				log.Debugf("resp %+v", resp)
			}
		}()
	}
	wg.Wait()
}

func TestClient_GetSnowflake(t *testing.T) {
	client, err := NewClient(context.Background(), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < 1024; j++ {
				resp, err := client.GetSnowflake(context.Background())
				if err != nil {
					log.Errorf("get segment err %+v", err)
					return
				}
				log.Debugf("resp %+v", resp)
			}
		}()
	}
	wg.Wait()
}
