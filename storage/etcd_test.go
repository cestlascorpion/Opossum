package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

func TestNewEtcdHolder(t *testing.T) {
	holder, err := NewEtcdHolder(context.Background(), "192.168.35.50", "9090", []string{"127.0.0.1:2379"}, "test", 1023)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer func() {
		_ = holder.Close(context.Background())
	}()

	for i := 0; i < 10; i++ {
		workerId, err := holder.GetWorkerId(context.Background())
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		fmt.Println(workerId)
		time.Sleep(time.Second * 5)
	}
}

func TestNewEtcdHolder2(t *testing.T) {
	holder := make([]*EtcdHolder, 0)
	for i := 0; i < 10; i++ {
		h, err := NewEtcdHolder(context.Background(), "192.168.35.50", fmt.Sprintf("%d", 9090+i), []string{"127.0.0.1:2379"}, "test", 1023)
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		holder = append(holder, h)
	}
	defer func() {
		for i := range holder {
			_ = holder[i].Close(context.Background())
		}
	}()

	for i := 0; i < 10; i++ {
		workerId, err := holder[i].GetWorkerId(context.Background())
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		fmt.Println(workerId)
		time.Sleep(time.Second * 5)
	}
}
