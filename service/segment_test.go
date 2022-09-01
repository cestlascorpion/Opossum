package service

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cestlascorpion/opossum/utils"
	log "github.com/sirupsen/logrus"
)

var segmentBench *Segment

func init() {
	log.SetLevel(log.DebugLevel)

	conf, err := utils.NewConfigForTest()
	if err != nil {
		fmt.Println(err)
	}
	svr, err := NewSegment(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
	}
	segmentBench = svr
}

func TestSegment_GetSegmentId(t *testing.T) {
	conf, err := utils.NewConfigForTest()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	svr, err := NewSegment(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer func() {
		_ = svr.Close(context.Background())
	}()
	fmt.Printf("%+v\n", svr)

	for i := 0; i < 1024; i++ {
		id, err := svr.GetSegmentId(context.Background(), "test")
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		fmt.Printf("id %d\n", id)
	}
}

func TestSegment_GetSegmentId2(t *testing.T) {
	conf, err := utils.NewConfigForTest()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	svrList := make([]*Segment, 0)
	for i := 0; i < 10; i++ {
		svr, err := NewSegment(context.Background(), conf)
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		fmt.Printf("%+v\n", svr)
		svrList = append(svrList, svr)
	}
	defer func() {
		for i := range svrList {
			_ = svrList[i].Close(context.Background())
		}
	}()

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			for i := 0; i < 1024; i++ {
				id, err := svrList[i%len(svrList)].GetSegmentId(context.Background(), "test")
				if err != nil {
					fmt.Println(err)
				}
				fmt.Printf("id %d\n", id)
				time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

func BenchmarkSegment_GetSegmentId(b *testing.B) {
	if segmentBench == nil {
		return
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := segmentBench.GetSegmentId(context.Background(), "test")
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}
