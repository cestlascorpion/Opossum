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

var snowflakeBench *Snowflake

func init() {
	log.SetLevel(log.DebugLevel)

	conf, err := utils.NewOpossumConfigForMock()
	if err != nil {
		fmt.Println(err)
	}
	svr, err := NewSnowflake(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
	}
	snowflakeBench = svr
}

func Test_GetHostAddress(t *testing.T) {
	result, err := getIpList()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	for k, v := range result {
		fmt.Println(k, v)
	}
	addr := getHostAddress("eno1")
	fmt.Println(addr)
}

func Test_TilNextMillis(t *testing.T) {
	ts1 := time.Now().Add(time.Second * 10).UnixMilli()
	ts2 := tilNextMillis(ts1)

	if ts2 <= ts1 {
		fmt.Println(ts1)
		fmt.Println(ts2)
		t.FailNow()
	}
}

func TestSnowflake_GetSnowflakeId(t *testing.T) {
	conf, err := utils.NewOpossumConfigForMock()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	svr, err := NewSnowflake(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer func() {
		_ = svr.Close(context.Background())
	}()
	fmt.Printf("%+v\n", svr)

	for i := 0; i < 2048; i++ {
		id, err := svr.GetSnowflakeId(context.Background())
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		fmt.Printf("id %b\n", id)
	}
}

func TestSnowflake_GetSnowflakeId2(t *testing.T) {
	conf, err := utils.NewOpossumConfigForMock()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	svrList := make([]*Snowflake, 0)
	for i := 0; i < 10; i++ {
		conf.Snowflake.Port = conf.Snowflake.Port + int16(i)
		svr, err := NewSnowflake(context.Background(), conf)
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

			for i := 0; i < 2048; i++ {
				id, err := svrList[i%len(svrList)].GetSnowflakeId(context.Background())
				if err != nil {
					fmt.Println(err)
				}
				fmt.Printf("id %b\n", id)
				time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

func BenchmarkNewSnowflake(b *testing.B) {
	if snowflakeBench == nil {
		return
	}

	for i := 0; i < b.N; i++ {
		_, err := snowflakeBench.GetSnowflakeId(context.Background())
		if err != nil {
			fmt.Println(err)
		}
	}
}

func TestSnowflake_DecodeSnowflakeId(t *testing.T) {
	conf, err := utils.NewOpossumConfigForMock()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	svr, err := NewSnowflake(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer func() {
		_ = svr.Close(context.Background())
	}()
	fmt.Printf("%+v\n", svr)

	for i := 0; i < 10; i++ {
		id, err := svr.GetSnowflakeId(context.Background())
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		content, err := svr.DecodeSnowflakeId(context.Background(), id)
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		fmt.Println(content)
	}
}

func BenchmarkSnowflake_DecodeSnowflakeId(b *testing.B) {
	if snowflakeBench == nil {
		return
	}

	for i := 0; i < b.N; i++ {
		id, err := snowflakeBench.GetSnowflakeId(context.Background())
		if err != nil {
			fmt.Println(err)
		}
		_, err = snowflakeBench.DecodeSnowflakeId(context.Background(), id)
		if err != nil {
			fmt.Println(err)
		}

	}
}
