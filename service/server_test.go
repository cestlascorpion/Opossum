package service

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/cestlascorpion/opossum/proto"
	"github.com/cestlascorpion/opossum/utils"
	log "github.com/sirupsen/logrus"
)

var bench *Server

func init() {
	log.SetLevel(log.InfoLevel)

	conf, err := utils.NewConfigForTest()
	if err != nil {
		fmt.Println(err)
	}
	svr, err := NewServer(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
	}
	bench = svr
}

func TestServer_Segment(t *testing.T) {
	conf, err := utils.NewConfigForTest()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	svr, err := NewServer(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer svr.Close(context.Background())

	resp, err := svr.GetSegment(context.Background(), &pb.GetSegmentIdReq{
		Key: "test_tag",
	})
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Printf("%+v\n", resp)
}

func TestServer_Snowflake(t *testing.T) {
	conf, err := utils.NewConfigForTest()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	svr, err := NewServer(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer svr.Close(context.Background())

	resp, err := svr.GetSnowflake(context.Background(), &pb.GetSnowflakeIdReq{
		Key: "test_key",
	})
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Printf("%+v\n", resp)
}

func BenchmarkServer_Segment(b *testing.B) {
	if bench == nil {
		return
	}

	for i := 0; i < b.N; i++ {
		_, err := bench.GetSegment(context.Background(), &pb.GetSegmentIdReq{
			Key: "test_tag",
		})
		if err != nil {
			fmt.Println(err)
		}
	}
}

func BenchmarkServer_Snowflake(b *testing.B) {
	if bench == nil {
		return
	}

	for i := 0; i < b.N; i++ {
		_, err := bench.GetSnowflake(context.Background(), &pb.GetSnowflakeIdReq{
			Key: "test_tag",
		})
		if err != nil {
			fmt.Println(err)
		}
	}
}

func TestServer_DecodeSnowflake(t *testing.T) {
	conf, err := utils.NewConfigForTest()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	svr, err := NewServer(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer svr.Close(context.Background())

	resp, err := svr.GetSnowflake(context.Background(), &pb.GetSnowflakeIdReq{
		Key: "test_key",
	})
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	resp2, err := svr.DecodeSnowflake(context.Background(), &pb.DecodeSnowflakeIdReq{
		Id: resp.Id,
	})
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(resp2.Content)
}
