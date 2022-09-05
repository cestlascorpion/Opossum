package client

import (
	"context"
	"time"

	pb "github.com/cestlascorpion/opossum/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	address = "127.0.0.1:8080"
)

type Client struct {
	pb.OpossumClient
}

func NewClient(ctx context.Context, opts ...grpc.DialOption) (*Client, error) {
	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		log.Errorf("connect fail err %+v", err)
		return nil, err
	}
	return &Client{
		OpossumClient: pb.NewOpossumClient(conn),
	}, nil
}

func (c *Client) GetSegment(ctx context.Context, tag string, opts ...grpc.CallOption) (int64, error) {
	resp, err := c.OpossumClient.GetSegment(ctx, &pb.GetSegmentIdReq{
		Key: tag,
	}, opts...)
	if err != nil {
		log.Errorf("get segment id err %+v", err)
		return 0, err
	}
	return resp.Id, nil
}

func (c *Client) GetSnowflake(ctx context.Context, opts ...grpc.CallOption) (int64, error) {
	resp, err := c.OpossumClient.GetSnowflake(ctx, &pb.GetSnowflakeIdReq{}, opts...)
	if err != nil {
		log.Errorf("get snowflake id err %+v", err)
		return 0, err
	}
	return resp.Id, nil
}

func (c *Client) DecodeSnowflake(ctx context.Context, id int64, opts ...grpc.CallOption) (time.Time, int64, int64, error) {
	resp, err := c.OpossumClient.DecodeSnowflake(ctx, &pb.DecodeSnowflakeIdReq{
		Id: id,
	}, opts...)
	if err != nil {
		log.Errorf("dec snowflake id err %+v", err)
		return time.Now(), 0, 0, err
	}
	return time.UnixMilli(resp.TimeStamp), resp.WorkerId, resp.SequenceId, nil
}
