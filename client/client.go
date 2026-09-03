package client

import (
	"context"
	"errors"
	"sync"
	"time"

	pb "github.com/cestlascorpion/opossum/proto"
	"github.com/cestlascorpion/opossum/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type segment struct {
	mutex sync.Mutex
	next  int64
	end   int64
}

type Client struct {
	pb.OpossumClient
	conn     *grpc.ClientConn
	mutex    sync.Mutex
	segments map[string]*segment
}

func NewClient(ctx context.Context, addr string, opts ...grpc.DialOption) (*Client, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		OpossumClient: pb.NewOpossumClient(conn),
		conn:          conn,
		segments:      make(map[string]*segment),
	}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) GetSegment(ctx context.Context, tag string, opts ...grpc.CallOption) (int64, error) {
	c.mutex.Lock()
	seg := c.segments[tag]
	if seg == nil {
		seg = &segment{}
		c.segments[tag] = seg
	}
	c.mutex.Unlock()

	seg.mutex.Lock()
	defer seg.mutex.Unlock()
	if seg.next >= seg.end {
		resp, err := c.OpossumClient.AllocSegment(ctx, &pb.AllocSegmentReq{Key: tag}, opts...)
		if err != nil {
			log.Errorf("alloc segment err %+v", err)
			return 0, err
		}
		if resp.Start >= resp.End {
			return 0, errors.New(utils.ErrInvalidParameter)
		}
		seg.next = resp.Start
		seg.end = resp.End
	}
	id := seg.next
	seg.next++
	return id, nil
}

func (c *Client) GetSnowflakes(ctx context.Context, count uint32, opts ...grpc.CallOption) ([]int64, error) {
	resp, err := c.OpossumClient.GetSnowflakes(ctx, &pb.GetSnowflakesReq{Count: count}, opts...)
	if err != nil {
		log.Errorf("get snowflake ids err %+v", err)
		return nil, err
	}
	if len(resp.Ids) != int(count) {
		return nil, errors.New(utils.ErrInvalidParameter)
	}
	return resp.Ids, nil
}

func (c *Client) GetSnowflake(ctx context.Context, opts ...grpc.CallOption) (int64, error) {
	ids, err := c.GetSnowflakes(ctx, 1, opts...)
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

func DecodeSnowflake(id int64) (time.Time, int64, int64) {
	const (
		epoch        = int64(1288834974657)
		workerBits   = 10
		sequenceBits = 12
		workerMask   = int64(^(-1 << workerBits))
		sequenceMask = int64(^(-1 << sequenceBits))
	)
	ts := (id >> (workerBits + sequenceBits)) + epoch
	workerId := (id >> sequenceBits) & workerMask
	sequence := id & sequenceMask
	return time.UnixMilli(ts), workerId, sequence
}
