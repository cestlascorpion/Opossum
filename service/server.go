package service

import (
	"context"
	"errors"

	pb "github.com/cestlascorpion/opossum/proto"
	"github.com/cestlascorpion/opossum/utils"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	*pb.UnimplementedOpossumServer
	segment   *Segment
	snowflake *Snowflake
}

func NewServer(ctx context.Context, conf *utils.Config) (*Server, error) {
	sg, err := NewSegment(ctx, conf)
	if err != nil {
		return nil, err
	}
	sf, err := NewSnowflake(ctx, conf)
	if err != nil {
		_ = sg.Close(ctx)
		return nil, err
	}
	return &Server{segment: sg, snowflake: sf}, nil
}

func (s *Server) AllocSegment(ctx context.Context, in *pb.AllocSegmentReq) (*pb.AllocSegmentResp, error) {
	if in == nil {
		return nil, errors.New(utils.ErrInvalidParameter)
	}
	start, end, err := s.segment.Alloc(ctx, in.Key)
	if err != nil {
		log.Errorf("alloc segment err %+v", err)
		return nil, err
	}
	return &pb.AllocSegmentResp{Start: start, End: end}, nil
}

func (s *Server) GetSnowflakes(ctx context.Context, in *pb.GetSnowflakesReq) (*pb.GetSnowflakesResp, error) {
	if in == nil {
		return nil, errors.New(utils.ErrInvalidParameter)
	}
	ids, err := s.snowflake.GetSnowflakeIds(ctx, in.Count)
	if err != nil {
		log.Errorf("get snowflake ids err %+v", err)
		return nil, err
	}
	return &pb.GetSnowflakesResp{Ids: ids}, nil
}

func (s *Server) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if s.segment != nil {
		_ = s.segment.Close(ctx)
	}
	if s.snowflake != nil {
		return s.snowflake.Close(ctx)
	}
	return nil
}
