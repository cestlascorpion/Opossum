package service

import (
	"context"

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
	log.Debugf("conf %+v", conf)

	sg, err := NewSegment(ctx, conf)
	if err != nil {
		log.Errorf("new segment impl err %+v", err)
		return nil, err
	}
	sf, err := NewSnowflake(ctx, conf)
	if err != nil {
		log.Errorf("new snowflake impl err %+v", err)
		return nil, err
	}
	return &Server{
		segment:   sg,
		snowflake: sf,
	}, nil
}

func (s *Server) GetSegment(ctx context.Context, in *pb.GetSegmentIdReq) (*pb.GetSegmentIdResp, error) {
	log.Debugf("segment req %+v", in)

	out := &pb.GetSegmentIdResp{}
	id, err := s.segment.GetSegmentId(ctx, in.Key)
	if err != nil {
		log.Errorf("get segment id err %+v", err)
		return out, err
	}
	out.Id = id
	return out, nil
}

func (s *Server) GetSnowflake(ctx context.Context, in *pb.GetSnowflakeIdReq) (*pb.GetSnowflakeIdResp, error) {
	log.Debugf("snowflake req %+v", in)

	out := &pb.GetSnowflakeIdResp{}
	id, err := s.snowflake.GetSnowflakeId(ctx)
	if err != nil {
		log.Errorf("get snowflake id err %+v", err)
		return out, err
	}
	out.Id = id
	return out, nil
}

func (s *Server) DecodeSnowflake(ctx context.Context, in *pb.DecodeSnowflakeIdReq) (*pb.DecodeSnowflakeIdResp, error) {
	log.Debugf("decode snowflake req %+v", in)

	out := &pb.DecodeSnowflakeIdResp{}
	ts, workerId, sequence, err := s.snowflake.DecodeSnowflakeId(ctx, in.Id)
	if err != nil {
		log.Errorf("decode snowflake id err %+v", err)
		return out, err
	}
	out.TimeStamp = ts
	out.WorkerId = workerId
	out.SequenceId = sequence
	return out, nil
}

func (s *Server) Close(ctx context.Context) error {
	_ = s.segment.Close(ctx)
	_ = s.snowflake.Close(ctx)
	return nil
}
