package service

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cestlascorpion/opossum/storage"
	"github.com/cestlascorpion/opossum/utils"
)

type workerHolder interface {
	GetWorkerId(context.Context) (int64, error)
	Valid(int64) bool
	Close(context.Context) error
}

type Snowflake struct {
	workerId int64
	sequence int64
	lastTs   int64
	holder   workerHolder
	mutex    sync.Mutex
}

func NewSnowflake(ctx context.Context, conf *utils.Config) (*Snowflake, error) {
	if conf == nil || conf.Snowflake == nil || conf.Snowflake.Table == "" {
		return nil, errors.New(utils.ErrInvalidParameter)
	}
	ip, port, ok := checkAddress(conf)
	if !ok {
		return nil, errors.New(utils.ErrInvalidParameter)
	}
	var endpoints []string
	for _, endpoint := range strings.Split(conf.Snowflake.Endpoints, ",") {
		if endpoint = strings.TrimSpace(endpoint); endpoint != "" {
			endpoints = append(endpoints, endpoint)
		}
	}
	if len(endpoints) == 0 {
		return nil, errors.New(utils.ErrInvalidParameter)
	}

	h, err := storage.NewEtcdHolder(ctx, ip, port, endpoints, conf.Snowflake.Table, maxWorkerId)
	if err != nil {
		return nil, err
	}
	id, err := h.GetWorkerId(ctx)
	if err != nil {
		_ = h.Close(ctx)
		return nil, err
	}
	return &Snowflake{workerId: id, holder: h}, nil
}

func (s *Snowflake) GetSnowflakeIds(ctx context.Context, count uint32) ([]int64, error) {
	if count == 0 || count > maxBatch {
		return nil, errors.New(utils.ErrInvalidParameter)
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()

	ids := make([]int64, 0, count)
	for i := uint32(0); i < count; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id, err := s.next(ctx)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *Snowflake) GetSnowflakeId(ctx context.Context) (int64, error) {
	ids, err := s.GetSnowflakeIds(ctx, 1)
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

func (s *Snowflake) next(ctx context.Context) (int64, error) {
	ts := time.Now().UnixMilli()
	if ts < s.lastTs {
		if s.lastTs-ts > maxClockBack {
			return 0, errors.New(utils.ErrInvalidClockTime)
		}
		var err error
		ts, err = tilNextMillis(ctx, s.lastTs-1)
		if err != nil {
			return 0, err
		}
	}
	if !s.holder.Valid(ts) {
		return 0, errors.New(utils.ErrWorkerLeaseExpired)
	}
	if s.lastTs == ts {
		s.sequence = (s.sequence + 1) & sequenceMask
		if s.sequence == 0 {
			var err error
			ts, err = tilNextMillis(ctx, s.lastTs)
			if err != nil {
				return 0, err
			}
			if !s.holder.Valid(ts) {
				return 0, errors.New(utils.ErrWorkerLeaseExpired)
			}
			s.sequence = rand.Int63n(100)
		}
	} else {
		s.sequence = rand.Int63n(100)
	}
	s.lastTs = ts
	return ((ts - twepoch) << timestampLeftShift) | (s.workerId << workerIdShift) | s.sequence, nil
}

func (s *Snowflake) Close(ctx context.Context) error {
	if s == nil || s.holder == nil {
		return nil
	}
	return s.holder.Close(ctx)
}

const (
	twepoch            = int64(1288834974657)
	workerIdBits       = 10
	sequenceBits       = 12
	maxWorkerId        = ^(-1 << workerIdBits)
	workerIdShift      = sequenceBits
	timestampLeftShift = sequenceBits + workerIdBits
	sequenceMask       = int64(^(-1 << sequenceBits))
	maxBatch           = uint32(4096)
	maxClockBack       = int64(5)
)

func checkAddress(conf *utils.Config) (string, string, bool) {
	if conf.Snowflake.Addr == "" {
		conf.Snowflake.Addr = getHostAddress(conf.Snowflake.Ethernet)
	}
	if conf.Snowflake.Addr == "" || conf.Snowflake.Port == 0 {
		return "", "", false
	}
	return conf.Snowflake.Addr, strconv.FormatInt(int64(conf.Snowflake.Port), 10), true
}

func getHostAddress(eth string) string {
	ipList, err := getIpList()
	if err != nil {
		return ""
	}
	if eth != "" {
		return ipList[eth]
	}
	keys := make([]string, 0, len(ipList))
	for key := range ipList {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return ""
	}
	return ipList[keys[0]]
}

func getIpList() (map[string]string, error) {
	list := make(map[string]string)
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, inf := range interfaces {
		addrs, err := inf.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || utils.SkipIPV6 && ip.To4() == nil {
				continue
			}
			list[inf.Name] = ip.String()
		}
	}
	return list, nil
}

func tilNextMillis(ctx context.Context, lastTs int64) (int64, error) {
	for {
		ts := time.Now().UnixMilli()
		if ts > lastTs {
			return ts, nil
		}
		timer := time.NewTimer(time.Duration(lastTs-ts+1) * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
}
