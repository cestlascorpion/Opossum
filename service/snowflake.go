package service

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cestlascorpion/opossum/storage"
	"github.com/cestlascorpion/opossum/utils"
	log "github.com/sirupsen/logrus"
)

type Snowflake struct {
	workerId int64               // worker id
	sequence int64               // sequence id
	lastTs   int64               // last update timestamp
	holder   *storage.EtcdHolder // worker id allocator
	mutex    sync.Mutex
}

func NewSnowflake(ctx context.Context, conf *utils.Config) (*Snowflake, error) {
	table := conf.Snowflake.Table
	if len(table) == 0 {
		return nil, errors.New(utils.ErrInvalidParameter)
	}

	ip, port, ok := checkAddress(conf)
	if !ok {
		log.Errorf("invalid parameter")
		return nil, errors.New(utils.ErrInvalidParameter)
	}

	endpoints := strings.Split(conf.Snowflake.Endpoints, ",")
	if len(endpoints) == 0 {
		log.Errorf("invalid etcd endpoints")
		return nil, errors.New(utils.ErrInvalidParameter)
	}

	path := conf.Snowflake.Table
	if len(path) == 0 {
		return nil, errors.New(utils.ErrInvalidParameter)
	}

	h, err := storage.NewEtcdHolder(ctx, ip, port, endpoints, path, maxWorkerId)
	if err != nil {
		log.Errorf("new etcd holer err %+v", err)
		return nil, err
	}

	id, err := h.GetWorkerId(ctx)
	if err != nil {
		log.Errorf("get worker id err %+v", err)
		return nil, err
	}

	return &Snowflake{
		workerId: id,
		sequence: 0,
		lastTs:   0,
		holder:   h,
	}, nil
}

func (s *Snowflake) GetSnowflakeId(ctx context.Context) (int64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	ts := time.Now().UnixMilli()
	if ts < s.lastTs {
		offset := s.lastTs - ts
		if offset <= 5 {
			time.Sleep(time.Duration(offset<<1) * time.Millisecond)
			ts = time.Now().UnixMilli()
			if ts < s.lastTs {
				return 0, errors.New(utils.ErrInvalidClockTime)
			}
		} else {
			return 0, errors.New(utils.ErrInvalidClockTime)
		}
	}
	if s.lastTs == ts {
		s.sequence = (s.sequence + 1) % sequenceMask
		if s.sequence == 0 {
			s.sequence = rand.Int63n(100)
			ts = tilNextMillis(s.lastTs)
		}
	} else {
		s.sequence = rand.Int63n(100)
	}
	s.lastTs = ts
	id := ((ts - twepoch) << timestampLeftShift) | (s.workerId << workerIdShift) | s.sequence

	return id, nil
}

func (s *Snowflake) DecodeSnowflakeId(ctx context.Context, id int64) (int64, int64, int64, error) {
	ts := (id >> (sequenceBits + workerIdBits)) + twepoch
	workerId := (id >> sequenceBits) ^ (id >> (sequenceBits + workerIdBits) << workerIdBits)
	sequence := id ^ (id >> sequenceBits << sequenceBits)

	return ts, workerId, sequence, nil
}

func (s *Snowflake) Close(ctx context.Context) error {
	return s.holder.Close(ctx)
}

// ---------------------------------------------------------------------------------------------------------------------

const (
	twepoch            = int64(1288834974657)
	workerIdBits       = 10
	sequenceBits       = 12
	maxWorkerId        = ^(-1 << workerIdBits)
	workerIdShift      = sequenceBits
	timestampLeftShift = sequenceBits + workerIdBits
	sequenceMask       = int64(^(-1 << sequenceBits))
)

func checkAddress(conf *utils.Config) (string, string, bool) {
	if len(conf.Snowflake.Addr) == 0 {
		conf.Snowflake.Addr = getHostAddress(conf.Snowflake.Ethernet)
	}

	if len(conf.Snowflake.Addr) == 0 || conf.Snowflake.Port == 0 {
		return "", "", false
	}

	return conf.Snowflake.Addr, strconv.FormatInt(int64(conf.Snowflake.Port), 10), true
}

func getHostAddress(eth string) string {
	ipList, err := getIpList()
	if err != nil {
		return ""
	}
	if len(eth) != 0 {
		if v, ok := ipList[eth]; ok {
			return v
		}
	} else {
		for i := range ipList {
			return ipList[i]
		}
	}
	return ""
}

func getIpList() (map[string]string, error) {
	list := make(map[string]string)
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for i := range interfaces {
		n, err := net.InterfaceByName(interfaces[i].Name)
		if err != nil {
			return nil, err
		}
		addr, err := n.Addrs()
		if err != nil {
			return nil, err
		}
		for j := range addr {
			var ip net.IP
			switch v := addr[j].(type) {
			case *net.IPNet:
				ip = v.IP
				list[n.Name] = ip.String()
			case *net.IPAddr:
				ip = v.IP
				list[n.Name] = ip.String()
			}
			if utils.SkipIPV6 {
				break
			}
		}
	}
	return list, nil
}

func tilNextMillis(lastTs int64) int64 {
	ts := time.Now().UnixMilli()
	for ts <= lastTs {
		ts = time.Now().UnixMilli()
	}
	return ts
}

// ---------------------------------------------------------------------------------------------------------------------
