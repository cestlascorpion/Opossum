package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cestlascorpion/opossum/utils"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
)

type EtcdHolder struct {
	endpoints    []string
	ip           string
	port         string
	addr         string
	maxId        int64
	path         *etcdPath
	lastUpdateTs int64
	workerId     int64
	cancel       context.CancelFunc
}

func NewEtcdHolder(ctx context.Context, ip, port string, endpoints []string, table string, maxId int64) (*EtcdHolder, error) {
	e := &EtcdHolder{
		endpoints: endpoints,
		ip:        ip,
		port:      port,
		addr:      fmt.Sprintf("%s:%s", ip, port),
		maxId:     maxId,
		path: &etcdPath{
			LockPath:    fmt.Sprintf(etcdLockerPath, table),
			ForeverPath: fmt.Sprintf(etcdForeverPath, table),
		},
	}

	locker, err := NewLocker(ctx, e.path.LockPath, endpoints)
	if err != nil {
		log.Errorf("new locker err %+v", err)
		return nil, err
	}

	err = locker.Lock(ctx)
	if err != nil {
		log.Errorf("locker lock err %+v", err)
		return nil, err
	}
	defer func() {
		_ = locker.UnLock(ctx)
	}()

	err = e.initWorkerId(ctx)
	if err != nil {
		log.Errorf("init worker id err %+v", err)
		return nil, err
	}

	log.Infof("new etcd holder ok %+v", e)
	return e, nil
}

func (e *EtcdHolder) GetWorkerId(ctx context.Context) (int64, error) {
	return e.workerId, nil
}

func (e *EtcdHolder) Close(ctx context.Context) error {
	e.cancel()
	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

const (
	etcdLockerPath  = "/snowflake/%s/locker"
	etcdForeverPath = "/snowflake/%s/forever"
	dialTimeout     = time.Second * 10
	updateInterval  = time.Second * 3
)

type etcdPath struct {
	LockPath    string
	ForeverPath string
}

type endpoint struct {
	IP   string `json:"ip"`
	Port string `json:"port"`
	Ts   int64  `json:"ts"`
}

func (e *EtcdHolder) initWorkerId(ctx context.Context) error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   e.endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		log.Errorf("new etcd client err %+v", err)
		return err
	}

	resp, err := cli.Get(ctx, e.path.ForeverPath, clientv3.WithPrefix())
	if err != nil {
		log.Errorf("get forever path err %+v", err)
		return err
	}

	if resp.Count == 0 {
		e.workerId = 0
		p, err := e.createNode(ctx, cli)
		if err != nil {
			log.Errorf("create node err %+v", err)
			return err
		}
		e.scheduledUploadData(ctx, cli, p)
		return nil
	}

	nodeMap := make(map[string]int64)
	realMap := make(map[string]string)
	for _, v := range resp.Kvs {
		nodeKey := strings.Split(filepath.Base(string(v.Key)), "-")
		if len(nodeKey) != 2 {
			log.Errorf("invalid node key %+v", nodeKey)
			return errors.New(utils.ErrInvalidNodeKey)
		}
		realMap[nodeKey[0]] = string(v.Key)
		id, err := strconv.ParseInt(nodeKey[1], 10, 64)
		if err != nil {
			log.Errorf("invalid node key %s", nodeKey[1])
			return errors.New(utils.ErrInvalidWorkerId)
		}
		nodeMap[nodeKey[0]] = id
	}

	if workerId, ok := nodeMap[e.addr]; ok {
		p := realMap[e.addr]
		if !e.checkInitTimeStamp(ctx, cli, p) {
			log.Errorf("check init ts failed")
			return errors.New(utils.ErrInvalidClockTime)
		}
		e.workerId = workerId
		e.scheduledUploadData(ctx, cli, p)
		log.Infof("restart node %s ok", e.addr)
		return nil
	}

	workerId := int64(0)
	for _, id := range nodeMap {
		if workerId < id {
			workerId = id
		}
	}
	e.workerId = workerId + 1
	if e.workerId > e.maxId {
		log.Errorf("invalid worker id")
		return errors.New(utils.ErrInvalidWorkerId)
	}

	p, err := e.createNode(ctx, cli)
	if err != nil {
		log.Errorf("create node err %+v", err)
		return err
	}
	e.scheduledUploadData(ctx, cli, p)
	return nil
}

func (e *EtcdHolder) createNode(ctx context.Context, client *clientv3.Client) (string, error) {
	path := fmt.Sprintf("%s/%s-%d", e.path.ForeverPath, e.addr, e.workerId)
	data, err := e.marshallNode()
	if err != nil {
		log.Errorf("build data err %+v", err)
		return "", err
	}
	_, err = client.Put(ctx, path, string(data))
	if err != nil {
		log.Errorf("put data err %+v", err)
		return "", err
	}
	return path, nil
}

func (e *EtcdHolder) checkInitTimeStamp(ctx context.Context, cli *clientv3.Client, path string) bool {
	resp, err := cli.Get(ctx, path)
	if err != nil {
		log.Errorf("get err %+v", err)
		return false
	}
	if resp == nil || len(resp.Kvs) == 0 {
		log.Errorf("no such key %s", path)
		return false
	}
	node, err := e.unmarshallNode(resp.Kvs[0].Value)
	return !(node.Ts > time.Now().UnixMilli())
}

func (e *EtcdHolder) scheduledUploadData(ctx context.Context, cli *clientv3.Client, path string) {
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel

	go func(ctx context.Context) {
		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := e.updateNewData(context.Background(), cli, path)
				if err != nil {
					log.Errorf("update new data err %+v", err)
				}
			}
		}
	}(ctx)
}

func (e *EtcdHolder) updateNewData(ctx context.Context, cli *clientv3.Client, path string) error {
	if time.Now().UnixMilli() < e.lastUpdateTs {
		log.Warnf("last update ts gt time now")
		return nil
	}
	bs, err := e.marshallNode()
	if err != nil {
		log.Errorf("build data err %+v", err)
		return err
	}
	_, err = cli.Put(ctx, path, string(bs))
	if err != nil {
		log.Errorf("put err %+v", err)
		return err
	}
	return nil
}

func (e *EtcdHolder) marshallNode() ([]byte, error) {
	node := &endpoint{
		IP:   e.ip,
		Port: e.port,
		Ts:   time.Now().UnixMilli(),
	}
	bs, err := json.Marshal(node)
	if err != nil {
		log.Errorf("json marshall err %+v", err)
		return nil, err
	}
	return bs, nil
}

func (e *EtcdHolder) unmarshallNode(val []byte) (*endpoint, error) {
	node := &endpoint{}
	err := json.Unmarshal(val, node)
	if err != nil {
		log.Errorf("json unmarshall err %+v", err)
		return nil, err
	}
	return node, nil
}

// ---------------------------------------------------------------------------------------------------------------------
