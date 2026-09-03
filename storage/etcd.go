package storage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cestlascorpion/opossum/utils"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type EtcdHolder struct {
	endpoints []string
	addr      string
	maxId     int64
	workerId  int64
	validTs   int64
	validNs   int64
	closed    int32
	started   time.Time
	path      *etcdPath
	token     string
	activeKey string
	fenceKey  string
	cancel    context.CancelFunc
	client    *clientv3.Client
	session   *concurrency.Session
}

func NewEtcdHolder(ctx context.Context, ip, port string, endpoints []string, table string, maxId int64) (*EtcdHolder, error) {
	e := &EtcdHolder{
		endpoints: endpoints,
		addr:      fmt.Sprintf("%s:%s", ip, port),
		maxId:     maxId,
		workerId:  -1,
		started:   time.Now(),
		path: &etcdPath{
			Lock:   fmt.Sprintf(etcdLockPath, table),
			Active: fmt.Sprintf(etcdActivePath, table),
			Fence:  fmt.Sprintf(etcdFencePath, table),
		},
	}

	locker, err := NewLocker(ctx, e.path.Lock, endpoints)
	if err != nil {
		return nil, err
	}
	if err = locker.Lock(ctx); err != nil {
		_ = locker.Close()
		return nil, err
	}
	defer func() {
		_ = locker.UnLock(ctx)
		_ = locker.Close()
	}()

	if err = e.init(ctx); err != nil {
		_ = e.Close(ctx)
		return nil, err
	}
	log.Infof("workerId %d allocated to %s", e.workerId, e.addr)
	return e, nil
}

func (e *EtcdHolder) GetWorkerId(context.Context) (int64, error) {
	if e == nil || e.workerId < 0 {
		return 0, errors.New(utils.ErrInvalidWorkerId)
	}
	return e.workerId, nil
}

func (e *EtcdHolder) Valid(ts int64) bool {
	if e == nil || atomic.LoadInt32(&e.closed) != 0 || ts > atomic.LoadInt64(&e.validTs) || e.session == nil {
		return false
	}
	if time.Since(e.started) > time.Duration(atomic.LoadInt64(&e.validNs)) {
		return false
	}
	select {
	case <-e.session.Done():
		return false
	default:
		return true
	}
}

func (e *EtcdHolder) Close(context.Context) error {
	if e == nil || !atomic.CompareAndSwapInt32(&e.closed, 0, 1) {
		return nil
	}
	atomic.StoreInt64(&e.validTs, 0)
	atomic.StoreInt64(&e.validNs, 0)
	if e.cancel != nil {
		e.cancel()
	}
	if e.session != nil {
		e.session.Orphan()
	}
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

const (
	etcdLockPath   = "/snowflake/%s/locker"
	etcdActivePath = "/snowflake/%s/active"
	etcdFencePath  = "/snowflake/%s/fence"
	dialTimeout    = 10 * time.Second
	updateInterval = time.Second
	validInterval  = 3 * time.Second
	activeTTL      = 10
	minTTL         = 6
)

type etcdPath struct {
	Lock   string
	Active string
	Fence  string
}

func (e *EtcdHolder) init(ctx context.Context) error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   e.endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return err
	}
	e.client = cli

	active, err := e.loadActive(ctx)
	if err != nil {
		return err
	}
	fences, err := e.loadFences(ctx)
	if err != nil {
		return err
	}
	e.workerId, err = findWorker(active, fences, time.Now().UnixMilli(), e.maxId)
	if err != nil {
		return err
	}
	return e.start(ctx)
}

func findWorker(active map[int64]struct{}, fences map[int64]int64, now, maxId int64) (int64, error) {
	for id := int64(0); id <= maxId; id++ {
		if _, ok := active[id]; ok || fences[id] >= now {
			continue
		}
		return id, nil
	}
	return 0, errors.New(utils.ErrNoWorkerId)
}

func (e *EtcdHolder) loadActive(ctx context.Context) (map[int64]struct{}, error) {
	prefix := e.path.Active + "/"
	resp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	ids := make(map[int64]struct{}, resp.Count)
	for _, kv := range resp.Kvs {
		id, err := parseId(prefix, string(kv.Key), e.maxId)
		if err != nil {
			return nil, err
		}
		ids[id] = struct{}{}
	}
	return ids, nil
}

func (e *EtcdHolder) loadFences(ctx context.Context) (map[int64]int64, error) {
	prefix := e.path.Fence + "/"
	resp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	fences := make(map[int64]int64, resp.Count)
	for _, kv := range resp.Kvs {
		id, err := parseId(prefix, string(kv.Key), e.maxId)
		if err != nil {
			return nil, err
		}
		ts, err := strconv.ParseInt(string(kv.Value), 10, 64)
		if err != nil {
			return nil, errors.New(utils.ErrInvalidClockTime)
		}
		fences[id] = ts
	}
	return fences, nil
}

func parseId(prefix, key string, maxId int64) (int64, error) {
	if !strings.HasPrefix(key, prefix) {
		return 0, errors.New(utils.ErrInvalidNodeKey)
	}
	id, err := strconv.ParseInt(strings.TrimPrefix(key, prefix), 10, 64)
	if err != nil || id < 0 || id > maxId {
		return 0, errors.New(utils.ErrInvalidWorkerId)
	}
	return id, nil
}

func (e *EtcdHolder) start(ctx context.Context) error {
	s, err := concurrency.NewSession(e.client, concurrency.WithTTL(activeTTL), concurrency.WithContext(ctx))
	if err != nil {
		return err
	}
	lease := s.Lease()
	ttl, err := e.client.TimeToLive(ctx, lease)
	if err != nil || ttl.TTL < minTTL {
		_ = s.Close()
		if err != nil {
			return err
		}
		return errors.New(utils.ErrWorkerLeaseExpired)
	}

	e.activeKey = fmt.Sprintf("%s/%d", e.path.Active, e.workerId)
	e.fenceKey = fmt.Sprintf("%s/%d", e.path.Fence, e.workerId)
	e.token = fmt.Sprintf("%s/%d", e.addr, lease)
	validTs := time.Now().Add(validInterval).UnixMilli()
	resp, err := e.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(e.activeKey), "=", 0)).
		Then(
			clientv3.OpPut(e.activeKey, e.token, clientv3.WithLease(lease)),
			clientv3.OpPut(e.fenceKey, strconv.FormatInt(validTs, 10)),
		).
		Commit()
	if err != nil {
		_ = s.Close()
		return err
	}
	if !resp.Succeeded {
		_ = s.Close()
		return errors.New(utils.ErrWorkerIdInUse)
	}

	e.session = s
	e.setValid(validTs)
	e.schedule(ctx)
	return nil
}

func (e *EtcdHolder) schedule(ctx context.Context) {
	x, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	go func() {
		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()
		for {
			select {
			case <-x.Done():
				atomic.StoreInt64(&e.validTs, 0)
				atomic.StoreInt64(&e.validNs, 0)
				return
			case <-e.session.Done():
				atomic.StoreInt64(&e.validTs, 0)
				atomic.StoreInt64(&e.validNs, 0)
				return
			case <-ticker.C:
				if err := e.refresh(x); err != nil {
					log.Warnf("refresh workerId %d fence failed %+v", e.workerId, err)
				}
			}
		}
	}()
}

func (e *EtcdHolder) refresh(ctx context.Context) error {
	ttl, err := e.client.TimeToLive(ctx, e.session.Lease())
	if err != nil {
		return err
	}
	if ttl.TTL < minTTL {
		return errors.New(utils.ErrWorkerLeaseExpired)
	}
	validTs := time.Now().Add(validInterval).UnixMilli()
	resp, err := e.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(e.activeKey), "=", e.token)).
		Then(clientv3.OpPut(e.fenceKey, strconv.FormatInt(validTs, 10))).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		atomic.StoreInt64(&e.validTs, 0)
		atomic.StoreInt64(&e.validNs, 0)
		return errors.New(utils.ErrWorkerIdInUse)
	}
	e.setValid(validTs)
	return nil
}

func (e *EtcdHolder) setValid(ts int64) {
	atomic.StoreInt64(&e.validTs, ts)
	atomic.StoreInt64(&e.validNs, int64(time.Since(e.started)+validInterval))
}
