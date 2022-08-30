package storage

import (
	"context"

	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Locker struct {
	mutex *concurrency.Mutex
}

func NewLocker(ctx context.Context, path string, endpoints []string) (*Locker, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		log.Errorf("new etcd client err %+v", err)
		return nil, err
	}

	session, err := concurrency.NewSession(cli)
	if err != nil {
		log.Errorf("new session err %+v", err)
		return nil, err
	}

	return &Locker{
		mutex: concurrency.NewMutex(session, path),
	}, nil
}

func (l *Locker) Lock(ctx context.Context) error {
	return l.mutex.Lock(ctx)
}

func (l *Locker) UnLock(ctx context.Context) error {
	return l.mutex.Unlock(ctx)
}
