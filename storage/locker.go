package storage

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
)

type Locker struct {
	mutex   *concurrency.Mutex
	session *concurrency.Session
	client  *clientv3.Client
}

func NewLocker(ctx context.Context, path string, endpoints []string) (*Locker, error) {
	cli, err := newEtcd(ctx, endpoints)
	if err != nil {
		log.Errorf("new etcd client err %+v", err)
		return nil, err
	}

	session, err := concurrency.NewSession(cli, concurrency.WithContext(ctx))
	if err != nil {
		log.Errorf("new session err %+v", err)
		_ = cli.Close()
		return nil, err
	}

	return &Locker{
		mutex:   concurrency.NewMutex(session, path),
		session: session,
		client:  cli,
	}, nil
}

func newEtcd(ctx context.Context, endpoints []string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
		Context:     ctx,
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
			grpc.WithChainUnaryInterceptor(timeoutUnary(dialTimeout)),
		},
	})
}

func timeoutUnary(d time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if _, ok := ctx.Deadline(); ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		x, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return invoker(x, method, req, reply, cc, opts...)
	}
}

func (l *Locker) Lock(ctx context.Context) error {
	return l.mutex.Lock(ctx)
}

func (l *Locker) UnLock(ctx context.Context) error {
	return l.mutex.Unlock(ctx)
}

func (l *Locker) Close() error {
	if l == nil {
		return nil
	}
	if l.session != nil {
		_ = l.session.Close()
	}
	if l.client != nil {
		return l.client.Close()
	}
	return nil
}
