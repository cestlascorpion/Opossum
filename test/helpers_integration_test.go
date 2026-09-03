//go:build integration

package test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cestlascorpion/opossum/utils"
	mysqlDriver "github.com/go-sql-driver/mysql"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type testEnv struct {
	mysqlAddr string
	mysqlUser string
	mysqlPass string
	etcdAddr  string
}

type dbFixture struct {
	admin *sql.DB
	conf  *utils.Config
	name  string
}

var nameSeq uint64

func loadEnv(t *testing.T) testEnv {
	t.Helper()
	e := testEnv{
		mysqlAddr: env("OPOSSUM_TEST_MYSQL_ADDR", "127.0.0.1:33306"),
		mysqlUser: env("OPOSSUM_TEST_MYSQL_USER", "root"),
		mysqlPass: os.Getenv("OPOSSUM_TEST_MYSQL_PASSWORD"),
		etcdAddr:  env("OPOSSUM_TEST_ETCD_ENDPOINT", "127.0.0.1:32379"),
	}
	if _, _, err := net.SplitHostPort(e.mysqlAddr); err != nil {
		t.Fatalf("invalid MySQL address %q: %v", e.mysqlAddr, err)
	}
	return e
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func uniqueName(prefix string) string {
	seq := atomic.AddUint64(&nameSeq, 1)
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), seq)
}

func setupDB(t *testing.T, e testEnv, step int64) *dbFixture {
	t.Helper()
	driverConf := mysqlDriver.NewConfig()
	driverConf.User = e.mysqlUser
	driverConf.Passwd = e.mysqlPass
	driverConf.Net = "tcp"
	driverConf.Addr = e.mysqlAddr
	admin, err := sql.Open("mysql", driverConf.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = admin.PingContext(ctx); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	name := uniqueName("opossum_test")
	if _, err = admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = admin.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+name)
		_ = admin.Close()
	})
	createTable := fmt.Sprintf(`CREATE TABLE %s.opossum_alloc_test (
        biz_tag varchar(128) NOT NULL PRIMARY KEY,
        max_id bigint NOT NULL,
        step int NOT NULL,
        update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB`, name)
	if _, err = admin.ExecContext(ctx, createTable); err != nil {
		t.Fatal(err)
	}
	insert := fmt.Sprintf("INSERT INTO %s.opossum_alloc_test (biz_tag, max_id, step) VALUES ('order', 0, %d)", name, step)
	if _, err = admin.ExecContext(ctx, insert); err != nil {
		t.Fatal(err)
	}
	host, portText, _ := net.SplitHostPort(e.mysqlAddr)
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	ns := uniqueName("opossum")
	conf := &utils.Config{
		Segment: &utils.SgConf{Table: "test"},
		Snowflake: &utils.SnConf{
			Table: ns, Addr: "127.0.0.1", Port: 8080, Endpoints: e.etcdAddr,
			Mysql: &utils.DB{
				Host: host, Port: port, Protocol: "tcp", Database: name,
				UserName: e.mysqlUser, Password: e.mysqlPass, Charset: "utf8mb4",
			},
		},
	}
	cleanupEtcd(t, e.etcdAddr, ns)
	return &dbFixture{admin: admin, conf: conf, name: name}
}

func cleanupEtcd(t *testing.T, endpoint, ns string) {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	prefix := "/snowflake/" + ns
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err = cli.Delete(ctx, prefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		cli.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		_ = cli.Close()
	})
}
