# Opossum

中文 | [English](README.md)

> 实验性软件 Opossum 是用于验证分布式 Id 分配方案的原型 API 存储结构和行为可能随时变化 请勿用于生产数据

Opossum 是一个使用 Go 编写并通过 gRPC 提供服务的分布式 Id 系统 支持数据库号段分配和 Snowflake Id 生成

## 功能

- Segment 模式通过 MySQL 分配互不重叠的 `[start, end)` 区间
- Go 客户端缓存 Segment 区间并在本地逐个返回 Id
- Snowflake 模式通过 etcd 自动分配和回收 workerId
- 使用带租约的活跃节点和持久化时间围栏保护 workerId 复用
- 单次 Snowflake 请求可生成 1 到 4096 个 Id
- Snowflake 解码在客户端本地完成 无需 RPC

## 环境要求

- Go 1.18 或更高版本
- MySQL 5.7 或更高版本
- etcd 3.5 或更高版本
- 重新生成协议代码时需要 `protoc` 及其 Go 插件

服务端监听 `:8080`

## 快速开始

### 1 创建 MySQL 表

`segment.table` 是表名后缀 设置为 `test` 时使用 `opossum_alloc_test`

```sql
CREATE DATABASE IF NOT EXISTS opossum;

CREATE TABLE opossum.opossum_alloc_test (
    biz_tag varchar(128) NOT NULL DEFAULT '',
    max_id bigint NOT NULL DEFAULT 1,
    step int NOT NULL,
    update_time timestamp NOT NULL
        DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (biz_tag)
) ENGINE=InnoDB;

INSERT INTO opossum.opossum_alloc_test (biz_tag, max_id, step)
VALUES ('order', 0, 10000);
```

每个 `biz_tag` 拥有独立的 Id 空间 `step` 决定每次分配给客户端的区间大小

### 2 启动 etcd

Opossum 在 etcd 中保存活跃 workerId 租约和持久化时间围栏 默认端点为 `127.0.0.1:2379`

```bash
etcd
etcdctl --endpoints=127.0.0.1:2379 endpoint health
```

### 3 创建配置

服务端从工作目录读取 `conf.json`

```json
{
  "segment": {
    "table": "test"
  },
  "snowflake": {
    "table": "opossum",
    "addr": "127.0.0.1",
    "port": 8080,
    "endpoints": "127.0.0.1:2379",
    "mysql": {
      "host": "127.0.0.1",
      "port": 3306,
      "protocol": "tcp",
      "database": "opossum",
      "username": "opossum",
      "password": "change-me",
      "charset": "utf8mb4"
    }
  }
}
```

| 字段 | 说明 |
| --- | --- |
| `segment.table` | 仅允许字母 数字和下划线的 MySQL 表名后缀 |
| `snowflake.table` | etcd namespace |
| `snowflake.addr` | 记录在租约所有者信息中的地址 |
| `snowflake.ethernet` | `addr` 为空时用于发现地址的网卡 |
| `snowflake.port` | 记录在租约所有者信息中的端口 |
| `snowflake.endpoints` | 逗号分隔的 etcd 端点 |
| `snowflake.mysql` | Segment 模式使用的 MySQL 连接 |

地址仅作为诊断信息 不参与 workerId 分配

启动时会校验 MySQL 连通性和 etcd 可用性 每个初始化检查最多等待 10 秒 避免进程无限阻塞

Snowflake workerId 使用 10 位 有效范围为 `0` 到 `1023` 每个 `snowflake.table` namespace 最多支持 1024 个并发活跃 worker workerId 会在租约失效且时间围栏到期后重新分配

### 4 启动服务端

```bash
go run ./server
```

## Go 客户端

```go
ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
defer cancel()

c, err := client.NewClient(
    ctx,
    "127.0.0.1:8080",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithBlock(),
)
if err != nil {
    log.Fatal(err)
}
defer c.Close()

segmentId, err := c.GetSegment(context.Background(), "order")
if err != nil {
    log.Fatal(err)
}

snowflakeIds, err := c.GetSnowflakes(context.Background(), 128)
if err != nil {
    log.Fatal(err)
}

tm, workerId, sequence := client.DecodeSnowflake(snowflakeIds[0])
```

连接和 RPC context 应设置超时时间

## gRPC API

| 方法 | 请求 | 响应 |
| --- | --- | --- |
| `AllocSegment` | `key` 中的业务标签 | `[start, end)` Id 区间 |
| `GetSnowflakes` | 1 到 4096 的 `count` | Snowflake Id 列表 |

协议定义位于 [`proto/opossum.proto`](proto/opossum.proto)

## 重新生成协议代码

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
cd proto
./gen.sh
```

## 测试

运行单元测试和静态检查

```bash
go test ./...
go vet ./...
```

使用临时 MySQL 和 etcd 实例运行完整测试

```bash
./test/run.sh
```

脚本要求 `mysqld` `mysqladmin` `etcd` 和 `etcdctl` 位于 `PATH` 中 脚本会创建隔离数据目录 在 `127.0.0.1:33306` 启动 MySQL 并在 `127.0.0.1:32379` 启动 etcd 然后使用竞态检测器执行集成测试 最后停止两个进程并删除临时数据 脚本不会调用 `brew services` 也不会配置组件随机器启动

如需使用已经运行的测试实例 可以设置以下环境变量后直接执行带标签的测试包

```bash
OPOSSUM_TEST_MYSQL_ADDR=127.0.0.1:33306 \
OPOSSUM_TEST_MYSQL_USER=root \
OPOSSUM_TEST_MYSQL_PASSWORD= \
OPOSSUM_TEST_ETCD_ENDPOINT=127.0.0.1:32379 \
go test -race -count=1 -timeout=90s -tags=integration ./test
```

## 项目结构

```text
client/   Go 客户端
proto/    协议定义和生成代码
server/   gRPC 服务入口
service/  Segment 和 Snowflake 实现
storage/  MySQL 和 etcd 存储
test/     单元测试 集成测试和隔离测试脚本
utils/    配置和共享类型
```

## License

[MIT](LICENSE)
