# Opossum

[中文](README.zh-CN.md) | English

> Experimental software. Opossum is a prototype for validating distributed Id
> allocation ideas. Its API, storage layout, and behavior may change without
> notice. Do not use it for production data.

Opossum is a distributed Id service written in Go and exposed through gRPC. It
supports database-backed segment allocation and Snowflake Id generation.

## Features

- Segment mode allocates non-overlapping `[start, end)` ranges in MySQL
- The Go client caches segment ranges and returns individual Ids locally
- Snowflake mode automatically allocates and recycles workerIds through etcd
- Leased active keys and persistent time fences protect workerId reuse
- A Snowflake request can generate from 1 through 4096 Ids
- Snowflake decoding runs locally without an RPC

## Requirements

- Go 1.18 or later
- MySQL 5.7 or later
- etcd 3.5 or later
- `protoc` and its Go plugins when regenerating protocol code

The server listens on `:8080`.

## Quick Start

### 1. Create the MySQL table

`segment.table` is the table suffix. A value of `test` selects
`opossum_alloc_test`.

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

Each `biz_tag` has an independent Id space. `step` controls the size of each
range allocated to a client.

### 2. Start etcd

Opossum stores active workerId leases and persistent time fences in etcd. The
default endpoint is `127.0.0.1:2379`.

```bash
etcd
etcdctl --endpoints=127.0.0.1:2379 endpoint health
```

### 3. Create the configuration

The server reads `conf.json` from its working directory:

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

| Field | Description |
| --- | --- |
| `segment.table` | MySQL table suffix |
| `snowflake.table` | etcd namespace |
| `snowflake.addr` | Address recorded for the lease owner |
| `snowflake.ethernet` | Interface used to discover an address when `addr` is empty |
| `snowflake.port` | Port recorded for the lease owner |
| `snowflake.endpoints` | Comma-separated etcd endpoints |
| `snowflake.mysql` | MySQL connection used by segment mode |

The address is diagnostic metadata and does not determine the workerId.

Snowflake workerIds use 10 bits and range from `0` through `1023`. Each
`snowflake.table` namespace supports up to 1024 concurrently active workers. A
workerId becomes reusable after its lease expires and its time fence passes.

### 4. Start the server

```bash
go run ./server
```

## Go Client

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

Use deadlines for connection and RPC contexts.

## gRPC API

| Method | Request | Response |
| --- | --- | --- |
| `AllocSegment` | Business tag in `key` | A `[start, end)` Id range |
| `GetSnowflakes` | `count` from 1 through 4096 | Snowflake Id list |

The protocol is defined in [`proto/opossum.proto`](proto/opossum.proto).

## Regenerate Protocol Code

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
cd proto
./gen.sh
```

## Testing

Run the unit tests and static checks:

```bash
go test ./...
go vet ./...
```

Run the complete suite with temporary MySQL and etcd instances:

```bash
./test/run.sh
```

The script requires `mysqld`, `mysqladmin`, `etcd`, and `etcdctl` on `PATH`. It
creates isolated data directories, starts MySQL on `127.0.0.1:33306` and etcd on
`127.0.0.1:32379`, runs the integration tests with the race detector, and then
stops both processes and removes their data. It does not call `brew services` or
configure either component to start with the machine.

To use existing test instances instead, set the following variables and run the
tagged package directly:

```bash
OPOSSUM_TEST_MYSQL_ADDR=127.0.0.1:33306 \
OPOSSUM_TEST_MYSQL_USER=root \
OPOSSUM_TEST_MYSQL_PASSWORD= \
OPOSSUM_TEST_ETCD_ENDPOINT=127.0.0.1:32379 \
go test -race -count=1 -timeout=90s -tags=integration ./test
```

## Project Layout

```text
client/   Go client
proto/    Protocol definitions and generated code
server/   gRPC server entry point
service/  Segment and Snowflake implementations
storage/  MySQL and etcd storage
test/     Unit tests integration tests and the isolated test runner
utils/    Configuration and shared types
```

## License

[MIT](LICENSE)
