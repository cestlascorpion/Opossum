# Opossum

Opossum is a distributed Id service written in Go and exposed through gRPC. Its
design is based on [Meituan Leaf](https://github.com/Meituan-Dianping/Leaf), and
it supports both segment and Snowflake Id generation.

## Features

- Segment mode
  - Stores the maximum value and allocation step for each business tag in MySQL
  - Prefetches segments in memory and refills the buffer at a low-water mark
  - Uses database transactions to prevent overlapping segments across instances
- Snowflake mode
  - Generates 64-bit, roughly time-ordered Ids
  - Allocates and persists workerId values through etcd
  - Uses leased keys to prevent live instances from sharing a workerId
  - Decodes the generation time, workerId, and sequence number from an Id
- gRPC API
  - `GetSegment`
  - `GetSnowflake`
  - `DecodeSnowflake`

## Requirements

- Go 1.18 or later
- MySQL 5.7 or later
- etcd 3.5
- `protoc` and its Go plugins when regenerating protocol code

The server always listens on `:8080`. The built-in Go client always connects to
`127.0.0.1:8080`.

## Quick Start

### 1. Create the MySQL table

`segment.table` is the table suffix. For example, a value of `test` makes
Opossum use the table `leaf_alloc_test`.

```sql
CREATE DATABASE IF NOT EXISTS leaf;

CREATE TABLE leaf.leaf_alloc_test (
    biz_tag varchar(128) NOT NULL DEFAULT '',
    max_id bigint NOT NULL DEFAULT 1,
    step int NOT NULL,
    update_time timestamp NOT NULL
        DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (biz_tag)
) ENGINE=InnoDB;

INSERT INTO leaf.leaf_alloc_test (biz_tag, max_id, step)
VALUES ('order', 0, 10000);
```

Each `biz_tag` defines an independent Id space. The `step` value determines how
many Ids the service obtains from MySQL in each allocation.

### 2. Start etcd

Opossum uses etcd to store workerId mappings and instance leases. The default
example connects to `127.0.0.1:2379`.

Check that etcd is available:

```bash
etcdctl --endpoints=127.0.0.1:2379 endpoint health
```

### 3. Create the configuration

The server reads `conf.json` from its current working directory. Create the file
in the repository root:

```json
{
  "segment": {
    "table": "test"
  },
  "snowflake": {
    "table": "opossum",
    "addr": "192.168.1.10",
    "port": 8080,
    "endpoints": "127.0.0.1:2379",
    "mysql": {
      "host": "127.0.0.1",
      "port": 3306,
      "protocol": "tcp",
      "database": "leaf",
      "username": "opossum",
      "password": "change-me",
      "charset": "utf8mb4"
    }
  }
}
```

Configuration fields:

| Field | Description |
| --- | --- |
| `segment.table` | MySQL table suffix |
| `snowflake.table` | etcd namespace |
| `snowflake.addr` | Stable address of this instance |
| `snowflake.ethernet` | Network interface used to discover an address when `addr` is empty |
| `snowflake.port` | Instance port, also used as part of the workerId identity |
| `snowflake.endpoints` | Comma-separated etcd endpoints |
| `snowflake.mysql` | MySQL connection used by segment mode |

The combination of `addr` and `port` must be unique among live instances.
Production deployments should configure `addr` explicitly to avoid selecting a
loopback or temporary network interface.

### 4. Start the server

Run the server from the repository root containing `conf.json`:

```bash
go mod download
go run ./server
```

The server listens on `0.0.0.0:8080` and enables gRPC reflection.

## Go Client

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cestlascorpion/opossum/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c, err := client.NewClient(
		ctx,
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

	snowflakeId, err := c.GetSnowflake(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	tm, workerId, sequence, err := c.DecodeSnowflake(
		context.Background(),
		snowflakeId,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(segmentId)
	fmt.Println(snowflakeId, tm, workerId, sequence)
}
```

Use a context with a deadline when creating a client. When `grpc.WithBlock()`
is enabled, a connection attempt will wait indefinitely if the server is
unavailable and the context has no deadline.

## gRPC API

| Method | Request | Response |
| --- | --- | --- |
| `GetSegment` | Business tag in `key` | Segment Id |
| `GetSnowflake` | Empty request | Snowflake Id |
| `DecodeSnowflake` | Snowflake Id | Generation time, workerId, and sequence number |

The protocol is defined in [`proto/opossum.proto`](proto/opossum.proto).

## Regenerate Protocol Code

Install the generators:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
```

Run the script from the `proto` directory:

```bash
cd proto
./gen.sh
```

The generated files are `proto/opossum.pb.go` and
`proto/opossum_grpc.pb.go`.

## Validation

Compile every package without connecting to external services:

```bash
go test -run '^$' ./...
go vet ./...
```

The full test suite contains integration tests that connect to local MySQL,
etcd, and `127.0.0.1:8080`. Prepare an isolated test environment before
running:

```bash
go test ./...
```

Some integration tests modify `max_id` in the test table. Do not run them
against a production database.

## Project Layout

```text
client/   Go client
proto/    Protobuf definitions and generated code
server/   gRPC server entry point
service/  Segment and Snowflake Id implementations
storage/  MySQL and etcd storage
utils/    Configuration and shared types
```

## License

[MIT](LICENSE)
