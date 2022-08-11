package utils

import "fmt"

type SegmentConfig struct {
	Table    string `json:"table,omitempty"`
	MaxStep  int64  `json:"max_step,omitempty"`
	Duration int64  `json:"duration,omitempty"`
}

type SnowflakeConfig struct {
	Table         string       `json:"table,omitempty"`
	Ethernet      string       `json:"ethernet,omitempty"`
	Addr          string       `json:"addr,omitempty"`
	Port          int16        `json:"port,omitempty"`
	EtcdEndpoints string       `json:"etcd_endpoints,omitempty"`
	Mysql         *MySQLConfig `json:"mysql,omitempty"`
}

type MySQLConfig struct {
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	Protocol string `json:"protocol,omitempty"`
	Database string `json:"database,omitempty"`
	UserName string `json:"user_name,omitempty"`
	Password string `json:"password,omitempty"`
	Charset  string `json:"charset,omitempty"`
}

type OpossumConfig struct {
	Segment   *SegmentConfig
	Snowflake *SnowflakeConfig
}

func NewOpossumConfigForMock() (*OpossumConfig, error) {
	return &OpossumConfig{
		Segment: &SegmentConfig{
			Table:    "for_test",
			MaxStep:  1000000,
			Duration: 15 * 60 * 1000,
		},
		Snowflake: &SnowflakeConfig{
			Table:         "for_test",
			Ethernet:      "eno1",
			Port:          9090,
			EtcdEndpoints: "127.0.0.1:2379",
			Mysql: &MySQLConfig{
				Host:     "localhost",
				Port:     3306,
				Protocol: "tcp",
				Database: "leaf",
				UserName: "hans",
				Password: "123456",
				Charset:  "utf8",
			},
		},
	}, nil
}

func (o *OpossumConfig) MySQLSourceName() string {
	mysql := o.Snowflake.Mysql
	return fmt.Sprintf("%s:%s@%s(%s:%d)/%s?charset=%s&parseTime=true&loc=Local",
		mysql.UserName,
		mysql.Password,
		mysql.Protocol,
		mysql.Host,
		mysql.Port,
		mysql.Database,
		mysql.Charset)
}
