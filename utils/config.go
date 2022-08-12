package utils

import (
	"encoding/json"
	"fmt"
)

type SgConf struct {
	Table    string `json:"table,omitempty"`
	MaxStep  int64  `json:"max_step,omitempty"`
	Duration int64  `json:"duration,omitempty"`
}

type SnConf struct {
	Table     string `json:"table,omitempty"`
	Ethernet  string `json:"ethernet,omitempty"`
	Addr      string `json:"addr,omitempty"`
	Port      int16  `json:"port,omitempty"`
	Endpoints string `json:"endpoints,omitempty"`
	Mysql     *DB    `json:"mysql,omitempty"`
}

type DB struct {
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	Protocol string `json:"protocol,omitempty"`
	Database string `json:"database,omitempty"`
	UserName string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Charset  string `json:"charset,omitempty"`
}

type Config struct {
	Segment   *SgConf `json:"segment,omitempty"`
	Snowflake *SnConf ` json:"snowflake,omitempty"`
}

func NewConfigForTest() (*Config, error) {
	return &Config{
		Segment: &SgConf{
			Table:    "for_test",
			MaxStep:  1000000,
			Duration: 15 * 60 * 1000,
		},
		Snowflake: &SnConf{
			Table:     "for_test",
			Ethernet:  "eno1",
			Port:      9090,
			Endpoints: "127.0.0.1:2379",
			Mysql: &DB{
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

func (o *Config) MySQLSourceName() string {
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

func (o *Config) String() string {
	bs, err := json.Marshal(o)
	if err != nil {
		return ""
	}
	return string(bs)
}
