package test

import (
	"strings"
	"testing"
	"time"

	"github.com/cestlascorpion/opossum/client"
	"github.com/cestlascorpion/opossum/utils"
)

func TestConfigStringMasksPassword(t *testing.T) {
	conf := &utils.Config{
		Segment: &utils.SgConf{Table: "test"},
		Snowflake: &utils.SnConf{
			Table: "test",
			Mysql: &utils.DB{UserName: "user", Password: "secret"},
		},
	}
	got := conf.String()
	if strings.Contains(got, "secret") || !strings.Contains(got, `"password":"******"`) {
		t.Fatalf("password was not masked: %s", got)
	}
	if conf.Snowflake.Mysql.Password != "secret" {
		t.Fatal("String mutated the source config")
	}
}

func TestDecodeSnowflake(t *testing.T) {
	const epoch = int64(1288834974657)
	tests := []struct {
		name     string
		ts       int64
		workerId int64
		sequence int64
	}{
		{name: "zero fields", ts: epoch},
		{name: "normal fields", ts: time.Now().UnixMilli(), workerId: 37, sequence: 1024},
		{name: "maximum fields", ts: epoch + 1, workerId: 1023, sequence: 4095},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := ((tt.ts - epoch) << 22) | (tt.workerId << 12) | tt.sequence
			tm, workerId, sequence := client.DecodeSnowflake(id)
			if tm.UnixMilli() != tt.ts || workerId != tt.workerId || sequence != tt.sequence {
				t.Fatalf("want (%d,%d,%d) got (%d,%d,%d)", tt.ts, tt.workerId, tt.sequence, tm.UnixMilli(), workerId, sequence)
			}
		})
	}
}
