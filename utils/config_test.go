package utils

import (
	"fmt"
	"testing"

	"github.com/jinzhu/configor"
)

func TestNewConfig(t *testing.T) {
	conf := &Config{}
	err := configor.Load(conf, "./conf.json")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(conf)
}
