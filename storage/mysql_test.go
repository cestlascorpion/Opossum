package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/cestlascorpion/opossum/utils"
	log "github.com/sirupsen/logrus"
)

var bench *MySQL

func init() {
	log.SetLevel(log.DebugLevel)

	conf, err := utils.NewOpossumConfigForMock()
	if err != nil {
		fmt.Println(err)
	}
	mysql, err := NewMySQL(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
	}
	bench = mysql
}

func TestMySQL_GetAllLeafAllocs(t *testing.T) {
	conf, err := utils.NewOpossumConfigForMock()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	mysql, err := NewMySQL(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer mysql.Close(context.Background())

	result, err := mysql.GetAllLeafAllocs(context.Background())
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	for i := range result {
		fmt.Printf("%+v\n", result[i])
	}
}

func BenchmarkMySQL_GetAllLeafAllocs(b *testing.B) {
	if bench == nil {
		return
	}

	createData()
	defer deleteData()

	for i := 0; i < b.N; i++ {
		_, err := bench.GetAllLeafAllocs(context.Background())
		if err != nil {
			fmt.Println(err)
		}
	}
}

func TestMySQL_UpdateMaxIdAndGetLeafAlloc(t *testing.T) {
	conf, err := utils.NewOpossumConfigForMock()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	mysql, err := NewMySQL(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer mysql.Close(context.Background())

	result, err := mysql.UpdateMaxIdAndGetLeafAlloc(context.Background(), "test_tag")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Printf("%+v", result)
}

func BenchmarkMySQL_UpdateMaxIdAndGetLeafAlloc(b *testing.B) {
	if bench == nil {
		return
	}

	createData()
	defer deleteData()

	for i := 0; i < b.N; i++ {
		_, err := bench.UpdateMaxIdAndGetLeafAlloc(context.Background(), "test_tag")
		if err != nil {
			fmt.Println(err)
		}
	}
}

func TestMySQL_UpdateMaxIdByCustomStepAndGetLeafAlloc(t *testing.T) {
	conf, err := utils.NewOpossumConfigForMock()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	mysql, err := NewMySQL(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer mysql.Close(context.Background())

	result, err := mysql.UpdateMaxIdByCustomStepAndGetLeafAlloc(context.Background(), "test_tag", 10)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Printf("%+v", result)
}

func BenchmarkMySQL_UpdateMaxIdByCustomStepAndGetLeafAlloc(b *testing.B) {
	if bench == nil {
		return
	}

	for i := 0; i < b.N; i++ {
		_, err := bench.UpdateMaxIdByCustomStepAndGetLeafAlloc(context.Background(), "test_tag", 10)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func TestMySQL_GetAllTag(t *testing.T) {
	conf, err := utils.NewOpossumConfigForMock()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	mysql, err := NewMySQL(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer mysql.Close(context.Background())

	result, err := mysql.GetAllTags(context.Background())
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	for i := range result {
		fmt.Printf("%+v\n", result[i])
	}
}

func BenchmarkMySQL_GetAllTags(b *testing.B) {
	if bench == nil {
		return
	}

	createData()
	defer deleteData()

	for i := 0; i < b.N; i++ {
		_, err := bench.GetAllTags(context.Background())
		if err != nil {
			fmt.Println(err)
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------

func createData() {
	sql := `insert into leaf_alloc_for_test (biz_tag, max_id, step) values ('test_biz_%s', 10, 2) on duplicate key update max_id=10`

	for i := 'a'; i <= 'z'; i++ {
		_, err := bench.DB.ExecContext(context.Background(), fmt.Sprintf(sql, string(byte(i))))
		if err != nil {
			fmt.Println(err)
		}
	}
}

func deleteData() {
	sql := `delete from leaf_alloc_for_test where biz_tag='test_biz_%s'`

	for i := 'a'; i <= 'z'; i++ {
		_, err := bench.DB.ExecContext(context.Background(), fmt.Sprintf(sql, string(byte(i))))
		if err != nil {
			fmt.Println(err)
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------

/*
DROP TABLE IF EXISTS `leaf_alloc_for_test`;

CREATE TABLE `leaf_alloc_for_test` (
  `biz_tag` varchar(128)  NOT NULL DEFAULT '',
  `max_id` bigint(20) NOT NULL DEFAULT '1',
  `step` int(11) NOT NULL,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`biz_tag`)
) ENGINE=InnoDB;
*/

// ---------------------------------------------------------------------------------------------------------------------
