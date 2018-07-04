package db

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"golib/logs"
)

const rotate_date_key = "g_blocks_rotate_date"

var (
	widgetRotateDate int64
	updateAfter      int64
	hasSqlErrors     int32
)

var hostName = "unknown"

func init() {
	setupHostname()
}

func setupHostname() {
	var err error
	for {
		hostName, err = os.Hostname()
		if err != nil {
			logs.Critical("Fail to get hostname, err: ", err.Error())
			SetUpdateDelay(1 * time.Minute)
			time.Sleep(5 * time.Second)
		} else {
			return
		}
	}
}

func SetUpdateDelay(d time.Duration) {
	atomic.StoreInt64(&updateAfter, time.Now().Add(d).Unix())
}

func UpdateConsumerLag(unixTime int64) {

	defer logs.Recover()

	key := fmt.Sprintf("capping_consumer_lag_%s", hostName)

	_, err := mysqlClient.Exec("INSERT INTO current_status (`key`,`host_name`, `value`) VALUES (?,?,?) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)",
		key, hostName, unixTime)

	if err != nil {
		logs.Critical(fmt.Sprintf("Critical: Cannot insert into partners.current_status, error \"%s\"", err.Error()))
		return
	}
}

func UpdateCurrentStatus() {

	if atomic.LoadInt32(&hasSqlErrors) != 0 {
		return
	}

	if atomic.LoadInt64(&updateAfter) > time.Now().Unix() {
		return
	}

	defer logs.Recover()

	sqlStr := fmt.Sprintf("INSERT INTO current_status (`key`,`host_name`, `value`, `visible`, `type`, `critical`, `description`) "+
		"VALUES ('capping_consumer_%s','%s', UNIX_TIMESTAMP(), 1, 'time', 120, 'Capping consumer') "+
		"ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)", hostName, hostName)

	if _, err := mysqlClient.Exec(sqlStr); err != nil {

		logs.Critical(fmt.Sprintf("Critical: Cannot insert into partners.current_status, error %q", err.Error()))
	}
}

func updateWidgetRotateDate() int64 {

	defer logs.Recover()

	sqlStr := "select unix_timestamp(when_change) from current_status where `key`=?"
	rows, err := mysqlClient.Query(sqlStr, rotate_date_key)
	if err != nil {
		logs.Critical(fmt.Sprintf("Critical: Cannot update `%s`, error \"%s\"", rotate_date_key, err.Error()))
		return 0
	}
	defer rows.Close()

	if rows.Next() {
		var newtm int64
		rows.Scan(&newtm)
		atomic.CompareAndSwapInt64(&widgetRotateDate, widgetRotateDate, newtm)
	}
	return widgetRotateDate
}

func isToday() bool {
	serverDate := time.Now().Format("2006-01-02")
	dbDate := time.Unix(updateWidgetRotateDate(), 0).Format("2006-01-02")
	return serverDate == dbDate
}
