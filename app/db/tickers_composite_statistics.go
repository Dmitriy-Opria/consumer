package db

import (
	"fmt"
	"strings"
	"time"

	"golib/logs"
	"golib/mysql-v2"
	"golib/services/capping_consumer/app/statistics"
)

type tickersCompositeStat struct {
	client mysql.IClient
}

func (t *tickersCompositeStat) Worker() {
	for {
		statisticsSyncPeriod := time.Duration(appConfig.Mysql.StatisticsSavePeriod)
		time.Sleep(statisticsSyncPeriod * time.Minute)
		go t.Save()
	}
}

func (t *tickersCompositeStat) Save() {

	data := statistics.GetTickersCompositeStat()
	defer logs.Recover()
	if len(data) == 0 {
		return
	}
	sqlStr := "INSERT INTO statistics.tickers_composite_statistics (tickers_composite_id, date, real_shows) VALUES "
	sqlODKU := " ON DUPLICATE KEY UPDATE real_shows = real_shows+VALUES(real_shows)"

	limit := appConfig.Mysql.StatisticsBlockSize
	values := make([]string, 0, limit)

	for key, val := range data {
		if val.RealShows == 0 {
			continue
		}
		values = append(values, fmt.Sprintf("(%d,'%s',%d)", key.TickersCompositeId, key.Date, val.RealShows))
		if len(values) >= int(limit) {
			t.client.Exec(sqlStr + strings.Join(values, ",") + sqlODKU)
			values = values[:0]
		}
	}
	if len(values) > 0 {
		t.client.Exec(sqlStr + strings.Join(values, ",") + sqlODKU)
	}
}
