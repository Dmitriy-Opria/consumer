package db

import (
	"fmt"
	"strings"
	"time"

	"golib/logs"
	"golib/mysql-v2"
	"golib/services/capping_consumer/app/statistics"
)

type tickersCompositeSummStat struct {
	client mysql.IClient
}

func (t *tickersCompositeSummStat) Worker() {
	for {
		statisticsSyncPeriod := time.Duration(appConfig.Mysql.StatisticsSavePeriod)
		time.Sleep(statisticsSyncPeriod * time.Minute)
		go t.Save()
	}
}

func (t *tickersCompositeSummStat) Save() {

	data := statistics.GetTickersCompositeSummStat()
	defer logs.Recover()
	if len(data) == 0 {
		return
	}
	sqlStr := "INSERT INTO partners.tickers_composite_statistics_summary (tickers_composite_id, real_shows_all) "
	sqlODKU := " ON DUPLICATE KEY UPDATE real_shows_all= real_shows_all+VALUES(real_shows_all)"

	limit := appConfig.Mysql.StatisticsBlockSize
	values := make([]string, 0, limit)

	for key, val := range data {
		if val.RealShows == 0 {
			continue
		}
		values = append(values, fmt.Sprintf("(%d, %d)", key.TickersCompositeId, val.RealShows))
		if len(values) >= int(limit) {
			t.client.Exec(sqlStr + strings.Join(values, ",") + sqlODKU)
			values = values[:0]
		}
	}
	if len(values) > 0 {
		t.client.Exec(sqlStr + strings.Join(values, ",") + sqlODKU)
	}
}
