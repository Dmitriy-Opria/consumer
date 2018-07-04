package db

import (
	"fmt"
	"strings"
	"time"

	"golib/logs"
	"golib/mysql-v2"
	"golib/services/capping_consumer/app/statistics"
)

type teaserRealShowsStat struct {
	client mysql.IClient
}

func (t *teaserRealShowsStat) Worker() {
	for {
		statisticsSyncPeriod := time.Duration(appConfig.Mysql.StatisticsSavePeriod)
		time.Sleep(statisticsSyncPeriod * time.Minute)
		go t.Save()
	}
}

func (t *teaserRealShowsStat) Save() {
	if !isToday() {
		return
	}

	data := statistics.GetTeaserRealShows()
	defer logs.Recover()
	if len(data) == 0 {
		return
	}
	sqlStr := "INSERT IGNORE INTO g_hits_1_statistics (id,real_shows_today,real_shows_all) VALUES "
	sqlODKU := " ON DUPLICATE KEY UPDATE real_shows_today = real_shows_today + VALUES (real_shows_today), real_shows_all = real_shows_all + VALUES (real_shows_all)"

	limit := appConfig.Mysql.StatisticsBlockSize
	values := make([]string, 0, limit)

	for key, val := range data {
		if val.Shows == 0 {
			continue
		}

		values = append(values, fmt.Sprintf("(%d,%d,%d)", key.TeaserId, val.Shows, val.Shows))
		if len(values) >= int(limit) {
			t.client.Exec(sqlStr + strings.Join(values, ",") + sqlODKU)
			values = values[:0]
		}
	}
	if len(values) > 0 {
		t.client.Exec(sqlStr + strings.Join(values, ",") + sqlODKU)
	}
}
