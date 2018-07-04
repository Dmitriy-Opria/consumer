package db

import (
	"fmt"
	"strings"
	"time"

	"golib/logs"
	"golib/mysql-v2"
	"golib/services/capping_consumer/app/statistics"
)

type gPartners struct {
	client mysql.IClient
}

func (g *gPartners) Worker() {
	for {
		statisticsSyncPeriod := time.Duration(appConfig.Mysql.StatisticsSavePeriod)
		time.Sleep(statisticsSyncPeriod * time.Minute)
		go g.Save()
	}
}

func (g *gPartners) Save() {
	data := statistics.GetGPartnersStat()
	defer logs.Recover()
	if len(data) == 0 {
		return
	}

	sqlStr := "UPDATE g_partners_1 SET real_shows_today=real_shows_today+%d, real_shows_all=real_shows_all+%d WHERE id=%d"

	limit := appConfig.Mysql.StatisticsBlockSize

	queries := make([]string, 0, limit)

	for key, value := range data {
		queries = append(queries, fmt.Sprintf(sqlStr, value.Shows, value.Shows, key.ID))
		if len(queries) >= int(limit) {
			g.client.Exec(strings.Join(queries, "; "))
			queries = queries[:0]
		}
	}
	if len(queries) > 0 {
		g.client.Exec(strings.Join(queries, "; "))
	}
}
