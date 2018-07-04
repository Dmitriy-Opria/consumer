package db

import (
	"fmt"
	"strings"
	"time"

	"golib/logs"
	"golib/mysql-v2"
	"golib/services/capping_consumer/app/statistics"
)

type subnetsRealShows struct {
	client mysql.IClient
}

func (s *subnetsRealShows) Worker() {
	for {
		statisticsSyncPeriod := time.Duration(appConfig.Mysql.StatisticsSavePeriod)
		time.Sleep(statisticsSyncPeriod * time.Minute)
		go s.Save()
	}
}

func (s *subnetsRealShows) Save() {

	data := statistics.GetSubnetRealShowsShows()
	defer logs.Recover()
	if len(data) == 0 {
		return
	}
	sqlStr := "INSERT IGNORE INTO statistics.geo_zones_subnets_history (geo_zones_id, subnets_id, date, real_shows) VALUES "
	sqlODKU := " ON DUPLICATE KEY UPDATE real_shows = real_shows + VALUES(real_shows)"

	limit := appConfig.Mysql.StatisticsBlockSize
	values := make([]string, 0, limit)

	for key, val := range data {
		if val.AllShows == 0 {
			continue
		}
		values = append(values, fmt.Sprintf("(%d,%d,%q,%d)", key.GeoZoneId, key.SubnetId, key.Day, val.AllShows))
		if len(values) >= int(limit) {
			s.client.Exec(sqlStr + strings.Join(values, ",") + sqlODKU)
			values = values[:0]
		}
	}
	if len(values) > 0 {
		s.client.Exec(sqlStr + strings.Join(values, ",") + sqlODKU)
	}
}
