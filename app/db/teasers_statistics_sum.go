package db

import (
	"time"

	"golib/clickhouse-v2"
	"golib/logs"
	"golib/services/capping_consumer/app/statistics"
)

type teaserStatisticsSum struct {
	client *clickhouse.Cluster
}

func (w *teaserStatisticsSum) Worker() {
	for {
		statisticsSyncPeriod := time.Duration(appConfig.Mysql.StatisticsSavePeriod)
		time.Sleep(statisticsSyncPeriod * time.Minute)
		go w.Save()
	}
}

func (w *teaserStatisticsSum) Save() {

	data := statistics.GetTeaserStatisticsSumShows()
	defer logs.Recover()
	if len(data) == 0 {
		return
	}

	tableName := "statistics.teaser_statistics_summ"
	columns := []string{
		"date",
		"hour",
		"teaser",
		"teaser_category",
		"campaign",
		"client",
		"composite",
		"uid",
		"subid",
		"country",
		"region",
		"os",
		"device",
		"browser",
		"provider",
		"subnet",
		"real_shows",
	}
	limit := appConfig.Clickhouse.StatisticsBlockSize
	values := make([]clickhouse.Row, 0, limit)

	for key, value := range data {
		val := clickhouse.Row{
			key.Date,
			key.Hour,
			key.TeaserID,
			key.TeaserCategory,
			key.Campaign,
			key.ClientID,
			key.CompositeID,
			key.UID,
			key.SubID,
			key.CountryID,
			key.RegionID,
			key.Os,
			key.DeviceType,
			key.BrowserID,
			key.Provider,
			key.Subnet,
			value.RealShows,
		}
		values = append(values, val)
		if len(values) >= int(limit) {
			w.client.Insert(tableName, columns, values, true)
			values = values[:0]
		}
	}

	if len(values) > 0 {
		w.client.Insert(tableName, columns, values, true)
	}
}
