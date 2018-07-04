package db

import (
	"time"

	"golib/clickhouse-v2"
	"golib/logs"
	"golib/services/capping_consumer/app/statistics"
)

type widgetStatisticsSum struct {
	client *clickhouse.Cluster
}

func (w *widgetStatisticsSum) Worker() {
	for {
		statisticsSyncPeriod := time.Duration(appConfig.Mysql.StatisticsSavePeriod)
		time.Sleep(statisticsSyncPeriod * time.Minute)
		go w.Save()
	}
}

func (w *widgetStatisticsSum) Save() {

	data := statistics.GetWidgetStatisticsSumShows()
	defer logs.Recover()
	if len(data) == 0 {
		return
	}

	tableName := "statistics.widget_statistics_summ"
	columns := []string{
		"date",
		"hour",
		"site",
		"composite",
		"uid",
		"country",
		"device",
		"os",
		"traffic_source",
		"traffic_type",
		"subid",
		"real_shows",
		"browser",
		"type",
	}
	limit := appConfig.Clickhouse.StatisticsBlockSize
	values := make([]clickhouse.Row, 0, limit)

	for key, value := range data {
		val := clickhouse.Row{
			key.Date,
			key.Hour,
			key.Site,
			key.Composite,
			key.Uid,
			key.Country,
			key.Device,
			key.Os,
			key.TrafficSource,
			key.TrafficType,
			key.Subid,
			value.RealShows,
			key.BrowserId,
			key.Type,
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
