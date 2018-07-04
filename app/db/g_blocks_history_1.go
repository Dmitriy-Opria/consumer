package db

import (
	"fmt"
	"strings"
	"time"

	"golib/logs"
	"golib/mysql-v2"
	"golib/services/capping_consumer/app/statistics"
)

type gBlocksHistory struct {
	client mysql.IClient
}

func (g *gBlocksHistory) Worker() {
	for {
		statisticsSyncPeriod := time.Duration(appConfig.Mysql.StatisticsSavePeriod)
		time.Sleep(statisticsSyncPeriod * time.Minute)
		go g.Save()
	}
}

func (g *gBlocksHistory) Save() {

	data := statistics.GetGBlocksShows()
	defer logs.Recover()
	if len(data) == 0 {
		return
	}

	sqlStr := "INSERT INTO `statistics`.`g_blocks_history_1` (" +
		"`id`, " +
		"`uid`, " +
		"`date`, " +
		"`parent`, " +
		"`client_id`, " +
		"`contracts_type`, " +
		"`price`, " +
		"`revenue_proc`, " +
		"`client_site_id`, " +
		"`agent_ghits_enabled`, " +
		"`inviter`, " +
		"`curator`, " +
		"`shows`, " +
		"`real_shows`, " +
		"`show_empty_teasers`" +
		") VALUES "

	sqlODKU := " ON DUPLICATE KEY UPDATE `shows`=`shows`+VALUES(`shows`), `real_shows`=`real_shows`+VALUES(`real_shows`), `show_empty_teasers`=`show_empty_teasers`+VALUES(`show_empty_teasers`)"
	limit := appConfig.Mysql.StatisticsBlockSize
	values := make([]string, 0, limit)

	for key, value := range data {
		tickersComposite, ok := GetInformerByTcId(key.TickersCompositeId)
		if !ok || tickersComposite.GBlock == nil {
			logs.Critical(fmt.Sprintf("Informer tickers_composite(%d) not found, unable to insert data in g_blocks_history_1", key.TickersCompositeId))
			continue
		}

		inf := tickersComposite
		var curator *string
		if inf.GBlock.Inviter == nil {
			curator = inf.GBlock.Curator
		} else {
			curator = inf.GBlock.Inviter
		}
		curStr := "NULL"
		invStr := "NULL"
		if inf.GBlock.Inviter != nil {
			invStr = "'" + (*inf.GBlock.Inviter) + "'"
		}
		if curator != nil {
			curStr = "'" + (*curator) + "'"
		}
		values = append(values, fmt.Sprintf("(%d,%d,%q,%d,%d,%q,%g,%g,%d,%d,%s,%s,%d,%d,%d)", inf.GBlock.Id, inf.GBlock.Uid, key.Day,
			key.TickersCompositeId, inf.GBlock.ClientId, inf.GBlock.ContractsType, inf.GBlock.Price, inf.GBlock.RevenueProc, inf.SiteId, inf.GBlock.AgentGhitsEnabled, invStr, curStr,
			0, value.RealShows, 0))
		if len(values) >= int(limit) {
			g.client.Exec(sqlStr + strings.Join(values, ",") + sqlODKU)
			values = values[:0]
		}
	}
	if len(values) > 0 {
		g.client.Exec(sqlStr + strings.Join(values, ",") + sqlODKU)
	}
}
