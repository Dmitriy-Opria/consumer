package db

import (
	"errors"
	"fmt"
	"time"

	"golib/logs"
	"golib/mysql-v2"
	"sync/atomic"
)

var campaignFlagsMap atomic.Value

type campaignFlags struct {
	clientId                                    int64
	isDsp, isCrosslocaionDsp, isAutoretargeting bool
}

func init() {
	campaignFlagsMap.Store(map[uint32]campaignFlags{})
}

func loadDspIds(client mysql.IClient) (err error) {
	defer logs.Recover()

	sqlStr := "SELECT id, " +
		"IFNULL(client_id,0), " +
		"campaign_types = 'dsp', " +
		"crosslocation_dsp = 1, use_autoretargeting=1 FROM g_partners_1"

	rows, err := client.Query(sqlStr)

	if err != nil {
		SetUpdateDelay(5 * time.Minute)
		return errors.New(fmt.Sprintf("Cannot read data from g_partners_1, error %q", err.Error()))
	}
	defer rows.Close()

	dsp := map[uint32]campaignFlags{}

	for rows.Next() {
		var id uint32
		var flags campaignFlags
		if err = rows.Scan(&id, &flags.clientId, &flags.isDsp, &flags.isCrosslocaionDsp, &flags.isAutoretargeting); err != nil {
			err = errors.New(fmt.Sprintf("Cannot scan data from g_partners_1, error %q", err.Error()))
			SetUpdateDelay(5 * time.Minute)
			continue
		}

		dsp[id] = flags
	}

	campaignFlagsMap.Store(dsp)

	return
}

func GetCampaignFlags(campaignId uint32) (isDsp, isCrosslocationDsp, isArtg bool, clientId int64) {
	if campaignId == 0 {
		return
	}
	flagsMap := campaignFlagsMap.Load().(map[uint32]campaignFlags)
	campaignFlags, ok := flagsMap[campaignId]
	if ok {
		clientId = campaignFlags.clientId
		isDsp = campaignFlags.isDsp
		isCrosslocationDsp = campaignFlags.isCrosslocaionDsp
		isArtg = campaignFlags.isAutoretargeting
	}
	return
}
