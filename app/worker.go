package app

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	dspdb "golib/dsp/db"
	"golib/logs"
	"golib/pool"
	"golib/services/capping_consumer/app/db"
	"golib/services/capping_consumer/app/statistics"
	"golib/ssp"
	"golib/utils"
)

func (app *App) worker(push bool) {
	var queue = app.kafka.Messages()

	if push {
		queue = app.pushKafka.Messages()
	}

	for msg := range queue {
		var err error
		var kafkaLine KafkaLine

		kafkaLine, err = ParseKafkaLine(msg)

		if err != nil {
			logs.Critical("got bad line from kafka, err: ", err)
			continue
		}

		app.saveStatistics(&kafkaLine)
	}
}
func (app *App) saveStatistics(k *KafkaLine) {
	if isShow, err := k.isShowsRequest(); err == nil {
		if isShow {
			saveShowsStatistics(k)
			app.SaveToCache(k)
			app.AerospikeSaveShows(k)
		} else {
			app.AerospikeSaveClicks(k)
		}
	}
}

func saveShowsStatistics(k *KafkaLine) {
	info := db.GetCompositeInfo(k.TickersCompositeId)
	if info == nil {
		logs.Debug(fmt.Sprintf("Composite info is null: %q", k.Raw))
		return
	}

	if k.First {

		key := statistics.WidgetStatisticsSumKey{
			Date:          k.ShowDate,
			Hour:          uint8(k.ShowTime.Hour()),
			Site:          uint32(info.SiteId),
			Composite:     uint32(info.CompositeId),
			Uid:           k.InformerUid,
			Country:       uint32(k.CountriesId),
			Os:            utils.ToUint8(k.OsId),
			TrafficSource: k.TrafficSource,
			TrafficType:   k.TrafficType,
			Device:        k.DeviceId,
			Subid:         k.SourceId,
			BrowserId:     utils.ToUint8(k.BrowserId),
			Type:          "real_show",
		}

		statistics.WidgetStatisticsSumSave(key, 1)
		if k.TickersCompositeId > 0 {
			statistics.GBlocksStatSave(k.ShowDate, int64(k.InformerUid), k.TickersCompositeId, 1)
			statistics.TickersCompositeStatSave(k.ShowDate, k.TickersCompositeId, 1)
			statistics.TickersCompositeStatSummSave(k.TickersCompositeId, 1)
		}
	}

	for _, p := range k.Params {

		if p.SignData.CheckItemType(ssp.ITEM_TYPE_INTEXCHANGE) {
			statistics.NewsShowsSave(p.SignData.TeaserId, 1)
		}
		if p.SignData.CheckItemType(ssp.ITEM_TYPE_GOODS) && p.SignData.PartnerId != 0 {
			if info.GBlock == nil {
				logs.Critical("cannot find gblock for tc id ", info.CompositeId)
				continue
			}
			statistics.GPartnersStatSave(p.SignData.PartnerId, 1)
			statistics.TeaserRealShowsSave(p.SignData.TeaserId, 1)
			statistics.TeaserGeoZoneSave(p.SignData.TeaserId, k.GeoZoneId, 1)
			statistics.SubnetRealShowsSave(k.ShowDate, k.GeoZoneId, p.SignData.Subnet, 1)

			isDsp, isCrosslocationDsp, isArtg, clientID := db.GetCampaignFlags(p.SignData.PartnerId)

			if !(isDsp || isCrosslocationDsp || isArtg) {

				key := statistics.TeaserStatisticsSumKey{
					Date:           k.ShowDate,
					Timestamp:      utils.ToInt64(k.Timestamp),
					TeaserID:       p.SignData.TeaserId,
					TeaserCategory: uint8(db.GetTeaserCategoryG(p.SignData.TeaserId)),
					Campaign:       p.SignData.PartnerId,
					ClientID:       uint32(clientID),
					CompositeID:    uint32(info.CompositeId),
					UID:            k.InformerUid,
					SubID:          k.SourceId,
					CountryID:      uint32(k.CountriesId),
					RegionID:       uint32(k.RegionId),
					Os:             utils.ToUint8(k.OsId),
					DeviceType:     k.DeviceType,
					BrowserID:      utils.ToUint8(k.BrowserId),
					Subnet:         p.SignData.Subnet,
					Provider:       uint8(k.Provider),
				}
				statistics.TeaserStatisticsSumSave(key, 1)
			}
		}
	}
}

func (app *App) AerospikeSaveShows(k *KafkaLine) {
	if k.CheckMuidn() {
		for _, p := range k.Params {
			k.Shows = append(k.Shows, p.SignData)
		}
	}
	dspdb.GetShowsSaver().RunSessionFunction(k.Muidn, k.Shows, db.GetAerospikeSession(), app.config.Aerospike.ShowFunctionName)
}

func (app *App) AerospikeSaveClicks(k *KafkaLine) {

	if k.CheckMuidn() {
		udfArguments := pool.GetBuffer()
		defer pool.PutBuffer(udfArguments)

		//$udfArguments = pack('C', $teaserType == self::GOODS_TYPE);

		var send bool

		if k.ClickSignData.PartnerId != 0 {

			limits := dspdb.GetLimitsInfo(k.ClickSignData.PartnerId, k.Type)
			cappingType := uint8(0)
			if limits.Clicks.Val != 0 {
				id := k.ClickSignData.PartnerId
				if limits.Clicks.WholeCamp == 0 {
					cappingType = 1
					if k.ClickSignData.TeaserId != 0 {
						id = k.ClickSignData.TeaserId
					} else {
						logs.Write(fmt.Sprintf("Aerospike save skipped, TeaserId is not set, %s", k.Raw))
						return
					}
				}
				binary.Write(udfArguments, binary.LittleEndian, k.ClickItemType)   // 1b
				binary.Write(udfArguments, binary.LittleEndian, cappingType)       // 1b
				binary.Write(udfArguments, binary.LittleEndian, id)                // 4b
				binary.Write(udfArguments, binary.LittleEndian, limits.Clicks.Val) // 2b
				send = true
			}
		}

		if send {
			db.AerospikeSave(k.Muidn, udfArguments, app.config.Aerospike.ClickFunctionName)
		}
	}
}

func (app *App) SaveToCache(k *KafkaLine) {
	info := db.GetCompositeInfo(k.TickersCompositeId)
	if info == nil {
		logs.Debug(fmt.Sprintf("Composite info is null: %q", k.Raw))
		return
	}

	if info.GBlock != nil && info.GBlock.ResponseCacheEnabled == 0 {

		defer logs.Recover()

		buf := pool.GetBuffer()
		defer pool.PutBuffer(buf)

		location := time.UTC
		if loc, ok := db.GetLocationByRegionId(k.RegionId); ok {
			location = loc
		}

		now := time.Now().In(location)

		isWeekend := 0
		switch now.Weekday() {
		case time.Saturday, time.Sunday:
			isWeekend = 1
		}

		firstTeaser := true

		for _, p := range k.Params {

			visibilityMask := utils.ToUint(p.VisibilityMask)
			teaserCategory, widgetCategory := 0, 0

			if k.Type == "N" {
				teaserCategory = db.GetTeaserCategoryN(p.SignData.TeaserId)
				widgetCategory = db.GetWidgetCategoryN(p.SignData.InformerUid)
			} else if k.Type == "V" {
				teaserCategory = db.GetVideoContentCategory(p.SignData.TeaserId)
				widgetCategory = db.GetWidgetCategoryG(p.SignData.InformerUid)
			} else {
				teaserCategory = db.GetTeaserCategoryG(p.SignData.TeaserId)
				widgetCategory = db.GetWidgetCategoryG(p.SignData.InformerUid)
			}

			if pos := strings.Index(k.Timestamp, "."); pos != -1 {
				k.Timestamp = k.Timestamp[:pos]
			}

			fmt.Fprint(buf, k.Timestamp, "\t")
			fmt.Fprint(buf, k.HttpReferer, "\t")
			fmt.Fprint(buf, k.Muidn, "\t")
			fmt.Fprint(buf, k.Ip, "\t")
			fmt.Fprint(buf, location.String(), "\t")
			fmt.Fprint(buf, now.Hour(), "\t")
			fmt.Fprint(buf, isWeekend, "\t")
			fmt.Fprint(buf, k.Type, "\t")
			fmt.Fprint(buf, p.SignData.TeaserId, "\t")
			fmt.Fprint(buf, teaserCategory, "\t")       // teaserCategory - 0, если нету
			fmt.Fprint(buf, p.SignData.PartnerId, "\t") //
			fmt.Fprint(buf, p.TeaserWidth, "\t")        //
			fmt.Fprint(buf, p.TeaserHeight, "\t")       //

			if visibilityMask&1 == 0 {
				fmt.Fprint(buf, "0\t") // teaserHasDescription - visibilityMask & 1
			} else {
				fmt.Fprint(buf, "1\t")
			}

			if visibilityMask&2 == 0 {
				fmt.Fprint(buf, "0\t") // teaserHasPrice - visibilityMask & 2
			} else {
				fmt.Fprint(buf, "1\t")
			}

			if visibilityMask&4 == 0 {
				fmt.Fprint(buf, "0\t") // teaserHasDomain - visibilityMask & 4
			} else {
				fmt.Fprint(buf, "1\t")
			}

			fmt.Fprint(buf, p.ShowHash, "\t")             //
			fmt.Fprint(buf, p.SignData.InformerUid, "\t") // uid

			if visibilityMask&8 == 0 {
				fmt.Fprint(buf, "0\t") // atf
			} else {
				fmt.Fprint(buf, "1\t")
			}

			switch visibilityMask & (16 + 32) {
			case 32:
				fmt.Fprint(buf, "left\t") // align
			case 16:
				fmt.Fprint(buf, "right\t") // align
			default:
				fmt.Fprint(buf, "center\t") // align
			}

			fmt.Fprint(buf, p.SignData.ItemType, "\t")  // itemType - из хешапоказа
			fmt.Fprint(buf, p.SignData.Time, "\t")      // visitTime - из хешапоказа
			fmt.Fprint(buf, k.RegionId, "\t")           // region - регион показа по ip
			fmt.Fprint(buf, widgetCategory, "\t")       // widgetCategory - 0, если нету
			fmt.Fprint(buf, p.SignData.Page, "\t")      // page - из хешапоказа
			fmt.Fprintf(buf, "%c\t", p.SignData.DcId)   // dcId - из хешапоказа
			fmt.Fprint(buf, k.OsId, "\t")               // osId - operating_systems_id из сервиса browscap
			fmt.Fprint(buf, k.BrowserId, "\t")          // browserId - browser_id из сервиса browscap
			fmt.Fprint(buf, p.SignData.UserGroup, "\t") // userGroup - из хешапоказа
			if k.First && firstTeaser {
				fmt.Fprint(buf, "1")
				firstTeaser = false
			} else {
				fmt.Fprint(buf, "0")
			}
			fmt.Fprint(buf, "\t", k.Uuid) // userGroup - из хешапоказа

			app.hdfsLogger.WriteString(utils.ToInt64(k.Timestamp), buf.String())
			buf.Truncate(0)
		}
	}
}
