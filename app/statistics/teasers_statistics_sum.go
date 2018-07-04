package statistics

import (
	"sync/atomic"

	"golib/utils"
)

type (
	TeaserStatisticsSumKey struct {
		Date           string
		Hour           uint8
		Timestamp      int64
		TeaserID       uint32
		TeaserCategory uint8
		Campaign       uint32
		ClientID       uint32
		CompositeID    uint32
		UID            uint32
		SubID          uint32
		CountryID      uint32
		RegionID       uint32
		Os             uint8
		DeviceType     string
		BrowserID      uint8
		Provider       uint8
		Subnet         uint8
	}

	TeaserStatisticsSumValue struct {
		RealShows int64
	}
)

var (
	TeaserStatisticsSum = utils.NewMapL()
)

func TeaserStatisticsSumSave(key TeaserStatisticsSumKey, shows int64) {

	valueDay := &TeaserStatisticsSumValue{
		RealShows: shows,
	}

	TeaserStatisticsSum.Inc(key, valueDay)
}

func GetTeaserStatisticsSumShows() map[TeaserStatisticsSumKey]TeaserStatisticsSumValue {
	shows := TeaserStatisticsSum.Init()

	result := make(map[TeaserStatisticsSumKey]TeaserStatisticsSumValue)

	for k := range shows {
		key := k.(TeaserStatisticsSumKey)
		value := shows[k].(*TeaserStatisticsSumValue)

		result[key] = *value
	}

	return result
}

func (hv *TeaserStatisticsSumValue) AtomicInc(v utils.AtomicIncrement) {
	n := v.(*TeaserStatisticsSumValue)
	if n.RealShows > 0 {
		atomic.AddInt64(&hv.RealShows, n.RealShows)
	}
}
