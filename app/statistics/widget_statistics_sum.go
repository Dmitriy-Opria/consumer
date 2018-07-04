package statistics

import (
	"sync/atomic"

	"golib/utils"
)

type (
	WidgetStatisticsSumKey struct {
		Date          string
		Hour          uint8
		Site          uint32
		Composite     uint32
		Uid           uint32
		Country       uint32
		Device        uint8
		Os            uint8
		TrafficSource string
		TrafficType   string
		Subid         uint32
		BrowserId     uint8
		Type          string // "real_show"
	}

	WidgetStatisticsSumValue struct {
		RealShows int64
	}
)

var (
	WidgetStatisticsSum = utils.NewMapL()
)

func WidgetStatisticsSumSave(key WidgetStatisticsSumKey, shows int64) {

	valueDay := &WidgetStatisticsSumValue{
		RealShows: shows,
	}

	WidgetStatisticsSum.Inc(key, valueDay)
}

func GetWidgetStatisticsSumShows() map[WidgetStatisticsSumKey]WidgetStatisticsSumValue {
	shows := WidgetStatisticsSum.Init()

	result := make(map[WidgetStatisticsSumKey]WidgetStatisticsSumValue)

	for k := range shows {
		key := k.(WidgetStatisticsSumKey)
		value := shows[k].(*WidgetStatisticsSumValue)

		result[key] = *value
	}

	return result
}

func (hv *WidgetStatisticsSumValue) AtomicInc(v utils.AtomicIncrement) {
	n := v.(*WidgetStatisticsSumValue)
	if n.RealShows > 0 {
		atomic.AddInt64(&hv.RealShows, n.RealShows)
	}
}
