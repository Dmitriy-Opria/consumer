package statistics

import (
	"sync/atomic"

	"golib/utils"
)

type (
	TickersCompositeStatKey struct {
		Date               string
		TickersCompositeId int64
	}

	TickersCompositeStatValue struct {
		RealShows int64
	}
)

var (
	TickersCompositeStatData = utils.NewMapL()
)

func TickersCompositeStatSave(date string, tickersComposiId int64, realShows int64) {

	keyDay := TickersCompositeStatKey{
		Date:               date,
		TickersCompositeId: tickersComposiId,
	}

	valueDay := &TickersCompositeStatValue{
		RealShows: realShows,
	}

	TickersCompositeStatData.Inc(keyDay, valueDay)
}

func GetTickersCompositeStat() map[TickersCompositeStatKey]TickersCompositeStatValue {
	shows := TickersCompositeStatData.Init()

	result := make(map[TickersCompositeStatKey]TickersCompositeStatValue)

	for k := range shows {
		key := k.(TickersCompositeStatKey)
		value := shows[k].(*TickersCompositeStatValue)

		result[key] = *value
	}

	return result
}

func (ts *TickersCompositeStatValue) AtomicInc(v utils.AtomicIncrement) {
	t := v.(*TickersCompositeStatValue)
	if t.RealShows > 0 {
		atomic.AddInt64(&ts.RealShows, t.RealShows)
	}
}
