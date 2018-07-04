package statistics

import (
	"sync/atomic"

	"golib/utils"
)

type (
	TickersCompositeStatSummKey struct {
		TickersCompositeId int64
	}

	TickersCompositeStatSummValue struct {
		RealShows int64
	}
)

var (
	TickersCompositeStatSummData = utils.NewMapL()
)

func TickersCompositeStatSummSave(tickersComposiId int64, realShows int64) {

	keyDay := TickersCompositeStatSummKey{
		TickersCompositeId: tickersComposiId,
	}

	valueDay := &TickersCompositeStatSummValue{
		RealShows: realShows,
	}

	TickersCompositeStatSummData.Inc(keyDay, valueDay)
}

func GetTickersCompositeSummStat() map[TickersCompositeStatSummKey]TickersCompositeStatSummValue {
	shows := TickersCompositeStatData.Init()

	result := make(map[TickersCompositeStatSummKey]TickersCompositeStatSummValue)

	for k := range shows {
		key := k.(TickersCompositeStatSummKey)
		value := shows[k].(*TickersCompositeStatSummValue)

		result[key] = *value
	}

	return result
}

func (ts *TickersCompositeStatSummValue) AtomicInc(v utils.AtomicIncrement) {
	t := v.(*TickersCompositeStatSummValue)
	if t.RealShows > 0 {
		atomic.AddInt64(&ts.RealShows, t.RealShows)
	}
}
