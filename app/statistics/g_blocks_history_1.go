package statistics

import (
	"sync/atomic"

	"golib/utils"
)

type (
	GBlocksStatKey struct {
		Day                string
		UID                int64
		TickersCompositeId int64
	}

	GBlocksStatValue struct {
		RealShows int64
	}
)

var (
	gBlocksShowsData = utils.NewMapL()
)

func GBlocksStatSave(showDate string, uid, tickersCompositeID, realShows int64) {

	keyDay := GBlocksStatKey{
		Day:                showDate,
		UID:                uid,
		TickersCompositeId: tickersCompositeID,
	}

	valueDay := &GBlocksStatValue{
		RealShows: realShows,
	}

	gBlocksShowsData.Inc(keyDay, valueDay)
}

func GetGBlocksShows() map[GBlocksStatKey]GBlocksStatValue {
	shows := gBlocksShowsData.Init()

	result := make(map[GBlocksStatKey]GBlocksStatValue)

	for k := range shows {
		key := k.(GBlocksStatKey)
		value := shows[k].(*GBlocksStatValue)

		result[key] = *value
	}

	return result
}

func (hv *GBlocksStatValue) AtomicInc(v utils.AtomicIncrement) {
	n := v.(*GBlocksStatValue)
	if n.RealShows > 0 {
		atomic.AddInt64(&hv.RealShows, n.RealShows)
	}
}
