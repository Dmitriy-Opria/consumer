package statistics

import (
	"sync/atomic"

	"golib/utils"
)

type (
	TeaserRealShowsKey struct {
		TeaserId uint32
	}

	TeaserRealShowsValue struct {
		Shows int64
	}
)

var (
	TeaserRealShowsData = utils.NewMapL()
)

func TeaserRealShowsSave(teaserId uint32, shows int64) {

	keyDay := TeaserRealShowsKey{
		TeaserId: teaserId,
	}

	valueDay := &TeaserRealShowsValue{
		Shows: shows,
	}

	TeaserRealShowsData.Inc(keyDay, valueDay)
}

func GetTeaserRealShows() map[TeaserRealShowsKey]TeaserRealShowsValue {
	shows := TeaserRealShowsData.Init()

	result := make(map[TeaserRealShowsKey]TeaserRealShowsValue)

	for k := range shows {
		key := k.(TeaserRealShowsKey)
		value := shows[k].(*TeaserRealShowsValue)

		result[key] = *value
	}

	return result
}

func (ts *TeaserRealShowsValue) AtomicInc(v utils.AtomicIncrement) {
	t := v.(*TeaserRealShowsValue)
	if t.Shows > 0 {
		atomic.AddInt64(&ts.Shows, t.Shows)
	}
}
