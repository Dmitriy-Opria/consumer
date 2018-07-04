package statistics

import (
	"sync/atomic"

	"golib/utils"
)

type (
	NewsShowsKey struct {
		NewsId uint32
	}

	NewsShowsValue struct {
		NewsShows int64
	}
)

var (
	NewsShowsData = utils.NewMapL()
)

func NewsShowsSave(newsId uint32, newsShows int64) {

	keyDay := NewsShowsKey{
		NewsId: newsId,
	}

	valueDay := &NewsShowsValue{
		NewsShows: newsShows,
	}

	NewsShowsData.Inc(keyDay, valueDay)
}

func GetNewsShows() map[NewsShowsKey]NewsShowsValue {
	shows := NewsShowsData.Init()

	result := make(map[NewsShowsKey]NewsShowsValue)

	for k := range shows {
		key := k.(NewsShowsKey)
		value := shows[k].(*NewsShowsValue)

		result[key] = *value
	}

	return result
}

func (ns *NewsShowsValue) AtomicInc(n utils.AtomicIncrement) {
	t := n.(*NewsShowsValue)
	if t.NewsShows > 0 {
		atomic.AddInt64(&ns.NewsShows, t.NewsShows)
	}
}
