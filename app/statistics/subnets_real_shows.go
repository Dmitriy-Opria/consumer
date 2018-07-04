package statistics

import (
	"sync/atomic"

	"golib/utils"
)

type (
	SubnetRealShowsKey struct {
		Day       string
		GeoZoneId uint16
		SubnetId  uint8
	}

	SubnetRealShowsValue struct {
		AllShows int64
	}
)

var (
	SubnetRealShowsData = utils.NewMapL()
)

func SubnetRealShowsSave(day string, geoZoneId uint16, subnetId uint8, allShows int64) {

	keyDay := SubnetRealShowsKey{
		Day:       day,
		GeoZoneId: geoZoneId,
		SubnetId:  subnetId,
	}

	valueDay := &SubnetRealShowsValue{
		AllShows: allShows,
	}

	SubnetRealShowsData.Inc(keyDay, valueDay)
}

func GetSubnetRealShowsShows() map[SubnetRealShowsKey]SubnetRealShowsValue {
	shows := SubnetRealShowsData.Init()

	result := make(map[SubnetRealShowsKey]SubnetRealShowsValue)

	for k := range shows {
		key := k.(SubnetRealShowsKey)
		value := shows[k].(*SubnetRealShowsValue)

		result[key] = *value
	}

	return result
}

func (sr *SubnetRealShowsValue) AtomicInc(v utils.AtomicIncrement) {
	s := v.(*SubnetRealShowsValue)
	if s.AllShows > 0 {
		atomic.AddInt64(&sr.AllShows, s.AllShows)
	}
}
