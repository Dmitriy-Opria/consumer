package statistics

import (
	"sync/atomic"

	"golib/utils"
)

type (
	TeaserGeoZoneKey struct {
		TeaserId  uint32
		GeoZoneId uint16
	}

	TeaserGeoZoneValue struct {
		Shows int64
	}
)

var (
	TeaserGeoZoneData = utils.NewMapL()
)

func TeaserGeoZoneSave(teaserId uint32, geoZoneId uint16, shows int64) {

	keyDay := TeaserGeoZoneKey{
		TeaserId:  teaserId,
		GeoZoneId: geoZoneId,
	}

	valueDay := &TeaserGeoZoneValue{
		Shows: shows,
	}

	TeaserGeoZoneData.Inc(keyDay, valueDay)
}

func GetTeaserGeoZoneShows() map[TeaserGeoZoneKey]TeaserGeoZoneValue {
	shows := TeaserGeoZoneData.Init()

	result := make(map[TeaserGeoZoneKey]TeaserGeoZoneValue)

	for k := range shows {
		key := k.(TeaserGeoZoneKey)
		value := shows[k].(*TeaserGeoZoneValue)

		result[key] = *value
	}

	return result
}

func (tg *TeaserGeoZoneValue) AtomicInc(v utils.AtomicIncrement) {
	t := v.(*TeaserGeoZoneValue)
	if t.Shows > 0 {
		atomic.AddInt64(&tg.Shows, t.Shows)
	}
}
