package statistics

import (
	"sync/atomic"

	"golib/utils"
)

type (
	GPartnersStatKey struct {
		ID uint32
	}

	GPartnersStatValue struct {
		Shows int64
	}
)

var (
	gPartnersStat = utils.NewMapL()
)

func GPartnersStatSave(id uint32, shows int64) {

	keyDay := GPartnersStatKey{
		ID: id,
	}

	valueDay := &GPartnersStatValue{
		Shows: shows,
	}

	gPartnersStat.Inc(keyDay, valueDay)
}

func GetGPartnersStat() map[GPartnersStatKey]GPartnersStatValue {
	shows := gPartnersStat.Init()

	result := make(map[GPartnersStatKey]GPartnersStatValue)

	for k := range shows {
		key := k.(GPartnersStatKey)
		value := shows[k].(*GPartnersStatValue)

		result[key] = *value
	}

	return result
}

func (hv *GPartnersStatValue) AtomicInc(v utils.AtomicIncrement) {
	n := v.(*GPartnersStatValue)
	if n.Shows > 0 {
		atomic.AddInt64(&hv.Shows, n.Shows)
	}
}
