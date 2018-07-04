package db

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"golib/mysql-v2"
)

var (
	globalRegionLocation atomic.Value
)

func init() {
	globalRegionLocation.Store(map[uint16]*time.Location{})
}

func loadRegionLocation(client mysql.IClient) error {

	sqlStr := "SELECT id, time_zone FROM partners.maxmind_geoip_regions" // iso, maxmind_name,

	rows, err := client.Query(sqlStr)
	if err != nil {
		return errors.New(fmt.Sprintf("cannot read data from maxmind_geoip_regions, error %q", err.Error()))
	}
	defer rows.Close()

	regions := map[uint16]*time.Location{}

	for rows.Next() {

		id, location := uint16(0), ""

		err = rows.Scan(&id, &location)
		if err != nil {
			return errors.New(fmt.Sprintf("cannot scan data from maxmind_geoip_regions, error %q", err.Error()))
		}

		if location != "" {

			location = strings.TrimSpace(location)

			if loc, err := time.LoadLocation(location); err == nil {
				regions[id] = loc
			} else {
				return errors.New(fmt.Sprintf("cannot load region location: %q", err.Error()))
			}
		}
	}
	globalRegionLocation.Store(regions)
	return nil
}

func GetLocationByRegionId(id uint16) (loc *time.Location, ok bool) {

	m := globalRegionLocation.Load().(map[uint16]*time.Location)
	loc, ok = m[id]
	return
}
