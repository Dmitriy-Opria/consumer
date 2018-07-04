package db

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"golib/mysql-v2"
)

type (
	BrKey struct {
		Browser string
		Version string
	}

	OsKey struct {
		Platform        string
		PlatformVersion string
		DeviceType      string
	}
)

var (
	globalBrowserId atomic.Value
	globalOsId      atomic.Value
)

func init() {

	globalBrowserId.Store(map[BrKey]int{})
	globalOsId.Store(map[OsKey]int{})
}

func loadBrowscapInfo(client mysql.IClient) error {

	rows, err := client.Query("SELECT `name`, `version`, `browsers_id` FROM `browsers_versions` " +
		"WHERE `browsers_id` IS NOT NULL")
	if err != nil {
		return errors.New(fmt.Sprintf("Critical: Cannot read data from browsers_versions, error %q", err.Error()))
	}
	defer rows.Close()

	browsersId := map[BrKey]int{}

	for rows.Next() {

		var key BrKey
		var browsers_id int

		err = rows.Scan(&key.Browser, &key.Version, &browsers_id)

		key.Browser = strings.ToLower(key.Browser)
		key.Version = strings.ToLower(key.Version)

		if err != nil {
			return errors.New(fmt.Sprintf("Critical: Cannot read data from browsers_versions, error %q", err.Error()))
		}

		browsersId[key] = browsers_id
	}

	//

	rows, err = client.Query("SELECT `platform`, IFNULL(`device_type`, ''), IFNULL(`platform_version`, ''), " +
		"`operating_systems_id` FROM `operating_systems_devices` WHERE `platform` IS NOT NULL AND `operating_systems_id` IS NOT NULL")
	if err != nil {
		return errors.New(fmt.Sprintf("Critical: Cannot read data from operating_systems_devices, error %q", err.Error()))
	}
	defer rows.Close()

	osId := map[OsKey]int{}

	for rows.Next() {

		var key OsKey
		var operatingSystemsId int

		err = rows.Scan(&key.Platform, &key.DeviceType, &key.PlatformVersion, &operatingSystemsId)

		key.Platform = strings.ToLower(key.Platform)
		key.DeviceType = strings.ToLower(key.DeviceType)
		key.PlatformVersion = strings.ToLower(key.PlatformVersion)

		if strings.HasPrefix(key.Platform, "win") {
			key.PlatformVersion = ""
		}

		if key.Platform != "android" {
			if pos := strings.Index(key.PlatformVersion, "."); pos != -1 {
				key.PlatformVersion = key.PlatformVersion[:pos]
			}
		}

		if err != nil {
			return errors.New(fmt.Sprintf("Critical: Cannot read data from operating_systems_devices, error %q", err.Error()))
		}

		osId[key] = operatingSystemsId
	}

	fmt.Println("browsersId", len(browsersId))
	fmt.Println("osId", len(osId))

	globalBrowserId.Store(browsersId)
	globalOsId.Store(osId)

	return nil
}

func GetBrowserId(browser, version string) int {

	m := globalBrowserId.Load().(map[BrKey]int)

	return m[BrKey{Browser: browser, Version: version}]
}

func GetOperatingSystemsId(platform, deviceType, platformVersion string) int {

	m := globalOsId.Load().(map[OsKey]int)

	return m[OsKey{Platform: platform, DeviceType: deviceType, PlatformVersion: platformVersion}]
}
