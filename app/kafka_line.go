package app

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"golib/services/capping_consumer/app/db"
	"golib/ssp"
	"golib/utils"
)

type (
	ClickItemType uint8
	Params        struct {
		Value          string
		TeaserWidth    string
		TeaserHeight   string
		VisibilityMask string
		ShowHash       string
		SignData       ssp.SignData
	}

	KafkaLine struct {
		Raw                string
		Timestamp          string
		Ip                 string
		Muidn              string
		RequestPath        string
		UserAgent          string
		HttpReferer        string
		Pv                 string // p
		Type               string // t
		Values             []string
		Params             []Params
		ParamsBad          []Params
		First              bool
		InformerUid        uint32
		SourceId           uint32
		TickersCompositeId int64
		DeviceType         string
		DeviceId           uint8
		OsId               string
		RegionId           uint16
		GeoZoneId          uint16
		CountriesId        uint16
		Crawler            int8
		ShowDate           string
		BrowserId          string
		Uuid               string
		TrafficSource      string
		TrafficType        string
		Hash2              string
		Provider           int64
		ShowTime           time.Time
		ClickSignData      ssp.SignData
		ClickItemType      ClickItemType

		Shows []ssp.SignData
	}
)

const (
	MuidnLength = 12

	NewsClickItem ClickItemType = 0
	GoodClickItem ClickItemType = 1
)

func (k *KafkaLine) isShowsRequest() (ret bool, err error) {
	if strings.HasPrefix(k.RequestPath, "/cl?") {
		ret = false
	} else if strings.HasPrefix(k.RequestPath, "/c?") {
		ret = true
	} else {
		err = fmt.Errorf("Request is not valid %q", k.Raw)
	}
	return
}

func ParseKafkaLine(line string) (kafkaLine KafkaLine, err error) {

	list := strings.Split(line, "\t")

	if len(list) >= 9 {

		var provider int64

		if len(list) >= 10 {
			provider = utils.ToInt64(list[9])
		}

		kafkaLine.Raw = line
		kafkaLine.Timestamp = list[0]
		kafkaLine.Ip = list[1]
		kafkaLine.Muidn = list[2]
		kafkaLine.RequestPath = list[3]
		kafkaLine.UserAgent = list[4]
		kafkaLine.HttpReferer = list[5]
		kafkaLine.DeviceType = list[6] //desktop,tablet,mobile
		kafkaLine.BrowserId = list[7]  //browscap id
		kafkaLine.OsId = list[8]       //browscap id
		kafkaLine.Provider = provider

		if isShow, err := kafkaLine.isShowsRequest(); err == nil {
			if isShow {
				err = kafkaLine.ParseShowRequest()
			} else {
				err = kafkaLine.ParseClickRequest()
			}
		}
		return
	}
	err = fmt.Errorf("invalid kafka line length")
	return
}

func (k *KafkaLine) CheckMuidn() bool {

	if len(k.Muidn) != MuidnLength {
		return false
	}

	for _, c := range k.Muidn {
		if (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' {
			continue
		}
		return false
	}
	return true
}

func (k *KafkaLine) ParseClickRequest() (err error) {
	paramStr := k.RequestPath

	if pos := strings.Index(k.RequestPath, "?"); pos != -1 {
		paramStr = k.RequestPath[pos+1:]
	}
	queryKV, err := url.ParseQuery(paramStr)
	if err != nil {
		return err
	}

	if p := queryKV.Get("p"); len(p) > 2 {
		itemType := p[0]
		switch itemType {
		case '0':
			k.Type = "N"
			k.ClickItemType = NewsClickItem
		case '1':
			k.Type = "G"
			k.ClickItemType = GoodClickItem
		}
		hash := p[1:]
		if !k.ClickSignData.DecodeFromString(hash) {
			return fmt.Errorf("request hash parse failed, checksum mismatch")
		}
	} else {
		return fmt.Errorf("request parse failed, parameter 'p' - is too short")
	}
	return
}

func (k *KafkaLine) ParseHash2() (cs bool, extData ssp.ExtData) {
	if k.Hash2 != "" {
		if cs = extData.DecodeFromString(k.Hash2); !cs {
			extData = ssp.ExtData{}
		}
		return
	} else {
		cs = true
	}
	return
}

func (k *KafkaLine) ParseShowRequest() (err error) {

	paramStr := k.RequestPath

	if pos := strings.Index(k.RequestPath, "?"); pos != -1 {
		paramStr = k.RequestPath[pos+1:]
	}

	params, err := url.ParseQuery(paramStr)
	if err != nil {
		return err
	}

	k.Pv = params.Get("pv")
	k.First = params.Get("f") == "1"
	value := params.Get("v") // teaserWidth | teaserHeight | visibilityMask | showHash

	k.TickersCompositeId = utils.ToInt64(params.Get("cid"))
	isComposite := k.TickersCompositeId > 0

	if k.TickersCompositeId <= 0 || !db.IsTickersCompositeId(k.TickersCompositeId) {
		return fmt.Errorf("!IsTickersCompositeId %d", k.TickersCompositeId)
	}

	k.TrafficSource = params.Get("ts")
	k.TrafficType = params.Get("tt")
	if k.TrafficType == "" {
		k.TrafficType = "Direct"
	}

	k.Values = params["v"]
	k.Hash2 = params.Get("h2")
	k.Uuid = params.Get("rid")

	badChecksum := false

	for _, value := range k.Values {

		if list := strings.Split(value, "|"); len(list) >= 4 {

			params := Params{
				TeaserWidth:    list[0],
				TeaserHeight:   list[1],
				VisibilityMask: list[2],
				ShowHash:       list[3],
			}

			if params.TeaserWidth == "-" {
				params.TeaserWidth = "0"
			}

			if params.TeaserHeight == "-" {
				params.TeaserHeight = "0"
			}

			if !params.SignData.DecodeFromString(params.ShowHash) {
				badChecksum = true
			}

			if k.InformerUid == 0 {
				k.InformerUid = params.SignData.InformerUid
			}

			if isComposite {
				switch extItemType := params.SignData.GetItemType(); extItemType {
				case ssp.ITEM_TYPE_GOODS:
					k.Type = "G"
				case ssp.ITEM_TYPE_NEWS:
					k.Type = "N"
				case ssp.ITEM_TYPE_INTEXCHANGE:
					k.Type = "E"
				case ssp.ITEM_TYPE_VIDEO_CONTENT:
					k.Type = "V"
				}
			}

			k.Params = append(k.Params, params)

		} else {
			continue
		}
	}

	if k.Pv == "" || k.Type == "" || value == "" || badChecksum {
		return errors.New(`kafkaLine.Pv == "" || kafkaLine.Type == "" || value == "" || badChecksum`)
	}

	switch k.Type {
	case "N", "G", "E", "V":
	default:
		return errors.New(`kafkaLine.Type not in ("N", "G", "E", "V")`)
	}

	if len(k.Params) > 0 {
		k.SourceId = k.Params[0].SignData.SourceId
	}

	if pos := strings.Index(k.Timestamp, "."); pos != -1 {
		k.Timestamp = k.Timestamp[:pos]
	}

	k.ShowTime = time.Unix(utils.ToInt64(k.Timestamp), 0)

	year, month, day := k.ShowTime.Date()

	k.ShowDate = fmt.Sprintf("%04d-%02d-%02d", year, month, day)

	if ok, extData := k.ParseHash2(); ok { // parsed or absent
		if extData.CheckSum != 0 {
			k.OsId = extData.GetOs()
			k.DeviceType = extData.GetDeviceType()
			k.BrowserId = extData.GetBrowser()
			k.CountriesId = extData.GetCountry()
			k.RegionId = extData.GetRegion()
			k.GeoZoneId = extData.GeoZoneId
			k.Crawler = extData.Crawler
		}
	}

	switch k.DeviceType {
	case "mobile":
		k.DeviceId = 2
	case "tablet":
		k.DeviceId = 3
	default:
		k.DeviceId = 1
	}

	return nil
}
