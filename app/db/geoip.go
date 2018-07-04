package db

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"golib/logs"
	"golib/utils"
)

var (
	ErrIncorrectLine = errors.New("INCORRECT_LINE")
	ErrNotFound      = errors.New("NOT_FOUND")
)

type (
	IpRange struct {
		StartIp  uint32
		EndIp    uint32
		Country  uint16
		Region   uint16 // maxmind_geoip_regions.id
		Type     uint16
		GeoGoods uint16
		GeoNews  uint16
	}

	Range struct {
		Ranges []IpRange
	}
)

func (r Range) Find(ip uint32) (idCountry, idRegion, idGoods uint16, err error) {

	length := len(r.Ranges)

	index := sort.Search(length, func(i int) bool {

		return r.Ranges[i].EndIp >= ip
	})

	if index < length && r.Ranges[index].StartIp <= ip && ip <= r.Ranges[index].EndIp {

		ipRange := r.Ranges[index]
		idCountry, idRegion, idGoods = ipRange.Country, ipRange.Region, ipRange.GeoGoods

	} else {

		err = ErrNotFound
	}

	return
}

func New(filename string) *Range {

	if filename == "" {
		logs.Critical("Config param maxmind_filepath not set")
		os.Exit(1)
	}

	file, err := os.Open(filename)
	if err != nil {
		logs.Critical(fmt.Sprintf("Cannot open GeoIP database, file \"%s\" do not exist", filename))
		os.Exit(1)
	}

	defer file.Close()

	scaner := bufio.NewScanner(file)
	scaner.Split(bufio.ScanLines)

	var line, lineCount, parseCount, lastIp uint32

	if scaner.Scan() {

		line++
		lineCount = utils.ToUint32(scaner.Text())

		data := &Range{
			Ranges: make([]IpRange, 0, lineCount),
		}

		for scaner.Scan() {

			line++

			if ipRange, err := ParseLine(scaner.Text()); err == nil {

				if lastIp >= ipRange.StartIp {
					continue
				}

				lastIp = ipRange.EndIp

				parseCount++
				data.Ranges = append(data.Ranges, ipRange)

			} else {

				logs.Critical(fmt.Sprintf("Cannot init GeoIP database, file \"%s\" has wrong format", filename))
				os.Exit(1)
			}
		}

		if lineCount == parseCount {
			return data
		}
	}

	logs.Critical(fmt.Sprintf("Cannot init GeoIP database, file \"%s\" has wrong format", filename))
	os.Exit(1)

	return nil
}

func ParseLine(str string) (ipRange IpRange, err error) {

	startPos := strings.Index(str, "s")
	endPos := strings.Index(str, "e")
	countryPos := strings.Index(str, "c")
	regionPos := strings.Index(str, "r")
	typePos := strings.Index(str, "t")
	geoGoodsPos := strings.Index(str, "gg")
	geoNewsPos := strings.Index(str, "gn")

	if startPos != 0 || endPos <= startPos || countryPos <= endPos || regionPos <= countryPos ||
		typePos <= regionPos || geoGoodsPos <= typePos || geoNewsPos <= geoGoodsPos {

		err = ErrIncorrectLine
		return
	}

	ipRange.StartIp = utils.ToUint32(str[startPos+1 : endPos])
	ipRange.EndIp = utils.ToUint32(str[endPos+1 : countryPos])
	ipRange.Country = utils.ToUint16(str[countryPos+1 : regionPos])
	ipRange.Region = utils.ToUint16(str[regionPos+1 : typePos])
	ipRange.Type = utils.ToUint16(str[typePos+1 : geoGoodsPos])
	ipRange.GeoGoods = utils.ToUint16(str[geoGoodsPos+2 : geoNewsPos])
	ipRange.GeoNews = utils.ToUint16(str[geoNewsPos+2:])

	if ipRange.Country == 0 || ipRange.StartIp == 0 || ipRange.EndIp == 0 {

		err = ErrIncorrectLine
		return
	}

	return
}
