package db

import (
	"errors"
	"fmt"
	"sync/atomic"

	"golib/mysql-v2"
)

var (
	globalTeaserCategoryN atomic.Value
	globalTeaserCategoryG atomic.Value
	globalWidgetCategoryN atomic.Value
	globalWidgetCategoryG atomic.Value
)

func init() {
	globalTeaserCategoryN.Store(map[uint32]int{})
	globalTeaserCategoryG.Store(map[uint32]int{})
	globalWidgetCategoryN.Store(map[uint32]int{})
	globalWidgetCategoryG.Store(map[uint32]int{})
}

func loadCategory(client mysql.IClient) error {

	teaserCategoryN, err := loadTeaserCategory(client, "news_1")
	if err != nil {
		return err
	}

	teaserCategoryG, err := loadTeaserCategory(client, "g_hits_1")
	if err != nil {
		return err
	}

	widgetCategoryN, err := loadWidgetCategory(client, "tickers")
	if err != nil {
		return err
	}

	widgetCategoryG, err := loadWidgetCategory(client, "g_blocks")
	if err != nil {
		return err
	}

	globalTeaserCategoryN.Store(teaserCategoryN)
	globalTeaserCategoryG.Store(teaserCategoryG)

	globalWidgetCategoryN.Store(widgetCategoryN)
	globalWidgetCategoryG.Store(widgetCategoryG)
	return nil
}

func loadTeaserCategory(client mysql.IClient, table string) (categorys map[uint32]int, err error) {

	categorys = make(map[uint32]int, 32)

	sqlStr := "SELECT id, id_category FROM " + table + " WHERE id_category IS NOT NULL"

	rows, err := client.Query(sqlStr)

	if err != nil {
		return nil, errors.New(fmt.Sprintf("cannot read data from %s, error: %q*", table, err))
	}
	defer rows.Close()

	for rows.Next() {

		id, categoryId := uint32(0), 0

		err = rows.Scan(&id, &categoryId)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("cannot scan data from %s, error: %q*", table, err))
		}

		categorys[id] = categoryId
	}
	return
}

func loadWidgetCategory(client mysql.IClient, table string) (categorys map[uint32]int, err error) {

	categorys = make(map[uint32]int, 32)

	sqlStr := "SELECT uid, category_platform FROM " + table

	rows, err := client.Query(sqlStr)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("cannot read data from %s, error: %q*", table, err))
	}
	defer rows.Close()

	for rows.Next() {

		id, categoryId := uint32(0), 0

		err = rows.Scan(&id, &categoryId)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("cannot scan data from %s, error: %q*", table, err))
		}

		categorys[id] = categoryId
	}
	return
}

func GetTeaserCategoryN(id uint32) int {
	m := globalTeaserCategoryN.Load().(map[uint32]int)
	return m[id]
}

func GetTeaserCategoryG(id uint32) int {
	m := globalTeaserCategoryG.Load().(map[uint32]int)
	return m[id]
}

func GetWidgetCategoryN(id uint32) int {
	m := globalWidgetCategoryN.Load().(map[uint32]int)
	return m[id]
}

func GetWidgetCategoryG(id uint32) int {
	m := globalWidgetCategoryG.Load().(map[uint32]int)
	return m[id]
}
