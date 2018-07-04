package db

import (
	"errors"
	"fmt"
	"sync/atomic"

	"golib/logs"
	"golib/mysql-v2"
)

type CompositeInfo struct {
	SiteId, CompositeId int64
	GBlock              *GBlockInfo
}

type GBlockInfo struct {
	Id                   int64 // id
	Uid                  int64 // uid
	Inviter, Curator     *string
	NumSlots             int
	Parent               int64 // parent
	ClientId             int64 // client_id
	ContractsType        []uint8
	Price                float32
	RevenueProc          float32
	AgentGhitsEnabled    int8
	ResponseCacheEnabled int8
}

var (
	atomicCompositeInfo atomic.Value
	atomicGBbyTcId      atomic.Value
)

func init() {
	atomicCompositeInfo.Store(map[int64]*CompositeInfo{})
	atomicGBbyTcId.Store(map[int64]*CompositeInfo{})
}

func loadCompositeInfo(client mysql.IClient) error {

	defer logs.Recover()

	sqlStr := "SELECT tc.id, " +
		"tc.client_site_id, " +
		"b.id, " +
		"b.uid, " +
		"b.count_news, " +
		"b.parent, " +
		"b.client_id, " +
		"b.contracts_type, " +
		"b.price, " +
		"b.revenue_proc, " +
		"b.agent_ghits_enabled, " +
		"b.inviter, " +
		"IFNULL(b.gb_response_cache_time, 0) > 0, " +
		"cc.curator " +
		"FROM tickers_composite tc " +
		"LEFT JOIN g_blocks b ON tc.id=b.tickers_composite_id AND b.auction_type='cpc'" +
		"LEFT JOIN clients_curators cc ON b.client_id = cc.client_id and cc.section = 'wages'" +
		"WHERE tc.client_site_id IS NOT NULL"

	rows, err := client.Query(sqlStr)

	if err != nil {
		return errors.New(fmt.Sprintf("Critical: Cannot read data from g_blocks, error %q", err.Error()))
	}
	defer rows.Close()

	blocks := map[int64]*CompositeInfo{}
	tcMap := map[int64]*CompositeInfo{}

	for rows.Next() {

		type ptrGBlockInfo struct {
			Id                   *int64 // id
			Uid                  *int64 // uid
			Inviter, Curator     *string
			NumSlots             *int
			Parent               *int64 // parent
			ClientId             *int64 // client_id
			ContractsType        []uint8
			Price                *float32
			RevenueProc          *float32
			AgentGhitsEnabled    *int8
			ResponseCacheEnabled *int8
		}
		var (
			info CompositeInfo
			gb   ptrGBlockInfo
		)

		if err = rows.Scan(&info.CompositeId, &info.SiteId,
			&gb.Id, &gb.Uid,
			&gb.NumSlots, &gb.Parent, &gb.ClientId, &gb.ContractsType, &gb.Price, &gb.RevenueProc, &gb.AgentGhitsEnabled,
			&gb.Inviter, &gb.ResponseCacheEnabled, &gb.Curator); err != nil {
			return errors.New(fmt.Sprintf("Critical: Cannot read data from g_blocks, error %q", err.Error()))
		}
		if gb.Uid != nil {
			info.GBlock = &GBlockInfo{
				Id:                   *gb.Id,
				Uid:                  *gb.Uid,
				NumSlots:             *gb.NumSlots,
				Parent:               *gb.Parent,
				ClientId:             *gb.ClientId,
				Price:                *gb.Price,
				RevenueProc:          *gb.RevenueProc,
				AgentGhitsEnabled:    *gb.AgentGhitsEnabled,
				Inviter:              gb.Inviter,
				Curator:              gb.Curator,
				ContractsType:        gb.ContractsType,
				ResponseCacheEnabled: *gb.ResponseCacheEnabled,
			}
			tcMap[info.CompositeId] = &info
		}
		blocks[info.CompositeId] = &info
	}

	atomicCompositeInfo.Store(blocks)
	atomicGBbyTcId.Store(tcMap)

	return nil
}

func GetCompositeInfo(compositeId int64) *CompositeInfo {

	m := atomicCompositeInfo.Load().(map[int64]*CompositeInfo)
	return m[compositeId]
}

func GetInformerByTcId(uid int64) (composite *CompositeInfo, ok bool) {
	m := atomicGBbyTcId.Load().(map[int64]*CompositeInfo)
	composite, ok = m[uid]
	return composite, ok
}

func IsTickersCompositeId(tickerId int64) (ok bool) {

	m := atomicCompositeInfo.Load().(map[int64]*CompositeInfo)
	_, ok = m[tickerId]
	return
}
