package db

import (
	"time"

	"golib/aerospike"
	"golib/clickhouse-v2"
	"golib/logs"
	"golib/mongo"
	"golib/mysql-v2"
	"golib/new/config"
)

type StatWriter interface {
	Save()
	Worker()
}

var mysqlClient mysql.IClient
var clickhouseClient *clickhouse.Cluster
var appConfig *config.Data
var statWriters []StatWriter
var mongoClient *mongo.MongoSession
var aerospikeClient aerospike.ISession

func Init(dbClient mysql.IClient, chClient *clickhouse.Cluster, aeClient aerospike.ISession, mgClient *mongo.MongoSession, conf *config.Data) error {
	appConfig = conf
	mysqlClient = dbClient
	mongoClient = mgClient
	aerospikeClient = aeClient
	clickhouseClient = chClient

	err := loadDictionaries()
	if err != nil {
		return err
	}

	initMysqlWriters()

	initClickhouseWriters()

	initMongoWriters()

	startWriterWorkers()

	startDictUpdater()

	startWidgetRotateDateWatcher()

	return nil
}

func initMysqlWriters() {
	statWriters = append(statWriters, &gPartners{mysqlClient})
	statWriters = append(statWriters, &gBlocksHistory{mysqlClient})
	statWriters = append(statWriters, &teaserGeoZoneStat{mysqlClient})
	statWriters = append(statWriters, &subnetsRealShows{mysqlClient})
	statWriters = append(statWriters, &teaserRealShowsStat{mysqlClient})
	statWriters = append(statWriters, &tickersCompositeStat{mysqlClient})
	statWriters = append(statWriters, &tickersCompositeSummStat{mysqlClient})
}

func initClickhouseWriters() {
	statWriters = append(statWriters, &widgetStatisticsSum{clickhouseClient})
	statWriters = append(statWriters, &teaserStatisticsSum{clickhouseClient})
}

func initMongoWriters() {
	statWriters = append(statWriters, &newsShowsStat{mongoClient})
}

func startDictUpdater() {
	go func() {
		for {
			time.Sleep(5 * time.Minute)
			loadDictionaries()
		}
	}()
}

func startWriterWorkers() {
	for i := range statWriters {
		go statWriters[i].Worker()
	}
}

func startWidgetRotateDateWatcher() {
	go func() {
		for {
			updateWidgetRotateDate()
			time.Sleep(time.Minute)
		}
	}()
}

func Shutdown() {
	for i := range statWriters {
		statWriters[i].Save()
	}
}

func loadDictionaries() error {

	err := loadCompositeInfo(mysqlClient)
	if err != nil {
		logs.Critical(err)
		return err
	}

	err = loadRegionLocation(mysqlClient)
	if err != nil {
		logs.Critical(err)
		return err
	}

	err = loadCategory(mysqlClient)
	if err != nil {
		logs.Critical(err)
		return err
	}

	err = loadBrowscapInfo(mysqlClient)
	if err != nil {
		logs.Critical(err)
		return err
	}

	err = loadDspIds(mysqlClient)
	if err != nil {
		logs.Critical(err)
		return err
	}

	err = loadVideoContentCategory(mongoClient)
	if err != nil {
		logs.Critical(err)
		return err
	}

	return nil
}
