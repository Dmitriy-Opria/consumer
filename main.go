package main

import (
	"flag"
	"golib/hdfs"
	"golib/logs"
	"golib/ssp"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"

	as "golib/aerospike"
	"golib/clickhouse-v2"
	"golib/mongo"
	"golib/mysql-v2"
	"golib/mysql-v2/dberr"
	"golib/new/client/kafka"
	"golib/new/config"
	"golib/services/capping_consumer/app"
	"golib/services/capping_consumer/app/db"
)

var mysqlClient mysql.IClient
var clickhouseClient *clickhouse.Cluster

var currentApp *app.App

func main() {
	currentApp = &app.App{}
	initConfig()

	initLog()

	initHashLog()

	initHdfs()

	initMysql()

	initMongodb()

	initKafka()

	initPushKafka()

	initGeoIpDB()

	initMongodb()

	initClickhouse()

	initAerospike()

	debug.SetGCPercent(200)

	currentApp.Start()

	runtime.Goexit()
}

func initLog() {
	logs.Init(currentApp.GetConfig().Log)
}

func initHashLog() {
	logger := logs.NewLogger(currentApp.GetConfig().HashLog)
	currentApp.SetHashLogger(logger)
}

func initClickhouse() {
	conf := currentApp.GetConfig()
	clickhouseConf := clickhouse.Config{
		Hosts:    conf.Clickhouse.Hosts,
		Username: conf.Clickhouse.Username,
		Password: conf.Clickhouse.Password,
		Enable:   1,
		ErrorWD:  conf.Clickhouse.ErrorWD,
		Database: "statistics",
	}
	var err error
	clickhouseClient, err = clickhouse.New(clickhouseConf)
	if err != nil {
		logs.Critical("Fail to init Clickhouse, err: ", err)
		os.Exit(1)
	}
	currentApp.SetClickhouse(clickhouseClient)
}

func initKafka() {
	consumer, err := kafka.Init(currentApp.GetConfig().Kafka)
	if err != nil {
		logs.Critical("Fail to init KAFKA, err: ", err)
		os.Exit(1)
	}
	logs.Write("Kafka initialized")
	currentApp.SetKafka(consumer)
}

func initPushKafka() {
	conf := currentApp.GetConfig().Kafka
	conf.Topic = "push_capping"
	consumer, err := kafka.Init(conf)
	if err != nil {
		logs.Critical("Fail to init push_capping KAFKA, err: ", err)
		os.Exit(1)
	}
	logs.Write("Push kafka initialized")
	currentApp.SetPushKafka(consumer)
}

func initMysql() {
	conf := currentApp.GetConfig()

	masterConfig := mysql.NewConfig(
		mysql.Database(conf.MysqlMasterV2.DbName),
		mysql.Host(conf.MysqlMasterV2.Port),
		mysql.Login(conf.MysqlMasterV2.Login),
		mysql.Password(conf.MysqlMasterV2.Password),
		mysql.MustConnected(true),
		mysql.DataCenterID(conf.DataCentreID),
		mysql.MaxIdleConns(conf.MysqlMasterV2.MaxIdleConns),
		mysql.MaxOpenConns(conf.MysqlMasterV2.MaxOpenConns),
		mysql.ConnMaxLifetime(conf.MysqlMasterV2.ConnMaxLifetime),
		mysql.SlaveLagThreshold(conf.MysqlMasterV2.SlaveLagThreshold),
	)

	slaveConfig := mysql.NewConfig(
		mysql.Database(conf.MysqlSlaveV2.DbName),
		mysql.Host(conf.MysqlSlaveV2.Port),
		mysql.Login(conf.MysqlSlaveV2.Login),
		mysql.Password(conf.MysqlSlaveV2.Password),
		mysql.MustConnected(true),
		mysql.DataCenterID(conf.DataCentreID),
		mysql.MaxIdleConns(conf.MysqlSlaveV2.MaxIdleConns),
		mysql.MaxOpenConns(conf.MysqlSlaveV2.MaxOpenConns),
		mysql.ConnMaxLifetime(conf.MysqlSlaveV2.ConnMaxLifetime),
		mysql.SlaveLagThreshold(conf.MysqlSlaveV2.SlaveLagThreshold),
	)

	var err error
	mysqlClient, err = mysql.New(masterConfig, slaveConfig)
	if err != nil {
		logs.Critical("Fail to init MySQL, err: ", err)
		os.Exit(1)
	}

	dbLogger, err := dberr.NewLogger("_clickmysql", func(s string) error {
		_, err := mysqlClient.Exec(s)
		return err
	})
	mysqlClient.ErrLogger(dbLogger)

	currentApp.SetMysql(mysqlClient)
}

func initHdfs() {
	var err error

	conf := currentApp.GetConfig().HDFS

	hdfsConf := hdfs.Config{
		Url:       conf.URL,
		OrcUrl:    conf.OrcURL,
		User:      conf.User,
		CachePath: conf.CachePath,
		Jar:       conf.Jar,
		Table:     conf.Table,
	}

	logger, err := hdfs.NewLogger(hdfsConf, hdfs.Options{
		CachePath:  conf.CachePath,
		Period:     conf.SyncPeriod,
		HdfsUrl:    conf.URL,
		HdfsOrcUrl: conf.OrcURL,
		HdfsUser:   conf.User,
		Workers:    conf.Workers,
		Operation:  hdfs.CREATE,
	})

	if err != nil {
		logs.Critical("Fail to init HDFS, err: ", err)
		os.Exit(1)
	}
	currentApp.SetHDFS(logger)
}

func initConfig() {
	configNamePtr := flag.String("f", "kafka.conf", "config file path")
	flag.Parse()
	conf, err := config.Init(*configNamePtr)
	if err != nil {
		logs.Critical("error loading config, err: ", err)
		os.Exit(1)
	}
	currentApp.SetConfig(conf)
	return
}

func initGeoIpDB() {
	conf := currentApp.GetConfig()
	geoipdb := db.New(conf.GeoipDb)
	currentApp.SetGeoIpDb(geoipdb)
}

func initMongodb() {
	conf := currentApp.GetConfig()
	mongoSession := mongo.Init(conf)

	currentApp.SetMongoSession(&mongoSession)
}

func initAerospike() {

	conf := currentApp.GetConfig()
	asConf := as.NewConfig()

	asConf.Namespace = conf.Aerospike.Namespace
	asConf.SetName = conf.Aerospike.SetName
	asConf.PackageName = conf.Aerospike.PackageName
	asConf.AsyncReconnect = false

	if len(conf.Aerospike.Hosts) > 0 {
		for _, h := range conf.Aerospike.Hosts {
			asConf.Hosts = append(asConf.Hosts, as.Host{Host: h.Host, Port: h.Port})
		}
	} else {
		asConf.Host, asConf.Port = conf.Aerospike.Host, conf.Aerospike.Port
	}

	asClient, _ := as.NewSession(asConf)
	currentApp.SetAerospike(asClient)
}

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

	Record struct {
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
		TickersCompositeId int64
		DeviceType         string
		OsId               string
		RegionId           uint16
		GeoZoneId          uint16
		BrowserId          string
		Uuid               string
		TrafficSource      string
		TrafficType        string
		Hash2              string
		Provider           int64
		ClickSignData      ssp.SignData
		ClickItemType      ClickItemType
	}
)
