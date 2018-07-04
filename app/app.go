package app

import (
	"log"
	"os"
	"time"

	"golib/aerospike"
	"golib/clickhouse-v2"
	"golib/hdfs"
	"golib/logs"
	"golib/mongo"
	"golib/mysql-v2"
	"golib/new/client/kafka"
	"golib/new/config"
	"golib/services/capping_consumer/app/db"
	"golib/shutdown"
)

type App struct {
	mysqlClient      mysql.IClient
	clickhouseClient *clickhouse.Cluster
	hdfsLogger       *hdfs.Logger
	config           *config.Data
	logger           *log.Logger
	kafka            *kafka.Consumer
	pushKafka        *kafka.Consumer
	geoIpDb          *db.Range
	mongoSession     *mongo.MongoSession
	aerospike        aerospike.ISession
	hashLogger       *log.Logger
	gSettings        *globalSettings
}

func (app *App) SetConfig(config *config.Data) {
	app.config = config
}

func (app *App) GetConfig() *config.Data {
	return app.config
}

func (app *App) SetHDFS(hdfs *hdfs.Logger) {
	app.hdfsLogger = hdfs
}

func (app *App) SetMysql(mysql mysql.IClient) {
	app.mysqlClient = mysql
}

func (app *App) SetKafka(consumer *kafka.Consumer) {
	app.kafka = consumer
}

func (app *App) SetPushKafka(consumer *kafka.Consumer) {
	app.pushKafka = consumer
}

func (app *App) SetClickhouse(cluster *clickhouse.Cluster) {
	app.clickhouseClient = cluster
}

func (app *App) SetGeoIpDb(rangeIp *db.Range) {
	app.geoIpDb = rangeIp
}

func (app *App) SetMongoSession(mongoSession *mongo.MongoSession) {
	app.mongoSession = mongoSession
}

func (app *App) SetAerospike(session aerospike.ISession) {
	app.aerospike = session
}

func (app *App) SetHashLogger(logger *log.Logger) {
	app.logger = logger
}

func (app *App) initDB() {
	err := db.Init(app.mysqlClient, app.clickhouseClient, app.aerospike, app.mongoSession, app.config)
	if err != nil {
		logs.Criticalf("Failed to initDB, err:%q", err.Error())
		os.Exit(1)
	} else {
		logs.Write("DB initialized")
	}
}

func (app *App) Start() {

	app.initDB()

	app.initGlobalSettings()

	app.currentStatusWorker()

	app.gracefulShutdown()

	for i := 0; i <= app.config.WorkerCount; i++ {
		go app.worker(false)
		go app.worker(true)
	}

	logs.Write("init complete")

}

func (app *App) gracefulShutdown() {
	shutdown.OnShutdown(func() {

		logs.Write("shutdown: begin")

		logs.Write("shutdown: consumer.Close")
		app.kafka.Close()

		logs.Write("shutdown: db.Shutdown()")
		db.Shutdown()

		logs.Write("shutdown: hdfs.Shutdown()")
		app.hdfsLogger.Shutdown()

		logs.Write("shutdown: end")

		os.Exit(0)
	})

}
func (app *App) currentStatusWorker() {
	app.hdfsLogger.SetUpdateDelay = db.SetUpdateDelay
	app.hdfsLogger.UpdateConsumerLag = db.UpdateConsumerLag
	go func() {
		for {
			db.UpdateCurrentStatus()
			time.Sleep(time.Minute)
		}
	}()

}
