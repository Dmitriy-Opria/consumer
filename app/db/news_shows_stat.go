package db

import (
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"time"

	"golib/logs"
	"golib/mongo"
	"golib/services/capping_consumer/app/statistics"
)

type newsShowsStat struct {
	client *mongo.MongoSession
}

func (n *newsShowsStat) Worker() {
	for {
		statisticsSyncPeriod := time.Duration(appConfig.Mongodb.StatisticsSavePeriod)
		time.Sleep(statisticsSyncPeriod * time.Minute)
		go n.Save()
	}
}

func (n *newsShowsStat) Save() {

	data := statistics.GetNewsShows()
	defer logs.Recover()
	if len(data) == 0 {
		return
	}
	_, sess, closer := n.client.Get()
	defer closer()

	c := sess.DB(dbname).C("news")

	bulk := c.Bulk()
	i := 0
	for key, value := range data {
		i++
		bulk.Update(bson.M{"_id": key.NewsId}, bson.M{"$inc": bson.M{"showsAll": value.NewsShows}})
		if i%1000 == 0 {
			if _, err := bulk.Run(); err != nil {
				mongo.OnMgoError(fmt.Errorf("bulk.Run failed: " + err.Error()))
			}
			bulk = c.Bulk()
		}
	}
	if _, err := bulk.Run(); err != nil {
		mongo.OnMgoError(fmt.Errorf("bulk.Run failed: " + err.Error()))
	}
}
