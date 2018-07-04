package db

import (
	"gopkg.in/mgo.v2/bson"
	"sync/atomic"

	"golib/mongo"
)

type VideoContent struct {
	VideoTeaserId uint32 `bson:"video_teaser_id"`
	CategoryId    int    `bson:"id_category"`
}

var (
	dbname                     = "exchange"
	videoDbName                = "video"
	videoContentCategory       atomic.Value
	providedVideoFields        = bson.M{"video_teaser_id": 1, "id_category": 1}
	providedVideoCategoryQuery = bson.M{"toSync": 3, "id_category": bson.M{"$exists": true}}
)

func init() {
	videoContentCategory.Store(map[uint32]int{})
}

func loadVideoContentCategory(mongoClient *mongo.MongoSession) error {

	categories := map[uint32]int{}

	_, db, closer := mongoClient.Get()

	defer closer()

	c := db.DB(videoDbName).C("provided_video")
	var results = make([]VideoContent, 0, 32)

	err := c.Find(providedVideoCategoryQuery).Select(providedVideoFields).All(&results)

	if err != nil {
		mongo.OnMgoError(err)
		return err
	}

	for n := range results {
		categories[results[n].VideoTeaserId] = results[n].CategoryId
	}

	videoContentCategory.Store(categories)
	return nil
}

func GetVideoContentCategory(id uint32) int {
	m := videoContentCategory.Load().(map[uint32]int)
	return m[id]
}
