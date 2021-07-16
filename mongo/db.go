package mongo

import (
	"github.com/crawlab-team/go-trace"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
)

func GetMongoDb(dbName string) (db *mongo.Database) {
	if dbName == "" {
		dbName = viper.GetString("mongo.db")
	}
	if dbName == "" {
		dbName = "test"
	}

	// client
	c, err := GetMongoClient()
	if err != nil {
		trace.PrintError(err)
		return nil
	}

	return c.Database(dbName, nil)
}
