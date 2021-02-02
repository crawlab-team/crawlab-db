package mongo

import (
	"fmt"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
)

type ColTestObject struct {
	dbName  string
	colName string
	col     *Col
}

func setupColTest() (to *ColTestObject, err error) {
	dbName := "test_db"
	colName := "test_col"
	viper.Set("mongo.db", dbName)
	if err := InitMongo(); err != nil {
		return nil, err
	}
	col := GetMongoCol(colName)
	if err := col.db.Drop(col.ctx); err != nil {
		return nil, err
	}
	return &ColTestObject{
		dbName:  dbName,
		colName: colName,
		col:     col,
	}, nil
}

func cleanupColTest(to *ColTestObject) {
	_ = to.col.db.Drop(to.col.ctx)
}

func TestGetMongoCol(t *testing.T) {
	colName := "test_col"
	err := InitMongo()
	require.Nil(t, err)

	col := GetMongoCol(colName)
	require.Equal(t, colName, col.c.Name())
}

func TestGetMongoColWithDb(t *testing.T) {
	dbName := "test_db"
	colName := "test_col"
	err := InitMongo()
	require.Nil(t, err)

	col := GetMongoColWithDb(colName, dbName)
	require.Equal(t, colName, col.c.Name())
	require.Equal(t, dbName, col.db.Name())
}

func TestCol_Insert(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	id, err := to.col.Insert(bson.M{"key": "value"})
	require.Nil(t, err)
	require.IsType(t, primitive.ObjectID{}, id)

	res, err := to.col.FindId(id)
	require.Nil(t, err)
	var doc map[string]string
	err = res.One(&doc)
	require.Nil(t, err)
	require.Equal(t, doc["key"], "value")

	cleanupColTest(to)
}

func TestCol_InsertMany(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	n := 10
	var docs []interface{}
	for i := 0; i < n; i++ {
		docs = append(docs, bson.M{"key": fmt.Sprintf("value-%d", i)})
	}
	ids, err := to.col.InsertMany(docs)
	require.Nil(t, err)
	require.Equal(t, n, len(ids))

	res, err := to.col.Find(nil, &FindOptions{Sort: bson.M{"_id": 1}})
	require.Nil(t, err)
	var resDocs []map[string]string
	err = res.All(&resDocs)
	require.Nil(t, err)
	require.Equal(t, n, len(resDocs))
	for i, doc := range resDocs {
		require.Equal(t, fmt.Sprintf("value-%d", i), doc["key"])
	}

	cleanupColTest(to)
}

func TestCol_UpdateId(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	id, err := to.col.Insert(bson.M{"key": "old-value"})
	require.Nil(t, err)

	err = to.col.UpdateId(id, bson.M{
		"$set": bson.M{
			"key": "new-value",
		},
	})
	require.Nil(t, err)

	var doc map[string]string
	res, err := to.col.FindId(id)
	require.Nil(t, err)
	err = res.One(&doc)
	require.Nil(t, err)
	require.Equal(t, "new-value", doc["key"])

	cleanupColTest(to)
}

func TestCol_Update(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	n := 10
	var docs []interface{}
	for i := 0; i < n; i++ {
		docs = append(docs, bson.M{"key": fmt.Sprintf("old-value-%d", i)})
	}

	err = to.col.Update(nil, bson.M{
		"$set": bson.M{
			"key": "new-value",
		},
	})
	require.Nil(t, err)

	var resDocs []map[string]string
	res, err := to.col.Find(nil, &FindOptions{Sort: bson.M{"_id": 1}})
	require.Nil(t, err)
	err = res.All(&resDocs)
	require.Nil(t, err)
	for _, doc := range resDocs {
		require.Equal(t, "new-value", doc["key"])
	}

	cleanupColTest(to)
}

func TestCol_ReplaceId(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	id, err := to.col.Insert(bson.M{"key": "old-value"})
	require.Nil(t, err)

	var doc map[string]interface{}
	res, err := to.col.FindId(id)
	require.Nil(t, err)
	err = res.One(&doc)
	require.Nil(t, err)
	doc["key"] = "new-value"

	err = to.col.ReplaceId(id, doc)
	require.Nil(t, err)

	res, err = to.col.FindId(id)
	require.Nil(t, err)
	err = res.One(&doc)
	require.Nil(t, err)
	require.Equal(t, "new-value", doc["key"])

	cleanupColTest(to)
}

func TestCol_Replace(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	id, err := to.col.Insert(bson.M{"key": "old-value"})
	require.Nil(t, err)

	var doc map[string]interface{}
	res, err := to.col.FindId(id)
	require.Nil(t, err)
	err = res.One(&doc)
	require.Nil(t, err)
	doc["key"] = "new-value"

	err = to.col.Replace(bson.M{"key": "old-value"}, doc)
	require.Nil(t, err)

	res, err = to.col.FindId(id)
	require.Nil(t, err)
	err = res.One(&doc)
	require.Nil(t, err)
	require.Equal(t, "new-value", doc["key"])

	cleanupColTest(to)
}

func TestCol_DeleteId(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	id, err := to.col.Insert(bson.M{"key": "value"})
	require.Nil(t, err)

	err = to.col.DeleteId(id)
	require.Nil(t, err)

	total, err := to.col.Count(nil)
	require.Nil(t, err)
	require.Equal(t, 0, total)

	cleanupColTest(to)
}

func TestCol_Delete(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	n := 10
	var docs []interface{}
	for i := 0; i < n; i++ {
		docs = append(docs, bson.M{"key": fmt.Sprintf("value-%d", i)})
	}
	ids, err := to.col.InsertMany(docs)
	require.Nil(t, err)
	require.Equal(t, n, len(ids))

	err = to.col.Delete(bson.M{"key": "value-0"})
	require.Nil(t, err)

	total, err := to.col.Count(nil)
	require.Nil(t, err)
	require.Equal(t, n-1, total)

	err = to.col.Delete(nil)
	require.Nil(t, err)

	total, err = to.col.Count(nil)
	require.Nil(t, err)
	require.Equal(t, 0, total)

	cleanupColTest(to)
}

func TestCol_FindId(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	id, err := to.col.Insert(bson.M{"key": "value"})
	require.Nil(t, err)

	var doc map[string]string
	res, err := to.col.FindId(id)
	require.Nil(t, err)
	err = res.One(&doc)
	require.Nil(t, err)
	require.Equal(t, "value", doc["key"])

	cleanupColTest(to)
}

func TestCol_Find(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	n := 10
	var docs []interface{}
	for i := 0; i < n; i++ {
		docs = append(docs, bson.M{"key": fmt.Sprintf("value-%d", i)})
	}
	ids, err := to.col.InsertMany(docs)
	require.Nil(t, err)
	require.Equal(t, n, len(ids))

	res, err := to.col.Find(nil, nil)
	require.Nil(t, err)
	err = res.All(&docs)
	require.Nil(t, err)
	require.Equal(t, n, len(docs))

	res, err = to.col.Find(bson.M{"key": bson.M{"$gte": fmt.Sprintf("value-%d", 5)}}, nil)
	require.Nil(t, err)
	err = res.All(&docs)
	require.Nil(t, err)
	require.Equal(t, n-5, len(docs))

	res, err = to.col.Find(nil, &FindOptions{
		Skip: 5,
	})
	require.Nil(t, err)
	err = res.All(&docs)
	require.Nil(t, err)
	require.Equal(t, n-5, len(docs))

	res, err = to.col.Find(nil, &FindOptions{
		Limit: 5,
	})
	require.Nil(t, err)
	err = res.All(&docs)
	require.Nil(t, err)
	require.Equal(t, 5, len(docs))

	var resDocs []map[string]string
	res, err = to.col.Find(nil, &FindOptions{
		Sort: bson.M{"key": 1},
	})
	require.Nil(t, err)
	err = res.All(&resDocs)
	require.Nil(t, err)
	require.Greater(t, len(resDocs), 0)
	require.Equal(t, "value-0", resDocs[0]["key"])

	res, err = to.col.Find(nil, &FindOptions{
		Sort: bson.M{"key": -1},
	})
	require.Nil(t, err)
	err = res.All(&resDocs)
	require.Nil(t, err)
	require.Greater(t, len(resDocs), 0)
	require.Equal(t, fmt.Sprintf("value-%d", n-1), resDocs[0]["key"])

	cleanupColTest(to)
}

func TestCol_CreateIndex(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	err = to.col.CreateIndex(mongo.IndexModel{
		Keys: bson.D{{"key", 1}},
	})
	require.Nil(t, err)

	indexes, err := to.col.ListIndexes()
	require.Nil(t, err)
	require.Equal(t, 2, len(indexes))

	cleanupColTest(to)
}

func TestCol_CreateIndexes(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	err = to.col.CreateIndexes([]mongo.IndexModel{
		{
			Keys: bson.D{{"key", 1}},
		},
		{
			Keys: bson.D{{"empty-key", 1}},
		},
	})
	require.Nil(t, err)

	indexes, err := to.col.ListIndexes()
	require.Nil(t, err)
	require.Equal(t, 3, len(indexes))

	cleanupColTest(to)
}

func TestCol_DeleteIndex(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	err = to.col.CreateIndex(mongo.IndexModel{
		Keys: bson.D{{"key", 1}},
	})
	require.Nil(t, err)

	indexes, err := to.col.ListIndexes()
	require.Nil(t, err)
	require.Equal(t, 2, len(indexes))
	for _, index := range indexes {
		name, ok := index["name"].(string)
		require.True(t, ok)

		if name != "_id_" {
			err = to.col.DeleteIndex(name)
			require.Nil(t, err)
			break
		}
	}

	indexes, err = to.col.ListIndexes()
	require.Nil(t, err)
	require.Equal(t, 1, len(indexes))

	cleanupColTest(to)
}

func TestCol_DeleteIndexes(t *testing.T) {
	to, err := setupColTest()
	require.Nil(t, err)

	err = to.col.CreateIndexes([]mongo.IndexModel{
		{
			Keys: bson.D{{"key", 1}},
		},
		{
			Keys: bson.D{{"empty-key", 1}},
		},
	})
	require.Nil(t, err)

	err = to.col.DeleteAllIndexes()
	require.Nil(t, err)

	indexes, err := to.col.ListIndexes()
	require.Nil(t, err)
	require.Equal(t, 1, len(indexes))

	cleanupColTest(to)
}
