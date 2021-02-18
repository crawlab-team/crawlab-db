package mongo

import (
	"context"
	"github.com/crawlab-team/crawlab-db/errors"
	"github.com/crawlab-team/go-trace"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ColInterface interface {
	Insert(doc interface{}) (id primitive.ObjectID, err error)
	InsertMany(docs []interface{}) (ids []primitive.ObjectID, err error)
	UpdateId(id primitive.ObjectID, update interface{}) (err error)
	Update(query bson.M, update interface{}) (err error)
	UpdateWithOptions(query bson.M, update interface{}, opts *options.UpdateOptions) (err error)
	ReplaceId(id primitive.ObjectID, doc interface{}) (err error)
	Replace(query bson.M, doc interface{}) (err error)
	ReplaceWithOptions(query bson.M, doc interface{}, opts *options.ReplaceOptions) (err error)
	DeleteId(id primitive.ObjectID) (err error)
	Delete(query bson.M) (err error)
	DeleteWithOptions(query bson.M, opts *options.DeleteOptions) (err error)
	Find(query bson.M, opts *FindOptions) (fr *FindResult)
	FindId(id primitive.ObjectID) (fr *FindResult)
	Count(query bson.M) (total int, err error)
	CreateIndex(indexModel mongo.IndexModel) (err error)
	CreateIndexes(indexModels []mongo.IndexModel) (err error)
	DeleteIndex(name string) (err error)
	DeleteAllIndexes() (err error)
	ListIndexes() (indexes []map[string]interface{}, err error)
	GetContext() (ctx context.Context)
	GetName() (name string)
}

type FindOptions struct {
	Skip  int
	Limit int
	Sort  bson.M
}

var ctx = context.Background()

type Col struct {
	ctx context.Context
	db  *mongo.Database
	c   *mongo.Collection
}

func (col *Col) Insert(doc interface{}) (id primitive.ObjectID, err error) {
	res, err := col.c.InsertOne(col.ctx, doc)
	if err != nil {
		return primitive.NilObjectID, trace.TraceError(err)
	}
	if id, ok := res.InsertedID.(primitive.ObjectID); ok {
		return id, nil
	}
	return primitive.NilObjectID, trace.TraceError(errors.ErrInvalidType)
}

func (col *Col) InsertMany(docs []interface{}) (ids []primitive.ObjectID, err error) {
	res, err := col.c.InsertMany(col.ctx, docs)
	if err != nil {
		return nil, trace.TraceError(err)
	}
	for _, v := range res.InsertedIDs {
		if id, ok := v.(primitive.ObjectID); ok {
			ids = append(ids, id)
			continue
		}
		return nil, trace.TraceError(errors.ErrInvalidType)
	}
	return ids, nil
}

func (col *Col) UpdateId(id primitive.ObjectID, update interface{}) (err error) {
	_, err = col.c.UpdateOne(col.ctx, bson.M{"_id": id}, update)
	if err != nil {
		return trace.TraceError(err)
	}
	return nil
}

func (col *Col) Update(query bson.M, update interface{}) (err error) {
	return col.UpdateWithOptions(query, update, nil)
}

func (col *Col) UpdateWithOptions(query bson.M, update interface{}, opts *options.UpdateOptions) (err error) {
	if opts == nil {
		_, err = col.c.UpdateMany(col.ctx, query, update)
	} else {
		_, err = col.c.UpdateMany(col.ctx, query, update, opts)
	}
	if err != nil {
		return trace.TraceError(err)
	}
	return nil
}

func (col *Col) ReplaceId(id primitive.ObjectID, doc interface{}) (err error) {
	return col.Replace(bson.M{"_id": id}, doc)
}

func (col *Col) Replace(query bson.M, doc interface{}) (err error) {
	return col.ReplaceWithOptions(query, doc, nil)
}

func (col *Col) ReplaceWithOptions(query bson.M, doc interface{}, opts *options.ReplaceOptions) (err error) {
	if opts == nil {
		_, err = col.c.ReplaceOne(col.ctx, query, doc)
	} else {
		_, err = col.c.ReplaceOne(col.ctx, query, doc, opts)
	}
	if err != nil {
		return trace.TraceError(err)
	}
	return nil
}

func (col *Col) DeleteId(id primitive.ObjectID) (err error) {
	_, err = col.c.DeleteOne(col.ctx, bson.M{"_id": id})
	if err != nil {
		return trace.TraceError(err)
	}
	return nil
}

func (col *Col) Delete(query bson.M) (err error) {
	return col.DeleteWithOptions(query, nil)
}

func (col *Col) DeleteWithOptions(query bson.M, opts *options.DeleteOptions) (err error) {
	if opts == nil {
		_, err = col.c.DeleteMany(col.ctx, query)
	} else {
		_, err = col.c.DeleteMany(col.ctx, query, opts)
	}
	if err != nil {
		return trace.TraceError(err)
	}
	return nil
}

func (col *Col) Find(query bson.M, opts *FindOptions) (fr *FindResult) {
	_opts := &options.FindOptions{}
	if opts != nil {
		if opts.Skip != 0 {
			skipInt64 := int64(opts.Skip)
			_opts.Skip = &skipInt64
		}
		if opts.Limit != 0 {
			limitInt64 := int64(opts.Limit)
			_opts.Skip = &limitInt64
		}
		if opts.Sort != nil {
			_opts.Sort = opts.Sort
		}
	}
	cur, err := col.c.Find(col.ctx, query, _opts)
	if err != nil {
		return &FindResult{
			col: col,
			err: err,
		}
	}
	fr = &FindResult{
		col: col,
		cur: cur,
	}
	return fr
}

func (col *Col) FindId(id primitive.ObjectID) (fr *FindResult) {
	res := col.c.FindOne(col.ctx, bson.M{"_id": id})
	if res.Err() != nil {
		return &FindResult{
			col: col,
			err: res.Err(),
		}
	}
	fr = &FindResult{
		col: col,
		res: res,
	}
	return fr
}

func (col *Col) Count(query bson.M) (total int, err error) {
	totalInt64, err := col.c.CountDocuments(col.ctx, query)
	if err != nil {
		return 0, err
	}
	total = int(totalInt64)
	return total, nil
}

func (col *Col) CreateIndex(indexModel mongo.IndexModel) (err error) {
	_, err = col.c.Indexes().CreateOne(col.ctx, indexModel)
	if err != nil {
		return trace.TraceError(err)
	}
	return nil
}

func (col *Col) CreateIndexes(indexModels []mongo.IndexModel) (err error) {
	_, err = col.c.Indexes().CreateMany(col.ctx, indexModels)
	if err != nil {
		return trace.TraceError(err)
	}
	return nil
}

func (col *Col) DeleteIndex(name string) (err error) {
	_, err = col.c.Indexes().DropOne(col.ctx, name)
	if err != nil {
		return trace.TraceError(err)
	}
	return nil
}

func (col *Col) DeleteAllIndexes() (err error) {
	_, err = col.c.Indexes().DropAll(col.ctx)
	if err != nil {
		return trace.TraceError(err)
	}
	return nil
}

func (col *Col) ListIndexes() (indexes []map[string]interface{}, err error) {
	cur, err := col.c.Indexes().List(col.ctx)
	if err != nil {
		return nil, err
	}
	if err := cur.All(col.ctx, &indexes); err != nil {
		return nil, err
	}
	return indexes, nil
}

func (col *Col) GetContext() (ctx context.Context) {
	return col.ctx
}

func (col *Col) GetName() (name string) {
	return col.c.Name()
}

func GetMongoCol(colName string) (col *Col) {
	return GetMongoColWithDb(colName, "")
}

func GetMongoColWithDb(colName, dbName string) (col *Col) {
	ctx := context.Background()
	db := GetMongoDb(dbName)
	c := db.Collection(colName)
	col = &Col{
		ctx: ctx,
		db:  db,
		c:   c,
	}
	return col
}

//func GetDataSourceCol(host string, port string, username string, password string, authSource string, database string, col string) (*mgo.Session, *mgo.Collection, error) {
//    timeout := time.Second * 10
//    dialInfo := mgo.DialInfo{
//        Addrs:         []string{net.JoinHostPort(host, port)},
//        Timeout:       timeout,
//        Database:      database,
//        PoolLimit:     100,
//        PoolTimeout:   timeout,
//        ReadTimeout:   timeout,
//        WriteTimeout:  timeout,
//        AppName:       "crawlab",
//        FailFast:      true,
//        MinPoolSize:   10,
//        MaxIdleTimeMS: 1000 * 30,
//    }
//    if username != "" {
//        dialInfo.Username = username
//        dialInfo.Password = password
//        dialInfo.Source = authSource
//    }
//    s, err := mgo.DialWithInfo(&dialInfo)
//    if err != nil {
//        log.Errorf("dial mongo error: " + err.Error())
//        debug.PrintStack()
//        return nil, nil, err
//    }
//    db := s.DB(database)
//    return s, db.C(col), nil
//}
