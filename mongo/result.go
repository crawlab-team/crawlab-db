package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
)

type FindResultInterface interface {
	One(val interface{}) (err error)
	All(val interface{}) (err error)
}

type FindResult struct {
	col *Col
	res *mongo.SingleResult
	cur *mongo.Cursor
}

func (fr *FindResult) One(val interface{}) (err error) {
	if fr.cur != nil {
		return fr.cur.Decode(val)
	}
	return fr.res.Decode(val)
}

func (fr *FindResult) All(val interface{}) (err error) {
	var ctx context.Context
	if fr.col == nil {
		ctx = context.Background()
	} else {
		ctx = fr.col.ctx
	}
	return fr.cur.All(ctx, val)
}
