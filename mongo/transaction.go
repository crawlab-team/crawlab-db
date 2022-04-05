package mongo

import (
	"context"
	"github.com/crawlab-team/go-trace"
	"go.mongodb.org/mongo-driver/mongo"
)

func RunTransaction(fn func(mongo.SessionContext) error) (err error) {
	s, err := _client.StartSession()
	if err != nil {
		return trace.TraceError(err)
	}
	if err := s.StartTransaction(); err != nil {
		return trace.TraceError(err)
	}
	if err := mongo.WithSession(context.Background(), s, func(sc mongo.SessionContext) error {
		if err := fn(sc); err != nil {
			return trace.TraceError(err)
		}
		if err = s.CommitTransaction(sc); err != nil {
			return trace.TraceError(err)
		}
		return nil
	}); err != nil {
		return trace.TraceError(err)
	}
	return nil
}
