package db

import (
	"github.com/apex/log"
	"github.com/cenkalti/backoff/v4"
	"github.com/globalsign/mgo"
	"github.com/spf13/viper"
	"net"
	"runtime/debug"
	"time"
)

var Session *mgo.Session

func GetSession() *mgo.Session {
	return Session.Copy()
}

func GetDb() (*mgo.Session, *mgo.Database) {
	s := GetSession()
	return s, s.DB(viper.GetString("mongo.db"))
}

func GetCol(collectionName string) (*mgo.Session, *mgo.Collection) {
	s := GetSession()
	db := s.DB(viper.GetString("mongo.db"))
	col := db.C(collectionName)
	return s, col
}

func GetGridFs(prefix string) (*mgo.Session, *mgo.GridFS) {
	s, db := GetDb()
	gf := db.GridFS(prefix)
	return s, gf
}

func GetDataSourceCol(host string, port string, username string, password string, authSource string, database string, col string) (*mgo.Session, *mgo.Collection, error) {
	timeout := time.Second * 10
	dialInfo := mgo.DialInfo{
		Addrs:         []string{net.JoinHostPort(host, port)},
		Timeout:       timeout,
		Database:      database,
		PoolLimit:     100,
		PoolTimeout:   timeout,
		ReadTimeout:   timeout,
		WriteTimeout:  timeout,
		AppName:       "crawlab",
		FailFast:      true,
		MinPoolSize:   10,
		MaxIdleTimeMS: 1000 * 30,
	}
	if username != "" {
		dialInfo.Username = username
		dialInfo.Password = password
		dialInfo.Source = authSource
	}
	s, err := mgo.DialWithInfo(&dialInfo)
	if err != nil {
		log.Errorf("dial mongo error: " + err.Error())
		debug.PrintStack()
		return nil, nil, err
	}
	db := s.DB(database)
	return s, db.C(col), nil
}

func InitMongo() error {
	var mongoHost = viper.GetString("mongo.host")
	var mongoPort = viper.GetString("mongo.port")
	var mongoDb = viper.GetString("mongo.db")
	var mongoUsername = viper.GetString("mongo.username")
	var mongoPassword = viper.GetString("mongo.password")
	var mongoAuth = viper.GetString("mongo.authSource")

	if Session == nil {
		var dialInfo mgo.DialInfo
		addr := net.JoinHostPort(mongoHost, mongoPort)
		timeout := time.Second * 10
		dialInfo = mgo.DialInfo{
			Addrs:         []string{addr},
			Timeout:       timeout,
			Database:      mongoDb,
			PoolLimit:     100,
			PoolTimeout:   timeout,
			ReadTimeout:   timeout,
			WriteTimeout:  timeout,
			AppName:       "crawlab",
			FailFast:      true,
			MinPoolSize:   10,
			MaxIdleTimeMS: 1000 * 30,
		}
		if mongoUsername != "" {
			dialInfo.Username = mongoUsername
			dialInfo.Password = mongoPassword
			dialInfo.Source = mongoAuth
		}
		bp := backoff.NewExponentialBackOff()
		var err error

		err = backoff.Retry(func() error {
			Session, err = mgo.DialWithInfo(&dialInfo)
			if err != nil {
				log.WithError(err).Warnf("waiting for connect mongo database, after %f seconds try again.", bp.NextBackOff().Seconds())
			}
			return err
		}, bp)
	}

	return nil
}
