package mongo

import (
	"context"
	"fmt"
	"github.com/apex/log"
	"github.com/cenkalti/backoff/v4"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var AppName = "crawlab-db"

var _client *mongo.Client

func GetMongoClient(opts ...ClientOption) (c *mongo.Client, err error) {
	if _client != nil {
		return _client, nil
	}

	_opts := &ClientOptions{
		Host:       "localhost",
		Port:       "27017",
		Db:         "crawlab",
		AuthSource: "admin",
	}
	for _, op := range opts {
		op(_opts)
	}

	if _opts.Uri == "" {
		_opts.Uri = viper.GetString("mongo.uri")
	}
	if _opts.Host == "" {
		_opts.Host = viper.GetString("mongo.host")
	}
	if _opts.Port == "" {
		_opts.Port = viper.GetString("mongo.port")
	}
	if _opts.Db == "" {
		_opts.Db = viper.GetString("mongo.db")
	}
	if len(_opts.Hosts) == 0 {
		_opts.Hosts = viper.GetStringSlice("mongo.hosts")
	}
	if _opts.Username == "" {
		_opts.Username = viper.GetString("mongo.username")
	}
	if _opts.Password == "" {
		_opts.Password = viper.GetString("mongo.password")
	}
	if _opts.AuthSource == "" {
		_opts.AuthSource = viper.GetString("mongo.authSource")
	}
	if _opts.AuthMechanism == "" {
		_opts.AuthMechanism = viper.GetString("mongo.authMechanism")
	}
	if _opts.AuthMechanismProperties == nil {
		_opts.AuthMechanismProperties = viper.GetStringMapString("mongo.authMechanismProperties")
	}

	mongoOpts := &options.ClientOptions{
		AppName: &AppName,
	}
	if _opts.Uri != "" {
		// uri is set
		mongoOpts.ApplyURI(_opts.Uri)
	} else {
		// uri is unset

		// username and password are set
		if _opts.Username != "" && _opts.Password != "" {
			mongoOpts.SetAuth(options.Credential{
				AuthMechanism:           _opts.AuthMechanism,
				AuthMechanismProperties: _opts.AuthMechanismProperties,
				AuthSource:              _opts.AuthSource,
				Username:                _opts.Username,
				Password:                _opts.Password,
				PasswordSet:             true,
			})
		}

		if len(_opts.Hosts) > 0 {
			// hosts are set
			mongoOpts.SetHosts(_opts.Hosts)
		} else {
			// hosts are unset
			mongoOpts.ApplyURI(fmt.Sprintf("mongodb://%s:%s/%s", _opts.Host, _opts.Port, _opts.Db))
		}
	}

	// attempt to connect with retry
	bp := backoff.NewExponentialBackOff()
	err = backoff.Retry(func() error {
		errMsg := fmt.Sprintf("waiting for connect mongo database, after %f seconds try again.", bp.NextBackOff().Seconds())
		c, err = mongo.NewClient(mongoOpts)
		if err != nil {
			log.WithError(err).Warnf(errMsg)
			return err
		}
		if err := c.Connect(context.TODO()); err != nil {
			log.WithError(err).Warnf(errMsg)
			return err
		}
		return nil
	}, bp)

	_client = c

	return c, nil
}
