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

var Client *mongo.Client

func InitMongo() error {
	var mongoUri = viper.GetString("mongo.uri")
	var mongoHost = viper.GetString("mongo.host")
	var mongoPort = viper.GetString("mongo.port")
	var mongoDb = viper.GetString("mongo.db")
	var mongoHosts = viper.GetStringSlice("mongo.hosts")
	var mongoUsername = viper.GetString("mongo.username")
	var mongoPassword = viper.GetString("mongo.password")
	var mongoAuthSource = viper.GetString("mongo.authSource")
	var mongoAuthMechanism = viper.GetString("mongo.authMechanism")
	var mongoAuthMechanismProperties = viper.GetStringMapString("mongo.authMechanismProperties")

	if Client == nil {
		if mongoHost == "" {
			mongoHost = "localhost"
		}
		if mongoPort == "" {
			mongoPort = "27017"
		}
		if mongoDb == "" {
			mongoDb = "crawlab"
		}
		if mongoAuthSource == "" {
			mongoAuthSource = "admin"
		}
		opts := &options.ClientOptions{
			AppName: &AppName,
		}
		if mongoUri != "" {
			// uri is set
			opts.ApplyURI(mongoUri)
		} else {
			// uri is unset

			// username and password are set
			if mongoUsername != "" && mongoPassword != "" {
				opts.SetAuth(options.Credential{
					AuthMechanism:           mongoAuthMechanism,
					AuthMechanismProperties: mongoAuthMechanismProperties,
					AuthSource:              mongoAuthSource,
					Username:                mongoUsername,
					Password:                mongoPassword,
					PasswordSet:             true,
				})
			}

			if len(mongoHosts) > 0 {
				// hosts are set
				opts.SetHosts(mongoHosts)
			} else {
				// hosts are unset
				opts.ApplyURI(fmt.Sprintf("mongodb://%s:%s/%s", mongoHost, mongoPort, mongoDb))
			}
		}

		// construct mongo client

		// attempt to connect with retry
		bp := backoff.NewExponentialBackOff()
		var err error
		err = backoff.Retry(func() error {
			errMsg := fmt.Sprintf("waiting for connect mongo database, after %f seconds try again.", bp.NextBackOff().Seconds())
			Client, err = mongo.NewClient(opts)
			if err != nil {
				log.WithError(err).Warnf(errMsg)
				return err
			}
			if err := Client.Connect(context.TODO()); err != nil {
				log.WithError(err).Warnf(errMsg)
				return err
			}
			return nil
		}, bp)
	}

	return nil
}
