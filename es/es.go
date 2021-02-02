package es

import (
	"context"
	"github.com/apex/log"
	"github.com/olivere/elastic/v7"
	"github.com/satori/go.uuid"
	"github.com/spf13/viper"
	"sync"
	"time"
)

var doOnce sync.Once
var ctx context.Context
var ESClient *elastic.Client

func InitEsClient() {
	esClientStr := viper.GetString("setting.esClient")
	ctx = context.Background()
	ESClient, _ = elastic.NewClient(elastic.SetURL(esClientStr), elastic.SetSniff(false))
}

// WriteMsg will write the msg and level into es
func WriteMsgToES(when time.Time, msg chan string, index string) {
	doOnce.Do(InitEsClient)
	values := make(map[string]interface{})
	values["@timestamp"] = when.Format(time.RFC3339)
	for {
		select {
		case values["@msg"] = <-msg:
			uid := uuid.NewV4().String()
			_, err := ESClient.Index().Index(index).Id(uid).BodyJson(values).Refresh("wait_for").Do(ctx)
			if err != nil {
				log.Error(err.Error())
				log.Error("send msg log to es error")
				return
			}
		case <-time.After(6 * time.Second):
			return
		}
	}
}
