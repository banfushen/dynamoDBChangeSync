package run

import (
	utils "ChangeSync/common"
	conf "ChangeSync/configure"
	"ChangeSync/filter"
	"ChangeSync/protocal"
	"ChangeSync/writer"
	"fmt"

	LOG "github.com/alecthomas/log4go"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

func SyncChangeKey(tbMap map[string]struct{}, StreamViewType string, dynamoSession *dynamodb.DynamoDB,
	dynamoStreamSession *dynamodbstreams.DynamoDBStreams, converter protocal.Converter) {
	for tb := range tbMap {
		LOG.Info("NewDynamoTable %v", tb)
		w := writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress,
			utils.NS{Database: conf.Options.IdRead, Collection: fmt.Sprintf("%s-change", tb)}, conf.Options.LogLevel)
		errW := writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress,
			utils.NS{Database: conf.Options.IdRead, Collection: fmt.Sprintf("%s-key-err", tb)}, conf.Options.LogLevel)
		dbt := NewDynamoTable(tb, StreamViewType, w, nil, errW, dynamoSession, dynamoStreamSession, converter)
		go dbt.converKeys()
		go dbt.keyToDB()
		dbt.loadRecords()
	}
}

func SyncChange(tbMap map[string]struct{}, StreamViewType string, dynamoSession *dynamodb.DynamoDB,
	dynamoStreamSession *dynamodbstreams.DynamoDBStreams, converter protocal.Converter) {
	for tb := range tbMap {
		incrR := writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress,
			utils.NS{Database: conf.Options.IdRead, Collection: fmt.Sprintf("%s-change", tb)}, conf.Options.LogLevel)

		incrW := writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress,
			utils.NS{Database: conf.Options.Id, Collection: tb}, conf.Options.LogLevel)

		errW := writer.NewWriter(conf.Options.TargetType, conf.Options.TargetAddress,
			utils.NS{Database: conf.Options.IdRead, Collection: fmt.Sprintf("%s-inr-err", tb)}, conf.Options.LogLevel)

		dbt := NewDynamoTable(tb, StreamViewType, incrR, incrW, errW, dynamoSession, dynamoStreamSession, converter)
		go dbt.fetchIncrKey()
		dbt.syncIncrChange()
	}
}

func Start() {
	LOG.Info("check connections")

	utils.IncrSyncInitHttpApi(conf.Options.IncrSyncHTTPListenPort)

	// init filter
	filter.Init(conf.Options.FilterCollectionWhite, conf.Options.FilterCollectionBlack)

	if err := utils.InitSession(conf.Options.SourceAccessKeyID, conf.Options.SourceSecretAccessKey,
		conf.Options.SourceSessionToken, conf.Options.SourceRegion, conf.Options.SourceEndpointUrl,
		conf.Options.SourceSessionMaxRetries, conf.Options.SourceSessionTimeout); err != nil {
		LOG.Crashf("init global session failed[%v]", err)
	}

	// create dynamo session
	dynamoSession, err := utils.CreateDynamoSession(conf.Options.LogLevel)
	if err != nil {
		LOG.Crashf("create dynamodb session failed[%v]", err)
	}

	// create dynamo stream client
	dynamoStreamSession, err := utils.CreateDynamoStreamSession(conf.Options.LogLevel)
	if err != nil {
		LOG.Crashf("create dynamodb stream session failed[%v]", err)
	}

	LOG.Info("create checkpoint writer: type=%v", conf.Options.CheckpointType)

	StreamViewType := "KEYS_ONLY"
	tbMap, err := utils.CheckDynamoDBStream(dynamoSession, dynamoStreamSession, StreamViewType)
	if err != nil {
		LOG.Crashf("CheckDynamoDBStream failed[%v]", err)
	}

	converter := protocal.NewConverter(conf.Options.ConvertType)
	if converter == nil {
		LOG.Crashf("NewConverter failed[%v]", err)
	}

	if conf.Options.SyncMode == utils.SyncChangeKey {
		SyncChangeKey(tbMap, StreamViewType, dynamoSession, dynamoStreamSession, converter)
	} else if conf.Options.SyncMode == utils.SyncChange {
		SyncChange(tbMap, StreamViewType, dynamoSession, dynamoStreamSession, converter)
	} else {
		LOG.Crashf("SyncMode err %s", conf.Options.SyncMode)
	}

	LOG.Info("------------------------prepare checkpoint done------------------------")

	select {}
}
