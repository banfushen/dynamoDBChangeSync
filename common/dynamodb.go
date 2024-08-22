package utils

import (
	"ChangeSync/filter"
	"fmt"
	"net/http"
	"time"

	LOG "github.com/alecthomas/log4go"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

var (
	globalSession *session.Session
)

/*
 * all client share the same session.
 * Sessions can be shared across all service clients that share the same base configuration
 * refer: https://docs.aws.amazon.com/sdk-for-go/api/aws/session/
 */
func InitSession(accessKeyID, secretAccessKey, sessionToken, region, endpoint string, maxRetries, timeout uint) error {
	config := &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKeyID, secretAccessKey, sessionToken),
		MaxRetries:  aws.Int(int(maxRetries)),
		HTTPClient: &http.Client{
			Timeout: time.Duration(timeout) * time.Millisecond,
		},
	}

	if endpoint != "" {
		config.Endpoint = aws.String(endpoint)
	}

	var err error
	globalSession, err = session.NewSession(config)
	if err != nil {
		return err
	}

	return nil
}

func CreateDynamoSession(logLevel string) (*dynamodb.DynamoDB, error) {
	if logLevel == "debug" {
		svc := dynamodb.New(globalSession, aws.NewConfig().WithLogLevel(aws.LogDebugWithHTTPBody))
		return svc, nil
	}
	svc := dynamodb.New(globalSession)
	return svc, nil
}

func CreateDynamoStreamSession(logLevel string) (*dynamodbstreams.DynamoDBStreams, error) {
	if logLevel == "debug" {
		svc := dynamodbstreams.New(globalSession, aws.NewConfig().WithLogLevel(aws.LogDebugWithHTTPBody))
		return svc, nil
	}
	svc := dynamodbstreams.New(globalSession)
	return svc, nil
}

func ParseIndexType(input []*dynamodb.AttributeDefinition) map[string]string {
	mp := make(map[string]string, len(input))

	for _, ele := range input {
		mp[*ele.AttributeName] = *ele.AttributeType
	}

	return mp
}

// fetch dynamodb table list
func FetchTableList(dynamoSession *dynamodb.DynamoDB) ([]string, error) {
	ans := make([]string, 0)
	var lastEvaluatedTableName *string

	for {
		out, err := dynamoSession.ListTables(&dynamodb.ListTablesInput{
			ExclusiveStartTableName: lastEvaluatedTableName,
		})

		if err != nil {
			return nil, err
		}

		ans = AppendStringList(ans, out.TableNames)
		if out.LastEvaluatedTableName == nil {
			// finish
			break
		}
		lastEvaluatedTableName = out.LastEvaluatedTableName
	}

	return ans, nil
}

func ParsePrimaryAndSortKey(primaryIndexes []*dynamodb.KeySchemaElement, parseMap map[string]string) (string, string, error) {
	var primaryKey string
	var sortKey string
	for _, index := range primaryIndexes {
		if *(index.KeyType) == "HASH" {
			if primaryKey != "" {
				return "", "", fmt.Errorf("duplicate primary key type[%v]", *(index.AttributeName))
			}
			primaryKey = *(index.AttributeName)
		} else if *(index.KeyType) == "RANGE" {
			if sortKey != "" {
				return "", "", fmt.Errorf("duplicate sort key type[%v]", *(index.AttributeName))
			}
			sortKey = *(index.AttributeName)
		} else {
			return "", "", fmt.Errorf("unknonw key type[%v]", *(index.KeyType))
		}
	}
	return primaryKey, sortKey, nil
}

func CheckDynamoDBStream(dynamoSession *dynamodb.DynamoDB,
	dynamoStreams *dynamodbstreams.DynamoDBStreams, StreamViewType string) (map[string]struct{}, error) {
	sourceTableList, err := FetchTableList(dynamoSession)
	if err != nil {
		return nil, fmt.Errorf("fetch dynamodb table list failed[%v]", err)
	}

	// filter
	sourceTableList = filter.FilterList(sourceTableList)
	sourceTableMap := StringListToMap(sourceTableList)
	checkTableMap := make(map[string]struct{})
	for k, v := range sourceTableMap {
		checkTableMap[k] = v
	}

	LOG.Info("CheckDynamoDBStream current streams")
	// traverse streams to check whether all streams enabled
	var lastEvaluateString *string = nil
	for {
		var listStreamInput *dynamodbstreams.ListStreamsInput
		if lastEvaluateString != nil {
			listStreamInput = &dynamodbstreams.ListStreamsInput{ExclusiveStartStreamArn: lastEvaluateString}
		} else {
			listStreamInput = &dynamodbstreams.ListStreamsInput{}
		}
		streamList, err := dynamoStreams.ListStreams(listStreamInput)
		if err != nil {
			return nil, fmt.Errorf("fetch dynamodb stream list failed[%v]", err)
		}

		LOG.Info("CheckDynamoDBStream streamList[%v]", streamList)

		for _, stream := range streamList.Streams {
			LOG.Info("check stream with table[%v]", *stream.TableName)
			describeStreamResult, err := dynamoStreams.DescribeStream(&dynamodbstreams.DescribeStreamInput{
				StreamArn: stream.StreamArn,
			})
			if err != nil {
				return nil, fmt.Errorf("describe stream[%v] with table[%v] failed[%v]", *stream.StreamArn,
					*stream.TableName, err)
			}

			if *describeStreamResult.StreamDescription.StreamStatus == "DISABLED" {
				// stream is disabled
				continue
			}

			if filter.IsFilter(*stream.TableName) {
				LOG.Info("table[%v] filtered", *stream.TableName)
				continue
			}

			// remove from sourceTableMap marks this stream already enabled
			delete(checkTableMap, *stream.TableName)
		}

		LOG.Info("CheckDynamoDBStream after traverse current streams")

		if streamList.LastEvaluatedStreamArn == nil {
			// end
			break
		} else {
			lastEvaluateString = streamList.LastEvaluatedStreamArn
		}
	}

	LOG.Info("CheckDynamoDBStream after traverse current streams %+v", checkTableMap)

	if len(checkTableMap) != 0 {
		LOG.Info("enable and marks streams: %v", checkTableMap)
		// enable and marks new stream
		for table := range checkTableMap {
			_, err := dynamoSession.UpdateTable(&dynamodb.UpdateTableInput{
				TableName: aws.String(table),
				StreamSpecification: &dynamodb.StreamSpecification{
					StreamEnabled:  aws.Bool(true),
					StreamViewType: aws.String(StreamViewType),
				},
			})
			if err != nil {
				return nil, fmt.Errorf("enable stream for table[%v] failed[%v]", table, err)
			}
		}

		LOG.Info("CheckDynamoDBStream wait 30 seconds for new streams created[%v]...", checkTableMap)
		time.Sleep(30 * time.Second) // wait new stream created
	}

	return sourceTableMap, nil
}

func getStreamShards(sequenceNumber string, stream *dynamodbstreams.Stream, dynamoStreams *dynamodbstreams.DynamoDBStreams) ([]*dynamodbstreams.Shard, []string, error) {
	var lastShardIdString *string = nil
	var shards []*dynamodbstreams.Shard
	var StartingSequenceNumbers []string = make([]string, 0)
	for {
		var describeStreamInput *dynamodbstreams.DescribeStreamInput
		if lastShardIdString != nil {
			describeStreamInput = &dynamodbstreams.DescribeStreamInput{
				StreamArn:             stream.StreamArn,
				ExclusiveStartShardId: lastShardIdString,
			}
		} else {
			describeStreamInput = &dynamodbstreams.DescribeStreamInput{
				StreamArn: stream.StreamArn,
			}
		}
		describeStreamResult, err := dynamoStreams.DescribeStream(describeStreamInput)
		if err != nil {
			return nil, nil, fmt.Errorf("describe stream[%v] with table[%v] failed[%v]", stream.StreamArn,
				stream.TableName, err)
		}
		if *describeStreamResult.StreamDescription.StreamStatus == "DISABLED" {
			// stream is disabled
			LOG.Info("[%s] stream with table[%v] disabled", *stream.StreamArn, *stream.TableName)
			break
		}

		LOG.Debug("getStreamShards describeStreamResult[%+v]", *describeStreamResult)

		shardsLen := len(describeStreamResult.StreamDescription.Shards)
		for i := 0; i < shardsLen; i++ {
			SequenceNumberRange := describeStreamResult.StreamDescription.Shards[i].SequenceNumberRange
			EndingSequenceNumber := SequenceNumberRange.EndingSequenceNumber
			StartingSequenceNumber := SequenceNumberRange.StartingSequenceNumber
			// sequenceNumber 比 EndingSequenceNumber 还大，说明这个 shard 早已经读完过了

			if EndingSequenceNumber != nil {
				if CompareBigIntStrings(sequenceNumber, *EndingSequenceNumber) {
					LOG.Info("getStreamShards CompareBigIntStrings fail sequenceNumber %s EndingSequenceNumber %s", sequenceNumber, *EndingSequenceNumber)
					continue
				}

				LOG.Debug("getStreamShards shard ok 1 sequenceNumber %s StartingSequenceNumber %s EndingSequenceNumber %s",
					sequenceNumber, *StartingSequenceNumber, *EndingSequenceNumber)
			}

			if CompareBigIntStrings(sequenceNumber, *StartingSequenceNumber) {
				StartingSequenceNumbers = append(StartingSequenceNumbers, sequenceNumber)
				LOG.Debug("getStreamShards shard ok 2 sequenceNumber %s StartingSequenceNumber %s",
					sequenceNumber, *StartingSequenceNumber)
			} else {
				StartingSequenceNumbers = append(StartingSequenceNumbers, *StartingSequenceNumber)
				LOG.Debug("getStreamShards shard ok 3 sequenceNumber %s StartingSequenceNumber %s",
					sequenceNumber, *StartingSequenceNumber)
			}
			shards = append(shards, describeStreamResult.StreamDescription.Shards[i])
		}

		LOG.Info("getStreamShards shardsLen[%d]", shardsLen)

		if describeStreamResult.StreamDescription.LastEvaluatedShardId == nil {
			break
		} else {
			lastShardIdString = describeStreamResult.StreamDescription.LastEvaluatedShardId
			LOG.Info("getStreamShards table[%v] have next shardId,LastEvaluatedShardId[%v]",
				*stream.TableName, *describeStreamResult.StreamDescription.LastEvaluatedShardId)
		}
	}

	return shards, StartingSequenceNumbers, nil
}

func fetchStreamShards(tb, sequenceNumber string, dynamoStreams *dynamodbstreams.DynamoDBStreams) (map[string][]*dynamodbstreams.Shard,
	map[string][]string, error) {
	streamShards := make(map[string][]*dynamodbstreams.Shard)
	streamStartings := make(map[string][]string)

	var lastEvaluateString *string = nil
	var maxLimit int64 = 100
	for {
		var listStreamInput *dynamodbstreams.ListStreamsInput
		if lastEvaluateString != nil {
			listStreamInput = &dynamodbstreams.ListStreamsInput{TableName: &tb, ExclusiveStartStreamArn: lastEvaluateString, Limit: &maxLimit}
		} else {
			listStreamInput = &dynamodbstreams.ListStreamsInput{TableName: &tb, Limit: &maxLimit}
		}
		streamList, err := dynamoStreams.ListStreams(listStreamInput)
		if err != nil {
			LOG.Error("TraverseStreamShards listStreams fail [%+v]", err)
			return streamShards, streamStartings, err
		}

		// LOG.Info("TraverseStreamShards stream with table[%v] streamList[%+v]", tb, streamList)

		for _, stream := range streamList.Streams {
			LOG.Info("TraverseStreamShards stream with table[%v] streamArn[%+v]", *stream.TableName, *stream)

			shards, StartingSequenceNumbers, err := getStreamShards(sequenceNumber, stream, dynamoStreams)
			if err != nil {
				LOG.Error("TraverseStreamShards getStreamShards err [%+v]", err)
				return streamShards, streamStartings, err
			}
			if len(shards) != len(StartingSequenceNumbers) {
				LOG.Crash("TraverseStreamShards getStreamShards len err, len(shards) %d, len(StartingSequenceNumbers) %d",
					len(shards), len(StartingSequenceNumbers))
			}

			streamShards[*stream.StreamArn] = append(streamShards[*stream.StreamArn], shards...)
			streamStartings[*stream.StreamArn] = append(streamStartings[*stream.StreamArn], StartingSequenceNumbers...)
		}

		if streamList.LastEvaluatedStreamArn == nil {
			// end
			break
		} else {
			lastEvaluateString = streamList.LastEvaluatedStreamArn
		}
	}

	return streamShards, streamStartings, nil
}

func TraverseErrStreamShards(tb, sequenceNumber string, dynamoStreams *dynamodbstreams.DynamoDBStreams, errStreamShards map[string][]*dynamodbstreams.Shard,
	errStreamStartings map[string][]string, ckeys chan map[string]*dynamodb.AttributeValue,
	cvalues chan map[string]*dynamodb.AttributeValue) (string, map[string][]*dynamodbstreams.Shard, map[string][]string, error) {
	maxSequenceNumber := sequenceNumber

	backErrStreamShards := make(map[string][]*dynamodbstreams.Shard)
	backErrStreamStartings := make(map[string][]string)

	LOG.Info("TraverseErrStreamShards streamShards len %d streamStartings len %d", len(errStreamShards), len(errStreamStartings))
	LOG.Debug("TraverseErrStreamShards streamShards streamStartings %+v", errStreamStartings)

	var backErr error
	for StreamArn, allShards := range errStreamShards {
		LOG.Info("TraverseErrStreamShards StreamArn %v start maxSequenceNumber %v", StreamArn, maxSequenceNumber)
		Startings := errStreamStartings[StreamArn]
		for k, s := range allShards {
			starting := Startings[k]
			IteratorTypeLatest := "AFTER_SEQUENCE_NUMBER"
			outShardIt, err := dynamoStreams.GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
				ShardId:           s.ShardId,
				ShardIteratorType: aws.String(IteratorTypeLatest),
				StreamArn:         &StreamArn,
				SequenceNumber:    aws.String(starting),
			})
			if err != nil {
				backErr = err
				LOG.Error("TraverseErrStreamShards StreamArn %v shard[%v] iterator failed[%v]", StreamArn, s.ShardId, err)

				backErrStreamShards[StreamArn] = append(backErrStreamShards[StreamArn], s)
				backErrStreamStartings[StreamArn] = append(backErrStreamStartings[StreamArn], starting)
				continue
			}

			LOG.Debug("TraverseErrStreamShards StreamArn %v noEndsequence ShardId[%v] outShardIt[%v]", StreamArn, *s.ShardId, outShardIt)

			_sequenceNumber, err := fetchRecords(outShardIt.ShardIterator, dynamoStreams, tb, sequenceNumber, ckeys, cvalues)
			if err != nil {
				backErr = err
				LOG.Error("TraverseErrStreamShards StreamArn %v fetchRecords failed[%v]", StreamArn, err)

				backErrStreamShards[StreamArn] = append(backErrStreamShards[StreamArn], s)
				backErrStreamStartings[StreamArn] = append(backErrStreamStartings[StreamArn], starting)
				continue
			}
			LOG.Info("_sequenceNumber %s maxSequenceNumber %s", _sequenceNumber, maxSequenceNumber)
			maxSequenceNumber = GettBigIntStrings(maxSequenceNumber, _sequenceNumber)
		}
		LOG.Info("TraverseErrStreamShards StreamArn %v end maxSequenceNumber %s", StreamArn, maxSequenceNumber)
	}

	return maxSequenceNumber, backErrStreamShards, backErrStreamStartings, backErr
}

func TraverseStreamShards(tb, sequenceNumber string, dynamoStreams *dynamodbstreams.DynamoDBStreams, ckeys chan map[string]*dynamodb.AttributeValue,
	cvalues chan map[string]*dynamodb.AttributeValue) (string, map[string][]*dynamodbstreams.Shard, map[string][]string, error) {
	maxSequenceNumber := sequenceNumber
	streamShards, streamStartings, err := fetchStreamShards(tb, sequenceNumber, dynamoStreams)
	if err != nil {
		return maxSequenceNumber, nil, nil, err
	}
	LOG.Info("TraverseStreamShards streamShards len %d streamStartings len %d", len(streamShards), len(streamStartings))
	LOG.Debug("TraverseStreamShards streamShards streamStartings %+v", streamStartings)

	var backErr error
	errStreamShards := make(map[string][]*dynamodbstreams.Shard)
	errStreamStartings := make(map[string][]string)
	for StreamArn, allShards := range streamShards {
		LOG.Info("TraverseStreamShards StreamArn %v start maxSequenceNumber %v", StreamArn, maxSequenceNumber)
		Startings := streamStartings[StreamArn]
		for k, s := range allShards {
			starting := Startings[k]
			IteratorTypeLatest := "AFTER_SEQUENCE_NUMBER"
			outShardIt, err := dynamoStreams.GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
				ShardId:           s.ShardId,
				ShardIteratorType: aws.String(IteratorTypeLatest),
				StreamArn:         &StreamArn,
				SequenceNumber:    aws.String(starting),
			})
			if err != nil {
				backErr = err
				LOG.Error("TraverseStreamShards StreamArn %v shard[%v] iterator failed[%v]", StreamArn, s.ShardId, err)

				errStreamShards[StreamArn] = append(errStreamShards[StreamArn], s)
				errStreamStartings[StreamArn] = append(errStreamStartings[StreamArn], starting)
				continue
			}

			LOG.Debug("TraverseStreamShards StreamArn %v noEndsequence ShardId[%v] outShardIt[%v]", StreamArn, *s.ShardId, outShardIt)

			_sequenceNumber, err := fetchRecords(outShardIt.ShardIterator, dynamoStreams, tb, sequenceNumber, ckeys, cvalues)
			if err != nil {
				backErr = err
				LOG.Error("TraverseStreamShards StreamArn %v fetchRecords failed[%v]", StreamArn, err)

				errStreamShards[StreamArn] = append(errStreamShards[StreamArn], s)
				errStreamStartings[StreamArn] = append(errStreamStartings[StreamArn], starting)
				continue
			}
			LOG.Info("_sequenceNumber %s maxSequenceNumber %s", _sequenceNumber, maxSequenceNumber)
			maxSequenceNumber = GettBigIntStrings(maxSequenceNumber, _sequenceNumber)
		}
		LOG.Info("TraverseStreamShards StreamArn %v end maxSequenceNumber %s", StreamArn, maxSequenceNumber)
	}

	return maxSequenceNumber, errStreamShards, errStreamStartings, backErr
}

func fetchRecords(shardIt *string, dynamoStreams *dynamodbstreams.DynamoDBStreams, table, sequenceNumber string,
	ckeys, cvalues chan map[string]*dynamodb.AttributeValue) (string, error) {
	LOG.Info("fetchRecords sequence number of shard[%v] table[%v] start", *shardIt, table)
	i := 0
	for {
		records, err := dynamoStreams.GetRecords(&dynamodbstreams.GetRecordsInput{
			ShardIterator: shardIt,
			Limit:         aws.Int64(1000),
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {

				switch aerr.Code() {
				case dynamodb.ErrCodeProvisionedThroughputExceededException:
					LOG.Warn("fetchRecords get records with iterator[%v] recv ProvisionedThroughputExceededException continue",
						*shardIt)
					time.Sleep(5 * time.Second)
					continue

				case request.ErrCodeSerialization:
					LOG.Warn("fetchRecords get records with iterator[%v] recv SerializationError[%v] continue",
						*shardIt, err)
					time.Sleep(5 * time.Second)
					continue

				case request.ErrCodeRequestError, request.CanceledErrorCode,
					request.ErrCodeResponseTimeout, request.HandlerResponseTimeout,
					request.WaiterResourceNotReadyErrorCode, request.ErrCodeRead,
					dynamodb.ErrCodeInternalServerError:
					LOG.Warn("fetchRecords get records with iterator[%v] recv Error[%v] continue",
						*shardIt, err)
					time.Sleep(5 * time.Second)
					continue

				default:
					LOG.Crashf("fetchRecords scan failed[%v] errcode[%v]", err, aerr.Code())
				}
			} else {
				LOG.Crashf("fetchRecords with iterator[%v] failed[%v]", *shardIt, err)
			}
		}

		for _, v := range records.Records {
			LOG.Info("fetchRecords record %v", v)

			if *v.EventName == "REMOVE" {
				continue
			}

			if v.Dynamodb.Keys != nil {
				if CompareBigIntStrings(sequenceNumber, *v.Dynamodb.SequenceNumber) {
					// 已经记录过的sequenceNumber >= *v.Dynamodb.SequenceNumber 说明已经读过了
					continue
				}

				select {
				case ckeys <- v.Dynamodb.Keys:
					SequenceNumberStr := v.Dynamodb.SequenceNumber
					sequenceNumber = GettBigIntStrings(sequenceNumber, *SequenceNumberStr)
					LOG.Info("fetchRecords Keys %v SequenceNumberStr %s sequenceNumber %s", v.Dynamodb.Keys, *SequenceNumberStr, sequenceNumber)
				default:
					LOG.Info("fetchRecords Keys len chan full %v", v.Dynamodb.Keys)
				}
			}

			if v.Dynamodb.NewImage != nil {
				select {
				case cvalues <- v.Dynamodb.NewImage:
					LOG.Info("fetchRecords NewImage %v", v.Dynamodb.NewImage)
				default:
					LOG.Info("fetchRecords NewImage len chan full %v", v.Dynamodb.NewImage)
				}
			}

		}

		shardIt = records.NextShardIterator

		time.Sleep(10 * time.Millisecond)
		if shardIt == nil {
			break
		}
		i++
		if i > 10 {
			break
		}
	}

	if shardIt != nil {
		LOG.Info("fetchRecords sequence number of shard[%v] table[%v]: not found, return empty", shardIt, table)
	}

	LOG.Info("fetchRecords sequence table[%v] end sequenceNumber %s", table, sequenceNumber)
	// what if no data? return empty
	return sequenceNumber, nil
}

func Scan(tb, k, v string, dynamoSession *dynamodb.DynamoDB) map[string]*dynamodb.AttributeValue {
	keyMap := map[string]string{
		k: v,
	}

	// 构建键条件
	key := make(map[string]*dynamodb.AttributeValue)
	for k, v := range keyMap {
		key[k] = &dynamodb.AttributeValue{S: aws.String(v)}
	}

	input := &dynamodb.GetItemInput{
		TableName: aws.String(tb), // 替换为你的表名
		Key:       key,
	}

	result, err := dynamoSession.GetItem(input)
	if err != nil {
		LOG.Error("Scan Failed to get item: %v", err)
	}

	if result.Item == nil {
		LOG.Info("Scan No item found for the provided key")
	}

	// 输出结果

	return result.Item
}
