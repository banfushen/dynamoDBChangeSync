package run

import (
	conf "ChangeSync/configure"
	"ChangeSync/protocal"
	"ChangeSync/writer"
	"reflect"
	"time"

	utils "ChangeSync/common"

	LOG "github.com/alecthomas/log4go"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"go.mongodb.org/mongo-driver/bson"
)

type dynamoTable struct {
	streamType    string
	w             writer.Writer
	incrW         writer.Writer
	errW          writer.Writer
	dynamoStream  *dynamodbstreams.DynamoDBStreams
	dynamoSession *dynamodb.DynamoDB
	converter     protocal.Converter
	ns            utils.NS
	recordKeysC   chan map[string]*dynamodb.AttributeValue
	recordValuesC chan map[string]*dynamodb.AttributeValue
	keysC         chan interface{}
	valuesC       chan interface{}
	changeC       chan bson.M
}

func NewDynamoTable(name, streamType string, w, incrW, errW writer.Writer, dynamoSession *dynamodb.DynamoDB,
	dynamoStream *dynamodbstreams.DynamoDBStreams, converter protocal.Converter) *dynamoTable {
	t := &dynamoTable{
		streamType:    streamType,
		w:             w,
		incrW:         incrW,
		errW:          errW,
		dynamoSession: dynamoSession,
		dynamoStream:  dynamoStream,
		converter:     converter,
		ns: utils.NS{
			Database:   conf.Options.Id,
			Collection: name,
		},
		recordKeysC:   make(chan map[string]*dynamodb.AttributeValue, 10240),
		recordValuesC: make(chan map[string]*dynamodb.AttributeValue, 10240),
		keysC:         make(chan interface{}, 10240),
		valuesC:       make(chan interface{}, 10240),
		changeC:       make(chan bson.M, 10240),
	}

	return t
}

func (dtb *dynamoTable) recordErr(batchGroup []interface{}, filterGroup []interface{}) {
	i := 0
	for {
		i += 1
		if err := dtb.w.WriteBulk(batchGroup, filterGroup); err != nil {
			LOG.Error("recordErr err[%v]", err)
			time.Sleep(100 * time.Millisecond)
		} else {
			LOG.Info("recordErr ok [%+v]", batchGroup)
			return
		}

		if i >= 300 {
			LOG.Crash("recordErr err fail [%+v] [%+v]", batchGroup, filterGroup)
		}
	}
}

func (dtb *dynamoTable) loadRecords() {
	// 最小必须是21位的长度
	// sequenceNumber := "0"
	// 438535400000000022133814786
	allErrStreamShards := make(map[string][]*dynamodbstreams.Shard)
	allErrStreamStartings := make(map[string][]string)
	sequenceNumber := "0"
	for {
		if len(allErrStreamShards) != 0 {
			_sequenceNumber, errStreamShards, errStreamStartings, err := utils.TraverseErrStreamShards(dtb.ns.Collection, sequenceNumber, dtb.dynamoStream,
				allErrStreamShards, allErrStreamStartings, dtb.recordKeysC, dtb.recordValuesC)
			if len(errStreamShards) != 0 {
				for k, v := range allErrStreamShards {
					allErrStreamShards[k] = v
					allErrStreamStartings[k] = errStreamStartings[k]
				}
			}
			if err != nil {
				LOG.Error("TraverseErrStreamShards err[%+v]", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// 处理错误的shard得到的sequenceNumber一定是准确的
			sequenceNumber = utils.GettBigIntStrings(sequenceNumber, _sequenceNumber)
			LOG.Info("TraverseErrStreamShards end sequenceNumber %s _sequenceNumber %s", sequenceNumber, _sequenceNumber)
		}

		_sequenceNumber, errStreamShards, errStreamStartings, err := utils.TraverseStreamShards(dtb.ns.Collection, sequenceNumber,
			dtb.dynamoStream, dtb.recordKeysC, dtb.recordValuesC)
		if len(errStreamShards) != 0 {
			for k, v := range allErrStreamShards {
				allErrStreamShards[k] = v
				allErrStreamStartings[k] = errStreamStartings[k]
			}
		}
		if err != nil {
			LOG.Error("TraverseStreamShards err[%+v]", err)
			time.Sleep(100 * time.Millisecond)
		}

		sequenceNumber = utils.GettBigIntStrings(sequenceNumber, _sequenceNumber)
		LOG.Info("TraverseStreamShards sequenceNumber %s _sequenceNumber %s", sequenceNumber, _sequenceNumber)
		// return
		time.Sleep(60 * time.Second)
	}
}

func (dtb *dynamoTable) converKeys() {
	for {
		select {
		case keyRecord := <-dtb.recordKeysC:
			out, err := dtb.converter.Run(keyRecord)
			if err != nil {
				LOG.Crash("keyRecord converKeys key %+v err[%+v]", keyRecord, err)
			}

			dtb.keyToChan(out)

			LOG.Info("converKeys converKeys: out[%+v] type [%+v] len chan %d", out, reflect.TypeOf(out), len(dtb.keysC))
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

func (dtb *dynamoTable) keyToChan(out interface{}) {
	for {
		select {
		case dtb.keysC <- out:
			return
		default:
			LOG.Info("keyToChan err key %+v len chan %d", out, len(dtb.keysC))
			if len(dtb.keysC) > 10240 {
				time.Sleep(100 * time.Millisecond)
			} else {
				time.Sleep(10 * time.Second)
			}
		}
	}
}

func (dtb *dynamoTable) keyToDB() {
	batchNumber := int(conf.Options.FullDocumentWriteBatch)
	batchGroup := make([]interface{}, 0, batchNumber)

	for {
		select {
		case key := <-dtb.keysC:
			switch v := key.(type) {
			case protocal.RawData:
				LOG.Info("keyToDB key %+v", v)
				if v.Size > 0 {
					batchGroup = append(batchGroup, v.Data)
				}

				if len(batchGroup) > batchNumber {
					if err := dtb.w.WriteBulk(batchGroup, batchGroup); err != nil {
						LOG.Error("keyToDB batchGroup err[%v]", err)
						dtb.recordErr(batchGroup, batchGroup)
					} else {
						LOG.Info("keyToDB batchGroup write ok [%+v]", batchGroup)
						batchGroup = batchGroup[:0]
					}
				}
			case map[string]*dynamodb.AttributeValue:
			}
		default:
			if len(batchGroup) != 0 {
				if err := dtb.w.WriteBulk(batchGroup, batchGroup); err != nil {
					LOG.Error("keyToDB err[%v]", err)
					dtb.recordErr(batchGroup, batchGroup)
				} else {
					LOG.Info("keyToDB write ok [%+v]", batchGroup)
				}
				batchGroup = batchGroup[:0]
			}
			LOG.Info("keyToDB empty len chan %d", len(dtb.keysC))
			time.Sleep(5 * time.Second)
		}
	}
}

func (dtb *dynamoTable) converValues() {
	for {
		select {
		case valueRecord := <-dtb.recordValuesC:
			out, err := dtb.converter.Run(valueRecord)
			LOG.Info("converValues converValues: out[%+v] err %v", out, err)
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

func (dtb *dynamoTable) fetchIncrKey() {
	dtb.w.FetchCollection(dtb.changeC)
}

func (dtb *dynamoTable) syncIncrChange() {
	batchNumber := int(conf.Options.FullDocumentWriteBatch)
	batchGroup := make([]interface{}, 0, batchNumber)
	filterGroup := make([]interface{}, 0, batchNumber)

	for {
		select {
		case change := <-dtb.changeC:
			for k, v := range change {
				LOG.Info("syncIncrChange k: %s, v: %s", k, v)
				_v, _ := v.(string)
				item := utils.Scan(dtb.ns.Collection, k, _v, dtb.dynamoSession)
				LOG.Debug("syncIncrChange Scan Item %+v", item)
				out, err := dtb.converter.Run(item)
				LOG.Info("syncIncrChange Scan Item out %+v err %v", out, err)

				if err == nil {
					switch _out := out.(type) {
					case protocal.RawData:
						LOG.Info("syncIncrChange out %+v", _out)
						if _out.Size > 0 {
							batchGroup = append(batchGroup, _out.Data)
							filterGroup = append(filterGroup, bson.M{k: _v})
						}
					case map[string]*dynamodb.AttributeValue:
					}
				} else {
					LOG.Crash("syncIncrChange dtb.converter.Run(item) item %+v err %+v", item, err)
				}

				if len(batchGroup) > batchNumber {
					if err := dtb.incrW.WriteBulk(batchGroup, filterGroup); err != nil {
						LOG.Error("syncIncrChange err[%v]", err)
						dtb.recordErr(batchGroup, filterGroup)
					} else {
						LOG.Info("syncIncrChange batchNumber write ok [%+v]", batchGroup)
						// LOG.Info("syncIncrChange batchNumber write ok filter [%+v]", filterGroup)
						batchGroup = batchGroup[:0]
						filterGroup = filterGroup[:0]
					}
				}
			}
		default:
			if len(batchGroup) != 0 {
				if err := dtb.incrW.WriteBulk(batchGroup, filterGroup); err != nil {
					LOG.Error("syncIncrChange err[%v]", err)
					dtb.recordErr(batchGroup, filterGroup)
				} else {
					LOG.Info("syncIncrChange write ok [%+v]", batchGroup)
					LOG.Info("syncIncrChange write ok filter [%+v]", filterGroup)
					batchGroup = batchGroup[:0]
					filterGroup = filterGroup[:0]
				}
			}
			LOG.Info("syncIncrChange len Change Chan %d", len(dtb.changeC))
			time.Sleep(10 * time.Second)
		}
	}
}
