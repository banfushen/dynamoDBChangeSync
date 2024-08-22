package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	utils "ChangeSync/common"
	conf "ChangeSync/configure"
	"ChangeSync/run"

	LOG "github.com/alecthomas/log4go"
	nimo "github.com/gugemichael/nimo4go"
)

func main() {
	defer LOG.Close()
	runtime.GOMAXPROCS(256)
	LOG.Info("max process: %d", runtime.GOMAXPROCS(0))

	configuration := flag.String("conf", "", "configuration path")
	flag.Parse()

	if *configuration == "" {
		flag.PrintDefaults()
		LOG.Crashf("not configuration path %s", *configuration)
		return
	}

	file, err := os.Open(*configuration)
	if err != nil {
		LOG.Crashf("Configure file open failed. %v", err)
	}

	configure := nimo.NewConfigLoader(file)
	configure.SetDateFormat(utils.GolangSecurityTime)
	if err := configure.Load(&conf.Options); err != nil {
		LOG.Crashf("Configure file %s parse failed. %v", *configuration, err)
	}

	// utils.InitialLogger(conf.Options.LogFile, conf.Options.LogLevel, conf.Options.LogBuffer)
	// sanitize options
	if err := sanitizeOptions(); err != nil {
		LOG.Crashf(fmt.Sprintf("Conf.Options check failed: %s", err.Error()), -4)
	}

	utils.Welcome()
	utils.StartTime = fmt.Sprintf("%v", time.Now().Format(utils.GolangSecurityTime))

	// write pid
	if err = utils.WritePidById(conf.Options.Id, "."); err != nil {
		LOG.Crashf(fmt.Sprintf("write pid failed. %v", err), -5)
	}

	// print configuration
	if opts, err := json.Marshal(conf.Options); err != nil {
		LOG.Crashf(fmt.Sprintf("marshal configuration failed[%v]", err), -6)
	} else {
		LOG.Info("%v configuration: %s", conf.Options.Id, string(opts))
	}

	run.Start()

	LOG.Info("ChangeSync complete!")
}

func sanitizeOptions() error {
	if len(conf.Options.Id) == 0 {
		return fmt.Errorf("id[%v] shouldn't be empty", conf.Options.Id)
	}

	if conf.Options.IncreasePersistDir == "" {
		conf.Options.IncreasePersistDir = "/tmp/"
	}

	if conf.Options.SourceAccessKeyID == "" {
		return fmt.Errorf("source.access_key_id shouldn't be empty")
	}

	if conf.Options.SourceSecretAccessKey == "" {
		return fmt.Errorf("source.secret_access_key shouldn't be empty")
	}

	if conf.Options.FilterCollectionBlack != "" && conf.Options.FilterCollectionWhite != "" {
		return fmt.Errorf("filter.collection.white and filter.collection.black can't both be given")
	}

	if conf.Options.QpsFull <= 0 || conf.Options.QpsIncr <= 0 {
		return fmt.Errorf("qps should > 0")
	}

	if conf.Options.QpsFullBatchNum <= 0 {
		conf.Options.QpsFullBatchNum = 128
	}
	if conf.Options.QpsIncrBatchNum <= 0 {
		conf.Options.QpsIncrBatchNum = 128
	}

	if conf.Options.TargetType != utils.TargetTypeMongo && conf.Options.TargetType != utils.TargetTypeAliyunDynamoProxy {
		return fmt.Errorf("conf.Options.TargetType[%v] supports {mongodb, aliyun_dynamo_proxy} currently", conf.Options.TargetType)
	}

	if len(conf.Options.TargetAddress) == 0 {
		return fmt.Errorf("target.address[%v] illegal", conf.Options.TargetAddress)
	}

	if conf.Options.FullConcurrency > 4096 || conf.Options.FullConcurrency == 0 {
		return fmt.Errorf("full.concurrency[%v] should in (0, 4096]", conf.Options.FullConcurrency)
	}

	if conf.Options.FullDocumentConcurrency > 4096 || conf.Options.FullDocumentConcurrency == 0 {
		return fmt.Errorf("full.document.concurrency[%v] should in (0, 4096]", conf.Options.FullDocumentConcurrency)
	}

	if conf.Options.FullDocumentWriteBatch <= 0 {
		if conf.Options.TargetType == utils.TargetTypeAliyunDynamoProxy {
			conf.Options.FullDocumentWriteBatch = 25
		} else {
			conf.Options.FullDocumentWriteBatch = 128
		}
	} else if conf.Options.FullDocumentWriteBatch > 25 && conf.Options.TargetType == utils.TargetTypeAliyunDynamoProxy {
		conf.Options.FullDocumentWriteBatch = 25
	}

	if conf.Options.FullReadConcurrency <= 0 {
		conf.Options.FullReadConcurrency = 1
	} else if conf.Options.FullReadConcurrency > 8192 {
		return fmt.Errorf("full.read.concurrency[%v] should in (0, 8192]", conf.Options.FullReadConcurrency)
	}

	if conf.Options.FullDocumentParser > 4096 || conf.Options.FullDocumentParser == 0 {
		return fmt.Errorf("full.document.parser[%v] should in (0, 4096]", conf.Options.FullDocumentParser)
	}

	// always enable
	conf.Options.FullEnableIndexPrimary = true

	if conf.Options.ConvertType == "" {
		conf.Options.ConvertType = utils.ConvertMTypeChange
	}
	if conf.Options.ConvertType != utils.ConvertTypeRaw &&
		conf.Options.ConvertType != utils.ConvertTypeChange &&
		conf.Options.ConvertType != utils.ConvertMTypeChange {
		return fmt.Errorf("convert.type[%v] illegal", conf.Options.ConvertType)
	}

	if conf.Options.IncreaseConcurrency == 0 {
		return fmt.Errorf("increase.concurrency should > 0")
	}

	if conf.Options.TargetMongoDBType != "" && conf.Options.TargetMongoDBType != utils.TargetMongoDBTypeReplica &&
		conf.Options.TargetMongoDBType != utils.TargetMongoDBTypeSharding {
		return fmt.Errorf("illegal target.mongodb.type[%v]", conf.Options.TargetMongoDBType)
	}

	if conf.Options.TargetType == utils.TargetTypeMongo && conf.Options.TargetDBExist != "" &&
		conf.Options.TargetDBExist != utils.TargetDBExistRename &&
		conf.Options.TargetDBExist != utils.TargetDBExistDrop ||
		conf.Options.TargetType == utils.TargetTypeAliyunDynamoProxy && conf.Options.TargetDBExist != "" &&
			conf.Options.TargetDBExist != utils.TargetDBExistDrop {
		return fmt.Errorf("target.mongodb.exist[%v] should be 'drop' when target.type=%v",
			conf.Options.TargetDBExist, conf.Options.TargetType)
	}
	// set ConvertType
	if conf.Options.TargetType == utils.TargetTypeAliyunDynamoProxy {
		conf.Options.ConvertType = utils.ConvertTypeSame
	}

	if conf.Options.TargetType == utils.TargetTypeAliyunDynamoProxy &&
		(!conf.Options.IncreaseExecutorUpsert || !conf.Options.IncreaseExecutorInsertOnDupUpdate) {
		return fmt.Errorf("increase.executor.upsert and increase.executor.insert_on_dup_update should be "+
			"enable when target type is %v", utils.TargetTypeAliyunDynamoProxy)
	}

	return nil
}
