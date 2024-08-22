package utils

import (
	"fmt"
	"math/big"
	"os"
	"strings"

	LOG "github.com/alecthomas/log4go"
)

const (
	GolangSecurityTime = "2006-01-02T15:04:05Z"

	SyncChangeKey = "syncChangeKey"
	SyncChange    = "syncChange"

	ConvertTypeRaw     = "raw"
	ConvertTypeChange  = "change"
	ConvertMTypeChange = "mchange"
	ConvertTypeSame    = "same" // used in dynamodb -> dynamo-proxy

	TargetTypeMongo             = "mongodb"
	TargetTypeAliyunDynamoProxy = "aliyun_dynamo_proxy"

	TargetMongoDBTypeReplica  = "replica"
	TargetMongoDBTypeSharding = "sharding"

	TargetDBExistRename = "rename"
	TargetDBExistDrop   = "drop"

	SIGNALPROFILE = 31
	SIGNALSTACK   = 30
)

var (
	Version   = "$"
	StartTime string
)

func InitialLogger(logFile string, level string, logBuffer bool) bool {
	logLevel := parseLogLevel(level)
	if len(logFile) != 0 {
		// create logs folder for log4go. because of its mistake that doesn't create !
		if err := os.MkdirAll("logs", os.ModeDir|os.ModePerm); err != nil {
			return false
		}
		if logBuffer {
			LOG.LogBufferLength = 32
		} else {
			LOG.LogBufferLength = 0
		}
		fileLogger := LOG.NewFileLogWriter(fmt.Sprintf("logs/%s", logFile), true)
		//fileLogger.SetRotateDaily(true)
		fileLogger.SetRotateSize(500 * 1024 * 1024)
		// fileLogger.SetFormat("[%D %T] [%L] [%s] %M")
		fileLogger.SetFormat("[%D %T] [%L] %M")
		fileLogger.SetRotateMaxBackup(100)
		LOG.AddFilter("file", logLevel, fileLogger)
	} else {
		LOG.AddFilter("console", logLevel, LOG.NewConsoleLogWriter())
	}
	return true
}

func parseLogLevel(level string) LOG.Level {
	switch strings.ToLower(level) {
	case "debug":
		return LOG.DEBUG
	case "info":
		return LOG.INFO
	case "warning":
		return LOG.WARNING
	case "error":
		return LOG.ERROR
	default:
		return LOG.DEBUG
	}
}

/**
 * block password in mongo_urls:
 * two kind mongo_urls:
 * 1. mongodb://username:password@address
 * 2. username:password@address
 */
func BlockMongoUrlPassword(url, replace string) string {
	colon := strings.Index(url, ":")
	if colon == -1 || colon == len(url)-1 {
		return url
	} else if url[colon+1] == '/' {
		// find the second '/'
		for colon++; colon < len(url); colon++ {
			if url[colon] == ':' {
				break
			}
		}

		if colon == len(url) {
			return url
		}
	}

	at := strings.Index(url, "@")
	if at == -1 || at == len(url)-1 || at <= colon {
		return url
	}

	newUrl := make([]byte, 0, len(url))
	for i := 0; i < len(url); i++ {
		if i <= colon || i > at {
			newUrl = append(newUrl, byte(url[i]))
		} else if i == at {
			newUrl = append(newUrl, []byte(replace)...)
			newUrl = append(newUrl, byte(url[i]))
		}
	}
	return string(newUrl)
}

func GettBigIntStrings(str1, str2 string) string {
	// Create new big.Int instances for both strings
	bigInt1 := new(big.Int)
	bigInt2 := new(big.Int)

	// Set the values from the strings
	if _, ok := bigInt1.SetString(str1, 10); !ok {
		LOG.Info("CompareBigIntStrings str1 %s fail", str1)
		return str1
	}
	if _, ok := bigInt2.SetString(str2, 10); !ok {
		LOG.Info("CompareBigIntStrings str2 %s fail", str2)
		return str1
	}

	// Compare the two big.Int values
	comparison := bigInt1.Cmp(bigInt2)

	switch comparison {
	case -1:
		return str2
	case 0:
		return str1
	case 1:
		return str1
	default:
		LOG.Info("CompareBigIntStrings str1 %s str2 %s fail", str1, str2)
		return str1
	}
}

func CompareBigIntStrings(str1, str2 string) bool {
	// Create new big.Int instances for both strings
	bigInt1 := new(big.Int)
	bigInt2 := new(big.Int)

	// Set the values from the strings
	if _, ok := bigInt1.SetString(str1, 10); !ok {
		LOG.Info("CompareBigIntStrings str1 %s fail", str1)
		return false
	}
	if _, ok := bigInt2.SetString(str2, 10); !ok {
		LOG.Info("CompareBigIntStrings str2 %s fail", str2)
		return false
	}

	// Compare the two big.Int values
	comparison := bigInt1.Cmp(bigInt2)

	switch comparison {
	case -1:
		// str1 is less than str2
		return false
	case 0:
		// str1 is equal to str2
		return true
	case 1:
		// str1 is greater than str2
		return true
	default:
		LOG.Info("CompareBigIntStrings str1 %s str2 %s fail", str1, str2)
		return false
	}
}
