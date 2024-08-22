package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"time"

	LOG "github.com/alecthomas/log4go"
	"github.com/jinzhu/copier"
	"github.com/vinllen/mgo"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	ConnectModePrimary            = "primary"
	ConnectModeSecondaryPreferred = "secondaryPreferred"
	ConnectModeStandalone         = "standalone"

	OplogNS = "oplog.rs"
)

var (
	NotFountErr   = "not found"
	NsNotFountErr = "ns not found"
)

type NS struct {
	Database   string
	Collection string
}

func (ns NS) Str() string {
	return fmt.Sprintf("%s.%s", ns.Database, ns.Collection)
}

type MongoConn struct {
	Session *mgo.Session
	URL     string
}

func NewMongoConn(url string, connectMode string, timeout bool) (*MongoConn, error) {
	if connectMode == ConnectModeStandalone {
		url += "?connect=direct"
	}

	session, err := mgo.Dial(url)
	if err != nil {
		LOG.Critical("Connect to [%s] failed. %v", url, err)
		return nil, err
	}
	// maximum pooled connections. the overall established sockets
	// should be lower than this value(will block otherwise)
	session.SetPoolLimit(256)
	if timeout {
		session.SetSocketTimeout(10 * time.Minute)
	} else {
		session.SetSocketTimeout(0)
	}

	// already ping in the session
	/*if err := session.Ping(); err != nil {
		LOG.Critical("Verify ping command to %s failed. %v", url, err)
		return nil, err
	}*/

	// Switch the session to a eventually behavior. In that case session
	// may read for any secondary node. default mode is mgo.Strong
	switch connectMode {
	case ConnectModePrimary:
		session.SetMode(mgo.Primary, true)
	case ConnectModeSecondaryPreferred:
		session.SetMode(mgo.SecondaryPreferred, true)
	case ConnectModeStandalone:
		session.SetMode(mgo.Monotonic, true)
	default:
		err = fmt.Errorf("unknown connect mode[%v]", connectMode)
		return nil, err
	}

	LOG.Info("New session to %s successfully", url)
	return &MongoConn{Session: session, URL: url}, nil
}

func (conn *MongoConn) Close() {
	LOG.Info("Close session with %s", conn.URL)
	conn.Session.Close()
}

func (conn *MongoConn) IsGood() bool {
	if err := conn.Session.Ping(); err != nil {
		return false
	}

	return true
}

func (conn *MongoConn) AcquireReplicaSetName() string {
	var replicaset struct {
		Id string `primitive:"set"`
	}
	if err := conn.Session.DB("admin").Run(primitive.M{"replSetGetStatus": 1}, &replicaset); err != nil {
		LOG.Warn("Replica set name not found in system.replset, %v", err)
	}
	return replicaset.Id
}

func (conn *MongoConn) HasOplogNs() bool {
	if ns, err := conn.Session.DB("local").CollectionNames(); err == nil {
		for _, table := range ns {
			if table == OplogNS {
				return true
			}
		}
	}
	return false
}

func (conn *MongoConn) HasUniqueIndex() bool {
	checkNs := make([]NS, 0, 128)
	var databases []string
	var err error
	if databases, err = conn.Session.DatabaseNames(); err != nil {
		LOG.Critical("Couldn't get databases from remote server %v", err)
		return false
	}

	for _, db := range databases {
		if db != "admin" && db != "local" {
			coll, _ := conn.Session.DB(db).CollectionNames()
			for _, c := range coll {
				if c != "system.profile" {
					// push all collections
					checkNs = append(checkNs, NS{Database: db, Collection: c})
				}
			}
		}
	}

	for _, ns := range checkNs {
		indexes, _ := conn.Session.DB(ns.Database).C(ns.Collection).Indexes()
		for _, idx := range indexes {
			// has unique index
			if idx.Unique {
				LOG.Info("Found unique index %s on %s.%s in auto shard mode", idx.Name, ns.Database, ns.Collection)
				return true
			}
		}
	}

	return false
}

func floatEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9 // 定义一个容许误差范围，例如1e-9
}

func deepEqualWithFloat(v1, v2 interface{}) bool {
	switch v1 := v1.(type) {
	case float64:
		v2, ok := v2.(float64)
		return ok && floatEqual(v1, v2)
	case map[string]interface{}:
		v2, ok := v2.(map[string]interface{})
		if !ok {
			return false
		}
		if len(v1) != len(v2) {
			return false
		}
		for k, v1Val := range v1 {
			v2Val, ok := v2[k]
			if !ok || !deepEqualWithFloat(v1Val, v2Val) {
				return false
			}
		}
		return true
	case primitive.M:
		v2, ok := v2.(primitive.M)
		if !ok {
			return false
		}
		if len(v1) != len(v2) {
			return false
		}
		for k, v1Val := range v1 {
			v2Val, ok := v2[k]
			if !ok || !deepEqualWithFloat(v1Val, v2Val) {
				return false
			}
		}
		return true
	case []interface{}:
		v2, ok := v2.([]interface{})
		if !ok {
			return false
		}
		if len(v1) != len(v2) {
			return false
		}
		for i := range v1 {
			if !deepEqualWithFloat(v1[i], v2[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(v1, v2)
	}
}

// first is from dynamo, second is from mongo
func Compareprimitive(first, second primitive.M) (bool, error) {
	v2 := make(primitive.M, len(second))
	if err := copier.Copy(&v2, &second); err != nil {
		return false, fmt.Errorf("copy[%v] failed[%v]", second, err)
	}

	delete(v2, "_id")

	// if !reflect.DeepEqual(first, v2) {
	// 	LOG.Crashf("11111111111111111 [%+v] [%+v]", first, v2)
	// 	return false, nil
	// }

	firstJSON, err := json.Marshal(normalizeNumbers(first))
	if err != nil {
		return false, fmt.Errorf("json marshal first failed: %v", err)
	}

	secondJSON, err := json.Marshal(normalizeNumbers(v2))
	if err != nil {
		return false, fmt.Errorf("json marshal second failed: %v", err)
	}

	if !bytes.Equal(firstJSON, secondJSON) {
		LOG.Info("11111111111111111 [%s] [%s]", string(firstJSON), string(secondJSON))
		LOG.Crashf("22222222222222222 [%+v] [%+v]", first, v2)
		return false, nil
	}
	return true, nil

	// return bytes.Equal(firstJSON, secondJSON), nil
	// return DeepEqual(first, v2), nil
}

// normalizeNumbers ensures that numbers are not in scientific notation
func normalizeNumbers(doc interface{}) interface{} {
	switch doc := doc.(type) {
	case primitive.M:
		for k, v := range doc {
			doc[k] = normalizeNumbers(v)
		}
		return doc
	case map[string]interface{}:
		for k, v := range doc {
			doc[k] = normalizeNumbers(v)
		}
		return doc
	case []interface{}:
		for i, v := range doc {
			doc[i] = normalizeNumbers(v)
		}
		return doc
	case float64:
		// Check if the float64 value is an integer
		if doc == float64(int64(doc)) {
			return int64(doc)
		}
		return doc
	default:
		return doc
	}
}
