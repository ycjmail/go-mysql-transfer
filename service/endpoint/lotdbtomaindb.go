package endpoint

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"go-mysql-transfer/util/tools"
	"log"

	//"github.com/Shopify/sarama"
	_ "github.com/siddontang/go-mysql/canal"
	_ "strings"
	"sync"

	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/model"
	"go-mysql-transfer/util/logs"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
)

type LotDbToMainDbEndpoint struct {
	//client   sarama.Client
	//producer sarama.AsyncProducer
	dsn          string
	dbDefault    *gorm.DB
	lotDbTopic   string
	dbParkLotIds []uint
	lotTopicDsn  model.LotTopicDsn
	retryLock    sync.Mutex
}

func newLotDbToMainDbEndpoint() *LotDbToMainDbEndpoint {
	r := &LotDbToMainDbEndpoint{}
	return r
}

func (s *LotDbToMainDbEndpoint) Connect() error {
	s.lotDbTopic = global.Cfg().LotDbTopic
	s.dsn = global.Cfg().MainBbDsn
	db, err := gorm.Open("mysql", s.dsn)
	if err != nil {
		fmt.Println(err)
		return err
	}
	db.LogMode(true)
	s.dbDefault = db

	//var dbParkLotIds []uint
	err = db.Model(model.LotDbParkLotId{}).
		Where("db_topic=?", s.lotDbTopic).
		Order("park_lot_id").Pluck("park_lot_id", &s.dbParkLotIds).Error
	if err != nil {
		panic(err)
	}
	if len(s.dbParkLotIds) == 0 {
		fmt.Println("请设置表lot_db_park_lot_ids相关数据")
		panic("请设置表lot_db_park_lot_ids相关数据")
	}

	err = db.Model(model.LotTopicDsn{}).
		Where("db_topic=?", s.lotDbTopic).
		Find(&s.lotTopicDsn).Error
	if err != nil {
		fmt.Println("请设置表lot_topic_dsns相关数据")
		panic(err)
	}

	//todo add connection to maindb
	//cfg := sarama.NewConfig()
	//cfg.Producer.Partitioner = sarama.NewRandomPartitioner

	//if global.Cfg().LotDbToMainDbSASLUser != "" && global.Cfg().LotDbToMainDbSASLPassword != "" {
	//	cfg.Net.SASL.Enable = true
	//	cfg.Net.SASL.User = global.Cfg().LotDbToMainDbSASLUser
	//	cfg.Net.SASL.Password = global.Cfg().LotDbToMainDbSASLPassword
	//}
	//
	//var err error
	//var client sarama.Client
	//ls := strings.Split(global.Cfg().LotDbToMainDbAddr, ",")
	//client, err = sarama.NewClient(ls, cfg)
	//if err != nil {
	//	return errors.Errorf("unable to create LotDbToMainDb client: %q", err)
	//}
	//
	//var producer sarama.AsyncProducer
	//producer, err = sarama.NewAsyncProducerFromClient(client)
	//if err != nil {
	//	return errors.Errorf("unable to create LotDbToMainDb producer: %q", err)
	//}
	//
	//s.producer = producer
	//s.client = client

	return nil
}

func (s *LotDbToMainDbEndpoint) Ping() error {
	return nil
	//return s.client.RefreshMetadata()
}

func (s *LotDbToMainDbEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	//var ms []*sarama.ProducerMessage
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		metrics.UpdateActionNum(row.Action, row.RuleKey)

		m, err := s.buildMessage(row, rule)
		if err != nil {
			return errors.Errorf(errors.ErrorStack(err))
		}
		//ms = append(ms, m)
		err = s.ProcessLotDbToMainDb(from, m)

		if err != nil {
			return errors.Errorf(errors.ErrorStack(err))
		}

		//if rule.LuaEnable() {
		//	ls, err := s.buildMessages(row, rule)
		//	if err != nil {
		//		log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
		//		return errors.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
		//	}
		//	ms = append(ms, ls...)
		//} else {
		//	m, err := s.buildMessage(row, rule)
		//	if err != nil {
		//		return errors.Errorf(errors.ErrorStack(err))
		//	}
		//	ms = append(ms, m)
		//}
	}

	//for _, m := range ms {
	//	s.producer.Input() <- m
	//	select {
	//	case err := <-s.producer.Errors():
	//		return err
	//	default:
	//	}
	//}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *LotDbToMainDbEndpoint) Stock(rows []*model.RowRequest) int64 {
	//todo,要debug跟踪函数这个在哪里调用,从原来没处理的消息得到的 rows?
	expect := true
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}
		//if rule.LuaEnable() {
		//	ls, err := s.buildMessages(row, rule)
		//	if err != nil {
		//		logs.Errorf(errors.ErrorStack(err))
		//		expect = false
		//		break
		//	}
		//	for _, m := range ls {
		//		s.producer.Input() <- m
		//		select {
		//		case err := <-s.producer.Errors():
		//			logs.Error(err.Error())
		//			expect = false
		//			break
		//		default:
		//		}
		//	}
		//	if !expect {
		//		break
		//	}
		//} else {
		//	m, err := s.buildMessage(row, rule)
		//	if err != nil {
		//		logs.Errorf(errors.ErrorStack(err))
		//		expect = false
		//		break
		//	}
		//	s.producer.Input() <- m
		//	select {
		//	case err := <-s.producer.Errors():
		//		logs.Error(err.Error())
		//		expect = false
		//		break
		//	default:
		//
		//	}
		//}
	}

	if !expect {
		return 0
	}

	return int64(len(rows))
}

//func (s *LotDbToMainDbEndpoint) buildMessages(row *model.RowRequest, rule *global.Rule) ([]*sarama.ProducerMessage, error) {
//	kvm := rowMap(row, rule, true)
//	ls, err := luaengine.DoMQOps(kvm, row.Action, rule)
//	if err != nil {
//		return nil, errors.Errorf("lua 脚本执行失败 : %s ", err)
//	}
//
//	var ms []*sarama.ProducerMessage
//	for _, resp := range ls {
//		m := &sarama.ProducerMessage{
//			Topic: resp.Topic,
//			Value: sarama.ByteEncoder(resp.ByteArray),
//		}
//		logs.Infof("topic: %s, message: %s", resp.Topic, string(resp.ByteArray))
//		ms = append(ms, m)
//	}
//
//	return ms, nil
//}

func (s *LotDbToMainDbEndpoint) buildMessage(row *model.RowRequest, rule *global.Rule) (*model.MQRespond, error) {
	kvm := rowMap(row, rule, false)
	resp := new(model.MQRespond)
	resp.Action = row.Action
	resp.Timestamp = row.Timestamp
	resp.TableName = rule.Table
	if rule.ValueEncoder == global.ValEncoderJson {
		resp.Data = kvm
	} else {
		resp.Data = encodeValue(rule, kvm)
	}

	if rule.ReserveRawData && canal.UpdateAction == row.Action {
		resp.Raw = oldRowMap(row, rule, false)
	}

	//body, err := json.Marshal(resp)
	//if err != nil {
	//	return nil, err
	//}
	//m := &sarama.ProducerMessage{
	//	Topic: rule.LotDbToMainDbTopic,
	//	Value: sarama.ByteEncoder(body),
	//}
	//logs.Infof("topic: %s, message: %s", rule.LotDbToMainDbTopic, string(body))
	//logs.Infof("topic: %s, message: %s", rule.KafkaTopic, string(body))
	return resp, nil
}

func (s *LotDbToMainDbEndpoint) Close() {
	//if s.producer != nil {
	//	s.producer.Close()
	//}
	//if s.client != nil {
	//	s.client.Close()
	//}
}

//ProcessLotDbToMainDb //to do,to process data to MainB,读取binlog的channel大小可设置成1 (原来代码:queue: make(chan interface{}, 4096),bulk_size=1),这样如果处理数据到MainDb有问题不会丢失数据,当前处理消息位置还要记录防止处理失败丢数据
func (s *LotDbToMainDbEndpoint) ProcessLotDbToMainDb(from mysql.Position, mqRespond *model.MQRespond) error {
	fmt.Printf("%+v\n", mqRespond)

	var kfDbChangeMsg model.KfDbChangeMsg

	btRet, err := json.Marshal(mqRespond)
	if err != nil {
		logs.Errorf("%s", err)
		return nil
	}

	err = json.Unmarshal(btRet, &kfDbChangeMsg)
	if err != nil {
		logs.Errorf("%s", err)
		return nil
	}

	if len(kfDbChangeMsg.TableName) == 0 || len(kfDbChangeMsg.MapData) == 0 {
		logs.Warnf("table_name or data is empty")
		return nil
	}

	MainDbName := "MainDbSlave" //todo, get from db

	db := s.dbDefault

	var colNames []string
	err = db.Raw("select column_name from information_schema.columns where table_schema=? and table_name=?", MainDbName, kfDbChangeMsg.TableName).
		Pluck("table_name", &colNames).Error
	if err != nil {
		logs.Errorf("%s", err)
		return nil
	}
	v_park_lot_id, _ := kfDbChangeMsg.MapData["park_lot_id"]
	f_park_lot_id, _ := v_park_lot_id.(float64)
	i_park_lot_id := int(f_park_lot_id)
	//v_lot_rec_id, _ := kfDbChangeMsg.MapData["lot_rec_id"]
	//f_lot_rec_id, _ := v_lot_rec_id.(float64)
	//i_lot_rec_id := int(f_lot_rec_id)
	v_id, _ := kfDbChangeMsg.MapData["id"]
	f_id, _ := v_id.(float64)
	i_id := uint(f_id)

	v_updated_at, ok := kfDbChangeMsg.MapData["updated_at"]
	var updatedAtValid bool
	if ok && v_updated_at != nil {
		updatedAtValid = true
	}

	if tools.InStringSlice("park_lot_id", colNames) &&
		//tools.InStringSlice("lot_rec_id", colNames) &&
		tools.InStringSlice("rec_operated_by", colNames) &&
		i_park_lot_id > 0 &&
		i_id > 0 &&
		updatedAtValid &&
		tools.SliceContains(uint(i_park_lot_id), s.dbParkLotIds) {

		if kfDbChangeMsg.Action == "insert" { //gorm2.0支持从map create
			if i_id%uint(s.lotTopicDsn.AutoIncrementIncrement) != uint(s.lotTopicDsn.AutoIncrementOffset) {
				logs.Errorf("id=%d,不符合车场数据库id规则,AutoIncrementIncrement=%d,AutoIncrementOffset=%d", i_id, s.lotTopicDsn.AutoIncrementIncrement, s.lotTopicDsn.AutoIncrementOffset)
				//panic(err)
				return nil
			}
			//if tools.InStringSlice("main_rec_id", colNames) {
			//	kfDbChangeMsg.MapData["main_rec_id"] = kfDbChangeMsg.MapData["id"]
			//}

			//{
			//	//kfDbChangeMsg.MapData["rec_operated_by"] = "LotDb"
			//	dbRet := db.Table(kfDbChangeMsg.TableName).Where("park_lot_id=? and lot_rec_id=?", kfDbChangeMsg.MapData["park_lot_id"], kfDbChangeMsg.MapData["lot_rec_id"]).Updates(kfDbChangeMsg.MapData)
			//	if dbRet.Error != nil {
			//		fmt.Println(err)
			//		panic(err)
			//		//break
			//	}
			//	if dbRet.RowsAffected > 0 { //如果没更新,下面继续insert
			//		return nil
			//	}
			//	dbRet = db.Table(kfDbChangeMsg.TableName).Where("id=? ", kfDbChangeMsg.MapData["id"]).Updates(kfDbChangeMsg.MapData)
			//	if dbRet.Error != nil {
			//		fmt.Println(err)
			//		panic(err)
			//		//break
			//	}
			//	if dbRet.RowsAffected > 0 { //如果没更新,下面继续insert
			//		return nil
			//	}
			//}
			//kfDbChangeMsg.MapData["id"] = nil
			//delete(kfDbChangeMsg.MapData, "id")

			sql, slColValues := tools.BuildInsertSql(kfDbChangeMsg.TableName, kfDbChangeMsg.MapData)
			err = db.Exec(sql, slColValues...).Error
			if err != nil {
				logs.Errorf("%s", err)
				//panic(err)
				return nil
			}

		} else if kfDbChangeMsg.Action == "update" {
			v_id_old, _ := kfDbChangeMsg.MapOld["id"]
			f_id_old, _ := v_id_old.(float64)
			i_id_old := uint(f_id_old)
			if i_id_old == 0 {
				logs.Errorf("update必须要原来记录id，检查go_mysql_transfer配置文件app.yml中reserve_raw_data: true")
				return nil
			}

			if i_id_old != i_id {
				strErr := fmt.Sprintf("从LotDb(车场数据库)到MainDb(主数据库),不允许改id,table_name=%s,id=%d,id_old=%d", kfDbChangeMsg.TableName, i_id, i_id_old)
				log.Println(strErr)
				logs.Errorf(strErr)
				return nil
			}

			dbRet := db.Table(kfDbChangeMsg.TableName).
				Where("id=?", kfDbChangeMsg.MapData["id"]).
				Where("park_lot_id in(?)", s.dbParkLotIds).
				Where("updated_at is null Or updated_at<?", kfDbChangeMsg.MapData["updated_at"]). //这个相等时间(updated_at<=?)不能更新,否者如果有两个相同时间的更新会导致循环不断,updated_at精度待改到微秒
				Updates(kfDbChangeMsg.MapData)
			if dbRet.Error != nil {
				logs.Errorf("%s", err)
				return nil
			}
			if dbRet.RowsAffected > 0 { //如果没更新,下面继续insert
				return nil
			}
		} else if kfDbChangeMsg.Action == "delete" {
			err = db.Table(kfDbChangeMsg.TableName).Where("id=?", kfDbChangeMsg.MapData["id"]).
				Where("park_lot_id in(?)", s.dbParkLotIds).
				Delete(kfDbChangeMsg.MapData).Error
		}
		if err != nil {
			logs.Errorf("%s", err)
			return nil
		}
	} else {
		fmt.Println("LotDbToMainDb 只能传递 updated_at有效,对应lotDb.park_lot_id的改变数据")
		return nil
	}
	//break

	//fmt.Println(row)
	return nil
}
