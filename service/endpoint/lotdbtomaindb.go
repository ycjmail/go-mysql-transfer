package endpoint

import (
	//"github.com/Shopify/sarama"
	_ "github.com/siddontang/go-mysql/canal"
	_ "log"
	_ "strings"
	"sync"

	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/model"
	"go-mysql-transfer/util/logs"
)

type LotDbToMainDbEndpoint struct {
	//client   sarama.Client
	//producer sarama.AsyncProducer

	retryLock sync.Mutex
}

func newLotDbToMainDbEndpoint() *LotDbToMainDbEndpoint {
	r := &LotDbToMainDbEndpoint{}
	return r
}

func (s *LotDbToMainDbEndpoint) Connect() error {
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

		return s.ProcessLotDbToMainDb(from, rows)

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

//func (s *LotDbToMainDbEndpoint) buildMessage(row *model.RowRequest, rule *global.Rule) (*sarama.ProducerMessage, error) {
//	kvm := rowMap(row, rule, false)
//	resp := new(model.MQRespond)
//	resp.Action = row.Action
//	resp.Timestamp = row.Timestamp
//	if rule.ValueEncoder == global.ValEncoderJson {
//		resp.Date = kvm
//	} else {
//		resp.Date = encodeValue(rule, kvm)
//	}
//
//	if rule.ReserveRawData && canal.UpdateAction == row.Action {
//		resp.Raw = oldRowMap(row, rule, false)
//	}
//
//	body, err := json.Marshal(resp)
//	if err != nil {
//		return nil, err
//	}
//	m := &sarama.ProducerMessage{
//		Topic: rule.LotDbToMainDbTopic,
//		Value: sarama.ByteEncoder(body),
//	}
//	logs.Infof("topic: %s, message: %s", rule.LotDbToMainDbTopic, string(body))
//	return m, nil
//}

func (s *LotDbToMainDbEndpoint) Close() {
	//if s.producer != nil {
	//	s.producer.Close()
	//}
	//if s.client != nil {
	//	s.client.Close()
	//}
}

//ProcessLotDbToMainDb //to do,to process data to MainB,读取binlog的channel大小可设置成1 (原来代码:queue: make(chan interface{}, 4096),bulk_size=1),这样如果处理数据到MainDb有问题不会丢失数据,当前处理消息位置还要记录防止处理失败丢数据
func (s *LotDbToMainDbEndpoint) ProcessLotDbToMainDb(from mysql.Position, rows []*model.RowRequest) error {
	return nil
}
