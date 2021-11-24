package model

import (
	"go-mysql-transfer/util/types"
)

type LotDbParkLotId struct {
	BaseModel
	ParkLotId uint   `gorm:"unique_index;type:int;not null;default:0;"`                  //表名
	TenantId  uint   `gorm:"index;type:int;not null;default:0;"`                         //表记录Id
	Topic     string `gorm:"type:varchar(50);not null;default:'';comment='Kafka Topic'"` //Kafka Topic
}

type LotTopicDsn struct {
	BaseModel
	DbTopic           string `gorm:"unique_index;type:varchar(50);not null;default:'';comment='Kafka Topic'"`       //Kafka Topic
	LotDbName         string `gorm:"unique_index;type:varchar(50);not null;default:'';comment='数据名,改kafka后这个暂时不用'"` //数据名
	LotDbDsn          string `gorm:"unique_index;type:varchar(300);not null;default:'';comment='Dsn'"`              //
	LotProcessWorking bool   `gorm:"not null;default:false;comment='车场服务进程是否在工作'"`                                  //true:车场进程正在工作
	//MainRecIdSyncNeeded    bool   `gorm:"not null;default:false;comment='有main_rec_id要同步到id'"`
	AutoIncrementIncrement uint16 `gorm:"not null;default:60000;comment='mysqld配置auto-increment-increment'"`
	AutoIncrementOffset    uint16 `gorm:"unique_index;not null;default:1;comment='mysqld 配置auto-increment-offset'"`
}

//其他Model的基类
type BaseModel struct {
	Id        uint `gorm:"primary_key"`
	CreatedAt types.JSONTime
	UpdatedAt types.JSONTime
}

type KfDbChangeMsg struct {
	//Topic     string      `json:"-"`
	Action    string                 `json:"action"`
	Timestamp uint32                 `json:"timestamp"`
	TableName string                 `json:"table_name"` //added by ycj
	MapData   map[string]interface{} `json:"data"`       //changed by ycj
	MapOld    map[string]interface{} `json:"raw"`        //changed by ycj
	//Date      interface{} `json:"date"`
	//Raw       interface{} `json:"raw,omitempty"`
	//ByteArray []byte      `json:"-"`
}
