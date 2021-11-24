package types

import (
	"database/sql/driver"
	"fmt"
	"time"
)

// JSONTime format json time field by myself
type JSONTime struct {
	time.Time
}

func (t JSONTime) SetTime(timeStamp time.Time) *JSONTime {
	t.Time = timeStamp
	return &t
}

func (t JSONTime) MarshalJSON() ([]byte, error) {
	formatted := fmt.Sprintf("\"%s\"", t.Format("2006-01-02 15:04:05"))
	return []byte(formatted), nil
}

// UnmarshalJSON 反序列化为JSON
func (t *JSONTime) UnmarshalJSON(data []byte) error {
	var err error
	loc, _ := time.LoadLocation("Local")
	t.In(loc)
	if len(data) < 20 {
		t.Time, err = time.ParseInLocation("2006-01-02 15:04:05"[0:len(data)-1], string(data)[1:len(data)-1], loc)
	} else {
		t.Time, err = time.ParseInLocation("2006-01-02 15:04:05", string(data)[1:20], loc)
	}
	return err
}

// Value insert timestamp into mysql need this function.
func (t JSONTime) Value() (driver.Value, error) {
	var zeroTime time.Time
	if t.Time.UnixNano() == zeroTime.UnixNano() {
		return nil, nil
	}
	return t.Time, nil
}

// Scan valueof time.Time
func (t *JSONTime) Scan(v interface{}) error {
	value, ok := v.(time.Time)
	//value = value.Add(0-time.Hour*8)
	if ok {
		*t = JSONTime{Time: value}
		return nil
	}
	return fmt.Errorf("can not convert %v to timestamp", v)
}

func (t *JSONTime) GetParkingLength(left *JSONTime) string {
	spanSeconds := int64(left.Sub(t.Time).Seconds())
	if spanSeconds/(24*60*60) > 0 {
		return fmt.Sprintf("%d天%d小时%d分%d秒", spanSeconds/(24*60*60), spanSeconds%(24*60*60)/(60*60), spanSeconds%(60*60)/60, spanSeconds%60)
	} else {
		return fmt.Sprintf("%d小时%d分%d秒", spanSeconds%(24*60*60)/(60*60), spanSeconds%(60*60)/60, spanSeconds%60)
	}
}
