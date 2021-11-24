package tools

func BuildInsertSql(tableName string, mapData map[string]interface{}) (string, []interface{}) {
	sql := "insert into " + tableName
	var strColNames string
	var strVals string
	var slColValues []interface{}
	for kData, vData := range mapData {
		if strColNames != "" {
			strColNames += ","
		}
		strColNames += kData
		if strVals != "" {
			strVals += ","
		}
		strVals += "?"
		slColValues = append(slColValues, vData)
	}
	sql += "(" + strColNames + ") values(" + strVals + ")"
	return sql, slColValues
}
