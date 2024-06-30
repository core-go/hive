package hive

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func Init(modelType reflect.Type) (map[string]int, *Schema, map[string]string, []string, []string, string, error) {
	fieldsIndex, err := GetColumnIndexes(modelType)
	if err != nil {
		return nil, nil, nil, nil, nil, "", err
	}
	schema := CreateSchema(modelType)
	fields := BuildFieldsBySchema(schema)
	jsonColumnMap := MakeJsonColumnMap(modelType)
	jm := GetWritableColumns(schema.Fields, jsonColumnMap)
	keys, arr := FindPrimaryKeys(modelType)
	return fieldsIndex, schema, jm, keys, arr, fields, nil
}

func MakeJsonColumnMap(modelType reflect.Type) map[string]string {
	numField := modelType.NumField()
	mapJsonColumn := make(map[string]string)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		column, ok := findTag(ormTag, "column")
		if ok {
			tag1, ok1 := field.Tag.Lookup("json")
			tagJsons := strings.Split(tag1, ",")
			if ok1 && len(tagJsons) > 0 {
				mapJsonColumn[tagJsons[0]] = column
			}
		}
	}
	return mapJsonColumn
}
func GetWritableColumns(fields map[string]*FieldDB, jsonColumnMap map[string]string) map[string]string {
	m := jsonColumnMap
	for k, v := range jsonColumnMap {
		for _, db := range fields {
			if db.Column == v {
				if db.Update == false && db.Key == false {
					delete(m, k)
				}
			}
		}
	}
	return m
}
func FindFieldIndex(modelType reflect.Type, fieldName string) int {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		if field.Name == fieldName {
			return i
		}
	}
	return -1
}
func GetFieldByIndex(ModelType reflect.Type, index int) (json string, col string, colExist bool) {
	fields := ModelType.Field(index)
	tag, _ := fields.Tag.Lookup("gorm")

	if has := strings.Contains(tag, "column"); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		json = fields.Name
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == "column" {
					jTag, jOk := fields.Tag.Lookup("json")
					if jOk {
						tagJsons := strings.Split(jTag, ",")
						json = tagJsons[0]
					}
					return json, str2[j+1], true
				}
			}
		}
	}
	return "", "", false
}
func JSONToColumns(model map[string]interface{}, m map[string]string) map[string]interface{} {
	if model == nil || m == nil {
		return model
	}
	r := make(map[string]interface{})
	for k, v := range model {
		col, ok := m[k]
		if ok {
			r[col] = v
		}
	}
	return r
}
func Contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}
func BuildToPatch(table string, model map[string]interface{}, keyColumns []string) string {
	return BuildToPatchWithVersion(table, model, keyColumns, "")
}
func BuildToPatchWithVersion(table string, model map[string]interface{}, keyColumns []string, version string) string { //version column name db
	values := make([]string, 0)
	where := make([]string, 0)
	for col, v := range model {
		if !Contains(keyColumns, col) && col != version {
			if v == nil {
				values = append(values, col+"=null")
			} else {
				v2, ok2 := GetDBValue(v, 2, "")
				if ok2 {
					values = append(values, col+"="+v2)
				}
			}
		}
	}
	for _, col := range keyColumns {
		v0, ok0 := model[col]
		if ok0 {
			v, ok1 := GetDBValue(v0, 2, "")
			if ok1 {
				where = append(where, col+"="+v)
			}
		}
	}
	if len(version) > 0 {
		v0, ok0 := model[version]
		if ok0 {
			switch v4 := v0.(type) {
			case int:
				values = append(values, version+"="+strconv.Itoa(v4+1))
				where = append(where, version+"="+strconv.Itoa(v4))
			case int32:
				v5 := int64(v4)
				values = append(values, version+"="+strconv.FormatInt(v5+1, 10))
				where = append(where, version+"="+strconv.FormatInt(v5, 10))
			case int64:
				values = append(values, version+"="+strconv.FormatInt(v4+1, 10))
				where = append(where, version+"="+strconv.FormatInt(v4, 10))
			}
		}
	}
	query := fmt.Sprintf("update %v set %v where %v", table, strings.Join(values, ","), strings.Join(where, " and "))
	return query
}
