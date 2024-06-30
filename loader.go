package hive

import (
	"fmt"
	"reflect"
	"strings"
)

func InitFields(modelType reflect.Type) (map[string]int, string, error) {
	fieldsIndex, err := GetColumnIndexes(modelType)
	if err != nil {
		return nil, "", err
	}
	fields := BuildFields(modelType)
	return fieldsIndex, fields, nil
}

func FindPrimaryKeys(modelType reflect.Type) ([]string, []string) {
	numField := modelType.NumField()
	var idColumnFields []string
	var idJsons []string
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				k, ok := findTag(ormTag, "column")
				if ok {
					idColumnFields = append(idColumnFields, k)
					tag1, ok1 := field.Tag.Lookup("json")
					tagJsons := strings.Split(tag1, ",")
					if ok1 && len(tagJsons) > 0 {
						idJsons = append(idJsons, tagJsons[0])
					}
				}
			}
		}
	}
	return idColumnFields, idJsons
}
func findTag(tag string, key string) (string, bool) {
	if has := strings.Contains(tag, key); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == key {
					return str2[j+1], true
				}
			}
		}
	}
	return "", false
}
func MapJsonColumn(modelType reflect.Type) map[string]string {
	numField := modelType.NumField()
	columnNameKeys := make(map[string]string)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				if has := strings.Contains(ormTag, "column"); has {
					str1 := strings.Split(ormTag, ";")
					num := len(str1)
					for i := 0; i < num; i++ {
						str2 := strings.Split(str1[i], ":")
						for j := 0; j < len(str2); j++ {
							if str2[j] == "column" {
								tagj, ok1 := field.Tag.Lookup("json")
								t := strings.Split(tagj, ",")
								if ok1 && len(t) > 0 {
									json := t[0]
									columnNameKeys[json] = str2[j+1]
								}
							}
						}
					}
				}
			}
		}
	}
	return columnNameKeys
}

func BuildSelectAllQuery(table string) string {
	return fmt.Sprintf("select * from %s", table)
}

func BuildFindById(selectAll string, id interface{}, mapJsonColumnKeys map[string]string, keys []string) string {
	var where = ""
	if len(keys) == 1 {
		v, _ := GetDBValue(id, 0, "")
		where = fmt.Sprintf("where %s = %s", mapJsonColumnKeys[keys[0]], v)
	} else {
		conditions := make([]string, 0)
		if ids, ok := id.(map[string]interface{}); ok {
			for _, keyJson := range keys {
				columnName := mapJsonColumnKeys[keyJson]
				if idk, ok1 := ids[keyJson]; ok1 {
					v, _ := GetDBValue(idk, 0, "")
					conditions = append(conditions, fmt.Sprintf("%s = %s", columnName, v))
				}
			}
			where = "where " + strings.Join(conditions, " and ")
		}
	}
	return fmt.Sprintf("%s %v", selectAll, where)
}
