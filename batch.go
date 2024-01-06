package hive

import (
	"fmt"
	"reflect"
	"strings"
)

func BuildToSaveBatch(table string, models interface{}, options ...*Schema) (string, error){
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return "", fmt.Errorf("models must be a slice")
	}
	slen := s.Len()
	if slen <= 0 {
		return "", nil
	}
	var cols []*FieldDB
	// var schema map[string]FieldDB
	if len(options) > 0 && options[0] != nil {
		cols = options[0].Columns
		// schema = options[0].Fields
	} else {
		first := s.Index(0).Interface()
		modelType := reflect.TypeOf(first)
		m := CreateSchema(modelType)
		cols = m.Columns
	}
	placeholders := make([]string, 0)
	icols := make([]string, 0)
	for _, fdb := range cols {
		if fdb.Insert {
			icols = append(icols, fdb.Column)
		}
	}
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		mv := reflect.ValueOf(model)
		values := make([]string, 0)
		for _, fdb := range cols {
			if fdb.Insert {
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if isNil {
					icols = append(icols, fdb.Column)
					values = append(values, "null")
				} else {
					icols = append(icols, fdb.Column)
					v, ok := GetDBValue(fieldValue, fdb.Scale, fdb.LayoutTime)
					if ok {
						values = append(values, v)
					}
					//TODO error here
				}
			}
		}
		x := "(" + strings.Join(values, ",") + ")"
		placeholders = append(placeholders, x)
	}
	query := fmt.Sprintf(fmt.Sprintf("insert into %s (%s) values %s",
		table,
		strings.Join(icols, ","),
		strings.Join(placeholders, ","),
	))
	return query, nil
}
