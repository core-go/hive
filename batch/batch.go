package batch

import (
	"fmt"
	"reflect"
	"strings"

	h "github.com/core-go/hive"
)

func BuildToSaveBatch[T any](table string, models []T, options ...*h.Schema) (string, error) {
	slen := len(models)
	if slen <= 0 {
		return "", nil
	}
	var cols []*h.FieldDB
	// var schema map[string]FieldDB
	if len(options) > 0 && options[0] != nil {
		cols = options[0].Columns
		// schema = options[0].Fields
	} else {
		var t T
		modelType := reflect.TypeOf(t)
		if modelType.Kind() == reflect.Ptr {
			modelType = modelType.Elem()
		}
		m := h.CreateSchema(modelType)
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
		model := models[j]
		mv := reflect.ValueOf(model)
		if mv.Kind() == reflect.Ptr {
			mv = mv.Elem()
		}
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
					values = append(values, "null")
				} else {
					v, ok := h.GetDBValue(fieldValue, fdb.Scale, fdb.LayoutTime)
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
