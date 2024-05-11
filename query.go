package hive

import (
	"context"
	hv "github.com/beltran/gohive"
	"reflect"
)

func Query(ctx context.Context, cursor *hv.Cursor, fieldsIndex map[string]int, results interface{}, sql string) error {
	cursor.Exec(ctx, sql)
	if cursor.Err != nil {
		return cursor.Err
	}
	modelType := reflect.TypeOf(results).Elem().Elem()
	tb, er3 := Scan(cursor, modelType, fieldsIndex)
	if er3 != nil {
		return er3
	}
	for _, element := range tb {
		appendToArray(results, element)
	}
	return nil
}

func appendToArray(arr interface{}, item interface{}) interface{} {
	arrValue := reflect.ValueOf(arr)
	elemValue := reflect.Indirect(arrValue)

	itemValue := reflect.ValueOf(item)
	if itemValue.Kind() == reflect.Ptr {
		itemValue = reflect.Indirect(itemValue)
	}
	elemValue.Set(reflect.Append(elemValue, itemValue))
	return arr
}

func Scan(cursors *hv.Cursor, modelType reflect.Type, fieldsIndex map[string]int) (t []interface{}, err error) {
	if fieldsIndex == nil {
		fieldsIndex, err = GetColumnIndexes(modelType)
		if err != nil {
			return
		}
	}
	columns, mcols, er0 := GetColumns(cursors)
	if er0 != nil {
		return nil, er0
	}
	ctx := context.Background()
	for cursors.HasMore(ctx) {
		initModel := reflect.New(modelType).Interface()
		r, _ := StructScan(initModel, columns, fieldsIndex)
		fieldPointers := cursors.RowMap(ctx)
		if cursors.Err != nil {
			return t, cursors.Err
		}
		for _, c := range columns {
			if colm, ok := mcols[c]; ok {
				if v, ok := fieldPointers[colm]; ok {
					if v != nil {
						v = reflect.Indirect(reflect.ValueOf(v)).Interface()
						if fieldValue, ok := r[c]; ok && !IsZeroOfUnderlyingType(v) {
							err1 := ConvertAssign(fieldValue, v)
							if err1 != nil {
								return nil, err1
							}
						}
					}
				}
			}
		}
		t = append(t, initModel)
	}
	return
}
