package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
)

type GenericAdapter[T any, K any] struct {
	*Adapter[*T]
	Map    map[string]int
	Fields string
	Keys   []string
	IdMap  bool
}

func NewGenericAdapterWithVersion[T any, K any](connection *hv.Connection, tableName string, versionField string) (*GenericAdapter[T, K], error) {
	adapter := NewAdapterWithVersion[*T](connection, tableName, versionField)
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	_, primaryKeys := h.FindPrimaryKeys(modelType)
	var k K
	kType := reflect.TypeOf(k)
	idMap := false
	if len(primaryKeys) > 1 {
		if kType.Kind() == reflect.Map {
			idMap = true
		} else if kType.Kind() != reflect.Struct {
			return nil, errors.New("for composite keys, K must be a struct or a map")
		}
	}

	fieldsIndex, er0 := h.GetColumnIndexes(modelType)
	if er0 != nil {
		return nil, er0
	}
	fields := h.BuildFieldsBySchema(adapter.Schema)
	return &GenericAdapter[T, K]{adapter, fieldsIndex, fields, primaryKeys, idMap}, nil
}
func (a *GenericAdapter[T, K]) All(ctx context.Context) ([]T, error) {
	query := h.BuildSelectAllQuery(a.Table)
	cursor := a.Connection.Cursor()
	defer cursor.Close()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return nil, cursor.Err
	}
	var res []T
	err := h.Query(ctx, cursor, a.Map, &res, query)
	return res, err
}
func toMap(obj interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	im := make(map[string]interface{})
	er2 := json.Unmarshal(b, &im)
	return im, er2
}
func (a *GenericAdapter[T, K]) getId(k K) (interface{}, error) {
	if len(a.Keys) >= 2 && !a.IdMap {
		ri, err := toMap(k)
		return ri, err
	} else {
		return k, nil
	}
}
func (a *GenericAdapter[T, K]) Load(ctx context.Context, id K) (*T, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return nil, er0
	}
	queryAll := fmt.Sprintf("select %s from %s ", a.Fields, a.Table)
	query := h.BuildFindById(queryAll, ip, a.JsonColumnMap, a.Keys)
	cursor := a.Connection.Cursor()
	defer cursor.Close()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return nil, cursor.Err
	}
	var res []T
	err := h.Query(ctx, cursor, a.Map, &res, query)
	if err != nil {
		return nil, err
	}
	if len(res) > 0 {
		return &res[0], nil
	}
	return nil, nil
}
func (a *GenericAdapter[T, K]) Exist(ctx context.Context, id K) (bool, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return false, er0
	}
	queryAll := fmt.Sprintf("select %s from %s ", a.Schema.SColumns[0], a.Table)
	query := h.BuildFindById(queryAll, ip, a.JsonColumnMap, a.Keys)
	cursor := a.Connection.Cursor()
	defer cursor.Close()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return false, cursor.Err
	}
	for cursor.HasMore(ctx) {
		return true, nil
	}
	return false, nil
}
func (a *GenericAdapter[T, K]) Delete(ctx context.Context, id K) (int64, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return 0, er0
	}
	queryAll := fmt.Sprintf("delete from %s ", a.Table)
	query := h.BuildFindById(queryAll, ip, a.JsonColumnMap, a.Keys)
	cursor := a.Connection.Cursor()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return -1, cursor.Err
	}
	return 1, nil
}
