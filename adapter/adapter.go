package query

import (
	"context"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
	"reflect"
)

type Adapter[T any, K any] struct {
	Connection    *hv.Connection
	Mp            func(ctx context.Context, model interface{}) (interface{}, error)
	keys          []string
	JsonColumnMap map[string]string
	Map           map[string]int
	Table         string
}

func NewAdapter[T any, K any](connection *hv.Connection, tableName string, options ...func(context.Context, interface{}) (interface{}, error)) (*Adapter[T, K], error) {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	_, idNames := h.FindPrimaryKeys(modelType)
	mapJsonColumnKeys := h.MapJsonColumn(modelType)

	fieldsIndex, er0 := h.GetColumnIndexes(modelType)
	if er0 != nil {
		return nil, er0
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	return &Adapter[T, K]{Connection: connection, Mp: mp, keys: idNames, JsonColumnMap: mapJsonColumnKeys, Map: fieldsIndex, Table: tableName}, nil
}

func (s *Adapter[T, K]) All(ctx context.Context) ([]T, error) {
	query := h.BuildSelectAllQuery(s.Table)
	cursor := s.Connection.Cursor()
	defer cursor.Close()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return nil, cursor.Err
	}
	var res []T
	err := h.Query(ctx, cursor, s.Map, &res, query)
	if err == nil {
		if s.Mp != nil {
			h.MapModels(ctx, &res, s.Mp)
		}
	}
	return res, err
}
func (s *Adapter[T, K]) Load(ctx context.Context, id K) (*T, error) {
	query := h.BuildFindById(s.Table, id, s.JsonColumnMap, s.keys)
	cursor := s.Connection.Cursor()
	defer cursor.Close()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return nil, cursor.Err
	}
	var res []T
	err := h.Query(ctx, cursor, s.Map, &res, query)
	if err != nil {
		return nil, err
	}
	if len(res) > 0 {
		return &res[0], nil
	}
	return nil, nil
}
func (s *Adapter[T, K]) Exist(ctx context.Context, id K) (bool, error) {
	query := h.BuildFindById(s.Table, id, s.JsonColumnMap, s.keys)
	cursor := s.Connection.Cursor()
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
